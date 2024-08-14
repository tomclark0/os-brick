#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import configparser
import fcntl
import glob
import logging
import os
import sched
import socket
import tempfile
import threading
import time
import traceback

from os_brick.initiator.connectors import entities as provisioner_entities
from os_brick.initiator.connectors import nvmeof
from os_brick.initiator.connectors import rest_client as \
    provisioner_rest_client
from os_brick import utils
from oslo_concurrency import lockutils
from oslo_concurrency import processutils as putils
from oslo_log import log
from oslo_utils.secretutils import md5


scheduler = sched.scheduler(time.time, time.sleep)
LOG = log.getLogger(__name__)
RAID_STATE_OK = 0
RAID_STATE_DEGRADED = 1
RAID_STATE_DEGRADED_SYNCING = 2
REPLICA_AVAILABLE = 'Available'
REPLICA_TERMINATING = 'Terminating'
REPLICA_MISSING = 'Missing'
REPLICA_UNKNOWN = 'Unknown'
REPLICA_SYNCING = 'Synchronizing'
BACKEND_AVAILABLE = 'Available'
PROV_CONF_FILE = '/etc/kioxia/provisioner.conf'

synchronized = lockutils.synchronized_with_prefix('os-brick-')
listen_sock = socket.socket(socket.AF_UNIX)
tmpdir = tempfile.gettempdir()
tmppath = os.path.join(tmpdir, 'ks')
sockpath = os.path.join(tmppath, 'nvmeof_agent.sock')
running = False


def instance_already_running():

    if not os.path.exists(tmppath):
        os.makedirs(tmppath)
    LOG.debug("[!] tmppath : %s", str(tmppath))
    LOG.debug("[!] os.path.exists : %s", str(os.path.exists(tmppath)))
    LOG.debug("[!] sockpath : %s", str(sockpath))
    try:
        original_umask = os.umask(0)
        lock_file_pointer = os.open(sockpath, os.O_WRONLY | os.O_CREAT)
        fcntl.lockf(lock_file_pointer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        already_running = False
    except (IOError, OSError):
        already_running = True
    finally:
        os.umask(original_umask)

    return already_running


class NVMeOFAgent:

    def __init__(self, executor, host_uuid, host_nqn, hostname):
        self.mds = {}
        self.host_uuid = host_uuid
        self.host_nqn = host_nqn
        self.hostname = hostname
        self.executor = executor
        self.prov_rest = None

    def init(self):
        global running
        if os.path.isfile(PROV_CONF_FILE):
            config = configparser.RawConfigParser()
            config.read(PROV_CONF_FILE)
            prov_dict = dict(config.items('DEFAULT'))
            prov_ip = prov_dict['prov_ip']
            prov_port = prov_dict['prov_port']
            cert_file = prov_dict['cert_file']
            client_id = None
            client_secret = None
            client_scope = None
            token_url = None
            token = None
            if 'token' in prov_dict:
                token = prov_dict['token']
            if 'client_id' in prov_dict:
                client_id = prov_dict['client_id']
            if 'client_secret' in prov_dict:
                client_secret = prov_dict['client_secret']
            if 'client_scope' in prov_dict:
                client_scope = prov_dict['client_scope']
            if 'token_url' in prov_dict:
                token_url = prov_dict['token_url']
            self.prov_rest = provisioner_rest_client.KioxiaProvisioner(
                [prov_ip], cert_file, token, prov_port)
            if client_id is not None and client_secret is not None \
                    and token_url is not None:
                self.prov_rest.set_auth_server_parameters(None, token_url,
                                                          client_id,
                                                          client_secret,
                                                          client_scope)

            scheduler.enter(1, 1, self.call_host_monitor, (30,))
            t = threading.Thread(target=scheduler.run)
            t.start()
            running = True
        else:
            LOG.warning("%s is not exists! - "
                        "abort NVMeOFAgent launch", PROV_CONF_FILE)

    @staticmethod
    def ensure_running(executor_connector, host):
        global running
        LOG.debug("[!] ensure_running START --------")
        # check if agent running - sched implementation:
        if running:
            LOG.debug("[!] there is already scheduler task running  --------")
            return

        is_running = instance_already_running()
        if is_running:
            LOG.warning("[!] There is agent already running!")
            return
        if not host:
            host = NVMeOFAgent._get_host_name()
        else:
            host = NVMeOFAgent._convert_host_name(host)
        agent = NVMeOFAgent(
            executor_connector,
            executor_connector._get_host_uuid(),
            utils.get_host_nqn(),
            host)

        LOG.debug("[!] agent.init() --------")
        agent.init()

    def call_host_monitor(self, interval):
        self.monitor_host()
        scheduler.enter(interval, 1, self.call_host_monitor, (interval,))

    @staticmethod
    def get_path_glob(path, index):
        result = []
        globes = glob.glob(path)
        for v in globes:
            result.append(v.split('/')[index])

        return result

    @staticmethod
    def get_md_names():
        return NVMeOFAgent.get_path_glob('/dev/md/*', 3)

    @staticmethod
    def get_nvme_states(md_name, search_path):
        """get nvme devices states. returns: dictionary of device:[states]"""
        dev_path = '/dev/'
        md_link = None
        devices_states = {}

        try:
            md_link = os.readlink(
                '/dev/md/{0}'.format(md_name)).replace('../', '')
        except OSError:
            LOG.error("Invalid mdadm device: %s", md_name)
        state_path = search_path.format(md_link)
        LOG.debug("state_path: %s", state_path)
        for fname in glob.glob(state_path):
            with open(fname) as f:
                device = dev_path + fname.split('/')[5]
                state = f.read().strip('\n')
                if device in devices_states:
                    devices_states[device].append(state)
                else:
                    devices_states[device] = [state]

        return devices_states

    def get_nvme_devices_by_state(self, md_name):
        """returns: nvme devices by state (all known, faulty, healthy)"""
        all_devices = []
        faulty_devices = []
        healthy_devices = []
        md_link = None

        try:
            md_link = os.readlink(
                '/dev/md/{0}'.format(md_name)).replace('../', '')
        except OSError:
            LOG.error("Invalid mdadm device: %s", md_name)
        state_path = '/sys/block/{0}/md/dev-*/state'.format(md_link)
        for fname in glob.glob(state_path):
            device = fname.split('/')[5].replace('dev-', '/dev/')
            with open(fname) as f:
                result = f.read()
                all_devices.append(device)
                if 'faulty' in result:
                    faulty_devices.append(device)
                if 'in_sync' in result:
                    healthy_devices.append(device)

        return all_devices, faulty_devices, healthy_devices

    def add_device_to_md(self, md_name, nvme_device):
        """add nvme device to mdadm"""
        try:
            cmd = ['mdadm', '/dev/md/' + md_name, '--add', '--failfast',
                   nvme_device]
            LOG.debug("[!] cmd: %s", str(cmd))
            cmd_out = nvmeof.NVMeOFConnector.run_mdadm(
                self.executor, cmd, True)
            LOG.debug("[!] cmd: %s cmd_out: %s", str(cmd), cmd_out)
        except Exception:
            return False

        return True

    def remove_device_from_md(self, md_name, nvme_device, fail_first):
        """remove nvme device from mdadm"""
        try:
            if fail_first:
                cmd = ['mdadm', '--manage', '/dev/md/' + md_name, '--fail',
                       nvme_device]
                LOG.debug("[!] cmd: %s", str(cmd))
                cmd_out = nvmeof.NVMeOFConnector.run_mdadm(self.executor, cmd)
                LOG.debug("[!] cmd: %s cmd_out: %s", str(cmd), cmd_out)

            cmd2 = ['mdadm', '--manage', '/dev/md/' + md_name, '--remove',
                    nvme_device]
            LOG.debug("[!] cmd2: %s", str(cmd2))
            cmd_out = nvmeof.NVMeOFConnector.run_mdadm(self.executor, cmd2)
            LOG.debug("[!] cmd: %s cmd_out: %s", str(cmd2), cmd_out)
        except Exception as ex:
            LOG.error("[!] remove_device_from_md exception: %s", str(ex))
            return False

        return True

    def fail_md(self, md_name, nvme_device):
        """fail md device. returns: command error, exit code on failure"""
        try:
            cmd = ['mdadm', '--manage', '/dev/md/' + md_name, '--fail',
                   nvme_device]
            cmd_out = nvmeof.NVMeOFConnector.run_mdadm(self.executor, cmd)
            LOG.debug("cmd: %s cmd_out: %s", str(cmd), cmd_out)
        except Exception:
            return False

        return True

    def modify_device_count(self, md_name, device_count):
        """grow mdadm device. returns: command error, exit code on failure"""
        try:
            raid_devices = '--raid-devices=' + str(device_count)
            LOG.debug("[!] raid_devices: %s", raid_devices)
            cmd = ['mdadm', '--grow', '/dev/md/' + md_name, raid_devices]
            if device_count == 1:
                cmd.append('--force')
            LOG.debug("[!] cmd: %s", cmd)
            cmd_out = nvmeof.NVMeOFConnector.run_mdadm(self.executor, cmd)
            LOG.debug("[!] cmd: %s , cmd_out: %s", str(cmd), cmd_out)
        except Exception:
            return False

        return True

    def get_volume_by_uuid(self, vol_uuid):
        ks_volume = None
        result = self.prov_rest.get_volumes_by_uuid(vol_uuid)
        if result.status == "Success":
            if len(result.prov_entities) == 0:
                return None
            else:
                ks_volume = result.prov_entities[0]

        return ks_volume

    def get_volume_by_alias(self, md_name):
        ks_volume = None
        result = self.prov_rest.get_volumes_by_alias(md_name)
        if result.status == "Success":
            if len(result.prov_entities) == 0:
                return None
            else:
                ks_volume = result.prov_entities[0]

        return ks_volume

    @staticmethod
    def get_num_degraded(md_name):
        """get nvme degraded. returns: dictionary of device:degraded"""
        md_link = None
        try:
            md_link = os.readlink(
                '/dev/md/{0}'.format(md_name)).replace('../', '')
        except OSError:
            LOG.error("Invalid mdadm device: %s", md_name)
        raid_degraded_path = '/sys/block/{0}/md/degraded'.format(md_link)

        with open(raid_degraded_path) as f:
            degraded = f.read().strip('\n')

        return int(degraded)

    def get_terminating_leg(self, volume):
        num_terminating = 0
        terminating = None
        for location in volume.location:
            if location.replicaState == REPLICA_TERMINATING:
                terminating = location
                num_terminating = num_terminating + 1

        return num_terminating, terminating

    @staticmethod
    def get_nvme_device_by_uuid(replica_uuid):
        """get nvme device by vol uuid. returns: nvme device (/dev/nvmeXnX)"""
        dev_path = '/dev/'
        uuid_path = '/sys/block/*/uuid'
        nvme_device = None

        for fname in glob.glob(uuid_path):
            with open(fname) as f:
                if replica_uuid in f.read():
                    LOG.debug('[!] fname = %s', fname)
                    LOG.debug('[!] fname.split = %s', fname.split('/')[3])
                    nvme_device = dev_path + fname.split('/')[3]

        return nvme_device

    @staticmethod
    def get_uuid_by_nvme_device(nvme_device):
        """get volume uuid by nvme device"""
        uuid_path = '/sys/block/{0}/uuid'.format(nvme_device)
        # LOG.debug("[!] uuid_path: %s", uuid_path)

        try:
            with open(uuid_path) as f:
                vol_uuid = f.read().strip('\n')
        except IOError as ex:
            LOG.error("Exception get_uuid_by_nvme_device: %s", ex.strerror)
            return None

        return vol_uuid

    @staticmethod
    def get_devices_uuids(devices):
        """get volume uuids for all nvme devices. returns: volume uuids"""
        uuids = []
        for device in devices:
            device = device.replace('/dev/', '')
            uuid = NVMeOFAgent.get_uuid_by_nvme_device(device)
            if uuid is not None:
                uuids.append(uuid)

        return uuids

    def set_replica_state(self, volume, replica_uuid, replica_state):
        vol_uuid = volume.uuid
        result = self.prov_rest.set_replica_state(
            vol_uuid, replica_uuid, replica_state)

        if result.status != "Success":
            LOG.error(
                "set_replica_state for %s failed with %s",
                replica_uuid,
                result.status)
            return False
        return True

    def add_replica(self, volume):
        vol_uuid = volume.uuid
        replica = provisioner_entities.Replica(False, [], [], [])
        result = self.prov_rest.add_replica(replica, vol_uuid)

        if result.status != "Success":
            LOG.error("add_replica %s failed with %s", vol_uuid, result.status)
            return False

        return True

    def delete_replica(self, volume, replica_uuid):
        vol_uuid = volume.uuid
        result = self.prov_rest.delete_replica(vol_uuid, replica_uuid)

        if result.status != "Success":
            LOG.error("delete_replica failed: " + result.status)
            return False

        return True

    def delete_replica_confirm(self, volume, deleted_replica_uuid):
        vol_uuid = volume.uuid
        result = self.prov_rest.delete_replica_confirm(
            vol_uuid, deleted_replica_uuid)

        if result.status != "Success":
            LOG.error(
                "delete_replica_confirm for %s failed with %s",
                deleted_replica_uuid,
                result.status)

    def host_probe(self, host_nqn, host_uuid, host_name):
        try:
            result = self.prov_rest.host_probe(host_nqn, host_uuid, host_name,
                                               'Agent', '3.21.111', 30)
            LOG.debug("[!] host_probe result %s", result.status)
            if result.status != "Success":
                LOG.error(
                    "host_probe for %s failed with %s",
                    host_uuid,
                    result.status)
        except Exception as ex:
            LOG.error("[!] host_probe exception: %s", str(ex))
            traceback.print_exc()
            LOG.debug("[!] Exception %s", traceback.format_exc())

    @staticmethod
    def is_connected(executor, target_nqn):
        connected = False
        try:
            nvmeof.NVMeOFConnector.get_nvme_controllers(executor, target_nqn)
            connected = True
        except Exception:
            LOG.debug("target_nqn %s is not connected [!] Exception %s",
                      target_nqn, traceback.format_exc())

        return connected

    def forward_logs(self, level, hostname, message, parameters_list):
        """forward logs to syslog"""
        forward_entity = provisioner_entities.ForwardEntity(
            'EVENT', level, hostname, 'nvmeof_agent', message, parameters_list)
        result = self.prov_rest.forward_log(forward_entity)
        LOG.debug("[!] forward_logs result: %s ", str(result))
        return result

    def get_portals(self, persistent_id):
        backends = self.prov_rest.get_backends()
        portals = []
        for backend in backends.prov_entities:
            if backend.persistentID == persistent_id:
                LOG.debug("[!] backend: %s", str(backend))
                if backend.portals[0] is None:
                    msg = "no portals found on KumoScale"
                    raise Exception(msg)
                portals = backend.portals

        return portals

    def get_targets(self, host_uuid, vol_uuid):
        targets = []

        try:
            result = self.prov_rest.get_targets(host_uuid, vol_uuid)
        except Exception:
            LOG.warning(
                "Exception fetching targets for existing host %s",
                host_uuid)
            msg = "Targets for Volume {} could not be fetched.".\
                format(host_uuid)
            raise Exception(msg)

        LOG.debug("[!] get_targets: %s", str(result))
        if result.status == "Success":
            LOG.debug("[!] get_targets status Success")
            if result.prov_entities is None or len(result.prov_entities) == 0:
                LOG.warning(
                    "[!] Targets for vol_uuid %s could not be found",
                    vol_uuid)
            elif result.prov_entities is not None:
                LOG.debug("[!] get_targets found targets: %s",
                          str(len(result.prov_entities)))
                targets = result.prov_entities
            else:
                LOG.warning("[!] get_targets prov_entities is None!")

        LOG.debug("[!] get_targets end")
        return targets

    def get_replica_persistent_id(self, volume, vol_replica_uuid):

        volume_replicas = volume.location
        for i in range(len(volume_replicas)):
            vol_replica = volume_replicas[i]
            replica_uuid = str(vol_replica.uuid)
            if replica_uuid == vol_replica_uuid:
                replica_backend = vol_replica.backend
                replica_backend_pi = str(replica_backend.persistentID)
                return replica_backend_pi

        return None

    def find_target(self, targets, persistent_id):
        if targets is not None and len(targets) > 0:
            for target in targets:
                LOG.debug("[!] target: %s", str(target))
                if target.backend.persistentID == persistent_id:
                    return target
        return None

    def append_portals_for_connect(self, backend_portals):

        str_portals = []
        for p in range(len(backend_portals)):
            portal = backend_portals[p]
            portal_ip = str(portal.ip)
            portal_port = str(portal.port)
            portal_transport = str(portal.transport)
            LOG.debug('[!] portal_ip %s', portal_ip)
            LOG.debug('[!] portal %s', str(portal))
            str_portals.append((portal_ip, portal_port, portal_transport))

        return str_portals

    def get_namespace_path(self, target_nqn, uuid, num_of_attempts):
        for i in range(num_of_attempts):
            try:
                return nvmeof.NVMeOFConnector.get_nvme_device_path(
                    self.executor, target_nqn, uuid)
            except Exception:
                nvmeof.NVMeOFConnector.rescan(self.executor, target_nqn)
                LOG.debug("[!] Wait one second to discover: %s", uuid)
                time.sleep(1)
        return None

    def connect_to_volume_at_location(self, volume, volume_location, target):
        target_nqn = target.targetName
        LOG.debug("[!] target_nqn: %s", target_nqn)
        source = None
        if target_nqn is not None:
            connected = NVMeOFAgent.is_connected(self.executor, target_nqn)
            LOG.debug("[!] connected: %s", str(connected))
            if connected:
                nvmeof.NVMeOFConnector.rescan(self.executor, target_nqn)
                source = self.get_namespace_path(target_nqn,
                                                 volume_location.uuid, 2)
                LOG.debug("[!] Received block device: %s", source)
                return source

            persistent_id = self.get_replica_persistent_id(
                volume, volume_location.uuid)
            LOG.debug("[!] persistent_id: %s", persistent_id)
            backend_portals = self.get_portals(persistent_id)
            nvme_ctrls = []
            if backend_portals is not None:
                LOG.debug("[!] backend_portals: %s", str(len(backend_portals)))
                str_portals = self.append_portals_for_connect(backend_portals)
                any_new_connect = \
                    nvmeof.NVMeOFConnector.connect_to_portals(self.executor,
                                                              target_nqn,
                                                              str_portals,
                                                              nvme_ctrls)
                LOG.debug("[!] any_new_connect: %s", str(any_new_connect))
                if any_new_connect:
                    self.forward_logs(
                        'INFO', self.host_uuid,
                        'NVMeoF connection to backend was established', {
                            'initiatorNQN': self.host_nqn,
                            'targetNQN': target_nqn})
                    source = self.get_namespace_path(target_nqn,
                                                     volume_location.uuid, 2)
                    LOG.debug("[!] Received block device: %s", source)
                else:
                    LOG.debug("Could not connect to target portals: %s",
                              target_nqn)
        if source is None:
            LOG.error("Could not find namespace path: %s", target_nqn)
            return source
        self.set_replica_state(volume, volume_location.uuid, 'Available')
        return source

    def handle_leg_added(
            self,
            volume,
            md,
            md_details,
            failed_legs,
            host_id):
        LOG.debug("[!] handle_leg_added start uuid: %s", volume.uuid)
        md_name = md
        devices = md_details['devices']
        LOG.debug("[!] volume: %s", str(volume))
        all_devices_uuids = self.get_devices_uuids(devices)
        LOG.debug("[!] all_devices_uuids: %s", str(all_devices_uuids))
        failed_uuids = NVMeOFAgent.get_devices_uuids(failed_legs)
        LOG.debug("[!] failed_uuids: %s", str(failed_uuids))
        added = False
        added_leg = ""
        volume_replicas = volume.location
        LOG.debug("[!] host_id: %s", host_id)
        targets = self.get_targets(host_id, volume.uuid)
        if targets is None or len(targets) < 1:
            LOG.warning("[!] Could not find all targets for volume: %s",
                        volume.uuid)
            return False

        for volume_replica in volume_replicas:
            backends = []
            LOG.debug("[!] volume_replica: %s", str(volume_replica))

            # Skip missing replicas on unknown backends
            if volume_replica.backend.persistentID == "":
                continue

            result = self.prov_rest.get_backend_by_id(
                volume_replica.backend.persistentID)
            if result.status == "Success":
                if len(result.prov_entities) == 0:
                    LOG.warning(
                        "[!] Could not find backend with persistentID: %s",
                        volume_replica.backend.persistentID)
                    continue
                else:
                    backends = result.prov_entities
            if len(backends) != 1:
                LOG.warning(
                    "[!] Invalid num of backends for persistentID: %s",
                    volume_replica.backend.persistentID)
                continue
            LOG.debug("[!] volume_replica uuid: %s", volume_replica.uuid)
            exists = volume_replica.uuid in all_devices_uuids
            if not exists:
                LOG.debug("[!] Location is not in md %s", md)
            failed = volume_replica.uuid in failed_uuids
            LOG.debug("[!] failed: %s", str(failed))
            replica_state = str(volume_replica.replicaState)
            backend_state = str(backends[0].state)
            LOG.debug("[!] replica_state: %s", replica_state)
            LOG.debug("[!] backend_state: %s", backend_state)
            if (not exists or failed) \
                    and replica_state != REPLICA_TERMINATING \
                    and backend_state == BACKEND_AVAILABLE:
                added_replica_id = volume_replica.uuid
                LOG.debug(
                    "[!] Trying to add location to md: "
                    "%s added_replica_id: %s",
                    md,
                    added_replica_id)
                try:
                    result = self.prov_rest.publish(host_id, volume.uuid)
                except Exception as ex:
                    LOG.error("[!] Exception publish: %s", str(ex))
                    continue

                LOG.debug("[!] publish result: %s", str(result))
                if result.status != "Success" \
                        and result.status != 'AlreadyPublished':
                    LOG.error(
                        "[!] Volume %s could not be published, "
                        "host_uuid=%s status=%s.",
                        volume.alias,
                        host_id,
                        result.status)
                    continue

                persistent_id = self.get_replica_persistent_id(
                    volume, added_replica_id)
                LOG.debug("[!] persistent_id: %s", persistent_id)
                target = self.find_target(targets, persistent_id)
                if target is None:
                    targets = self.get_targets(host_id, volume.uuid)
                    if targets is None or len(targets) < 1:
                        LOG.warning("Could not find all targets for "
                                    "volume: %s",
                                    volume.uuid)
                        return False
                    target = self.find_target(targets, persistent_id)
                    if target is None:
                        LOG.error("Target could not be found, "
                                  "persistent_id: %s",
                                  persistent_id)
                        return False

                path = None
                for i in range(10):
                    path = self.connect_to_volume_at_location(volume,
                                                              volume_replica,
                                                              target)
                    if path is None:
                        LOG.error("[!] Sleeping one second for leg detection")
                        time.sleep(1)
                        continue
                    else:
                        break

                if path is None:
                    continue

                removed_ok = True
                LOG.debug("[!] failed_legs: %s", str(failed_legs))
                LOG.debug("[!] path: %s", path)
                if len(failed_legs) > 0 and failed:
                    removed_ok = self.remove_device_from_md(md_name, path,
                                                            False)
                added = False
                LOG.debug("[!] removed_ok: %s", str(removed_ok))
                if removed_ok:
                    added = self.add_device_to_md(md_name, path)
                    LOG.debug("[!] added: %s", str(added))
                    if not added:
                        self.remove_device_from_md(md_name, path, True)
                        LOG.debug("[!] added: %s", str(added))
                        added = self.add_device_to_md(md_name, path)
                        LOG.debug("[!] added: %s", str(added))
                    modified = False
                    added_leg = path
                    # avoid growing if there are failed legs since it might
                    # get stuck
                    if added and len(devices) < len(volume_replicas) \
                            and len(failed_legs) == 0:
                        modified = self.modify_device_count(md_name,
                                                            len(devices) + 1)
                    LOG.debug("[!] modified: %s", str(modified))
                    if modified:
                        md_details = {'state': RAID_STATE_DEGRADED_SYNCING}
                        self.check_md_events(
                            host_id, md_details, failed_legs, md_name,
                            volume.alias, True)
                        self.set_replica_state(
                            volume, added_replica_id, 'Synchronizing')

        LOG.debug("[!] handle_leg_added end with added = %s.", str(added))
        return added_leg, added

    def handle_terminating_leg(
            self,
            md_name,
            md_details,
            deleted_replica_uuid,
            volume):
        devices = md_details['devices']
        removed = False
        found = False

        LOG.debug("[!] devices: %s", str(devices))
        for device in devices:
            device_name = device.replace('/dev/', '')
            uuid = NVMeOFAgent.get_uuid_by_nvme_device(device_name)
            LOG.debug("[!] uuid: %s", uuid)
            if not uuid:
                LOG.error("get_uuid_by_nvme_device for %s not found", device)
                continue
            if uuid == deleted_replica_uuid:
                # found terminating leg - remove and confirm
                found = True
                removed = self.remove_device_from_md(md_name, device, True)
                if not removed:
                    LOG.error("remove_device_from_md for %s failed", device)
                break

        LOG.debug("[!] removed: %s found: %s", str(removed), str(found))
        if removed or not found:
            # reduce number of raid devices in case leg was terminated and no
            # other leg replaced it
            if len(devices) > 1 and len(devices) > len(volume.location) - 1:
                modified = self.modify_device_count(md_name, len(devices) - 1)
                if not modified:
                    LOG.error(
                        "Could not reduce number of legs: %s %s ",
                        md_name,
                        len(devices) - 1)
            self.delete_replica_confirm(volume, deleted_replica_uuid)

        return removed

    def sync_replica_state(
            self,
            volume,
            failed_legs,
            all_legs,
            active_legs,
            nvme_live_legs):
        """sync replica state on provisioner"""
        devices = all_legs
        all_devices_uuids = self.get_devices_uuids(devices)
        failed_devices_uuids = self.get_devices_uuids(failed_legs)
        active_devices_uuids = self.get_devices_uuids(active_legs)
        any_nvme_leg = nvme_live_legs is not None and len(nvme_live_legs) > 0
        reported_missing = []

        LOG.debug("[!] all_devices_uuids: %s", str(all_devices_uuids))
        LOG.debug("[!] failed_devices_uuids: %s", str(failed_devices_uuids))
        LOG.debug("[!] active_devices_uuids: %s", str(active_devices_uuids))
        LOG.debug("[!] any_nvme_leg: %s", str(any_nvme_leg))
        for location in volume.location:
            LOG.debug("[!] location: %s replicaState: %s",
                      str(location), str(location.replicaState))
            if location.replicaState != 'Available' \
                    and location.replicaState != 'Terminating':
                available = location.uuid in active_devices_uuids
                if available and any_nvme_leg:
                    vol_uuid = volume.uuid
                    LOG.debug("[!] vol_uuid1: %s", vol_uuid)
                    result = self.prov_rest.set_replica_state(
                        vol_uuid, location.uuid, 'Available')
                    if result.status != "Success":
                        msg = 'Set replica state available for volume ' \
                              + volume.alias + \
                            ' Replica: ' + vol_uuid \
                              + ' finished with status: ' + result.description
                        LOG.warning("[!] %s", msg)

            if location.replicaState != 'Synchronizing' \
                    and location.replicaState != 'Terminating':
                available = location.uuid in active_devices_uuids
                syncing = not available \
                    and location.uuid not in failed_devices_uuids and \
                    location.uuid in all_devices_uuids
                LOG.debug(
                    "[!] available: %s syncing: %s",
                    str(available),
                    str(syncing))
                if syncing and any_nvme_leg:
                    vol_uuid = volume.uuid
                    LOG.debug("[!] vol_uuid2: %s", vol_uuid)
                    result = self.prov_rest.set_replica_state(
                        vol_uuid, location.uuid, 'Synchronizing')
                    if result.status != "Success":
                        msg = 'Set replica state synchronizing for volume ' \
                              + volume.alias + ' Replica: ' + vol_uuid + \
                            ' finished with status: ' + result.description \
                              + ' for active_legs: ' + str(active_legs)
                        LOG.debug("[!] %s", msg)

            if location.replicaState != 'Missing' \
                    and location.replicaState != 'Terminating':
                if not any_nvme_leg or location.uuid not in all_devices_uuids \
                        or location.uuid in failed_devices_uuids:
                    vol_uuid = volume.uuid
                    LOG.debug("[!] vol_uuid3: %s", vol_uuid)
                    reported_missing.append(location.uuid)
                    result = self.prov_rest.set_replica_state(
                        vol_uuid, location.uuid, 'Missing')
                    if result.status != "Success":
                        msg = 'Set replica state missing for volume ' \
                              + volume.alias + \
                            ' Replica: ' + vol_uuid \
                              + ' finished with status: ' \
                              + result.description
                        LOG.debug("[!] %s", msg)
            else:
                reported_missing.append(location.uuid)

        for failed_leg in failed_devices_uuids:
            if failed_leg not in reported_missing:
                result = self.prov_rest.set_replica_state(volume.uuid,
                                                          failed_leg,
                                                          'Missing')
                if result.status != "Success":
                    msg = 'Set replica state missing for volume ' + \
                          volume.alias + ' Replica: ' + volume.uuid + \
                          ' finished with status: ' + result.description
                    LOG.error("[!] %s", msg)
                else:
                    msg = 'Set replica state missing (for replica which is ' \
                          'not in provisioner) for volume ' + volume.alias + \
                          ' Replica: ' + volume.uuid + \
                          ' finished with status: ' + result.description
                    LOG.debug("[!] %s", msg)

    def monitor_host(self):
        try:
            LOG.debug("[!] START host_nqn = %s", self.host_nqn)
            self.disconnect_unused_targets(self.host_nqn)
            LOG.debug("[!] host_nqn %s", self.host_nqn)
            LOG.debug("[!] host_uuid %s", self.host_uuid)
            LOG.debug("[!] host_name %s", self.hostname)
            self.host_probe(
                self.host_nqn, self.host_uuid,
                self.hostname)
            md_names = NVMeOFAgent.get_md_names()
            LOG.debug("[!] md_names %s", str(md_names))
            handled_volumes = []

            for md_name in md_names:
                LOG.debug("[!] ----- md_name: %s", md_name)
                active_legs, failed_legs, all_legs, \
                    nvme_live_legs, err = self.handle_leg_failure(md_name,False)
                if err:
                    continue

                device_index = md_name.rfind('/')
                volume_alias_from_md = md_name[device_index + 1:]

                LOG.debug("[!] nvme_live_legs %s", str(nvme_live_legs))
                if len(nvme_live_legs) < 1:
                    volume_alias = self.get_volume_alias("",
                                                         volume_alias_from_md)
                    volume = self.get_volume_if_connected_to_me(volume_alias)
                    if not volume:
                        continue
                    md_details = {
                        'state': RAID_STATE_DEGRADED,
                        'working_devices': 0}
                    self.check_md_events(
                        self.host_nqn,
                        md_details,
                        failed_legs,
                        md_name,
                        volume_alias,
                        False)
                    self.sync_replica_state(
                        volume, failed_legs, all_legs, active_legs,
                        nvme_live_legs)
                    continue

                md_details = self.get_any_leg_raid_details(nvme_live_legs)
                if md_details is None:
                    md_details = {'raid_devices': len(all_legs), 'name': ''}
                md_details['devices'] = all_legs
                md_details['total_devices'] = len(all_legs)
                md_details['failed_devices'] = len(failed_legs)
                md_details['working_devices'] = len(active_legs)
                if 'raid_devices' not in md_details:
                    md_details['raid_devices'] = len(all_legs)

                md_details['state'] = self.calc_raid_state(
                    active_legs, failed_legs, all_legs, md_name)
                LOG.debug("[!] md_details %s", str(md_details))
                volume_alias = self.get_volume_alias(
                    md_details['name'], volume_alias_from_md)
                LOG.debug("[!] volume_alias %s", str(volume_alias))
                LOG.debug("[!] handled_volumes %s", str(handled_volumes))
                if volume_alias in handled_volumes:
                    old_details = self.mds[md_name]
                    LOG.debug("[!] old_details %s", str(old_details))
                    if old_details is not None:
                        self.mds['/dev/md/' + volume_alias] = old_details
                        del self.mds[md_name]
                    if md_details['name'] != '':
                        files_path = '/dev/md/' + md_details['name'] + '*'
                        NVMeOFAgent.delete_files(files_path)
                    continue
                else:
                    handled_volumes.append(volume_alias)

                LOG.debug("[!] handled_volumes2 %s", str(handled_volumes))
                volume = self.get_volume_if_connected_to_me(volume_alias)
                if volume is None:
                    continue
                active_legs, failed_legs, all_legs, \
                nvme_live_legs, err = self.handle_leg_failure(md_name, True)
                if err:
                    continue
                LOG.debug("[!] volume %s", str(volume))
                LOG.debug("[!] call check_md_events...")
                self.check_md_events(
                    self.host_nqn,
                    md_details,
                    failed_legs,
                    md_name,
                    volume_alias,
                    False)
                LOG.debug("[!] call sync_replica_state...")
                self.sync_replica_state(
                    volume,
                    failed_legs,
                    all_legs,
                    active_legs,
                    nvme_live_legs)
                LOG.debug("[!] call check_volume...")
                vol_changed = self.check_volume(
                    volume,
                    md_name,
                    md_details,
                    failed_legs,
                    self.host_uuid)
                LOG.debug("[!] vol_changed %s", str(vol_changed))
                if not vol_changed:
                    self.enhanced_self_healing(
                        volume,
                        md_name,
                        md_details,
                        active_legs,
                        failed_legs,
                        self.host_uuid)
            LOG.debug("[!] END [!]")
        except Exception as ex:
            LOG.error("[!] monitor_host exception: %s", str(ex))
            traceback.print_exc()
            LOG.debug("[!] Exception %s", traceback.format_exc())

    def enhanced_self_healing(
            self,
            volume,
            md,
            md_details,
            active_legs,
            failed_legs,
            host_id):
        added = False
        added_leg = ""
        deleted = False
        LOG.debug(
            "[!] enhanced_self_healing maxReplicaDownTime: %s", str(
                volume.maxReplicaDownTime))
        if volume.numReplicas == 4 or volume.maxReplicaDownTime == 0:
            return
        failed_uuids = NVMeOFAgent.get_devices_uuids(failed_legs)
        active_uuids = NVMeOFAgent.get_devices_uuids(active_legs)
        LOG.debug("[!] failed_uuids: %s", str(failed_uuids))
        LOG.debug("[!] active_uuids: %s", str(active_uuids))
        for location in volume.location:
            if (location.uuid in failed_uuids
                or location.uuid not in active_uuids) \
                    and location.replicaState == REPLICA_MISSING:
                replica_state_time = \
                    location.currentStateTime / (1000 * 60) % 60
                LOG.debug("[!] replica %s state_time: %s", location.uuid,
                          str(replica_state_time))
                if 0 < volume.maxReplicaDownTime <= replica_state_time:
                    LOG.debug("[!] Call add_replica fr volume %s", volume.uuid)
                    success = self.add_replica(volume)
                    LOG.debug("[!] success: %s", str(success))
                    if success:
                        LOG.debug(
                            "[!] Add replica for volume %s replica %s "
                            "finished successfully.",
                            volume.alias,
                            volume.uuid)
                        volume = self.get_volume_by_uuid(volume.uuid)
                        if volume is None:
                            LOG.error(
                                '[!!] Could not find volume after '
                                'adding replica')
                            break
                        try:
                            LOG.debug("[!] Call handle_leg_added...")
                            added_leg, added = self.handle_leg_added(
                                volume, md, md_details, failed_legs,
                                host_id)
                        except Exception as ex:
                            LOG.error(
                                "[!] handle_leg_added exception: %s",
                                str(ex))
                        LOG.debug("[!] added: %s, added_leg: %s", str(added),
                                  added_leg)
                        if added or added_leg != "":
                            deleted = self.delete_replica(volume,
                                                          location.uuid)
                            LOG.debug("[!] deleted : %s", str(deleted))
                            if deleted:
                                LOG.debug(
                                    "[!] Delete replica for "
                                    "volume %s replica %s "
                                    "finished successfully.",
                                    volume.alias,
                                    volume.uuid)
                                break
                    else:
                        break

        LOG.debug(
            "[!] enhanced_self_healing ended with added: %s and deleted: %s",
            str(added),
            str(deleted))

    def check_volume(
            self,
            volume,
            md,
            md_details,
            failed_legs,
            host_id):
        num_terminating, terminating = self.get_terminating_leg(volume)
        if num_terminating == 1:
            LOG.debug("[!] handle_terminating_leg ...")
            vol_changed = self.handle_terminating_leg(
                md, md_details, terminating.uuid, volume)
            return vol_changed

        prov_num_legs = len(volume.location)
        added = False
        added_leg = ""
        LOG.debug("[!] md_details %s", str(md_details))
        if md_details['raid_devices'] < prov_num_legs \
                or md_details['total_devices'] < prov_num_legs \
                or md_details['failed_devices'] > 0:
            LOG.debug("[!] handle_leg_added ...")
            try:
                added_leg, added = self.handle_leg_added(
                    volume, md, md_details, failed_legs, host_id)
            except Exception as ex:
                LOG.warning("[!] handle_leg_added Exception: %s", str(ex))
                traceback.print_exc()
        LOG.debug("[!] added: %s, added_leg: %s", str(added), added_leg)
        LOG.debug("[!] check_volume end ...")
        return added

    @staticmethod
    def delete_files(glob_path):
        for fname in glob.glob(glob_path):
            os.remove(fname)

    def get_any_leg_raid_details(self, legs):
        for leg in legs:
            md_details = self.get_raid_details(leg, True)
            if md_details is not None:
                return md_details
        return None

    def get_raid_details(self, raid_name, from_device):
        raid_details = {}
        cmd = ['mdadm']

        if from_device:
            cmd.append('--examine')
        else:
            cmd.append('--detail')

        cmd.append(raid_name)
        LOG.debug("[!] cmd = " + str(cmd))

        try:
            lines, err = self.executor._execute(
                *cmd, run_as_root=True, root_helper=self.executor._root_helper)
            for line in lines.split('\n'):
                pair = line.split(' : ')
                if len(pair) > 3 or len(pair) < 2:
                    continue
                if 'Name' in pair[0]:
                    raid_details_name = pair[len(pair) - 1]
                    if ' ' in raid_details_name:
                        pair2 = raid_details_name.split(' ')
                        raid_details['name'] = pair2[0]
                    else:
                        raid_details['name'] = raid_details_name
                elif 'UUID' in pair[0]:
                    raid_details['uuid'] = pair[1]
                elif 'Version' in pair[0]:
                    raid_details['version'] = pair[1]
                elif 'Creation Time' in pair[0]:
                    raid_details['creation_time'] = pair[1]
                elif 'Raid Level' in pair[0]:
                    raid_details['raid_level'] = pair[1]
                elif 'Array Size' in pair[0]:
                    sub_line = pair[1].split(' ')
                    raid_details['array_size'] = int(sub_line[0]) * 1024
                elif 'Raid Devices' in pair[0]:
                    raid_details['raid_devices'] = int(pair[1])
                elif 'Total Devices' in pair[0]:
                    raid_details['total_devices'] = int(pair[1])
                elif 'Active Devices' in pair[0]:
                    raid_details['active_devices'] = int(pair[1])
                elif 'Working Devices' in pair[0]:
                    raid_details['working_devices'] = int(pair[1])
                elif 'Failed Devices' in pair[0]:
                    raid_details['failed_devices'] = int(pair[1])
                elif 'Spare Devices' in pair[0]:
                    raid_details['spare_devices'] = int(pair[1])
        except putils.ProcessExecutionError as ex:
            LOG.warning("[!] Could not run mdadm: %s", str(ex))
            return None
        LOG.debug("[!] raid_details = " + str(raid_details))
        return raid_details

    def calc_raid_state(self, active_legs, failed_legs, all_legs, md):
        degraded = self.get_num_degraded(md)
        LOG.debug("[!] degraded %s", str(degraded))
        if degraded > 0:
            raid_state = RAID_STATE_DEGRADED
        else:
            return RAID_STATE_OK

        for leg in all_legs:
            if leg not in failed_legs and leg not in active_legs:
                return RAID_STATE_DEGRADED_SYNCING

        return raid_state

    def check_md_events(
            self,
            host,
            md_details,
            failed_legs,
            md,
            volume_alias,
            initiated_sync):

        try:
            old_details = None
            LOG.debug("[!] md: %s ", md)
            LOG.debug("[!] mds: %s ", str(self.mds))
            if not self.mds:
                old_details = None
            elif md in self.mds:
                old_details = self.mds[md]
            LOG.debug("[!] old_details: %s ", str(old_details))
            LOG.debug("[!] md_details: %s ", str(md_details))
            if (not old_details and md_details['state'] != RAID_STATE_OK) \
                or (old_details is not None
                    and (old_details['state'] != md_details['state']
                         or initiated_sync is True)):
                if old_details is not None \
                        and 'working_devices' in old_details \
                        and 'working_devices' in md_details \
                        and old_details['state'] == \
                        RAID_STATE_DEGRADED_SYNCING \
                        and md_details['working_devices'] > \
                        old_details['working_devices']:
                    num_healed = md_details['working_devices'] - \
                        old_details['working_devices']
                    LOG.debug("[!] num_healed: %s ", str(num_healed))
                    for i in range(num_healed):
                        self.send_md_events(
                            host, md_details, failed_legs, volume_alias, True)

                LOG.debug(
                    "[!] check_md_events md_details: %s ",
                    str(md_details))
                # we don't want to send event when moving from
                # RAID_STATE_DEGRADED_SYNCING -> RAID_STATE_DEGRADED
                if old_details is not None \
                        and old_details['state'] != \
                        RAID_STATE_DEGRADED_SYNCING \
                        or md_details['state'] != RAID_STATE_DEGRADED:
                    self.send_md_events(
                        host, md_details, failed_legs, volume_alias, False)

            LOG.debug(
                "[!] check_md_events end with md_details: %s ",
                str(md_details))
            self.mds[md] = md_details
        except Exception:
            LOG.exception("Exception check_md_events  %s", md)

    def send_md_events(
            self,
            host,
            md_details,
            failed_legs,
            volume_alias,
            sync_completed):
        LOG.debug("[!] send_md_events for state: %s ",
                  str(md_details['state']))
        if sync_completed:
            report_level = 'WARN'
            report_message = 'Volume synchronization ended.'
        elif md_details['state'] == RAID_STATE_OK:
            report_level = 'INFO'
            report_message = 'Volume is healed.'
        elif md_details['state'] == RAID_STATE_DEGRADED:
            report_level = 'FATAL'
            report_message = 'Volume is degraded.'
        elif md_details['state'] == RAID_STATE_DEGRADED_SYNCING:
            report_level = 'WARN'
            report_message = 'Volume synchronization started.'
        else:
            msg = "Cannot issue event for unknown RAID state: " + \
                str(md_details['state'])
            LOG.debug("[!] %s", msg)
            return

        report_params = {'volumeAlias': volume_alias}
        if md_details['state'] == RAID_STATE_DEGRADED and not sync_completed:
            failed_devices_uuids = NVMeOFAgent.get_devices_uuids(
                failed_legs)
            LOG.debug(
                "[!] failed_devices_uuids: %s ",
                str(failed_devices_uuids))
            prefix = 'uuid'
            i = 0
            for uuid in failed_devices_uuids:
                report_params[prefix + str(i)] = uuid

        self.forward_logs(report_level, host, report_message, report_params)

    def get_volume_if_connected_to_me(self, vol_alias):
        LOG.debug("[!] get_volume_if_connected_to_me volume: %s ", vol_alias)
        volume = self.get_volume_by_alias(vol_alias)
        if volume is None:
            return None
        LOG.debug("[!] connected volume: %s ", str(volume))
        targets = self.get_targets(None, volume.uuid)
        for target in targets:
            LOG.debug("[!] connected target: %s ", str(target))
            if target.numNamespaces < 1:
                continue
            connected = NVMeOFAgent.is_connected(self.executor,
                                                 target.targetName)
            LOG.debug("[!] connected: %s ", str(connected))
            if connected:
                return volume
        return None

    def get_volume_alias(self, md_name, md_path):
        if md_name == '':
            volume_alias = md_path
        else:
            volume_alias = md_name
        LOG.debug("[!] volume_alias %s", volume_alias)
        alias_index = volume_alias.find(":")
        LOG.debug("[!] alias_index %s", str(alias_index))
        if alias_index > 0:
            volume_alias = volume_alias[alias_index + 1:]
        return volume_alias

    def handle_leg_failure(self, md_name, ourVolumeForSure):
        md_link = None

        try:
            md_link = os.readlink(
                '/dev/md/{0}'.format(md_name)).replace('../', '')
        except OSError:
            LOG.error("Invalid mdadm device: %s", md_name)

        device_index = md_link.rfind('/')
        md__dev_name = md_link[device_index + 1:]
        LOG.debug("md__dev_name: %s", md__dev_name)
        if not self.is_exists_in_md_stat(self.executor, md__dev_name):
            msg = 'device ' + md__dev_name + ' is not a local array'
            return None, None, None, None, msg

        all_legs, failed_legs, active_legs = self.get_nvme_devices_by_state(
            md_name)
        search_path = '/sys/block/{0}/slaves/*/device/state'
        nvme_legs_states = self.get_nvme_states(md_name, search_path)
        LOG.debug("nvme_legs_states: %s", str(nvme_legs_states))

        nvme_live_legs = []
        if len(nvme_legs_states) <= 0:
            # retry on nvme mpath kernels on different path
            search_path = '/sys/block/{0}/slaves/*/device/nvme*/state'
            nvme_legs_states = self.get_nvme_states(md_name, search_path)
            LOG.debug("mpath nvme_legs_states: %s", str(nvme_legs_states))
            if len(nvme_legs_states) <= 0:
                LOG.debug("0 nvme legs: %s", md_name)
                return active_legs, failed_legs, all_legs, nvme_live_legs, None

        LOG.debug("failed_legs: %s", str(failed_legs))
        for device, states in nvme_legs_states.items():
            LOG.debug("device: %s states: %s", device, states)
            if 'live' not in states and device not in failed_legs and ourVolumeForSure:
                msg = "Device : " + device + \
                      " Is not live but not failed in md, removing the md leg"
                LOG.debug("[!] %s", msg)
                success = self.fail_md(md_name, device)
                if success:
                    failed_legs.append(device)
                else:
                    LOG.warning(
                        "Removing leg from md: %s leg: %s failed!",
                        md_name,
                        device)
            elif 'live' in states:
                nvme_live_legs.append(device)
        return active_legs, failed_legs, all_legs, nvme_live_legs, None

    @staticmethod
    def is_exists_in_md_stat(executor, name):
        lines, _err = executor._execute(
            'cat', '/proc/mdstat', run_as_root=True,
            root_helper=executor._root_helper)
        for line in lines.split('\n'):
            if name in line:
                return True
        return False

    def _get_connected_nqns(self):
        nqns = []
        nvme_device_path = '/sys/class/nvme-fabrics/ctl/nvme*'
        ctrls = glob.glob(nvme_device_path)
        for ctrl in ctrls:
            LOG.debug("[!] ctrl = %s", ctrl)
            file_path = ctrl + '/subsysnqn'
            lines, _err = self.executor._execute(
                'cat', file_path, run_as_root=True,
                root_helper=self.executor._root_helper)
            line = None
            for line in lines.split('\n'):
                break
            candidate_nqn = line
            LOG.debug("[!] candidate_nqn = %s", candidate_nqn)
            nqns.append(candidate_nqn)
        return nqns

    def get_num_namespaces(self, nvme_ctrl):
        nvme_device_path = \
            '/sys/class/nvme-fabrics/ctl/' + nvme_ctrl + '/nvme*'
        nss = glob.glob(nvme_device_path)
        return len(nss)

    @staticmethod
    def disconnect(executor, target_nqn):
        LOG.debug("[!] target_nqn = %s", target_nqn)
        nvme_command = ('disconnect', '-n', target_nqn)
        try:
            nvmeof.NVMeOFConnector.run_nvme_cli(executor, nvme_command)
        except Exception:
            logging.exception("Could not disconnect target_nqn %s", target_nqn)

    @synchronized('connect_volume')
    def disconnect_unused_targets(self, host):
        nqns = self._get_connected_nqns()
        LOG.debug("[!] nqns = %s", str(nqns))
        for nqn in nqns:
            try:
                ctrl_device_paths = \
                    nvmeof.NVMeOFConnector.get_nvme_controllers(self.executor,
                                                                nqn)
                LOG.debug("[!] ctrl_device_paths = %s", ctrl_device_paths)
                for ctrl_device_path in ctrl_device_paths:
                    num_namespaces = self.get_num_namespaces(ctrl_device_path)
                    LOG.debug("[!] num_namespaces = %s", num_namespaces)
                    if num_namespaces < 1:
                        LOG.debug("[!] disconnect = %s", nqn)
                        self.disconnect(self.executor, nqn)
                        self.forward_logs(
                            'INFO', host,
                            'NVMeoF connection to backend was closed.', {
                                'initiatorNQN': host, 'targetNQN': nqn})
            except Exception:
                LOG.debug("[!] Exception %s", traceback.format_exc())

    @staticmethod
    def _get_host_name():
        name = socket.gethostname()
        name = NVMeOFAgent._convert_host_name(name)
        return name

    @staticmethod
    def _convert_host_name(name):
        if name is None:
            return ""
        if len(name) > 32:
            name = md5(name.encode('utf-8'), usedforsecurity=False).hexdigest()
        else:
            name = name.replace('.', '-').lower()
        return name
