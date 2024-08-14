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

import errno
import glob
import json
import os.path
import re
import time

from oslo_concurrency import lockutils
from oslo_concurrency import processutils as putils
from oslo_log import log as logging

from os_brick import exception
from os_brick.i18n import _
from os_brick.initiator.connectors import base
try:
    from os_brick.initiator.connectors import nvmeof_agent
except ImportError:
    nvmeof_agent = None
from os_brick.privileged import rootwrap as priv_rootwrap
from os_brick import utils

DEV_SEARCH_PATH = '/dev/'

DEVICE_SCAN_ATTEMPTS_DEFAULT = 5

synchronized = lockutils.synchronized_with_prefix('os-brick-')

LOG = logging.getLogger(__name__)

g_host = None


class NVMeOFConnector(base.BaseLinuxConnector):
    """Connector class to attach/detach NVMe-oF volumes."""

    native_multipath_supported = None

    def __init__(self, root_helper, driver=None, use_multipath=False,
                 device_scan_attempts=DEVICE_SCAN_ATTEMPTS_DEFAULT,
                 *args, **kwargs):
        super(NVMeOFConnector, self).__init__(
            root_helper,
            driver=driver,
            device_scan_attempts=device_scan_attempts,
            *args, **kwargs)
        self.use_multipath = use_multipath
        self._set_native_multipath_supported()
        if self.use_multipath and not \
                NVMeOFConnector.native_multipath_supported:
            LOG.warning('native multipath is not enabled')
        LOG.debug("[!] use_multipath: %s", self.use_multipath)

    @staticmethod
    def get_search_path():
        return DEV_SEARCH_PATH

    def get_volume_paths(self, connection_properties):
        device_path = connection_properties.get('device_path')
        if device_path:
            return [device_path]
        volume_replicas = connection_properties.get('volume_replicas')
        if not volume_replicas:  # compatibility
            return []
        replica_count = connection_properties.get('replica_count')

        try:
            if volume_replicas and replica_count > 1:
                return ['/dev/md/' + connection_properties.get('alias')]
            if volume_replicas and replica_count == 1:
                return [NVMeOFConnector.get_namespace_path(
                    self, volume_replicas[0]['target_nqn'],
                    volume_replicas[0]['vol_uuid'], 1, None, False)]
            else:
                return [NVMeOFConnector.get_namespace_path(
                    self, connection_properties.get('target_nqn'),
                    connection_properties.get('vol_uuid'), 1, None, False)]
        except exception.VolumeDeviceNotFound:
            return []

    @classmethod
    def nvme_present(cls):
        try:
            priv_rootwrap.custom_execute('nvme', 'version')
            return True
        except Exception as exc:
            if isinstance(exc, OSError) and exc.errno == errno.ENOENT:
                LOG.debug('nvme not present on system')
            else:
                LOG.warning('Unknown error when checking presence of nvme: %s',
                            exc)
        return False

    @classmethod
    def get_connector_properties(cls, root_helper, *args, **kwargs):
        """The NVMe-oF connector properties (initiator uuid and nqn.)"""
        global g_host
        execute = kwargs.get('execute') or priv_rootwrap.execute
        nvmf = NVMeOFConnector(root_helper=root_helper, execute=execute)
        ret = {}
        nqn = None
        uuid = nvmf._get_host_uuid()
        suuid = nvmf._get_system_uuid()
        if cls.nvme_present():
            nqn = utils.get_host_nqn()
        if uuid:
            ret['uuid'] = uuid
        if suuid:
            ret['system uuid'] = suuid  # compatibility
        if nqn:
            ret['nqn'] = nqn
        g_host = kwargs.get('host', None)
        ret['nvme_native_multipath'] = cls._set_native_multipath_supported()
        return ret

    def _get_host_uuid(self):
        with open('/etc/kioxia/uuid.conf') as f:
            host_uuid = f.read()
        return(host_uuid)

    def _get_system_uuid(self):
        # RSD requires system_uuid to let Cinder RSD Driver identify
        # Nova node for later RSD volume attachment.
        try:
            out, err = self._execute('cat', '/sys/class/dmi/id/product_uuid',
                                     root_helper=self._root_helper,
                                     run_as_root=True)
        except putils.ProcessExecutionError:
            try:
                out, err = self._execute('dmidecode', '-ssystem-uuid',
                                         root_helper=self._root_helper,
                                         run_as_root=True)
                if not out:
                    LOG.warning('dmidecode returned empty system-uuid')
            except putils.ProcessExecutionError as e:
                LOG.debug("Unable to locate dmidecode. For Cinder RSD Backend,"
                          " please make sure it is installed: %s", e)
                out = ""
        return out.strip()

    @classmethod
    def _set_native_multipath_supported(cls):
        if cls.native_multipath_supported is None:
            cls.native_multipath_supported = \
                cls._is_native_multipath_supported()
        return cls.native_multipath_supported

    @staticmethod
    def _is_native_multipath_supported():
        try:
            with open('/sys/module/nvme_core/parameters/multipath', 'rt') as f:
                return f.read().strip() == 'Y'
        except Exception:
            LOG.warning("Could not find nvme_core/parameters/multipath")
        return False

    def _get_nvme_devices(self):
        nvme_devices = []
        # match nvme devices like /dev/nvme10n10
        pattern = r'/dev/nvme[0-9]+n[0-9]+'
        cmd = ['nvme', 'list']
        for retry in range(1, self.device_scan_attempts + 1):
            try:
                (out, err) = self._execute(*cmd,
                                           root_helper=self._root_helper,
                                           run_as_root=True)
                for line in out.split('\n'):
                    result = re.match(pattern, line)
                    if result:
                        nvme_devices.append(result.group(0))
                LOG.debug("_get_nvme_devices returned %(nvme_devices)s",
                          {'nvme_devices': nvme_devices})
                return nvme_devices

            except putils.ProcessExecutionError:
                LOG.warning(
                    "Failed to list available NVMe connected controllers, "
                    "retrying.")
                time.sleep(retry ** 2)
        else:
            msg = _("Failed to retrieve available connected NVMe controllers "
                    "when running nvme list.")
            raise exception.CommandExecutionFailed(message=msg)

    @utils.retry(exception.VolumePathsNotFound)
    def _get_device_path(self, current_nvme_devices):
        all_nvme_devices = self._get_nvme_devices()
        LOG.debug("all_nvme_devices are %(all_nvme_devices)s",
                  {'all_nvme_devices': all_nvme_devices})
        path = set(all_nvme_devices) - set(current_nvme_devices)
        if not path:
            raise exception.VolumePathsNotFound()
        return list(path)[0]

    @utils.retry(exception.VolumeDeviceNotFound)
    def _get_device_path_by_nguid(self, nguid):
        device_path = os.path.join(DEV_SEARCH_PATH,
                                   'disk',
                                   'by-id',
                                   'nvme-eui.%s' % nguid)
        LOG.debug("Try to retrieve symlink to %(device_path)s.",
                  {"device_path": device_path})
        try:
            path, _err = self._execute('readlink',
                                       '-e',
                                       device_path,
                                       run_as_root=True,
                                       root_helper=self._root_helper)
            if not path:
                raise exception.VolumePathsNotFound()
            return path.rstrip()
        except putils.ProcessExecutionError as e:
            LOG.exception(e)
            raise exception.VolumeDeviceNotFound(device=device_path)

    @utils.retry(putils.ProcessExecutionError)
    def _try_connect_nvme(self, cmd):
        try:
            self._execute(*cmd, root_helper=self._root_helper,
                          run_as_root=True)
        except putils.ProcessExecutionError as e:
            # Idempotent connection to target.
            # Exit code 70 means that target is already connected.
            if e.exit_code == 70:
                return
            raise

    def _get_nvme_subsys(self):
        # Example output:
        # {
        #   'Subsystems' : [
        #     {
        #       'Name' : 'nvme-subsys0',
        #       'NQN' : 'nqn.2016-06.io.spdk:cnode1'
        #     },
        #     {
        #       'Paths' : [
        #         {
        #           'Name' : 'nvme0',
        #           'Transport' : 'rdma',
        #           'Address' : 'traddr=10.0.2.15 trsvcid=4420'
        #         }
        #       ]
        #     }
        #   ]
        # }
        #

        cmd = ['nvme', 'list-subsys', '-o', 'json']
        ret_val = self._execute(*cmd, root_helper=self._root_helper,
                                run_as_root=True)

        return ret_val

    @staticmethod
    def _filter_nvme_devices(current_nvme_devices, nvme_controller):
        LOG.debug("Filter NVMe devices belonging to controller "
                  "%(nvme_controller)s.",
                  {"nvme_controller": nvme_controller})
        nvme_name_pattern = "/dev/%sn[0-9]+" % nvme_controller
        nvme_devices_filtered = list(
            filter(
                lambda device: re.match(nvme_name_pattern, device),
                current_nvme_devices
            )
        )
        return nvme_devices_filtered

    @utils.retry(exception.NotFound, retries=5)
    def _is_nvme_available(self, nvme_name):
        current_nvme_devices = self._get_nvme_devices()
        if self._filter_nvme_devices(current_nvme_devices, nvme_name):
            return True
        else:
            LOG.error("Failed to find nvme device")
            raise exception.NotFound()

    def _wait_for_blk(self, nvme_transport_type, conn_nqn,
                      target_portal, port):
        # Find nvme name in subsystem list and wait max 15 seconds
        # until new volume will be available in kernel
        nvme_name = ""
        nvme_address = "traddr=%s trsvcid=%s" % (target_portal, port)

        # Get nvme subsystems in order to find
        # nvme name for connected nvme
        try:
            (out, err) = self._get_nvme_subsys()
        except putils.ProcessExecutionError:
            LOG.error("Failed to get nvme subsystems")
            raise

        # Get subsystem list. Throw exception if out is currupt or empty
        try:
            subsystems = json.loads(out)['Subsystems']
        except Exception:
            return False

        # Find nvme name among subsystems
        for i in range(0, int(len(subsystems) / 2)):
            subsystem = subsystems[i * 2]
            if 'NQN' in subsystem and subsystem['NQN'] == conn_nqn:
                for path in subsystems[i * 2 + 1]['Paths']:
                    if (path['Transport'] == nvme_transport_type
                            and path['Address'] == nvme_address):
                        nvme_name = path['Name']
                        break

        if not nvme_name:
            return False

        # Wait until nvme will be available in kernel
        return self._is_nvme_available(nvme_name)

    @utils.trace
    @synchronized('connect_volume', external=True)
    def connect_volume(self, connection_properties):
        """Discover and attach the volume.

        :param connection_properties: The dictionary that describes all
                                      of the target volume attributes.
               connection_properties must include:
               nqn - NVMe subsystem name to the volume to be connected
               target_port - NVMe target port that hosts the nqn sybsystem
               target_portal - NVMe target ip that hosts the nqn sybsystem
        :type connection_properties: dict
        :returns: dict
        """
        if connection_properties.get('vol_uuid'):  # compatibility
            return self._connect_volume_by_uuid(connection_properties)

        current_nvme_devices = self._get_nvme_devices()

        device_info = {'type': 'block'}
        conn_nqn = connection_properties['nqn']
        target_portal = connection_properties['target_portal']
        port = connection_properties['target_port']
        nvme_transport_type = connection_properties['transport_type']
        host_nqn = connection_properties.get('host_nqn')
        device_nguid = connection_properties.get('volume_nguid')
        cmd = [
            'nvme', 'connect',
            '-t', nvme_transport_type,
            '-n', conn_nqn,
            '-a', target_portal,
            '-s', port]
        if host_nqn:
            cmd.extend(['-q', host_nqn])

        self._try_connect_nvme(cmd)
        try:
            self._wait_for_blk(nvme_transport_type, conn_nqn,
                               target_portal, port)
        except exception.NotFound:
            LOG.error("Waiting for nvme failed")
            raise exception.NotFound(message="nvme connect: NVMe device "
                                             "not found")
        if device_nguid:
            path = self._get_device_path_by_nguid(device_nguid)
        else:
            path = self._get_device_path(current_nvme_devices)
        device_info['path'] = path
        LOG.debug("NVMe device to be connected to is %(path)s",
                  {'path': path})
        return device_info

    @utils.trace
    @synchronized('connect_volume', external=True)
    def disconnect_volume(self, connection_properties, device_info,
                          force=False, ignore_errors=False):
        """Flush the volume.

        Disconnect of volumes happens on storage system side. Here we could
        remove connections to subsystems if no volumes are left. But new
        volumes can pop up asynchronously in the meantime. So the only thing
        left is flushing or disassembly of a correspondng RAID device.

        :param connection_properties: The dictionary that describes all
                                      of the target volume attributes.
               connection_properties must include:
               device_path - path to the volume to be connected
        :type connection_properties: dict

        :param device_info: historical difference, but same as connection_props
        :type device_info: dict

        """
        if connection_properties.get('vol_uuid'):  # compatibility
            return self._disconnect_volume_replicated(
                connection_properties, device_info,
                force=force, ignore_errors=ignore_errors)

        conn_nqn = connection_properties['nqn']
        if device_info and device_info.get('path'):
            device_path = device_info.get('path')
        else:
            device_path = connection_properties['device_path'] or ''
        current_nvme_devices = self._get_nvme_devices()
        if device_path not in current_nvme_devices:
            LOG.warning("Trying to disconnect device %(device_path)s with "
                        "subnqn %(conn_nqn)s that is not connected.",
                        {'device_path': device_path, 'conn_nqn': conn_nqn})
            return

        try:
            self._linuxscsi.flush_device_io(device_path)
        except putils.ProcessExecutionError:
            if not ignore_errors:
                raise

    @utils.trace
    @synchronized('extend_volume', external=True)
    def extend_volume(self, connection_properties):
        """Update the local kernel's size information.

        Try and update the local kernel's size information
        for an LVM volume.
        """
        if connection_properties.get('vol_uuid'):  # compatibility
            return self._extend_volume_replicated(connection_properties)

        volume_paths = self.get_volume_paths(connection_properties)
        if volume_paths:
            if connection_properties.get('volume_nguid'):
                for path in volume_paths:
                    return self._linuxscsi.get_device_size(path)
            return self._linuxscsi.extend_volume(
                volume_paths, use_multipath=self.use_multipath)
        else:
            LOG.warning("Couldn't find any volume paths on the host to "
                        "extend volume for %(props)s",
                        {'props': connection_properties})
            raise exception.VolumePathsNotFound()

    @utils.trace
    def _connect_volume_by_uuid(self, connection_properties):
        """connect to volume on host

        connection_properties for NVMe-oF must include:
        target_portals - list of ip,port,transport for each portal
        target_nqn - NVMe-oF Qualified Name
        vol_uuid - UUID for volume/replica
        """
        volume_replicas = connection_properties.get('volume_replicas')
        replica_count = connection_properties.get('replica_count')
        volume_alias = connection_properties.get('alias')
        replicable = connection_properties.get('replicable', False)
        volume_uuid = connection_properties['vol_uuid']
        writable = connection_properties.get('writable', True)
        if replica_count > 1:
            replicable = True

        if volume_replicas:
            host_device_paths = []

            for replica in volume_replicas:
                try:
                    rep_host_device_path = self._connect_target_volume(
                        replica['target_nqn'], replica['vol_uuid'],
                        replica['portals'])
                    if rep_host_device_path:
                        host_device_paths.append(rep_host_device_path)
                except Exception as ex:
                    LOG.error("_connect_target_volume: %s", ex)
            if not host_device_paths:
                raise exception.VolumeDeviceNotFound(
                    device=volume_replicas)

            if replicable:
                device_path = \
                    NVMeOFConnector.handle_replicated_volume(self, volume_uuid,
                                                             host_device_paths,
                                                             volume_alias,
                                                             replica_count,
                                                             writable)
            else:
                device_path = self._handle_single_replica(
                    volume_uuid, host_device_paths, volume_alias, writable)
        else:
            device_path = self._connect_target_volume(
                connection_properties['target_nqn'],
                volume_uuid,
                connection_properties['portals'])

        if nvmeof_agent:
            nvmeof_agent.NVMeOFAgent.ensure_running(self, g_host)

        return {'type': 'block', 'path': device_path}

    @utils.trace
    def _disconnect_volume_replicated(self, connection_properties, device_info,
                                      force=False, ignore_errors=False):
        device_path = None
        volume_replicas = connection_properties.get('volume_replicas')
        replica_count = connection_properties.get('replica_count')
        replicable = \
            connection_properties.get('replicable') or replica_count > 1
        if device_info and device_info.get('path'):
            device_path = device_info['path']
        elif connection_properties.get('device_path'):
            device_path = connection_properties['device_path']
        elif replicable:
            device_path = '/dev/md/' + connection_properties['alias']

        self.try_disconnect_target_controllers(volume_replicas, False)
        if replicable:
            NVMeOFConnector.end_raid(self, device_path)
        else:
            if NVMeOFConnector.get_fs_type(self, device_path) == \
                    'linux_raid_member':
                NVMeOFConnector.end_raid(self, device_path)
        self.try_disconnect_target_controllers(volume_replicas, True)

    def _extend_volume_replicated(self, connection_properties):
        volume_replicas = connection_properties.get('volume_replicas')
        replica_count = connection_properties.get('replica_count')

        if volume_replicas and replica_count > 1:
            device_path = '/dev/md/' + connection_properties['alias']
            NVMeOFConnector.run_mdadm(
                self, ['mdadm', '--grow', '--size', 'max', device_path])
        else:
            target_nqn = None
            vol_uuid = None
            if not volume_replicas:
                target_nqn = connection_properties['target_nqn']
                vol_uuid = connection_properties['vol_uuid']
            elif len(volume_replicas) == 1:
                target_nqn = volume_replicas[0]['target_nqn']
                vol_uuid = volume_replicas[0]['vol_uuid']
            device_path = NVMeOFConnector.get_namespace_path(
                self, target_nqn, vol_uuid, 1, None, False)

        return self._linuxscsi.get_device_size(device_path)

    def _connect_target_volume(self, target_nqn, vol_uuid, portals):
        nvme_ctrls = NVMeOFConnector.rescan(self, target_nqn)
        addresses, any_new_connect = \
            NVMeOFConnector.connect_to_portals(self, target_nqn, portals,
                                               nvme_ctrls)
        if not any_new_connect and len(nvme_ctrls) == 0:
            # no new connections and any pre-exists controllers
            LOG.error("No successful connections to: %s", target_nqn)
            raise exception.VolumeDeviceNotFound(device=target_nqn)
        if any_new_connect:
            # new connections - refresh controllers map
            nvme_ctrls = \
                NVMeOFConnector.get_nvme_controllers_map(self, target_nqn)
        dev_path = NVMeOFConnector.get_namespace_path(self, target_nqn,
                                                      vol_uuid, 1, nvme_ctrls,
                                                      False)
        LOG.debug("[!] dev_path: %s", dev_path)
        if not dev_path:
            LOG.error("Target %s volume %s not found", target_nqn, vol_uuid)
            raise exception.VolumeDeviceNotFound(device=vol_uuid)
        return dev_path

    @staticmethod
    def connect_to_portals(executor, target_nqn, target_portals, nvme_ctrls):
        # connect to any of NVMe-oF target portals -
        # check if the controller exist before trying to connect
        # in multipath connect all given target portals
        any_new_connect = False
        addresses = []
        no_multipath = not executor.use_multipath or not \
            NVMeOFConnector.native_multipath_supported
        LOG.debug("[!] target_portals: %s", len(target_portals))
        for portal in target_portals:
            portal_address = portal[0]
            portal_port = portal[1]
            address = "traddr={0},trsvcid={1}".format(portal_address,
                                                      portal_port)
            addresses.append(address)
            LOG.debug("[!] portal: %s:%s", portal_address, portal_port)
            if NVMeOFConnector.is_portal_connected(portal_address, portal_port,
                                                   nvme_ctrls):
                LOG.debug("[!] portal %s is already connected", address)
                if no_multipath:
                    break
                continue
            LOG.debug("[!] portal %s is NOT connected", address)
            if portal[2] == 'RoCEv2':
                portal_transport = 'rdma'
            else:
                portal_transport = 'tcp'
            nvme_command = (
                'connect', '-a', portal_address, '-s', portal_port, '-t',
                portal_transport, '-n', target_nqn, '-Q', '128', '-l', '-1')
            try:
                NVMeOFConnector.run_nvme_cli(executor, nvme_command)
                any_new_connect = True
                LOG.debug("[!] executor.use_multipath: %s",
                          executor.use_multipath)
                if no_multipath:
                    break
            except Exception:
                LOG.exception("Could not connect to portal %s", portal)
        LOG.debug("[!] any_new_connect: %s", any_new_connect)
        return addresses, any_new_connect

    @staticmethod
    def is_portal_connected(portal_address, portal_port, nvme_ctrls):
        address = f"traddr={portal_address},trsvcid={portal_port}"
        return address in nvme_ctrls and nvme_ctrls[address] is not None and \
            nvme_ctrls[address].name != ''

    @staticmethod
    def get_nvme_controllers(executor, target_nqn):
        nvme_controllers = \
            NVMeOFConnector.get_nvme_controllers_map(executor, target_nqn)
        if len(nvme_controllers) > 0:
            return nvme_controllers.values()
        raise exception.VolumeDeviceNotFound(device=target_nqn)

    @staticmethod
    def get_nvme_controllers_map(executor, target_nqn):
        """returns map of all live controllers and their addresses """
        nvme_controllers = dict()
        ctrls = glob.glob('/sys/class/nvme-fabrics/ctl/nvme*')
        LOG.debug("[!] ctrls: %s|%s", target_nqn, ctrls)
        for ctrl in ctrls:
            LOG.debug("[!] ctrl: %s", ctrl)
            try:
                lines, _err = executor._execute(
                    'cat', ctrl + '/subsysnqn', run_as_root=True,
                    root_helper=executor._root_helper)
                LOG.debug("[!] lines: %s", lines)
                for line in lines.split('\n'):
                    LOG.debug("[!] line: %s", line)
                    if line == target_nqn:
                        state, _err = executor._execute(
                            'cat', ctrl + '/state', run_as_root=True,
                            root_helper=executor._root_helper)
                        LOG.debug("[!] state: %s", state)
                        address_file = ctrl + '/address'
                        try:
                            with open(address_file, 'rt') as f:
                                address = f.read().strip()
                        except Exception:
                            LOG.warning("Failed to read file %s", address_file)
                            continue
                        ctrl_name = os.path.basename(ctrl)
                        ctrl_device_path = "/dev/" + ctrl_name
                        is_alive = 'live' in state
                        fv_ctrl = \
                            NVMeOFConnector.NvmeController(ctrl_name,
                                                           ctrl_device_path,
                                                           is_alive)
                        LOG.debug("[!] address: %s|%s", address, fv_ctrl)
                        nvme_controllers[address] = fv_ctrl
            except putils.ProcessExecutionError as e:
                LOG.exception(e)
        return nvme_controllers

    @staticmethod
    def handle_replicated_volume(executor, volume_uuid, host_device_paths,
                                 volume_alias, num_of_replicas, writable):
        path_in_raid = False
        for dev_path in host_device_paths:
            path_in_raid = NVMeOFConnector._is_device_in_raid(executor,
                                                              dev_path)
            if path_in_raid:
                break
        device_path = '/dev/md/' + volume_alias
        if path_in_raid:
            NVMeOFConnector.stop_and_assemble_raid(executor, volume_uuid,
                                                   host_device_paths,
                                                   device_path, not writable)
        else:
            paths_found = len(host_device_paths)
            if num_of_replicas > paths_found:
                LOG.error(
                    'Cannot create MD as %s out of %s legs were found.',
                    paths_found, num_of_replicas)
                raise exception.VolumeDeviceNotFound(device=volume_alias)
            if num_of_replicas == 1:
                host_device_paths.append('missing')
            NVMeOFConnector.create_raid(executor, volume_uuid,
                                        host_device_paths,
                                        '1', volume_alias, volume_alias,
                                        not writable)
        return device_path

    def _handle_single_replica(self, volume_uuid, host_device_paths,
                               volume_alias, writable):
        if NVMeOFConnector.get_fs_type(self, host_device_paths[0]) == \
                'linux_raid_member':
            md_path = '/dev/md/' + volume_alias
            NVMeOFConnector.stop_and_assemble_raid(
                self, volume_uuid, host_device_paths, md_path, not writable)
            return md_path
        return host_device_paths[0]

    @staticmethod
    def run_mdadm(executor, cmd, raise_exception=False):
        cmd_output = None
        try:
            lines, err = executor._execute(
                *cmd, run_as_root=True, root_helper=executor._root_helper)
            for line in lines.split('\n'):
                cmd_output = line
                break
        except putils.ProcessExecutionError as ex:
            LOG.warning("[!] Could not run mdadm: %s", str(ex))
            if raise_exception:
                raise ex
        return cmd_output

    @staticmethod
    def _is_device_in_raid(self, device_path):
        cmd = ['mdadm', '--examine', device_path]
        raid_expected = device_path + ':'
        try:
            lines, err = self._execute(
                *cmd, run_as_root=True, root_helper=self._root_helper)
            for line in lines.split('\n'):
                if line == raid_expected:
                    return True
                else:
                    return False
        except putils.ProcessExecutionError:
            return False

    @staticmethod
    def ks_readlink(dest):
        try:
            return os.readlink(dest)
        except Exception:
            return ''

    @staticmethod
    def get_md_name(executor, device_name):
        get_md_cmd = (
            'cat /proc/mdstat | grep -w ' + device_name +
            ' | awk \'{print $1;}\'')
        cmd = ['bash', '-c', get_md_cmd]
        LOG.debug("[!] cmd = " + str(cmd))
        cmd_output = None

        try:
            lines, err = executor._execute(
                *cmd, run_as_root=True, root_helper=executor._root_helper)

            for line in lines.split('\n'):
                cmd_output = line
                break

            LOG.debug("[!] cmd_output = " + cmd_output)
            if err:
                return None

            return cmd_output
        except putils.ProcessExecutionError as ex:
            LOG.warning("[!] Could not run cmd: %s", str(ex))
        return None

    @staticmethod
    def stop_and_assemble_raid(executor, volume_uuid, drives, md_path,
                               read_only):
        md_name = None
        i = 0
        assembled = False
        link = ''
        while i < 5 and not assembled:
            for drive in drives:
                device_name = drive[5:]
                if md_name is None or md_name == '':
                    md_name = NVMeOFConnector.get_md_name(executor, device_name)
                link = NVMeOFConnector.ks_readlink(md_path)
                if link != '':
                    link = os.path.basename(link)
                if md_name and md_name == link and link != '':
                    return
                LOG.debug(
                    "sleeping 1 sec -allow auto assemble link = " +
                    link + " md path = " + md_path + " md name = " + md_name)
                time.sleep(1)

            if md_name and md_name != link:
                NVMeOFConnector.stop_raid(executor, md_name)

            try:
                LOG.debug(
                    "Assembling md: " + md_path + " from drives " + str(drives))
                assembled = NVMeOFConnector.assemble_raid(
                    executor, volume_uuid, drives, md_path, read_only)
            except Exception:
                i += 1

    @staticmethod
    def assemble_raid(executor, volume_uuid, drives, md_path, read_only):
        cmd = ['mdadm', '--assemble', '--run', md_path]

        if read_only:
            cmd.append('-o')
        elif volume_uuid:
            cmd.append('--update=uuid')
            cmd.append('--uuid='+volume_uuid)
        for i in range(len(drives)):
            cmd.append(drives[i])

        try:
            NVMeOFConnector.run_mdadm(executor, cmd, True)
        except putils.ProcessExecutionError as ex:
            LOG.warning("[!] Could not _assemble_raid: %s", str(ex))
            raise ex

        return True

    @staticmethod
    def create_raid(executor, volume_uuid, drives, raid_type, device_name, name,
                    read_only):
        cmd = ['mdadm']
        num_drives = len(drives)
        cmd.append('-C')

        if read_only:
            cmd.append('-o')

        cmd.append(device_name)
        cmd.append('-R')

        if name:
            cmd.append('-N')
            cmd.append(name)
        if volume_uuid:
            cmd.append('--uuid='+volume_uuid)
        cmd.append('--level')
        cmd.append(raid_type)
        cmd.append('--raid-devices=' + str(num_drives))
        cmd.append('--bitmap=internal')
        cmd.append('--homehost=any')
        cmd.append('--failfast')
        cmd.append('--assume-clean')

        for i in range(len(drives)):
            cmd.append(drives[i])

        LOG.debug('[!] cmd = ' + str(cmd))
        NVMeOFConnector.run_mdadm(executor, cmd)
        # sometimes under load, md is not created right away, so we wait
        for i in range(60):
            try:
                is_exist = os.path.exists("/dev/md/" + name)
                LOG.debug("[!] md is_exist = %s", is_exist)
                if is_exist:
                    return
                time.sleep(1)
            except Exception:
                LOG.debug('[!] Exception_wait_raid!')
        msg = _("md: /dev/md/%s not found.") % name
        LOG.error(msg)
        raise exception.NotFound(message=msg)

    @staticmethod
    def end_raid(executor, device_path):
        md_linq = NVMeOFConnector.ks_readlink(device_path)
        raid_exists = NVMeOFConnector.is_raid_exists(executor, device_path)
        if raid_exists:
            for i in range(10):
                try:
                    cmd_out = NVMeOFConnector.stop_raid(
                        executor, device_path, True)
                    if not cmd_out:
                        break
                except Exception:
                    time.sleep(1)

            try:
                is_exist = os.path.exists(device_path)
                LOG.debug("[!] is_exist = %s", is_exist)
                # sometimes after stop md disappears and sometimes not
                if is_exist:
                    NVMeOFConnector.remove_raid(executor, device_path)
                    if md_linq != '':
                        os.remove(md_linq)
                    os.remove(device_path)
            except Exception:
                LOG.debug('[!] Exception_stop_raid!')
        if md_linq != '' and nvmeof_agent is not None:
            nvmeof_agent.NVMeOFAgent.remove_md(device_path)

    @staticmethod
    def stop_raid(executor, md_path, raise_exception=False):
        cmd = ['mdadm', '--stop', md_path]
        LOG.debug("[!] cmd = " + str(cmd))
        cmd_out = NVMeOFConnector.run_mdadm(executor, cmd, raise_exception)
        return cmd_out

    @staticmethod
    def is_raid_exists(executor, device_path):
        cmd = ['mdadm', '--detail', device_path]
        LOG.debug("[!] cmd = " + str(cmd))
        raid_expected = device_path + ':'
        try:
            lines, err = executor._execute(
                *cmd, run_as_root=True, root_helper=executor._root_helper)

            for line in lines.split('\n'):
                LOG.debug("[!] line = " + line)
                if line == raid_expected:
                    return True
                else:
                    return False
        except putils.ProcessExecutionError:
            return False

    @staticmethod
    def remove_raid(executor, device_path):
        cmd = ['mdadm', '--remove', device_path]
        LOG.debug("[!] cmd = " + str(cmd))
        NVMeOFConnector.run_mdadm(executor, cmd)

    @staticmethod
    def run_nvme_cli(executor, nvme_command, **kwargs):
        (out, err) = executor._execute('nvme', *nvme_command, run_as_root=True,
                                       root_helper=executor._root_helper,
                                       check_exit_code=True)
        msg = ("nvme %(nvme_command)s: stdout=%(out)s stderr=%(err)s" %
               {'nvme_command': nvme_command, 'out': out, 'err': err})
        LOG.debug("[!] " + msg)

        return out, err

    @staticmethod
    def rescan(executor, target_nqn):
        nvme_ctrls = NVMeOFConnector.get_nvme_controllers_map(executor,
                                                              target_nqn)
        for nvme_ctrl in nvme_ctrls.values():
            is_alive = nvme_ctrl.is_live
            if is_alive is False:
                continue
            ctrl_path = nvme_ctrl.path
            nvme_command = ('ns-rescan', ctrl_path)
            try:
                NVMeOFConnector.run_nvme_cli(executor, nvme_command)
            except Exception as e:
                LOG.exception(e)
        return nvme_ctrls

    @staticmethod
    def get_fs_type(executor, device_path):
        cmd = ['blkid', device_path, '-s', 'TYPE', '-o', 'value']
        LOG.debug("[!] cmd = " + str(cmd))
        fs_type = None

        try:
            lines, err = executor._execute(
                *cmd, run_as_root=True, root_helper=executor._root_helper)

            fs_type = lines.split('\n')[0]
        except putils.ProcessExecutionError:
            return None

        return fs_type

    def try_disconnect_target_controllers(self, vol_replicas, live):
        NVMeOFConnector.try_disconnect_target_controllers_internal(self,
                                                                   vol_replicas,
                                                                   None, None,
                                                                   live)

    @staticmethod
    def try_disconnect_target_controllers_internal(executor, volume_replicas,
                                                   volume, target_nqn, live):
        LOG.debug("[!] try_disconnect_target_controllers")
        replica_uuids = []
        if volume is not None:
            for replica in volume.location:
                replica_uuids.append(replica.uuid)
        else:
            target_nqn = volume_replicas[0]['target_nqn']
            for replica in volume_replicas:
                replica_uuids.append(replica['vol_uuid'])

        nvme_ctrls = NVMeOFConnector.get_nvme_controllers_map(executor,
                                                              target_nqn)
        if len(nvme_ctrls) == 0:
            LOG.warning("[!] Could not find nvme_ctrls for target_nqn: %s",
                        target_nqn)
        else:
            num_disconnected = 0
            for address in nvme_ctrls:
                ctrl_device = nvme_ctrls[address]
                LOG.debug("[!] ctrl_device %s", ctrl_device.name)
                if ctrl_device.is_live != live:
                    continue
                num_namespaces = \
                    NVMeOFConnector.get_num_namespaces(ctrl_device.name)
                LOG.debug("[!] num_namespaces %s", str(num_namespaces))
                if num_namespaces > 1:
                    continue
                should_disconnect = False
                if num_namespaces == 1:
                    temp_map = dict()
                    temp_map[address] = ctrl_device
                    for replica_uuid in replica_uuids:
                        device = \
                            NVMeOFConnector.get_namespace_path(executor,
                                                               target_nqn,
                                                               replica_uuid,
                                                               1, temp_map,
                                                               False)
                        if device is not None:
                            should_disconnect = True
                            break
                else:
                    should_disconnect = True
                if should_disconnect:
                    LOG.debug("[!] Disconnect ctrl_device %s", ctrl_device.name)
                    NVMeOFConnector.disconnect(executor, ctrl_device.name)
                    num_disconnected = num_disconnected + 1

    @staticmethod
    def disconnect(executor, ctrl_device):
        LOG.debug("[!] ctrl_device = %s", ctrl_device)
        nvme_command = ('disconnect', '-d', ctrl_device)
        try:
            NVMeOFConnector.run_nvme_cli(executor, nvme_command)
        except Exception:
            logging.exception("Could not disconnect ctrl_device %s",
                              ctrl_device)

    @staticmethod
    def get_namespace_path(executor, target_nqn, uuid, num_of_attempts,
                           ctrl_device_map, live_only):
        LOG.debug("[!] target_nqn: %s, uuid: %s", target_nqn, uuid)
        LOG.debug("[!] ctrl_device_map: %s", str(ctrl_device_map))
        if ctrl_device_map is None or len(ctrl_device_map) < 1:
            ctrl_device_map = \
                NVMeOFConnector.get_nvme_controllers_map(executor, target_nqn)
            if ctrl_device_map is None or len(ctrl_device_map) < 1:
                return None
        nvme_controllers = ctrl_device_map.values()
        for i in range(num_of_attempts):
            for nvme_ctrl in nvme_controllers:
                if live_only and nvme_ctrl.is_live is False:
                    continue
                ctrl_name = nvme_ctrl.name
                block_dev_path = '/sys/class/nvme-fabrics/ctl/' + ctrl_name \
                                 + '/nvme*'
                LOG.debug("[!] block_dev_path: %s", block_dev_path)
                block_device_path = \
                    NVMeOFConnector.find_block_device_by_uuid(executor, uuid,
                                                              block_dev_path)
                if block_device_path is not None:
                    return block_device_path
            if i < num_of_attempts:
                NVMeOFConnector.rescan(executor, target_nqn)
                LOG.debug("[!] Wait one second to discover: %s", uuid)
                time.sleep(1)
        LOG.error("Could not find namespace path: %s", uuid)
        return None

    @staticmethod
    def find_block_device_by_uuid(executor, uuid, block_dev_path):
        LOG.debug("[!] >>>> uuid = %s", uuid)
        for block_device in glob.glob(block_dev_path):
            LOG.debug("[!] block_device = %s", block_device)
            uuid_file = block_device + '/uuid'
            candidate_block_device_uuid = \
                NVMeOFConnector.read_line_from_file(executor, uuid_file)
            LOG.debug("[!] candidate_block_device_uuid = %s",
                      candidate_block_device_uuid)
            if candidate_block_device_uuid == uuid:
                block_index = block_device.rfind('/')
                block = block_device[block_index + 1:]
                LOG.debug("[!] block = %s", block)
                if 'c' in block_device:
                    # regex_pattern = re.compile('c[\d]*')
                    block = re.sub(r"c[\d]*", '', block)
                block_device_path = '/dev/' + block
                return block_device_path
        return None

    @staticmethod
    def read_line_from_file(executor, file_path):
        lines, _err = executor._execute('cat', file_path, run_as_root=True,
                                        root_helper=executor._root_helper)
        line = None
        for line in lines.split('\n'):
            break
        return line

    @staticmethod
    def get_num_namespaces(nvme_ctrl):
        nvme_device_path = \
            '/sys/class/nvme-fabrics/ctl/' + nvme_ctrl + '/nvme*'
        nss = glob.glob(nvme_device_path)
        return len(nss)

    class NvmeController:
        def __init__(self, name, path, is_live):
            self.name = name
            self.path = path
            self.is_live = is_live
