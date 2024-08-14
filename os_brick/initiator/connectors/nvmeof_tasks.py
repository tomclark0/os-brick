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
import glob
import os
import sched
import threading
import time
from locale import atoi
from os_brick.initiator.connectors import entities as provisioner_entities
from os_brick.initiator.connectors import nvmeof
from oslo_concurrency import lockutils
from oslo_log import log


tasks_scheduler = sched.scheduler(time.time, time.sleep)
LOG = log.getLogger(__name__)
FAILED = 'FAILED'
FINISHED = 'FINISHED'
IDLE = 'IDLE'
SUCCESS = 'Success'
INVALID_INPUT = 'InvalidInput'

synchronized = lockutils.synchronized_with_prefix('os-brick-')
tasks_running = False
interval = 1


class NVMeOFTasks:

    def __init__(self, nvmeof_agent):
        self.mds = nvmeof_agent.mds
        self.tasks = {}
        self.executor = nvmeof_agent.executor
        self.host_uuid = nvmeof_agent.host_uuid
        self.prov_rest = nvmeof_agent.prov_rest
        self.nvmeof_agent = nvmeof_agent

    def init(self):
        global tasks_running
        global interval
        tasks_scheduler.enter(1, 1, self.handle_tasks, (interval,))
        t = threading.Thread(target=tasks_scheduler.run)
        t.start()
        tasks_running = True

    @staticmethod
    def ensure_tasks_running(agent):
        global tasks_running
        LOG.debug("[!] ensure_tasks_running START --------")
        # check if tasks job running - sched implementation:
        if tasks_running:
            LOG.debug("[!] there is already scheduler tasks job running  -----")
            return
        nvmeof_tasks = NVMeOFTasks(agent)
        LOG.debug("[!] nvmeof_tasks.init() --------")
        nvmeof_tasks.init()

    def handle_new_task(self, task):
        LOG.debug("handle_new_task task: %s", task.type)
        if task.type == 'RemoteMigration':
            self.handle_migration(task)
        elif task.type == 'ConnectVolume':
            self.handle_connect(task)
        elif task.type == 'DisconnectVolume':
            self.handle_disconnect(task)
        elif task.type == 'SetPollingRate':
            self.handle_set_polling_rate(task)
        else:
            LOG.error("Cannot handle task: %s, Unknown task type: %s",
                      task.type)

    def handle_migration(self, task):
        md_details = None
        vol_id, replica_id, volume_alias = self.get_task_volume_ids(task)
        LOG.info(">>>>> Started handling migration for volume: %s", vol_id)
        if vol_id == "" or replica_id == "" or volume_alias == "":
            LOG.error("Failing task: %s Invalid configuration: vol_id %s, "
                      "replica_id %s, volume_alias %s", task.taskId, vol_id,
                      replica_id, volume_alias)
            task.taskStatus = INVALID_INPUT
            task.statusDescription = "Invalid configuration"
        else:
            md_path = "/dev/md/" + volume_alias
            md_name = volume_alias
            LOG.debug("md_name: %s", md_name)
            LOG.debug("md_path: %s", md_path)
            LOG.debug("tasks in memory: %s", str(self.tasks))
            exist_task = None
            if task.taskId in self.tasks:
                exist_task = self.tasks[task.taskId]
                LOG.debug("exist_task: %s", exist_task)
            if exist_task is not None:
                task.taskStatus = 'TaskAlreadyRunning'
                msg = "Volume migration is already running for volume: " + \
                      volume_alias
                task.statusDescription = msg
            else:
                if md_name in self.mds:
                    md_details = self.mds[md_name]
                    LOG.debug("md_details: %s", md_details)
                if md_details is None:
                    task.taskStatus = 'VolumeNotFound'
                    msg = "Could not find volume: " + volume_alias
                    task.statusDescription = msg
                else:
                    md_name = volume_alias
                    active_legs, failed_legs, all_legs, nvme_live_legs, err = \
                        self.nvmeof_agent.handle_leg_failure(md_name, True)
                    if len(active_legs) <= 0 or len(nvme_live_legs) <= 0:
                        LOG.error("Cannot migrate volume as not enough replicas"
                                  " are available. active_legs: %s, "
                                  "nvme_live_legs: %s", active_legs,
                                  nvme_live_legs)
                        task.taskStatus = 'NotEnoughReplicas'
                        msg = "Not enough replicas are available and connected"
                        task.statusDescription = msg

        LOG.debug("taskStatus: %s", task.taskStatus)
        if task.taskStatus != "" and task.taskStatus != "N/A":
            task.state = FAILED
            LOG.debug("update_task: %s", task)
            self.update_task(task)
            return

        replica = provisioner_entities.Replica(False, [], [], [])
        res = self.prov_rest.add_replica(replica, vol_id)
        if res.status != SUCCESS:
            LOG.error("add_replica %s failed with %s", vol_id, res.status)
            task.taskStatus = res.status
            task.statusDescription = res.description
            task.state = FAILED
            LOG.debug("update_task: %s", task)
            self.update_task(task)
            return
        volume = self.nvmeof_agent.get_volume_by_uuid(vol_id)
        if volume is None:
            LOG.error("Could not find volume after adding replica %s", volume)
            task.taskStatus = 'VolumeNotFound'
            msg = "Could not find volume after adding replica"
            task.statusDescription = msg
            task.state = FAILED
            LOG.debug("update_task: %s", task)
            self.update_task(task)
            return
        failed_legs = []
        md_name = volume_alias
        added_leg, added = \
            self.nvmeof_agent.handle_leg_added(volume, md_name, md_details,
                                               failed_legs,
                                               self.nvmeof_agent.host_uuid,
                                               self.nvmeof_agent.hostname)
        LOG.debug("added_leg: %s", added_leg)
        if added is False:  # could not connect and add replica
            LOG.error("Could not add leg for %s", vol_id)
            # try to remove added replica
            deleted = self.nvmeof_agent.delete_replica(volume, replica_id)
            if deleted:
                res = self.nvmeof_agent.delete_replica_confirm(volume,
                                                               replica_id)
                if res is None or res.status != SUCCESS:
                    LOG.error("Could not confirm delete replica for vol_id %s, "
                              "replica_id %s", vol_id, replica_id)
                else:
                    targets = self.nvmeof_agent.get_targets(None, volume.uuid)
                    if targets is None or len(targets) != 1:
                        LOG.warning("[!] Could not find all targets for "
                                    "volume: %s", volume.uuid)

                    target = targets[0]
                    target_nqn = target.name
                    nvmeof.NVMeOFConnector.try_disconnect_target_controllers_internal(
                        self.executor, [], volume, target_nqn, False)
            else:
                LOG.error("Could not delete replica for vol_id %s, "
                          "replica_id %s", vol_id, replica_id)
            task.taskStatus = 'CommunicationError'
            msg = "Could not connect to replica after adding it"
            task.statusDescription = msg
            task.state = FAILED
            LOG.debug("update_task: %s", task)
            self.update_task(task)
            return

        task.state = 'RUNNING'
        task.taskStatus = SUCCESS
        task.progress = 0
        add_leg_tag = provisioner_entities.TagEntity("addedReplicaId",
                                                     replica_id)
        LOG.debug("task: %s", task)
        LOG.debug("taskConfiguration: %s", task.taskConfiguration)
        if task.taskConfiguration == "N/A":
            task.taskConfiguration = [add_leg_tag]
        else:
            task.taskConfiguration.append(add_leg_tag)
        LOG.debug("update_task: %s", task)
        self.update_task(task)
        self.tasks[task.taskId] = task
        LOG.info(">>>>> Finished handling migration for volume: %s", vol_id)

    @synchronized('connect_volume')
    def handle_disconnect(self, task):
        vol_id, replica_id, volume_alias = self.get_task_volume_ids(task)
        LOG.info(">>>>> Started handling disconnect for volume: %s", vol_id)
        LOG.debug("task: %s", task)

        if vol_id == '':
            LOG.warning("Failing task: %s, Invalid configuration: empty vol_id",
                        task.taskId)
            task.taskStatus = INVALID_INPUT
            task.statusDescription = "Invalid configuration"
            task.state = FAILED
        else:
            md, err = self.get_md(vol_id)
            if err is not None:
                task.taskStatus = 'VolumeNotFound'
                task.statusDescription = 'VolumeNotFound'
                task.state = FAILED
            elif md != '':
                nvmeof.NVMeOFConnector.end_raid(self, md)
            if err is None:
                task.taskStatus = SUCCESS
                task.statusDescription = SUCCESS
                task.state = FINISHED
        LOG.debug("update_task: %s", task)
        self.update_task(task)
        LOG.info(">>>>> Finished handling disconnect for volume: %s", vol_id)

    def handle_connect(self, task):
        vol_id, replica_id, volume_alias = self.get_task_volume_ids(task)
        LOG.info(">>>>> Started handling connect for volume: %s", vol_id)

        if vol_id == '':
            LOG.warning("Failing task: %s, Invalid configuration: empty vol_id",
                        task.taskId)
            task.taskStatus = INVALID_INPUT
            task.statusDescription = "Invalid configuration"
            task.state = FAILED
        else:
            md, err = self.get_md(vol_id)
            source = None
            connected_locations = []
            if err is None:
                source, _, _, _, connected_locations, err = \
                    self.stage_block_volume(self.host_uuid, vol_id, md)
            if err is not None:
                task.taskStatus = 'ConnectionFailed'
                task.statusDescription = 'ConnectionFailed'
                task.state = FAILED
            else:
                task.state = FINISHED
                task.taskStatus = SUCCESS
                task.statusDescription = SUCCESS
                path_tag = provisioner_entities.TagEntity("path", source)
                task.taskConfiguration.append(path_tag)
                for i, connected_location in enumerate(connected_locations):
                    tag_name = "connectedLocation" + str(i+1)
                    tag_val = connected_locations[i]
                    path_tag = provisioner_entities.TagEntity(tag_name, tag_val)
                    task.taskConfiguration.append(path_tag)
        LOG.debug("update_task: %s", task)
        self.update_task(task)
        LOG.info(">>>>> Finished handling connect for volume: %s", vol_id)

    def stage_block_volume(self, host_uuid, volume_id, existing_md):
        LOG.info(">>>>> Started stage_block_volume for volume: %s", volume_id)
        connected_locations = []
        volume = self.nvmeof_agent.get_volume_by_uuid(volume_id)
        if volume is None:
            msg = "Stage details not supplied and the volume was not found: " \
                  + volume_id
            LOG.error(msg)
            return '', '', None, False, connected_locations, 'VolumeNotFound'
        targets = []
        if volume.protocol == 'NVMeoF':
            targets = self.nvmeof_agent.get_targets(host_uuid, volume_id)
            if targets is None or len(targets) < 1:
                msg = "Could not find all targets for volume: " + volume_id
                LOG.error(msg)
                return '', '', None, False, connected_locations, \
                       'TargetNotFound'
        is_ro_snapshot_volume = \
            volume.snapshotID != "" and volume.writable is False
        if is_ro_snapshot_volume:
            snapshots = self.prov_rest.get_snapshots(volume.snapshotID)
            if snapshots is None or len(snapshots) < 1:
                msg = "Could not find snapshot for snapshotID: " + \
                      volume.snapshotID
                LOG.error(msg)
                return '', '', None, False, connected_locations, \
                       'SnapshotNotFound'
            source_volume = \
                self.nvmeof_agent.get_volume_by_uuid(snapshots[0].volumeID)
            if source_volume is None:
                msg = "Could not find source volume: " + snapshots[0].volumeID
                LOG.error(msg)
                return '', '', None, False, connected_locations, \
                       'SourceVolumeNotFound'
            is_source_replicated = source_volume.replicable
            source_vol_dev_path = "/dev/md/" + source_volume.alias
            if is_source_replicated and os.path.exists(source_vol_dev_path):
                msg = "Cannot stage replicated volume and replicated volume " \
                      "read-only snapshot at the same node."
                LOG.error(msg)
                return '', '', None, False, connected_locations, \
                       'FailedPrecondition'
        is_replicated_volume = volume.replicable
        locations = volume.location
        paths = []
        found_paths = 0
        LOG.info("Connecting to volume: " + volume.alias)
        for location in locations:
            LOG.debug("location: " + str(location))
            persistent_id = location.backend.persistentID
            result = self.prov_rest.get_backend_by_id(persistent_id)
            backends = []
            if result.status == "Success":
                backends = result.prov_entities
            if backends is None or len(backends) != 1:
                msg = "Could not find backend: " + persistent_id
                LOG.error(msg)
            else:
                location.backend = backends[0]
            LOG.debug("volume.protocol: " + volume.protocol)
            if volume.protocol == 'Local':
                sys_block_path = "/sys/class/block/nvmfbd*"
                path = \
                    nvmeof.NVMeOFConnector.find_block_device_by_uuid(self.executor,
                                                                     location.uuid,
                                                                     sys_block_path)
            else:
                target = targets[0]
                LOG.debug("location.backend: %s| uuid: %s", location.backend,
                          location.uuid)
                path = self.nvmeof_agent.connect_to_volume_at_location(volume,
                                                                       location,
                                                                       target)
                if path != "":
                    connected_locations.append(persistent_id)
            if path is None:
                LOG.warning("[!] Could not connect to volume %s at location %s",
                            location.uuid, location.backend.mgmtIPs[0])
                self.prov_rest.set_replica_state(volume.uuid, location.uuid,
                                                 "Missing")
            else:
                found_paths = found_paths + 1
                paths.append(path)
                self.prov_rest.set_replica_state(volume.uuid, location.uuid,
                                                 "Available")
        LOG.debug("found_paths: " + str(found_paths))
        if found_paths < 1:
            return '', '', None, False, connected_locations, 'Unavailable'
        source = paths[0]
        current_fs_type = nvmeof.NVMeOFConnector.get_fs_type(self.executor,
                                                             source)
        if current_fs_type == "linux_raid_member":
            is_replicated_volume = True
        if is_replicated_volume:
            source = "/dev/md/" + volume.alias

        LOG.debug("existing_md: " + existing_md)
        if existing_md != "":
            return source, current_fs_type, volume, False, \
                   connected_locations, None

        raid_created = False
        if is_replicated_volume:
            source = \
                nvmeof.NVMeOFConnector.handle_replicated_volume(self.executor,
                                                                volume.uuid,
                                                                paths,
                                                                volume.alias,
                                                                volume.numReplicas,
                                                                volume.writable)
            LOG.debug("source: " + source)
        LOG.info(">>>>> Finished stage_block_volume for volume: %s", volume_id)
        return source, current_fs_type, volume, raid_created, connected_locations, None

    def get_md(self, vol_id):
        volume = self.nvmeof_agent.get_volume_by_uuid(vol_id)
        if volume is None:
            msg = "Could not find volume: " + vol_id
            LOG.error(msg)
            return '', 'VolumeNotFound'
        if volume.Replicable is False:
            # simple volume
            return '', None
        nvme_devices_path = "/dev/md/*"
        mds = glob.glob(nvme_devices_path)

        for md in mds:
            LOG.debug("md: %s", md)
            try:
                origin_file = os.readlink(md)
                LOG.debug("origin_file: %s", origin_file)
            except OSError:
                LOG.error("Could not read md link for md: %s", md)
                continue
            device_index = md.rfind('/')
            md_name = md[device_index + 1:]
            LOG.debug("md_name: %s", md_name)
            all_legs, failed_legs, active_legs = \
                self.nvmeof_agent.get_nvme_devices_by_state(md_name)
            uuids = \
                self.nvmeof_agent.get_devices_uuids_from_sysfs(self.executor,
                                                               all_legs)
            LOG.info("Found md uuids: %s", str(uuids))
            LOG.debug("volume.location: %s", str(volume.location))
            for uuid in uuids:
                for loc in volume.location:
                    LOG.debug("loc: %s", str(loc))
                    if loc.uuid == uuid:
                        LOG.info("Found md: %s for vol_id: %s", md, vol_id)
                        return md, None
        return '', None

    def handle_running_task(self, task):
        LOG.info("***** handle_running_task task: %s", str(task))
        LOG.debug("mds: %s", str(self.mds))
        orig_task_configuration = task.taskConfiguration
        vol_id, replica_id, volume_alias = self.get_task_volume_ids(task)
        md_path = '/dev/md/' + volume_alias
        md_name = volume_alias
        md_details = None
        if md_name in self.mds:
            md_details = self.mds[md_name]
        finished = False
        if md_details is None:
            LOG.warning("Could not find md device for volume: %s, try later",
                        volume_alias)
            return

        try:
            origin_file = os.readlink(md_path)
        except OSError:
            LOG.error("Could not read md link for md: %s", md_path)
            return

        added_leg, added_leg_uuid = self.get_task_added_leg(task)
        if added_leg_uuid is None:
            LOG.warning("Task failed - could not find added UUID in task "
                        "configuration")
            task.taskStatus = 'CommunicationError'
            task.statusDescription = "Missing task configuration"
            task.state = FAILED
            LOG.debug("update_task: %s", task)
            finished = self.update_task(task)
            if finished and task.taskId in self.tasks:
                del self.tasks[task.taskId]
            return

        if added_leg is None:
            return

        LOG.debug("origin_file: %s", origin_file)
        progress = self.get_sync_progress(origin_file)
        LOG.info("Task: %s, Progress: %s", task.taskId, progress)
        all_legs = None
        if progress == -1:
            all_legs, failed_legs, active_legs = \
                self.nvmeof_agent.get_nvme_devices_by_state(md_name)
            LOG.debug("active_legs: %s", str(active_legs))
            LOG.debug("task: %s", str(task))
            if added_leg in active_legs:
                LOG.info("Task completed after leg is in active legs: %s", task)
                progress = 100
            else:
                LOG.debug("Task failed - active legs: %s", str(active_legs))
                task.taskStatus = 'CommunicationError'
                task.statusDescription = "Volume migration was interrupted."
                task.state = FAILED
                task.taskConfiguration = orig_task_configuration
                LOG.debug("update_task: %s", task)
                finished = self.update_task(task)
                if finished and task.taskId in self.tasks:
                    del self.tasks[task.taskId]
                return
        if progress == 100:
            result = self.prov_rest.set_replica_state(vol_id, added_leg_uuid,
                                                      "Available")
            if result.status != SUCCESS:
                LOG.error('set_replica_state failed due to error: %s',
                          result.status)
            else:
                LOG.info("Set replica state available for volume %s "
                         "Replica: %s finished with status: %s", vol_id,
                         added_leg_uuid, result.description)

            res = self.prov_rest.delete_replica(vol_id, replica_id)
            if res.status != SUCCESS:
                task.state = FAILED
                if res is not None:
                    task.taskStatus = res.status
                    task.statusDescription = res.Description
                else:
                    task.taskStatus = 'CommunicationError'
                    task.statusDescription = "Could not delete replica after " \
                                             "migration"
                LOG.error("Could not delete replica after migration: %s",
                          res.status)
            else:
                LOG.debug("delete_replica Success for volume %s ", vol_id)
                volume = self.nvmeof_agent.get_volume_by_uuid(vol_id)
                if volume is None:
                    LOG.error("Could not find volume after adding replica")
                    task.taskStatus = 'VolumeNotFound'
                    task.statusDescription = \
                        "Could not find volume after adding replica"
                    task.state = FAILED
                else:
                    num_terminating, terminating = \
                        self.nvmeof_agent.get_terminating_leg(volume)
                    LOG.debug("num_terminating: %s, terminating: %s",
                              num_terminating, terminating)
                    if num_terminating == 1:
                        # for now, we can handle one failre
                        LOG.debug("added_leg: %s", added_leg)
                        md_details = \
                            self.nvmeof_agent.get_raid_details(added_leg, True)
                        LOG.debug("md_details: %s", md_details)
                        md_details['devices'] = all_legs
                        vol_changed = \
                            self.nvmeof_agent.handle_terminating_leg(md_name,
                                                                     md_details,
                                                                     terminating.uuid,
                                                                     volume)
                        if vol_changed is False:
                            LOG.error(
                                "Could not find terminating leg after deleting "
                                "replica: %s", replica_id)
                            task.taskStatus = 'CommunicationError'
                            task.statusDescription = 'CommunicationError'
                            task.state = FAILED
                        else:
                            LOG.debug("handle_terminating_leg Success "
                                      "for leg %s ", terminating.uuid)
                            task.state = FINISHED
                            task.taskStatus = SUCCESS
                            task.statusDescription = SUCCESS
                finished = True
        task.progress = progress
        LOG.debug("update_task: %s", task)
        updated = self.update_task(task)
        if updated and finished and task.taskId in self.tasks:
            del self.tasks[task.taskId]

    # GetSyncProgress - Get array sync progress
    def get_sync_progress(self, name):
        device_index = name.rfind('/')
        md_name = name[device_index + 1:]
        sync_progress_path = '/sys/block/' + md_name + '/md/sync_completed'
        LOG.debug("sync_progress_path: %s", sync_progress_path)
        progress = \
            nvmeof.NVMeOFConnector.read_line_from_file(self.executor,
                                                       sync_progress_path)
        LOG.debug("progress: %s", progress)
        if progress == 'none':
            return -1
        progress_vals_strs = progress.split('/')
        LOG.debug("progress_vals_strs: %s", str(progress_vals_strs))
        if len(progress_vals_strs) != 2:
            LOG.error("Received bad progress: %s", progress)
            return -1
        val1 = atoi(progress_vals_strs[0].strip())
        val2 = atoi(progress_vals_strs[1].strip())

        return val1 * 100 / val2

    @staticmethod
    def get_task_volume_ids(task):
        vol_id = None
        replica_id = None
        volume_alias = None
        task_configuration = task.taskConfiguration
        for tag in task_configuration:
            tag_name, tag_val = NVMeOFTasks.get_tag_key_and_val(tag)
            if tag_name == "volId":
                vol_id = tag_val
            if tag_name == "replicaId":
                replica_id = tag_val
            if tag_name == "volumeAlias":
                volume_alias = tag_val
        return vol_id, replica_id, volume_alias

    @staticmethod
    def get_tag_key_and_val(tag):
        tag_name = None
        tag_val = None
        LOG.debug("tag: %s", tag)
        try:
            tag_name = tag.name
        except Exception as e:
            print(e)
        try:
            tag_val = tag.value
        except Exception as e:
            print(e)
        LOG.debug("tag_name: %s, tag_val: %s", tag_name, tag_val)
        return tag_name, tag_val

    @synchronized('agent')
    def handle_tasks(self, first_interval):
        # TODO
        # LOG.debug("+++++ handle_tasks +++++")
        tasks = self.get_tasks(None, self.host_uuid)
        if tasks is not None and len(tasks) > 0:
            for task in tasks:
                if task.state == IDLE:
                    self.handle_new_task(task)
                elif task.state == 'RUNNING' and task.taskId not in self.tasks:
                    self.tasks[task.taskId] = task
        for task_id in list(self.tasks):
            task = self.tasks[task_id]
            self.handle_running_task(task)
        tasks_scheduler.enter(interval, 1, self.handle_tasks, (interval,))

    @staticmethod
    def get_task_polling_rate(task):
        for tag in task.taskConfiguration:
            tag_name, tag_val = NVMeOFTasks.get_tag_key_and_val(tag)
            if tag_name == 'pollingRate':
                polling_rate = atoi(tag_val)
                return polling_rate
        return -1

    def update_task(self, task):
        result = self.prov_rest.update_task(task.taskId, self.host_uuid,
                                            task.state, task.progress,
                                            task.taskStatus,
                                            task.statusDescription,
                                            task.taskConfiguration)
        if result.status != SUCCESS:
            LOG.error('Update task failed due to error: %s', result.status)
            return False
        return True

    def handle_set_polling_rate(self, task):
        global interval
        polling_rate = self.get_task_polling_rate(task)
        LOG.info("Started handling set polling rate to %s millis",
                 str(polling_rate))
        LOG.debug("task: %s", task)
        if polling_rate == -1:
            task.taskStatus = INVALID_INPUT
            task.statusDescription = 'Could not get polling rate'
            task.state = FAILED
            self.update_task(task)
            return

        if polling_rate == 0:  # back to default
            interval = 1
        else:
            interval = polling_rate/1000

        task.taskStatus = SUCCESS
        task.statusDescription = SUCCESS
        task.state = FINISHED
        LOG.debug("update_task: %s", task)
        self.update_task(task)
        LOG.info("Finished handling set polling rate to %s millis",
                 str(polling_rate))

    def get_tasks(self, task_id, host_uuid):
        tasks = []
        try:
            result = self.prov_rest.get_tasks(task_id, host_uuid)
        except Exception:
            LOG.warning("Exception fetching tasks for existing host %s",
                        host_uuid)
            msg = "Tasks for host {} could not be fetched.".format(host_uuid)
            raise Exception(msg)
        if result.status == "Success":
            if result.prov_entities is not None:
                tasks = result.prov_entities
        return tasks

    def get_task_added_leg(self, task):
        added_leg_uuid = None
        added_leg = None
        for tag in task.taskConfiguration:
            tag_name, tag_val = NVMeOFTasks.get_tag_key_and_val(tag)
            if tag_name == "addedReplicaId":
                added_leg_uuid = tag_val
                break
        if added_leg_uuid is not None:
            block_dev_path = "/sys/block/nvme*"
            added_leg = \
                nvmeof.NVMeOFConnector.find_block_device_by_uuid(self.executor,
                                                                 added_leg_uuid,
                                                                 block_dev_path)
            if added_leg is None:
                LOG.error("[!] get added_leg failed for : %s", added_leg_uuid)
                return None, added_leg_uuid
        return added_leg, added_leg_uuid
