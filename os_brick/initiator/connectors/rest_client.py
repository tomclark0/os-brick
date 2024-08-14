#    (c)  Copyright  Kioxia Corporation 2021 All Rights Reserved.
#
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

import abc
import json
import os
import ssl
import time

import requests
import urllib3

from . import entities

urllib3.disable_warnings()
RUN_COMMAND_TRIALS = 20
RUN_COMMAND_SLEEP = 0.5


class ClassBuilder(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if value is not None:
                self.__dict__[key] = value

    def to_json(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=4)


class JsonToClass(object):

    def __init__(self, json_object, first=False):
        if isinstance(json_object, list):
            self.records = []
            for list_index in range(len(json_object)):
                list_item = JsonToClass(json_object[list_index])
                self.records.append(list_item)
        else:
            if first:
                self.records = None
            self.build_class(json_object)
        if first:
            if 'status' not in json_object:
                self.status = "Success"
            if 'description' not in json_object:
                self.description = "Success."
        pass

    def __getattr__(self, item):
        return "N/A"

    def to_json(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=4)

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def is_exist(self, item):
        if item in self.__dict__.keys() and self.__dict__[item] is not None:
            return True
        return False

    def build_class(self, json_object):
        json_keys = json_object.keys()
        for key in json_keys:
            if isinstance(json_object[key], list):
                self.__dict__[key] = []
                for i in range(len(json_object[key])):
                    if isinstance(json_object[key][i], dict):
                        sub_object = JsonToClass(json_object[key][i])
                    else:
                        sub_object = json_object[key][i]
                    self.__dict__[key].append(sub_object)
                continue
            if not isinstance(json_object[key], dict):
                self.__dict__[key] = json_object[key]
                continue
            self.__dict__[key] = {}
            sub_object = JsonToClass(json_object[key])
            self.__dict__[key] = sub_object


class ProvisionerVisitor(object):
    #
    # Provisioner Visitor
    #

    def __init__(self, http, command_str):
        self.http = http
        self.command_str = command_str

    @abc.abstractmethod
    def visit(self, url):
        return


class ProvisionerGetVisitor(ProvisionerVisitor):
    #
    # Provisioner Get Visitor
    #

    def visit(self, url, token=None):
        r = self.http.request(
            'GET', url, headers={
                "Authorization": "Bearer " + token})
        return r


class ProvisionerPostVisitor(ProvisionerVisitor):
    #
    # Provisioner Post Visitor
    #

    def __init__(self, http, command_str, json_body):
        ProvisionerVisitor.__init__(self, http, command_str)
        self.json_body = json_body

    def visit(self, url, token=None):
        r = self.http.request(
            'POST',
            url,
            body=self.json_body,
            headers={
                'Content-Type': 'application/json',
                "Authorization": "Bearer " +
                                 token})
        return r


class ProvisionerDeleteVisitor(ProvisionerVisitor):
    #
    # Provisioner Delete Visitor
    #

    def __init__(self, http, command_str):
        ProvisionerVisitor.__init__(self, http, command_str)

    def visit(self, url, token=None):
        r = self.http.request(
            'DELETE',
            url,
            body=None,
            headers={
                'Content-Type': 'application/json',
                "Authorization": "Bearer " +
                                 token})
        return r


class ProvisionerPatchVisitor(ProvisionerVisitor):
    #
    # Provisioner Patch Visitor
    #

    def __init__(self, http, command_str, json_body=None):
        ProvisionerVisitor.__init__(self, http, command_str)
        self.json_body = json_body

    def visit(self, url, token=None):
        r = self.http.request(
            'PATCH',
            url,
            body=self.json_body,
            headers={
                'Content-Type': 'application/json',
                "Authorization": "Bearer " +
                                 token})
        return r


class ProvisionerPutVisitor(ProvisionerVisitor):
    #
    # Provisioner Put Visitor
    #

    def __init__(self, http, command_str, json_body):
        ProvisionerVisitor.__init__(self, http, command_str)
        self.json_body = json_body

    def visit(self, url, token=None):
        r = self.http.request(
            'PUT',
            url,
            body=self.json_body,
            headers={
                'Content-Type': 'application/json',
                "Authorization": "Bearer " +
                                 token})
        return r


class ProvisionerPostDataVisitor(ProvisionerVisitor):
    #
    # Provisioner Post Data Visitor
    #

    def __init__(self, http, command_str, path):
        ProvisionerVisitor.__init__(self, http, command_str)
        self.path = path
        self.timeout = 90

    def visit(self, url, token=None):
        binary_data = open(self.path, 'rb').read()
        disposition = "inline; filename=" + os.path.basename(self.path)

        r = self.http.request(
            'POST',
            url,
            body=binary_data,
            headers={
                'Content-Type': 'application/x-gtar',
                'Content-Disposition': disposition,
                "Authorization": "Bearer " +
                                 token},
            timeout=self.timeout)
        return r


class ProvisionerConnector(object):
    #
    # Provisioner Connector
    #

    def __init__(self, ips, port, visitor):
        self.visitor = visitor
        self.ips = ips
        self.port = port

    def visit_provisioner(self, token=None):
        r = None
        if self.ips:
            num_of_ips = len(self.ips)
            if num_of_ips > 0:
                for i in range(num_of_ips):
                    ip = self.ips[i]
                    url = 'https://' + ip + ':' + \
                          str(self.port) + '/' + self.visitor.command_str
                    try:
                        if token is None:
                            token = "Unknown"
                        r = self.visitor.visit(url, token)
                        if r:
                            if i != 0:
                                KioxiaProvisioner.switch_path(i)
                            return r
                    except BaseException:
                        continue
                return r
            return r
        return r


class KioxiaProvisioner(object):
    #
    # REST client class that interacts with a specific Provisioner
    # :type ips: str array
    # :param ips: Provisioner management IPs
    # :type cert: str
    # :param cert: KumoScale keystore pem file full path
    #

    mgmt_ips = []

    class TimeoutException(Exception):   # Custom exception class
        pass

    class ErrorException(Exception):   # Custom exception class
        pass

    def __init__(self, ips, cert, token=None, port=8090):
        self.mgmt_ips = ips
        self.port = port
        self.user = None
        self.token = token
        self.token_url = None
        self.client_id = None
        self.client_secret = None
        self.client_scope = None
        self.generate_time = None
        self.status = "OK"
        if cert is None:
            cert = os.path.dirname(__file__) + "/ssdtoolbox.pem"
        KioxiaProvisioner.mgmt_ips = ips
        self.http = urllib3.PoolManager(
            cert_reqs=ssl.CERT_NONE,
            cert_file=cert,
            assert_hostname=False,
            timeout=urllib3.Timeout(
                connect=5.0,
                read=60.0))
        self.result = self.get_info()
        if self.result is not None and self.result.status == "Success":
            retrieved_ips = ips
            for ip in retrieved_ips:
                if ip not in KioxiaProvisioner.mgmt_ips:
                    KioxiaProvisioner.mgmt_ips.append(ip)

    def token_generate(self, token_url, client_id, client_secret,
                       client_scope):
        access_token = None
        expires_in = None
        client_data = {"grant_type": "client_credentials"}
        client_data["client_id"] = client_id
        client_data["client_secret"] = client_secret
        verify_value = True
        if client_scope is not None:
            client_data["scope"] = client_scope
            verify_value = False
        try:
            response = requests.post(token_url,
                    data=client_data,
                    verify=verify_value)
            if response.reason == 'Not Found':
                self.status = "Invalid URL path: " + str(token_url)
            elif response.reason == 'Unauthorized':
                self.status = "Secret string is invalid: " + str(client_secret)
            elif response.reason == 'Bad Request':
                self.status = "Invalid client credentials: " + str(client_id)
            elif response.reason == 'OK':
                access_token = response.json()["access_token"]
                expires_in = response.json()["expires_in"]
                self.status = "OK"
            else:
                self.status = "Unknown system error: " + str(token_url)
        except requests.exceptions.ConnectionError:
            self.status = "Error Connecting: " + str(token_url)
            return None, None
        except Exception as e:
            self.status = "System error: " + str(e)
            return None, None
        except BaseException:
            self.status = "Unknown system error1" + str(token_url)
        if expires_in is None:
            expires_in = 60
        return access_token, expires_in

    def set_token(self, user, token):
        self.user = user
        self.token = token

    def set_auth_server_parameters(self, expires_in, token_url, client_id,
                                   client_secret, client_scope):
        if self.token is None:
            token, expires_in = self.token_generate(token_url, client_id,
                                                    client_secret,
                                                    client_scope)
            if token is None:
                return 1
            self.token = token
        expires = int(expires_in)
        self.generate_time = time.time() + expires - 5
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_scope = client_scope
        return 0

    def result_support(self, result):
        if result is not None:
            if result.data is not None:
                if "Status 401" in str(result.data):
                    ClassBuilder()
                    return entities.ProvisionerResponse(
                        None, None, "Bad credentials")
                if "Status 403" in str(result.data):
                    return entities.ProvisionerResponse(
                        None, None, "Access is denied")
                if "Status 404" in str(result.data):
                    return entities.ProvisionerResponse(
                        None, None, "Provisioner Communication Error")
                if str(result.data) == "":
                    return entities.ProvisionerResponse([], None, "Success")
                try:
                    result_data = json.loads(result.data)
                    if ('status' in result_data and
                            result_data['status'] != "Success"):
                        return entities.ProvisionerResponse(
                            result_data, None, result_data['status'],
                            result_data['description'])
                    return entities.ProvisionerResponse(result_data)
                except Exception as e:
                    return entities.ProvisionerResponse(
                        None, None, "Communication Error")
        return entities.ProvisionerResponse(
            None,
            None,
            "Provisioner Communication Error",
            "Provisioner Communication Error")

    # Call to switch last successful connected ip
    @staticmethod
    def switch_path(ip_idx):
        temp = KioxiaProvisioner.mgmt_ips[0]
        KioxiaProvisioner.mgmt_ips[0] = KioxiaProvisioner.mgmt_ips[ip_idx]
        KioxiaProvisioner.mgmt_ips[ip_idx] = temp

    def check_to_replace_token(self, api_name):
        if self.generate_time is not None \
                and time.time() >= self.generate_time and api_name != 'info':
            token, expire = self.token_generate(self.token_url,
                                                self.client_id,
                                                self.client_secret,
                                                self.client_scope)
            if token is None:
                self.result_support(None)
            self.token = token
            self.generate_time = time.time() + expire - 5

    # Call Provisioner with get request
    def provisioner_get_request(self, api_name):
        self.check_to_replace_token(api_name)
        get_visitor = ProvisionerGetVisitor(self.http, api_name)
        provisioner_connector = ProvisionerConnector(
            self.mgmt_ips, self.port, get_visitor)
        r = provisioner_connector.visit_provisioner(self.token)
        return self.result_support(r)

    # Call Provisioner with delete request
    def provisioner_delete_request(self, api_name):
        self.check_to_replace_token(api_name)
        delete_visitor = ProvisionerDeleteVisitor(self.http, api_name)
        provisioner_connector = ProvisionerConnector(
            self.mgmt_ips, self.port, delete_visitor)
        r = provisioner_connector.visit_provisioner(self.token)
        return self.result_support(r)

    # Call Provisioner with patch request
    def provisioner_patch_request(self, api_name, json_body=None):
        self.check_to_replace_token(api_name)
        patch_visitor = ProvisionerPatchVisitor(self.http, api_name, json_body)
        provisioner_connector = ProvisionerConnector(
            self.mgmt_ips, self.port, patch_visitor)
        r = provisioner_connector.visit_provisioner(self.token)
        return self.result_support(r)

    # Call Provisioner with update request
    def provisioner_put_request(self, api_name, json_body):
        self.check_to_replace_token(api_name)
        put_visitor = ProvisionerPutVisitor(self.http, api_name, json_body)
        provisioner_connector = ProvisionerConnector(
            self.mgmt_ips, self.port, put_visitor)
        r = provisioner_connector.visit_provisioner(self.token)
        return self.result_support(r)

    # Call Provisioner with post request
    def provisioner_post_request(self, api_name, json_body, password=None):
        self.check_to_replace_token(api_name)
        post_visitor = ProvisionerPostVisitor(self.http, api_name, json_body)
        provisioner_connector = ProvisionerConnector(
            KioxiaProvisioner.mgmt_ips, self.port, post_visitor)
        r = provisioner_connector.visit_provisioner(self.token)
        return self.result_support(r)

    def get_info(self):
        # Call to Get Info API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain Provisioner information
        #
        result_response = self.provisioner_get_request('info')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity)
        return result_response

    def get_provisioner_info(self):
        # Call to Get Info API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain Provisioner information
        #
        result_response = self.provisioner_get_request('info')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return result_entity
        return result_response

    def add_backend(self, backend_entity):
        # Call to Add Backend API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        j = backend_entity.to_json()
        result_response = self.provisioner_post_request('backends', j)
        return result_response

    def update_backend(self, backend_entity, persistent_id):
        # all to Update Backend API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        j = backend_entity.to_json()
        result_response = self.provisioner_put_request(
            'backends/' + persistent_id, j)
        return result_response

    def delete_backend(self, persistent_id):
        # Call to Delete Backend API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_delete_request(
            'backends/' + persistent_id)
        return result_response

    def get_backends(self):
        # Call to List of Backends API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Backends
        #

        result_response = self.provisioner_get_request('backends')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_backend_by_id(self, uuid):
        # Call to List of Backends API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Backends
        #

        result_response = self.provisioner_get_request('backends/' + uuid)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_volumes(self, tenant_uuid=None):
        # Call to List of Volumes API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_get_request(tenant_id + 'volumes')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_volumes_by_alias(self, alias, tenant_uuid=None):
        # Call to List of Volumes API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_get_request(
            tenant_id + 'volumes_by_alias/' + alias)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_volumes_by_uuid(
            self,
            volume_uuid,
            tenant_uuid=None):
        # Call to List of Volumes API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_get_request(
            tenant_id + 'volumes/' + volume_uuid)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def add_replica(
            self,
            replica_entity,
            volume_uuid,
            tenant_uuid=None):
        # Call to Add Replica API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        j = replica_entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'replica/' + volume_uuid, j)
        return result_response

    def delete_replica(
            self,
            volume_uuid,
            replica_uuid,
            tenant_uuid=None):
        # Call to Delete Replica API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_patch_request(
            tenant_id + 'replica/' + volume_uuid + "/" + replica_uuid)
        return result_response

    def delete_replica_confirm(
            self,
            volume_uuid,
            replica_uuid,
            tenant_uuid=None):
        # Call to Delete Replica Confirm API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_delete_request(
            tenant_id + 'replica/' + volume_uuid + "/" + replica_uuid)
        return result_response

    def create_volume(self, volume_entity, tenant_uuid=None):
        # Call to Create Volume API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        j = volume_entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'volumes', j)
        return result_response

    def delete_volume(self, volume_uuid, tenant_uuid=None):
        # Call to Delete Volume API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_delete_request(
            tenant_id + 'volumes/' + volume_uuid)
        return result_response

    def expand_volume(
            self,
            new_capacity,
            volume_uuid,
            tenant_uuid=None):
        # Call to Expand Volume API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        entity = ClassBuilder(newCapacity=str(new_capacity))
        j = entity.to_json()
        result_response = self.provisioner_patch_request(
            tenant_id + 'volumes/' + volume_uuid, j)
        return result_response

    def set_replica_state(
            self,
            volume_uuid,
            replica_uuid,
            state,
            tenant_uuid=None):
        # Call to Set Replica State API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_patch_request(
            tenant_id + 'replica/' + volume_uuid + "/" +
            replica_uuid + "/" + str(state))
        return result_response

    def get_snapshots(
            self,
            snapshot_uuid=None,
            tenant_uuid=None):
        # Call to List of Snapshots API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        if snapshot_uuid is None:
            result_response = self.provisioner_get_request(
                tenant_id + 'snapshots')
        else:
            result_response = self.provisioner_get_request(
                tenant_id + 'snapshots/' + snapshot_uuid)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_snapshots_by_vol(
            self,
            volume_uuid,
            tenant_uuid=None):
        # Call to Get Snapshot Information via Volume UUID API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_get_request(
            tenant_id + 'snapshots_by_vol/' + volume_uuid)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_snapshots_by_alias(self, alias, tenant_uuid=None):
        # Call to Get Snapshot Information via alias API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_get_request(
            tenant_id + 'snapshots_by_alias/' + alias)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def set_license(self, license_key):
        # Call to Set License API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        entity = ClassBuilder(license=license_key)
        j = entity.to_json()
        result_response = self.provisioner_post_request('license', j)
        return result_response

    def get_license(self):
        # Call to Get License API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_get_request('license')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity)
        return result_response

    def get_inventory(self):
        # Call to Get Inventory API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_get_request('inventory')
        return result_response

    def reset_inventory(self):
        # Call to Reset Inventory API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_delete_request('reset_inventory')
        return result_response

    def get_syslogs(self):
        # Call to Get Syslogs API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_get_request('syslog')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def create_snapshot(
            self,
            snapshot_entity,
            tenant_uuid=None):
        # Call to Create Snapshot API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        j = snapshot_entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'snapshots', j)
        return result_response

    def delete_snapshot(
            self,
            snapshot_uuid,
            tenant_uuid=None):
        # Call to Delete Snapshot API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        result_response = self.provisioner_delete_request(
            tenant_id + 'snapshots/' + snapshot_uuid)
        return result_response

    def create_snapshot_volume(
            self,
            snapshot_volume_entity,
            tenant_uuid=None):
        # Call to Create Snapshot Volume API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        j = snapshot_volume_entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'snapshot_volumes', j)
        return result_response

    def forward_log(self, forward_entity):
        # Call to Forward Log API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        j = forward_entity.to_json()
        result_response = self.provisioner_post_request('forward_log', j)
        return result_response

    def get_hosts(self):
        # Call to Get Hosts API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_get_request('hosts')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def get_hosts_by_name(self, host_name):
        # Call to Get Hosts API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_get_request(
            'hosts?hostName=' + host_name)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def delete_host(self, host_uuid):
        # Call to Delete Host API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        result_response = self.provisioner_delete_request('hosts/' + host_uuid)
        return result_response

    def get_targets(self, host_uuid, volume_uuid):
        # Call to Get Targets API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        if host_uuid is None and volume_uuid is None:
            return entities.ProvisionerResponse(
                None, None, "ParametersError", "All parameters missing")
        if host_uuid is not None:
            request = "?hostId=" + host_uuid
        else:
            request = "?volId=" + volume_uuid
        if host_uuid is not None and volume_uuid is not None:
            request += "&volId=" + volume_uuid
        result_response = self.provisioner_get_request('targets' + request)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def publish(
            self,
            host_uuid,
            volume_uuid,
            tenant_uuid=None):
        # Call to Pablish API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        entity = ClassBuilder(hostId=host_uuid, volId=volume_uuid)
        j = entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'publish', j)
        return result_response

    def unpublish(
            self,
            host_uuid,
            volume_uuid,
            tenant_uuid=None):
        # Call to UnPablish API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        entity = ClassBuilder(hostId=host_uuid, volId=volume_uuid)
        j = entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'unpublish', j)
        return result_response

    def host_probe(self, host_nqn, host_uuid, host_name,
                   client_type, sw_version, duration_in_sec,
                   multipath=False,
                   used_namespaces_ids=None):
        # Call to Host Probe API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        entity = ClassBuilder(
            hostNqn=host_nqn,
            hostId=host_uuid,
            name=host_name,
            clientType=client_type,
            version=sw_version,
            duration=duration_in_sec,
            multipath=multipath,
            usedNamespacesIds=used_namespaces_ids)
        j = entity.to_json()
        result_response = self.provisioner_post_request('host_probe', j)
        return result_response

    def migrate_volume(
            self,
            volume_uuid,
            replica_uuid=None,
            tenant_uuid=None,
            handler=None,
            destination_backend_id=None):
        # Call to Migrate Volume API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        entity = ClassBuilder(volId=volume_uuid, repId=replica_uuid,
            migrationHandler=handler,
            destinationBackendId=destination_backend_id)
        j = entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'migrate_volume', j)
        return result_response

    def get_tasks(self, task_id=None, host_id=None):
        # Call to Get Tasks API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        if task_id is not None:
            cmd = "tasks?taskId=" + str(task_id)
        elif host_id is not None:
            cmd = "tasks?hostId=" + str(host_id)
        else:
            cmd = "tasks"
        result_response = self.provisioner_get_request(cmd)
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def remove_task(self, task_id, host_id=None):
        # Call to Remove Task API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        cmd = 'tasks?taskId=' + task_id
        if host_id is not None:
            cmd += "&hostId=" + host_id
        result_response = self.provisioner_delete_request(cmd)
        return result_response

    def update_task(self, task_id, host_id, state=None, progress=None,
                    status=None, description=None, tags=None):
        # Call to Update Task API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        entity = ClassBuilder(
            taskId=task_id,
            hostId=host_id,
            state=state,
            progress=progress,
            taskStatus=status,
            statusDescription=description,
            taskConfiguration=tags)
        j = entity.to_json()
        result_response = self.provisioner_put_request('tasks', j)
        return result_response

    def create_tenant(self, tenant_entity):
        # Call to Create Tenant API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        j = tenant_entity.to_json()
        result_response = self.provisioner_post_request('tenants', j)
        return result_response

    def delete_tenant(self, tenant_uuid):
        # Call to Delete Tenant API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        result_response = self.provisioner_delete_request(
            'tenants/' + tenant_uuid)
        return result_response

    def modify_tenant(self, tenant_entity, tenant_uuid):
        # Call to Modify Tenant API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        j = tenant_entity.to_json()
        result_response = self.provisioner_put_request(
            'tenants/' + tenant_uuid, j)
        return result_response

    def get_tenants(self):
        # Call to List of Tenants API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain List of Volumes
        #

        result_response = self.provisioner_get_request('tenants')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return entities.ProvisionerResponse(result_entity.records)
        return result_response

    def clone_volume(self, clone_entity, tenant_uuid=None):
        # Call to Clone Volume API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #
        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        j = clone_entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'clone_volume', j)
        return result_response

    def get_auth_server(self):
        # Call to authorization server configuration API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data contain authorization server
        #           configuration
        #
        result_response = self.provisioner_get_request('auth_server')
        if result_response.status == "Success":
            result_entity = JsonToClass(result_response.prov_entities, True)
            return result_entity
        return result_response

    def connect(
            self,
            host_uuid,
            volume_uuid,
            tenant_uuid=None):
        # Call to Connect API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        entity = ClassBuilder(hostId=host_uuid, volId=volume_uuid)
        j = entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'connect', j)
        return result_response

    def disconnect(
            self,
            host_uuid,
            volume_uuid,
            tenant_uuid=None):
        # Call to Disconnect API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        tenant_id = ""
        if tenant_uuid is not None:
            tenant_id = tenant_uuid + "/"
        entity = ClassBuilder(hostId=host_uuid, volId=volume_uuid)
        j = entity.to_json()
        result_response = self.provisioner_post_request(
            tenant_id + 'disconnect', j)
        return result_response

    def set_auth_server(
            self,
            auth_entity):
        # Call to Delete authorization server API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        j = auth_entity.to_json()
        result_response = self.provisioner_post_request('auth_server', j)
        return result_response

    def delete_auth_server(
            self):
        # Call to Delete authorization server API

        # @rtype: ProvisionerResponse
        # @returns: Provisioner response data
        #

        result_response = self.provisioner_delete_request('auth_server')
        return result_response
