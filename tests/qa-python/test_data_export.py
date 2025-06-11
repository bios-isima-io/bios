#!/usr/bin/env python3
#
# Copyright (C) 2025 Isima, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import copy
import pprint
import random
import string
import sys
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url


def get_random_string(length=random.randint(4, 8)):
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for _ in range(length))
    return result_str + "," + str(random.randint(4, 100))


@unittest.skip("Unable to run without AWS")
class DataExportTest(unittest.TestCase):
    ADMIN_USER = admin_user + "@" + TENANT_NAME
    SIGNAL_NAME = "dataExportSignal"
    EXTERNAL_STORAGE_NAME = "testS3"
    SIGNAL_EXPORT_TEMPLATE = {
        "signalName": SIGNAL_NAME,
        "missingAttributePolicy": "Reject",
        "attributes": [
            {"attributeName": "one", "type": "string"},
            {"attributeName": "two", "type": "integer"},
        ],
        "postStorageStage": {
            "features": [
                {
                    "featureName": "byOne",
                    "dimensions": ["one"],
                    "attributes": ["two"],
                    "featureInterval": 60000,
                    "featureAsContext": {"state": "Enabled"},
                }
            ]
        },
        "exportDestinationId": "",
    }
    TENANT_POSTFIX = "@" + TENANT_NAME
    S3_BUCKET_1 = "export-s3-bios-test"
    EXPORT_DESTINATION_CONFIG = {
        "exportDestinationName": EXTERNAL_STORAGE_NAME,
        "storageType": "S3",
        "status": "Enabled",
        "storageConfig": {
            "s3BucketName": S3_BUCKET_1,
            "s3AccessKeyId": "",
            "s3SecretAccessKey": "",
            "s3Region": "ap-south-1",
        },
    }

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

    def tearDown(self):
        with bios.login(ep_url(), admin_user + self.TENANT_POSTFIX, admin_pass) as admin:
            tenant_config = admin.get_tenant(detail=True)
            for export_dest in tenant_config.get("exportDestinations") or []:
                admin.delete_export_destination(export_dest.get("exportDestinationId"))

    # @unittest.skip("Unstable BIOS-2440")
    def test_data_export(self):
        with bios.login(ep_url(), admin_user + self.TENANT_POSTFIX, admin_pass) as admin:
            try:
                admin.delete_signal(self.SIGNAL_EXPORT_TEMPLATE.get("signalName"))
            except ServiceError:
                pass

            created = admin.create_export_destination(self.EXPORT_DESTINATION_CONFIG)
            export_destination_id = created.get("exportDestinationId")
            assert export_destination_id is not None
            temp = copy.deepcopy(created)
            del temp["exportDestinationId"]
            assert temp == self.EXPORT_DESTINATION_CONFIG

            signal = copy.deepcopy(self.SIGNAL_EXPORT_TEMPLATE)
            signal["exportDestinationId"] = export_destination_id
            created_signal = admin.create_signal(signal)

            pprint.pprint(created_signal)
            print(export_destination_id)

            admin.start_data_export(export_destination_id)
            ret = admin.get_export_destination(export_destination_id)
            self.assertEqual(ret["exportDestinationName"], self.EXTERNAL_STORAGE_NAME)
            self.assertEqual(ret["storageType"], "S3")
            self.assertEqual(ret["status"], "Enabled")

            # Automation is not availalbe for testing the exported data
            # for i in range(1, 11):
            #     bulk_request = [get_random_string() for _ in range(50)]
            #     request = (
            #         ISqlRequest()
            #         .insert()
            #         .into(self.SIGNAL_EXPORT_TEMPLATE["signalName"])
            #         .csv_bulk(bulk_request)
            #         .build()
            #     )
            #     ret = admin.execute(request)
            #     self.assertEqual(len(ret.records), len(bulk_request))
            #     time.sleep(60)

            # you can't delete the destination while a signal is referring
            with pytest.raises(ServiceError) as excinfo:
                admin.delete_export_destination(export_destination_id)
            error = excinfo.value
            assert error.error_code == ErrorCode.BAD_INPUT

            admin.delete_signal(self.SIGNAL_EXPORT_TEMPLATE.get("signalName"))

            # now it's ready to delete
            admin.delete_export_destination(export_destination_id)
            with pytest.raises(ServiceError) as excinfo:
                admin.get_export_destination(export_destination_id)
            error = excinfo.value
            assert error.error_code == ErrorCode.NOT_FOUND

    def test_negative_missing_name(self):
        self._test_negative_generic("exportDestinationName")

    def test_negative_missing_type(self):
        self._test_negative_generic("storageType")

    def test_negative_missing_status(self):
        self._test_negative_generic("status")

    def test_negative_missing_storage_config(self):
        self._test_negative_generic("storageConfig")

    def test_negative_missing_bucket(self):
        self._test_negative_generic("storageConfig", "s3BucketName")

    def test_negative_missing_access_key_id(self):
        self._test_negative_generic("storageConfig", "s3AccessKeyId")

    def test_negative_missing_secret_access_key(self):
        self._test_negative_generic("storageConfig", "s3SecretAccessKey")

    def test_negative_missing_region(self):
        self._test_negative_generic("storageConfig", "s3Region")

    def _test_negative_generic(self, *paths):
        with bios.login(ep_url(), admin_user + self.TENANT_POSTFIX, admin_pass) as admin:
            config = copy.deepcopy(self.EXPORT_DESTINATION_CONFIG)
            created = admin.create_export_destination(config)
            current = config
            for i, path in enumerate(paths):
                if i < len(paths) - 1:
                    current = current.get(path)
                else:
                    del current[path]
            with pytest.raises(ServiceError) as excinfo:
                admin.create_export_destination(config)
            assert excinfo.value.error_code == ErrorCode.BAD_INPUT
            with pytest.raises(ServiceError) as excinfo:
                admin.update_export_destination(created.get("exportDestinationId"), config)
            assert excinfo.value.error_code == ErrorCode.BAD_INPUT


if __name__ == "__main__":
    pytest.main(sys.argv)
