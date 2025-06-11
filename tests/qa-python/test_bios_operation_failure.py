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

import json
import sys
import time
import unittest
from pprint import pprint

import bios
import pytest
from bios import ServiceError
from bios.errors import ErrorCode
from bios.isql_request import ISqlRequest
from bios.models.app_type import AppType
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

TEST_TENANT_NAME = "biosOperationFailureTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
SIGNAL_JSON_FILE = "../resources/bios-signal-schema.json"


class OperationFailureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(SIGNAL_JSON_FILE, "r", encoding="utf-8") as json_file:
            cls.LOADED_SIGNAL = json.loads(json_file.read())
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})
            sadmin.create_user(
                bios.User(
                    email=f"app-master@{TEST_TENANT_NAME}",
                    full_name="app master",
                    tenant_name=TEST_TENANT_NAME,
                    password="app-master",
                    roles=["SchemaExtractIngest", "Report", "AppMaster"],
                )
            )
        cls.sysadmin_session = bios.login(
            ep_url(), sadmin_user, sadmin_pass, "AppOperationFailureTest", AppType.REALTIME
        )
        cls.admin_session = bios.login(
            ep_url(), ADMIN_USER, admin_pass, "AppOperationFailureTest", AppType.REALTIME
        )
        cls.signal_name = cls.LOADED_SIGNAL["signalName"]
        cls.admin_session.create_signal(cls.LOADED_SIGNAL)

    @classmethod
    def tearDownClass(cls):
        cls.admin_session.delete_signal(cls.signal_name)
        cls.admin_session.close()
        cls.sysadmin_session.close()
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def insert_bulk_validation_error(self, num_records=20):
        req_bulk = []
        for i in range(num_records):
            req_bulk.append("{0},2,3,0.1,0.5,test,MALE,IN,MH,INVALID_NUM".format(i))
        with self.assertRaises(ServiceError) as error_context:
            self.admin_session.execute(
                ISqlRequest().insert().into(self.signal_name).csv_bulk(req_bulk).build()
            )
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def insert_validation_error(self, num_records=20):
        with self.assertRaises(ServiceError) as error_context:
            self.admin_session.execute(
                ISqlRequest()
                .insert()
                .into(self.signal_name)
                .csv("1,2,3,0.1,0.5,test,MALE,IN,MH,INVALID_NUM")
                .build()
            )
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_get_signal(self):
        signal = self.admin_session.get_signal("_operationFailure")
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], "_operationFailure")

    def test_with_insert_bulk(self):
        t1 = bios.time.now()
        self.insert_bulk_validation_error()
        time.sleep(10)
        t2 = bios.time.now()

        resp = self.admin_session.execute(
            ISqlRequest()
            .select()
            .from_signal("_operationFailure")
            .where(f"stream = '{self.signal_name}'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.data_windows), 1)
        self.assertEqual(len(resp.data_windows[0].records), 20)

        resp2 = self.sysadmin_session.execute(
            ISqlRequest()
            .select()
            .from_signal("_allOperationFailure")
            .where(f"stream = '{self.signal_name}' AND tenant = '{TEST_TENANT_NAME}'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp2)
        self.assertEqual(len(resp2.data_windows), 1)
        self.assertEqual(len(resp2.data_windows[0].records), 20)

    def test_with_insert(self):
        t1 = bios.time.now()
        self.insert_validation_error()
        time.sleep(10)
        t2 = bios.time.now()

        resp = self.admin_session.execute(
            ISqlRequest()
            .select()
            .from_signal("_operationFailure")
            .where(f"stream = '{self.signal_name}'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.data_windows), 1)
        self.assertEqual(len(resp.data_windows[0].records), 1)

        resp2 = self.sysadmin_session.execute(
            ISqlRequest()
            .select()
            .from_signal("_allOperationFailure")
            .where(f"stream = '{self.signal_name}' AND tenant = '{TEST_TENANT_NAME}'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp2)
        self.assertEqual(len(resp2.data_windows), 1)
        self.assertEqual(len(resp2.data_windows[0].records), 1)

    def test_delegation_error(self):
        """Tests a delegated operatoin failure, regression BIOS-6607"""
        with bios.login(
            ep_url(),
            f"app-master@{TEST_TENANT_NAME}",
            "app-master",
            app_name="AppOperationFailureTest",
            app_type=AppType.REALTIME,
        ) as session:
            context_name = "simple"
            self.admin_session.create_context(
                {
                    "contextName": context_name,
                    "missingAttributePolicy": "reject",
                    "attributes": [
                        {"attributeName": "key", "type": "string"},
                        {"attributeName": "value", "type": "string"},
                    ],
                    "primaryKey": ["key"],
                }
            )
            start_time = bios.time.now()
            with pytest.raises(ServiceError) as exc_info:
                session.for_tenant("uber").execute(
                    bios.isql().upsert().into("simple").csv("key,value").build()
                )
            assert exc_info.value.error_code == ErrorCode.NO_SUCH_TENANT
            time.sleep(10)
            end_time = bios.time.now()

            resp = self.admin_session.execute(
                ISqlRequest()
                .select()
                .from_signal("_operationFailure")
                .where(f"stream = '{context_name}'")
                .time_range(end_time, start_time - end_time)
                .build()
            )
            self.assertIsNotNone(resp)
            self.assertEqual(len(resp.data_windows), 1)
            pprint(resp.data_windows[0])
            self.assertEqual(len(resp.data_windows[0].records), 1)
            failure_record = resp.data_windows[0].records[0]
            assert failure_record.get("stream") == context_name
            assert failure_record.get("request") == "PutContextEntries"
            assert failure_record.get("errorName") == "TENANT_NOT_FOUND"
            assert failure_record.get("errorMessage") == "Tenant not found: uber"
            assert failure_record.get("appType") == "REALTIME"
            assert failure_record.get("appName") == "AppOperationFailureTest"

            resp2 = self.sysadmin_session.execute(
                ISqlRequest()
                .select()
                .from_signal("_allOperationFailure")
                .where(f"stream = '{context_name}' AND tenant = '{TEST_TENANT_NAME}'")
                .time_range(end_time, start_time - end_time)
                .build()
            )
            self.assertIsNotNone(resp2)
            self.assertEqual(len(resp2.data_windows), 1)
            pprint(resp2.data_windows[0])
            self.assertEqual(len(resp2.data_windows[0].records), 1)
            all_failure_record = resp2.data_windows[0].records[0]
            assert all_failure_record.get("tenant") == TEST_TENANT_NAME
            assert all_failure_record.get("stream") == context_name
            assert all_failure_record.get("request") == "PutContextEntries"
            assert all_failure_record.get("errorName") == "TENANT_NOT_FOUND"
            assert all_failure_record.get("errorMessage") == "Tenant not found: uber"
            assert all_failure_record.get("appType") == "REALTIME"
            assert all_failure_record.get("appName") == "AppOperationFailureTest"


if __name__ == "__main__":
    pytest.main(sys.argv)
