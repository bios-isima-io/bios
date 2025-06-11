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

import pprint
import time
import unittest

from rest_client import HttpConnection, RestClient

import bios
from tsetup import (
    admin_user,
    admin_pass,
)
from tsetup import get_endpoint_url as ep_url
from tsetup import get_webhook2_url


class TestWebhookFlattener(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "simpleSignal"
        cls.conn = HttpConnection(get_webhook2_url())
        cls.WEBHOOK_PATH = "simple-signal/flattener-test"
        cls.WEBHOOK_PATH2 = "simple-signal/flattener-test2"

    def test_simple_flatttener(self):
        test_name = "test simple flattener"
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            message = {"user": f"admin@{self.TENANT_NAME}", "pass": "admin", "items": []}
            for i in range(20):
                item = {
                    "one": test_name,
                    "two": str(i),
                }
                message["items"].append(item)

            time0 = int(time.time() * 1000)
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=message)
            statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_NAME)
                .where(f"testName = '{test_name}'")
                .time_range(time0, bios.time.minutes(1))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            self.assertEqual(len(records), 20)
            for record in records:
                value = int(record.get("testParam"))
                self.assertTrue(0 <= value < 20)

    def test_flattener_with_common_parameter(self):
        test_name = "test flattener with common param"
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            message = {
                "user": f"admin@{self.TENANT_NAME}",
                "pass": "admin",
                "testName": test_name,
                "items": [],
            }
            for i in range(20):
                item = {
                    "two": str(i),
                }
                message["items"].append(item)

            time0 = int(time.time() * 1000)
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=message)
            statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_NAME)
                .where(f"testName = '{test_name}'")
                .time_range(time0, bios.time.minutes(1))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            self.assertEqual(len(records), 20)
            for record in records:
                value = int(record.get("testParam"))
                self.assertTrue(0 <= value < 20)

    def test_flattener_with_nested_common_parameter(self):
        test_name = "test flattener with nested common param"
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            message = {
                "user": f"admin@{self.TENANT_NAME}",
                "pass": "admin",
                "common": {
                    "title": test_name,
                    "date": "today",
                },
                "items": [],
            }
            for i in range(20):
                item = {
                    "sequenceNo": str(i),
                }
                message["items"].append(item)

            time0 = int(time.time() * 1000)
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH2, data=message)
            statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_NAME)
                .where(f"testName = '{test_name}'")
                .time_range(time0, bios.time.minutes(1))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            self.assertEqual(len(records), 20)
            for record in records:
                value = int(record.get("testParam"))
                self.assertTrue(0 <= value < 20)


if __name__ == "__main__":
    unittest.main()
