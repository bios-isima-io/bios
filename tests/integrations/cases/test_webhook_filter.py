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
from tsetup import get_webhook_url


class TestWebhookFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "simpleSignal"
        cls.conn = HttpConnection(get_webhook_url())
        cls.WEBHOOK_PATH = "simple-signal/filter-test"

    def test_positive_filter(self):
        test_name = "test positive filter"
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            body = {
                "zero": "abc",
                "one": test_name,
                "user": f"admin@{self.TENANT_NAME}",
                "pass": "admin",
            }
            time0 = int(time.time() * 1000)
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=body)
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
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].get("testParam"), "filter for abc")

    def test_negative_filter(self):
        test_name = "test negative filter"
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            body = {
                "zero": None,
                "one": test_name,
                "user": f"admin@{self.TENANT_NAME}",
                "pass": "admin",
            }
            time0 = int(time.time() * 1000)
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=body)
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
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].get("testParam"), "filter for none")

    def test_no_filter_match(self):
        test_name = "test no filter match"
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            body = {
                "zero": "123",
                "one": test_name,
                "user": f"admin@{self.TENANT_NAME}",
                "pass": "admin",
            }
            time0 = int(time.time() * 1000)
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=body)
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
            self.assertEqual(len(records), 0)


if __name__ == "__main__":
    unittest.main()
