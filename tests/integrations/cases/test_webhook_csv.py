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


class TestWebhookCsv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "simpleSignal"
        cls.conn = HttpConnection(get_webhook2_url())
        cls.WEBHOOK_PATH = "header-auth"

    def test_newline(self):
        test_name = "test value with newline"
        test_param = "hello\nworld"
        self._verify_webhook(test_name, test_param)

    def test_carriage_return(self):
        test_name = "test value with carriage return"
        test_param = "hello\rworld"
        self._verify_webhook(test_name, test_param)

    def test_double_quotes(self):
        test_name = "test value with double quotes"
        test_param = 'one two "three"'
        self._verify_webhook(test_name, test_param)

    def test_quotes(self):
        test_name = "test value with quotes"
        test_param = "one 'two' three"
        self._verify_webhook(test_name, test_param)

    def test_commas(self):
        test_name = "test value with commas"
        test_param = "one, two, three"
        self._verify_webhook(test_name, test_param)

    def test_empty_string(self):
        test_name = "test empty string"
        test_param = ""
        self._verify_webhook(test_name, test_param)

    def test_blank_string(self):
        test_name = "test blank string"
        test_param = "  "
        self._verify_webhook(test_name, test_param)

    def _verify_webhook(self, test_name, test_param):
        body = {
            "testName": test_name,
            "testParam": test_param,
        }
        headers = {
            "user": f"{admin_user}@{self.TENANT_NAME}",
            "pass": admin_pass,
        }
        time0 = int(time.time() * 1000)
        RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, headers=headers, data=body)
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
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
            self.assertEqual(records[0].get("testParam"), test_param)


if __name__ == "__main__":
    unittest.main()
