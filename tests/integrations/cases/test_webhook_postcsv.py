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

import bios
from rest_client import HttpConnection, RestClient
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_webhook2_url


class TestWebhookPostCsv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "covidDataSignal"
        cls.conn = HttpConnection(get_webhook2_url())
        cls.WEBHOOK_PATH = "csv"

    def test_csv(self):
        resources_dir = "../resources/files/csv_no_header"
        with open(resources_dir + "/" + "file01.csv", mode="r") as f:
            csv_data = f.read()
        self._verify_webhook(csv_data)

    def _verify_webhook(self, body):
        headers = {
            "user": f"{admin_user}@{self.TENANT_NAME}",
            "pass": admin_pass,
        }
        headers.update({"Content-Type": "text/csv"})
        time0 = int(time.time() * 1000)
        RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, headers=headers, data=body)
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_NAME)
                .where("testName = 'csv without header'")
                .time_range(time0, bios.time.minutes(1))
                .build()
            )
            resp = client.execute(statement)
            records = resp.to_dict()
            self.assertEqual(len(records), 2)


if __name__ == "__main__":
    unittest.main()
