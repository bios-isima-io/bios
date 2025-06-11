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

import os
import pprint
import time
import unittest
from configparser import ConfigParser

import bios


class TestConfigLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = ConfigParser()
        deli_home = os.environ.get("DELI_HOME")
        if not deli_home:
            raise Exception("Environment variable DELI_HOME is not set")
        config.read(f"{deli_home}/integrations.ini")
        pull = "PullConfigLoader"
        cls.ENDPOINT = config.get(pull, "endpoint")
        cls.CERT_FILE = config.get(pull, "sslCertFile")
        cls.USER = config.get(pull, "user")
        cls.PASSWORD = config.get(pull, "password")

    def test_alerts(self):
        with bios.login(self.ENDPOINT, self.USER, self.PASSWORD, cafile=self.CERT_FILE) as client:
            insert_statement = bios.isql().insert().into("monitoredSignal").csv("hello").build()
            resp = client.execute(insert_statement)
            timestamp = resp.records[0].timestamp
            print(f"timestamp={timestamp}")
            time.sleep(120)
            statement = (
                bios.isql()
                .select()
                .from_signal("alertsSignal")
                .time_range(timestamp, bios.time.minutes(3))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            pprint.pprint(records)
            self.assertEqual(len(records), 1)


if __name__ == "__main__":
    unittest.main()
