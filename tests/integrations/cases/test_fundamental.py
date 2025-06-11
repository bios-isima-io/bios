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
from tsetup import (
    admin_user,
    admin_pass,
)
from tsetup import get_endpoint_url as ep_url


class TestAlertsFundamental(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "simpleMonitoringTarget"

    def test_alerts(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            insert_statement = (
                bios.isql().insert().into(self.SIGNAL_NAME).csv_bulk(["hello", "world"]).build()
            )
            resp = client.execute(insert_statement)
            timestamp = resp.records[0].timestamp
            print(f"timestamp={timestamp}")
            time.sleep(120)
            statement = (
                bios.isql()
                .select()
                .from_signal("alertsSignal")
                .where(f"signalName = '{self.SIGNAL_NAME}'")
                .time_range(timestamp, bios.time.minutes(3))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            pprint.pprint(records)
            self.assertEqual(len(records), 1)


if __name__ == "__main__":
    unittest.main()
