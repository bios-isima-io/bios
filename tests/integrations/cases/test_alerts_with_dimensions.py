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


class TestAlertsWithDimensions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "monitoringTargetWithDimensions"

    def test_count_with_dimensions(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:

            # test count condition
            insert_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_NAME)
                .csv_bulk(["one,two,2", "one,two,2", "one,two,2", "one,two,2", "three,two,2"])
                .build()
            )
            resp = client.execute(insert_statement)
            timestamp = resp.records[0].timestamp
            print(f"timestamp={timestamp}")
            time.sleep(120)
            statement = (
                bios.isql()
                .select()
                .from_signal("alertsSignal")
                .where(f"signalName = '{self.SIGNAL_NAME}' AND alertName = 'CountAlert'")
                .time_range(timestamp, bios.time.minutes(3))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            pprint.pprint(records)
            self.assertEqual(len(records), 1)

            # test min condition
            insert_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_NAME)
                .csv_bulk(["two,one,1", "two,one,-21", "two,one,3"])
                .build()
            )
            resp = client.execute(insert_statement)
            timestamp = resp.records[0].timestamp
            print(f"timestamp={timestamp}")
            time.sleep(120)
            statement = (
                bios.isql()
                .select()
                .from_signal("alertsSignal")
                .where(f"signalName = '{self.SIGNAL_NAME}' AND alertName = 'MinimumValue'")
                .time_range(timestamp, bios.time.minutes(3))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            pprint.pprint(records)
            self.assertEqual(len(records), 1)

            # test max condition
            insert_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_NAME)
                .csv_bulk(["two,three,1", "two,three,21", "two,three,21"])
                .build()
            )
            resp = client.execute(insert_statement)
            timestamp = resp.records[0].timestamp
            print(f"timestamp={timestamp}")
            time.sleep(120)
            statement = (
                bios.isql()
                .select()
                .from_signal("alertsSignal")
                .where(f"signalName = '{self.SIGNAL_NAME}' AND alertName = 'MaximumValue'")
                .time_range(timestamp, bios.time.minutes(3))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            pprint.pprint(records)
            self.assertEqual(len(records), 1)

            # test sum and count conditions
            insert_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_NAME)
                .csv_bulk(["one,three,9", "one,three,9", "one,three,9", "one,three,9"])
                .build()
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
            self.assertEqual(len(records), 2)
            for record in records:
                self.assertTrue(
                    record.get("alertName") in {"CountAlert", "SumOfValues"},
                    record.get("alertName"),
                )


if __name__ == "__main__":
    unittest.main()
