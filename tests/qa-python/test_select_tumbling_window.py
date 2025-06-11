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
import sys
import unittest

import pytest

import bios
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url


class TestSelectTumblingWindow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # reusing JS test setup. See $ROOT/sdk-js/it.sh
        cls.ENV = "TIMESTAMP_ROLLUP_TWO_VALUES"
        cls.TIMESTAMP = int(os.environ.get(cls.ENV) or "0")
        if not cls.TIMESTAMP:
            raise unittest.SkipTest(f"Unable to execute: {cls.ENV} is not set")
        cls.TENANT_NAME = "jsSimpleTest"
        cls.SIGNAL_NAME = "rollupTwoValues"

    def test_single_window(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("count()")
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(2), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 19)

    def test_bb_1350_regression(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement1 = (
                bios.isql()
                .select("country", "count()")
                .from_signal(self.SIGNAL_NAME)
                .where(
                    "country in ('US', 'Japan', 'Australia', 'India')"
                    + " AND state IN ('California', 'Oregon', 'Utah',"
                    + " 'Queensland', 'New South Wales',"
                    + " '東京', '大阪', 'Hok海do',"
                    + " 'Goa')"
                )
                .group_by(["country", "state"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(2), bios.time.minutes(1))
                .build()
            )
            statement2 = (
                bios.isql()
                .select("country", "count()")
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(2), bios.time.minutes(1))
                .build()
            )
            result1 = admin.execute(statement1)
            result2 = admin.execute(statement2)

            aggregates = {}
            for record in result1.data_windows[0].records:
                key = record.get("country")
                count = aggregates.get(key) or 0
                count += record.get("count()")
                aggregates[key] = count

            for record in result2.data_windows[0].records:
                self.assertEqual(record.get("count()"), aggregates.get(record.get("country")))


if __name__ == "__main__":
    pytest.main(sys.argv)
