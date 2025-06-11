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

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

# Pattern A: The last rollup period is in the first half of tumbling window
# Pattern B: The last rollup period is in the last half of tumbling window


class OnTheFlyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # reusing JS test setup. See $ROOT/sdk-js/it.sh
        cls.ENV = "TIMESTAMP_ON_THE_FLY_TEST"
        cls.TIMESTAMP = int(os.environ.get(cls.ENV) or "0")
        if not cls.TIMESTAMP:
            raise unittest.SkipTest(
                f"Unable to execute: Environment variable {cls.ENV} is not set"
            )
        cls.TENANT_NAME = "onTheFlySelectTest"
        cls.SIGNAL_NAME = "onTheFlyTestSignal"
        cls.SIGNAL_NAME_NO_FEATURES = "onTheFlyNoFeaturesSignal"

    def test_tumbling_without_group_pattern_a_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "avg(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "avg(duration)",
                    "distinctcount(city)",
                )
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            self.assertEqual(len(result.data_windows), 2)
            data_window0 = result.data_windows[0]
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 19)
            self.assertEqual(data_window0.records[0].get("sum(score)"), 486)
            self.assertEqual(data_window0.records[0].get("min(score)"), 1)
            self.assertEqual(data_window0.records[0].get("max(score)"), 95)
            self.assertAlmostEqual(data_window0.records[0].get("avg(score)"), 486 / 19, 7)
            self.assertAlmostEqual(data_window0.records[0].get("sum(duration)"), 845.5, 7)
            self.assertAlmostEqual(data_window0.records[0].get("min(duration)"), 1.2, 7)
            self.assertAlmostEqual(data_window0.records[0].get("max(duration)"), 191.4, 7)
            self.assertAlmostEqual(data_window0.records[0].get("avg(duration)"), 845.5 / 19, 7)
            self.assertEqual(data_window0.records[0].get("distinctcount(city)"), 9)
            data_window1 = result.data_windows[1]
            self.assertEqual(len(data_window1.records), 0)

    def test_tumbling_without_group_no_features_pattern_a_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                )
                .from_signal(self.SIGNAL_NAME_NO_FEATURES)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_tumbling_without_group_no_features_pattern_a_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                )
                .from_signal(self.SIGNAL_NAME_NO_FEATURES)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .on_the_fly()
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_tumbling_without_group_pattern_a_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            metrics = [
                "count()",
                "sum(score)",
                "min(score)",
                "max(score)",
                "avg(score)",
                "sum(duration)",
                "min(duration)",
                "max(duration)",
                "avg(duration)",
                "distinctcount(city)",
            ]
            statement = (
                bios.isql()
                .select(*metrics)
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self._snap(self.TIMESTAMP, step), bios.time.minutes(4), step)
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            self.assertEqual(len(result.data_windows), 2)

            def verify_first_two_windows(result):
                data_window0 = result.data_windows[0]
                self.assertEqual(data_window0.records[0].get("count()"), 19)
                self.assertEqual(data_window0.records[0].get("sum(score)"), 486)
                self.assertEqual(data_window0.records[0].get("min(score)"), 1)
                self.assertEqual(data_window0.records[0].get("max(score)"), 95)
                self.assertAlmostEqual(data_window0.records[0].get("avg(score)"), 486 / 19, 7)
                self.assertAlmostEqual(data_window0.records[0].get("sum(duration)"), 845.5, 7)
                self.assertAlmostEqual(data_window0.records[0].get("min(duration)"), 1.2, 7)
                self.assertAlmostEqual(data_window0.records[0].get("max(duration)"), 191.4, 7)
                self.assertAlmostEqual(data_window0.records[0].get("avg(duration)"), 845.5 / 19, 7)
                self.assertEqual(data_window0.records[0].get("distinctcount(city)"), 9)
                data_window1 = result.data_windows[1]
                self.assertEqual(data_window1.records[0].get("count()"), 8)
                self.assertEqual(data_window1.records[0].get("sum(score)"), 276)
                self.assertEqual(data_window1.records[0].get("min(score)"), 3)
                self.assertEqual(data_window1.records[0].get("max(score)"), 88)
                self.assertAlmostEqual(data_window1.records[0].get("avg(score)"), 34.5, 7)
                self.assertAlmostEqual(data_window1.records[0].get("sum(duration)"), 450.9, 7)
                self.assertAlmostEqual(data_window1.records[0].get("min(duration)"), 3.8, 7)
                self.assertAlmostEqual(data_window1.records[0].get("max(duration)"), 223.1, 7)
                self.assertAlmostEqual(data_window1.records[0].get("avg(duration)"), 450.9 / 8, 7)
                self.assertEqual(data_window1.records[0].get("distinctcount(city)"), 7)

            verify_first_two_windows(result)

            statement = (
                bios.isql()
                .select(*metrics)
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), step)
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            self.assertEqual(len(result.data_windows), 3)
            verify_first_two_windows(result)
            data_window2 = result.data_windows[2]
            assert len(data_window2.records) == 0

    def test_tumbling_without_group_pattern_b_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "avg(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "avg(duration)",
                    "distinctcount(city)",
                )
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(
                    self.TIMESTAMP - bios.time.minutes(1),
                    bios.time.minutes(4),
                    bios.time.minutes(1),
                )
                .build()
            )
            result = admin.execute(statement)
            self.assertEqual(len(result.data_windows), 2)
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 10)
            self.assertEqual(result.data_windows[0].records[0].get("distinctcount(city)"), 6)
            data_window1 = result.data_windows[1]
            self.assertEqual(data_window1.records[0].get("count()"), 9)
            self.assertEqual(data_window1.records[0].get("sum(score)"), 286)
            self.assertEqual(data_window1.records[0].get("min(score)"), 5)
            self.assertEqual(data_window1.records[0].get("max(score)"), 95)
            self.assertAlmostEqual(data_window1.records[0].get("avg(score)"), 286 / 9, 7)
            self.assertAlmostEqual(data_window1.records[0].get("sum(duration)"), 457.1, 7)
            self.assertAlmostEqual(data_window1.records[0].get("min(duration)"), 11.2, 7)
            self.assertAlmostEqual(data_window1.records[0].get("max(duration)"), 191.4, 7)
            self.assertAlmostEqual(data_window1.records[0].get("avg(duration)"), 457.1 / 9, 7)
            self.assertEqual(data_window1.records[0].get("distinctcount(city)"), 6)

    def test_tumbling_without_group_pattern_b_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            metrics = [
                "count()",
                "sum(score)",
                "min(score)",
                "max(score)",
                "avg(score)",
                "sum(duration)",
                "min(duration)",
                "max(duration)",
                "avg(duration)",
                "distinctcount(city)",
            ]
            statement = (
                bios.isql()
                .select(*metrics)
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(
                    self._snap(self.TIMESTAMP - bios.time.minutes(1), step),
                    bios.time.minutes(4),
                    step,
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)

            def verify_two_windows(result):
                self.assertEqual(result.data_windows[0].records[0].get("count()"), 10)
                data_window1 = result.data_windows[1]
                self.assertEqual(data_window1.records[0].get("count()"), 17)
                self.assertEqual(data_window1.records[0].get("sum(score)"), 562)
                self.assertEqual(data_window1.records[0].get("min(score)"), 3)
                self.assertEqual(data_window1.records[0].get("max(score)"), 95)
                self.assertAlmostEqual(data_window1.records[0].get("avg(score)"), 562 / 17, 7)
                self.assertAlmostEqual(data_window1.records[0].get("sum(duration)"), 908.0, 7)
                self.assertAlmostEqual(data_window1.records[0].get("min(duration)"), 3.8, 7)
                self.assertAlmostEqual(data_window1.records[0].get("max(duration)"), 223.1, 7)
                self.assertAlmostEqual(data_window1.records[0].get("avg(duration)"), 908.0 / 17, 7)
                self.assertEqual(data_window1.records[0].get("distinctcount(city)"), 11)

            self.assertEqual(len(result.data_windows), 2)
            verify_two_windows(result)

            statement = (
                bios.isql()
                .select(*metrics)
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(
                    self.TIMESTAMP - bios.time.minutes(1),
                    bios.time.minutes(4),
                    step,
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            self.assertEqual(len(result.data_windows), 3)
            verify_two_windows(result)
            data_window2 = result.data_windows[2]
            assert len(data_window2.records) == 0

    def _verify_tumbling_with_group_pattern_a_first_window(self, data_windows):
        window0 = data_windows[0]
        self.assertEqual(len(window0.records), 4)
        self.assertEqual(window0.records[0].get("country"), "Australia")
        self.assertEqual(window0.records[0].get("count()"), 5)
        self.assertEqual(window0.records[0].get("sum(score)"), 103)
        self.assertEqual(window0.records[0].get("min(score)"), 10)
        self.assertEqual(window0.records[0].get("max(score)"), 51)
        self.assertAlmostEqual(window0.records[0].get("avg(score)"), 103 / 5, 7)
        self.assertAlmostEqual(window0.records[0].get("sum(duration)"), 87.1, 7)
        self.assertAlmostEqual(window0.records[0].get("min(duration)"), 4.2, 7)
        self.assertAlmostEqual(window0.records[0].get("max(duration)"), 38.2, 7)
        self.assertAlmostEqual(window0.records[0].get("avg(duration)"), 87.1 / 5, 7)
        self.assertEqual(window0.records[0].get("distinctcount(city)"), 3)

        self.assertEqual(window0.records[1].get("country"), "India")
        self.assertEqual(window0.records[1].get("count()"), 1)
        self.assertEqual(window0.records[1].get("sum(score)"), 17)
        self.assertAlmostEqual(window0.records[1].get("avg(score)"), 17, 7)
        self.assertEqual(window0.records[1].get("sum(duration)"), 1.2)
        self.assertAlmostEqual(window0.records[1].get("avg(duration)"), 1.2, 7)
        self.assertEqual(window0.records[1].get("distinctcount(city)"), 1)

        self.assertEqual(window0.records[2].get("country"), "Japan")
        self.assertEqual(window0.records[2].get("count()"), 4)
        self.assertEqual(window0.records[2].get("sum(score)"), 96)
        self.assertAlmostEqual(window0.records[2].get("avg(score)"), 24, 7)
        self.assertEqual(window0.records[2].get("sum(duration)"), 225)
        self.assertAlmostEqual(window0.records[2].get("avg(duration)"), 56.25, 7)
        self.assertEqual(window0.records[2].get("distinctcount(city)"), 2)

        self.assertEqual(window0.records[3].get("country"), "US")
        self.assertEqual(window0.records[3].get("count()"), 9)
        self.assertEqual(window0.records[3].get("sum(score)"), 270)
        self.assertAlmostEqual(window0.records[3].get("avg(score)"), 270 / 9, 7)
        self.assertAlmostEqual(window0.records[3].get("sum(duration)"), 532.2, 7)
        self.assertAlmostEqual(window0.records[3].get("avg(duration)"), 532.2 / 9, 7)
        self.assertEqual(window0.records[3].get("distinctcount(city)"), 5)

    def test_tumbling_with_group_pattern_a_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "country",
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "avg(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "avg(duration)",
                    "distinctcount(city)",
                )
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)
            self.assertEqual(len(result.data_windows), 2)
            self._verify_tumbling_with_group_pattern_a_first_window(result.data_windows)

            window1 = result.data_windows[1]
            self.assertEqual(len(window1.records), 0)

    def test_tumbling_with_group_and_filter(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("count()", "sum(score)", "sum(duration)")
                .from_signal(self.SIGNAL_NAME)
                .where("country in ('US', 'Japan')")
                .group_by(["city"])
                .order_by("city")
                .tumbling_window(bios.time.minutes(1))
                .time_range(self.TIMESTAMP, bios.time.minutes(3), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result)
            self.assertEqual(len(result.data_windows), 3)
            self._verify_tumbling_with_group_and_filter_common_windows(result.data_windows)

            statement2 = (
                bios.isql()
                .select("count()", "sum(score)", "sum(duration)")
                .from_signal(self.SIGNAL_NAME)
                .where("country in ('US', 'Japan')")
                .group_by(["city"])
                .order_by("city")
                .tumbling_window(bios.time.minutes(1))
                .time_range(
                    self.TIMESTAMP, bios.time.now() - self.TIMESTAMP, bios.time.minutes(1)
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement2)
            # pprint.pprint(result)
            self.assertGreaterEqual(len(result.data_windows), 3)
            self._verify_tumbling_with_group_and_filter_common_windows(result.data_windows)
            window3 = result.data_windows[2]
            self.assertEqual(len(window3.records), 3)
            self.assertEqual(window3.records[0].get("city"), "New York")
            self.assertEqual(window3.records[0].get("count()"), 1)
            self.assertEqual(window3.records[0].get("sum(score)"), 11)
            self.assertAlmostEqual(window3.records[0].get("sum(duration)"), 69.4, 7)

            self.assertEqual(window3.records[1].get("city"), "Redwood City")
            self.assertEqual(window3.records[1].get("count()"), 1)
            self.assertEqual(window3.records[1].get("sum(score)"), 51)
            self.assertAlmostEqual(window3.records[1].get("sum(duration)"), 10.6, 7)

            self.assertEqual(window3.records[2].get("city"), "Yokohama")
            self.assertEqual(window3.records[2].get("count()"), 1)
            self.assertEqual(window3.records[2].get("sum(score)"), 88)
            self.assertAlmostEqual(window3.records[2].get("sum(duration)"), 3.8, 7)

    def _verify_tumbling_with_group_and_filter_common_windows(self, data_windows):
        window0 = data_windows[0]
        self.assertEqual(len(window0.records), 5)
        self.assertEqual(window0.records[0].get("city"), "Campbell")
        self.assertEqual(window0.records[0].get("count()"), 1)
        self.assertEqual(window0.records[0].get("sum(score)"), 22)
        self.assertAlmostEqual(window0.records[0].get("sum(duration)"), 31.6, 7)

        self.assertEqual(window0.records[1].get("city"), "Portland")
        self.assertEqual(window0.records[1].get("count()"), 2)
        self.assertEqual(window0.records[1].get("sum(score)"), 92)
        self.assertAlmostEqual(window0.records[1].get("sum(duration)"), 123.6, 7)

        self.assertEqual(window0.records[2].get("city"), "Redwood City")
        self.assertEqual(window0.records[2].get("count()"), 2)
        self.assertEqual(window0.records[2].get("sum(score)"), 8)
        self.assertAlmostEqual(window0.records[2].get("sum(duration)"), 135.7, 7)

        self.assertEqual(window0.records[3].get("city"), "San Mateo")
        self.assertEqual(window0.records[3].get("count()"), 1)
        self.assertEqual(window0.records[3].get("sum(score)"), 33)
        self.assertAlmostEqual(window0.records[3].get("sum(duration)"), 71.1, 7)

        self.assertEqual(window0.records[4].get("city"), "Tokyo")
        self.assertEqual(window0.records[4].get("count()"), 2)
        self.assertEqual(window0.records[4].get("sum(score)"), 18)
        self.assertAlmostEqual(window0.records[4].get("sum(duration)"), 21.0, 7)

        window1 = data_windows[1]
        self.assertEqual(len(window1.records), 4)
        self.assertEqual(window1.records[0].get("city"), "Campbell")
        self.assertEqual(window1.records[0].get("count()"), 1)
        self.assertEqual(window1.records[0].get("sum(score)"), 95)
        self.assertAlmostEqual(window1.records[0].get("sum(duration)"), 88.0, 7)

        self.assertEqual(window1.records[1].get("city"), "Kobe")
        self.assertEqual(window1.records[1].get("count()"), 1)
        self.assertEqual(window1.records[1].get("sum(score)"), 5)
        self.assertAlmostEqual(window1.records[1].get("sum(duration)"), 191.4, 7)

        self.assertEqual(window1.records[2].get("city"), "Redwood City")
        self.assertEqual(window1.records[2].get("count()"), 1)
        self.assertEqual(window1.records[2].get("sum(score)"), 10)
        self.assertAlmostEqual(window1.records[2].get("sum(duration)"), 11.3, 7)

        self.assertEqual(window1.records[3].get("city"), "Tokyo")
        self.assertEqual(window1.records[3].get("count()"), 2)
        self.assertEqual(window1.records[3].get("sum(score)"), 83)
        self.assertAlmostEqual(window1.records[3].get("sum(duration)"), 83.5, 7)

    def test_tumbling_with_group_and_combined_filter(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("city", "count()", "sum(score)", "sum(duration)")
                .from_signal(self.SIGNAL_NAME)
                .where("country = 'US' AND city in ('Redwood City', 'Campbell')")
                .group_by(["city"])
                .order_by("city")
                .tumbling_window(bios.time.minutes(1))
                .time_range(self.TIMESTAMP, bios.time.minutes(3), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result)
            self.assertEqual(len(result.data_windows), 3)
            self._verify_tumbling_with_group_and_combined_filter_common_windows(
                result.data_windows
            )

            statement2 = (
                bios.isql()
                .select("city", "count()", "sum(score)", "sum(duration)")
                .from_signal(self.SIGNAL_NAME)
                .where("country = 'US' AND city in ('Redwood City', 'Campbell')")
                .group_by(["city"])
                .order_by("city")
                .tumbling_window(bios.time.minutes(1))
                .time_range(
                    self.TIMESTAMP, bios.time.now() - self.TIMESTAMP, bios.time.minutes(1)
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement2)
            # pprint.pprint(result)
            self.assertGreaterEqual(len(result.data_windows), 3)
            self._verify_tumbling_with_group_and_combined_filter_common_windows(
                result.data_windows
            )
            window3 = result.data_windows[2]
            self.assertEqual(len(window3.records), 1)
            self.assertEqual(window3.records[0].get("city"), "Redwood City")
            self.assertEqual(window3.records[0].get("count()"), 1)
            self.assertEqual(window3.records[0].get("sum(score)"), 51)
            self.assertAlmostEqual(window3.records[0].get("sum(duration)"), 10.6, 7)

    def _verify_tumbling_with_group_and_combined_filter_common_windows(self, data_windows):
        window0 = data_windows[0]
        self.assertEqual(len(window0.records), 2)
        self.assertEqual(window0.records[0].get("city"), "Campbell")
        self.assertEqual(window0.records[0].get("count()"), 1)
        self.assertEqual(window0.records[0].get("sum(score)"), 22)
        self.assertAlmostEqual(window0.records[0].get("sum(duration)"), 31.6, 7)

        self.assertEqual(window0.records[1].get("city"), "Redwood City")
        self.assertEqual(window0.records[1].get("count()"), 2)
        self.assertEqual(window0.records[1].get("sum(score)"), 8)
        self.assertAlmostEqual(window0.records[1].get("sum(duration)"), 135.7, 7)

        window1 = data_windows[1]
        self.assertEqual(len(window1.records), 2)
        self.assertEqual(window1.records[0].get("city"), "Campbell")
        self.assertEqual(window1.records[0].get("count()"), 1)
        self.assertEqual(window1.records[0].get("sum(score)"), 95)
        self.assertAlmostEqual(window1.records[0].get("sum(duration)"), 88.0, 7)

        self.assertEqual(window1.records[1].get("city"), "Redwood City")
        self.assertEqual(window1.records[1].get("count()"), 1)
        self.assertEqual(window1.records[1].get("sum(score)"), 10)
        self.assertAlmostEqual(window1.records[1].get("sum(duration)"), 11.3, 7)

    def test_tumbling_without_group_with_combined_filter(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("count()", "sum(score)", "sum(duration)")
                .from_signal(self.SIGNAL_NAME)
                .where("country = 'US' AND city in ('Redwood City', 'Campbell')")
                .tumbling_window(bios.time.minutes(1))
                .time_range(self.TIMESTAMP, bios.time.minutes(3), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result)
            self.assertEqual(len(result.data_windows), 3)
            self._verify_tumbling_without_group_with_combined_filter_common_windows(
                result.data_windows
            )

            statement2 = (
                bios.isql()
                .select("count()", "sum(score)", "sum(duration)")
                .from_signal(self.SIGNAL_NAME)
                .where("country = 'US' AND city in ('Redwood City', 'Campbell')")
                .tumbling_window(bios.time.minutes(1))
                .time_range(
                    self.TIMESTAMP, bios.time.now() - self.TIMESTAMP, bios.time.minutes(1)
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement2)
            # pprint.pprint(result)
            self.assertGreaterEqual(len(result.data_windows), 3)
            self._verify_tumbling_without_group_with_combined_filter_common_windows(
                result.data_windows
            )
            window3 = result.data_windows[2]
            self.assertEqual(len(window3.records), 1)
            self.assertEqual(window3.records[0].get("count()"), 1)
            self.assertEqual(window3.records[0].get("sum(score)"), 51)
            self.assertAlmostEqual(window3.records[0].get("sum(duration)"), 10.6, 7)

    def _verify_tumbling_without_group_with_combined_filter_common_windows(self, data_windows):
        window0 = data_windows[0]
        self.assertEqual(len(window0.records), 1)
        self.assertEqual(window0.records[0].get("count()"), 3)
        self.assertEqual(window0.records[0].get("sum(score)"), 30)
        self.assertAlmostEqual(window0.records[0].get("sum(duration)"), 167.3, 7)

        window1 = data_windows[1]
        self.assertEqual(len(window1.records), 1)
        self.assertEqual(window1.records[0].get("count()"), 2)
        self.assertEqual(window1.records[0].get("sum(score)"), 105)
        self.assertAlmostEqual(window1.records[0].get("sum(duration)"), 99.3, 7)

    def test_tumbling_with_group_pattern_a_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "avg(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "avg(duration)",
                    "distinctcount(city)",
                )
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self._snap(self.TIMESTAMP, step), bios.time.minutes(4), step)
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)
            self.assertEqual(len(result.data_windows), 2)
            self._verify_tumbling_with_group_pattern_a_first_window(result.data_windows)

            window1 = result.data_windows[1]
            self.assertEqual(len(window1.records), 5)
            self.assertEqual(window1.records[0].get("country"), "Australia")
            self.assertEqual(window1.records[0].get("count()"), 1)
            self.assertEqual(window1.records[0].get("sum(score)"), 13)
            self.assertAlmostEqual(window1.records[0].get("avg(score)"), 13, 7)
            self.assertAlmostEqual(window1.records[0].get("sum(duration)"), 4.5, 7)
            self.assertAlmostEqual(window1.records[0].get("avg(duration)"), 4.5, 7)
            self.assertEqual(window1.records[0].get("distinctcount(city)"), 1)

            self.assertEqual(window1.records[1].get("country"), "France")
            self.assertEqual(window1.records[1].get("count()"), 2)
            self.assertEqual(window1.records[1].get("sum(score)"), 26)
            self.assertEqual(window1.records[1].get("min(score)"), 3)
            self.assertEqual(window1.records[1].get("max(score)"), 23)
            self.assertAlmostEqual(window1.records[1].get("avg(score)"), 13, 7)
            self.assertAlmostEqual(window1.records[1].get("sum(duration)"), 242.6, 7)
            self.assertAlmostEqual(window1.records[1].get("min(duration)"), 19.5, 7)
            self.assertAlmostEqual(window1.records[1].get("max(duration)"), 223.1, 7)
            self.assertAlmostEqual(window1.records[1].get("avg(duration)"), 121.3, 7)
            self.assertEqual(window1.records[1].get("distinctcount(city)"), 2)

            self.assertEqual(window1.records[2].get("country"), "India")
            self.assertEqual(window1.records[2].get("count()"), 2)
            self.assertEqual(window1.records[2].get("sum(score)"), 87)
            self.assertAlmostEqual(window1.records[2].get("avg(score)"), 43.5, 7)
            self.assertAlmostEqual(window1.records[2].get("sum(duration)"), 120, 7)
            self.assertAlmostEqual(window1.records[2].get("avg(duration)"), 60, 7)
            self.assertEqual(window1.records[2].get("distinctcount(city)"), 1)

            self.assertEqual(window1.records[3].get("country"), "Japan")
            self.assertEqual(window1.records[3].get("count()"), 1)
            self.assertEqual(window1.records[3].get("sum(score)"), 88)
            self.assertAlmostEqual(window1.records[3].get("avg(score)"), 88, 7)
            self.assertAlmostEqual(window1.records[3].get("sum(duration)"), 3.8, 7)
            self.assertAlmostEqual(window1.records[3].get("avg(duration)"), 3.8, 7)
            self.assertEqual(window1.records[3].get("distinctcount(city)"), 1)

            self.assertEqual(window1.records[4].get("country"), "US")
            self.assertEqual(window1.records[4].get("count()"), 2)
            self.assertEqual(window1.records[4].get("sum(score)"), 62)
            self.assertAlmostEqual(window1.records[4].get("avg(score)"), 31, 7)
            self.assertAlmostEqual(window1.records[4].get("sum(duration)"), 80, 7)
            self.assertAlmostEqual(window1.records[4].get("avg(duration)"), 40, 7)
            self.assertEqual(window1.records[4].get("distinctcount(city)"), 2)

    def _verify_tumbling_with_group_pattern_b_first_window(self, data_windows):
        window0 = data_windows[0]
        self.assertEqual(len(window0.records), 4)
        self.assertEqual(window0.records[0].get("country"), "Australia")
        self.assertEqual(window0.records[0].get("count()"), 1)
        self.assertEqual(window0.records[0].get("sum(score)"), 10)
        self.assertEqual(window0.records[0].get("min(score)"), 10)
        self.assertEqual(window0.records[0].get("max(score)"), 10)
        self.assertAlmostEqual(window0.records[0].get("avg(score)"), 10, 7)
        self.assertAlmostEqual(window0.records[0].get("sum(duration)"), 4.2, 7)
        self.assertAlmostEqual(window0.records[0].get("min(duration)"), 4.2, 7)
        self.assertAlmostEqual(window0.records[0].get("max(duration)"), 4.2, 7)
        self.assertAlmostEqual(window0.records[0].get("avg(duration)"), 4.2, 7)
        self.assertEqual(window0.records[0].get("distinctcount(city)"), 1)

        self.assertEqual(window0.records[1].get("country"), "India")
        self.assertEqual(window0.records[1].get("count()"), 1)
        self.assertEqual(window0.records[1].get("sum(score)"), 17)
        self.assertAlmostEqual(window0.records[1].get("avg(score)"), 17, 7)
        self.assertAlmostEqual(window0.records[1].get("sum(duration)"), 1.2, 7)
        self.assertAlmostEqual(window0.records[1].get("avg(duration)"), 1.2, 7)
        self.assertEqual(window0.records[1].get("distinctcount(city)"), 1)

        self.assertEqual(window0.records[2].get("country"), "Japan")
        self.assertEqual(window0.records[2].get("count()"), 2)
        self.assertEqual(window0.records[2].get("sum(score)"), 18)
        self.assertAlmostEqual(window0.records[2].get("avg(score)"), 9, 7)
        self.assertAlmostEqual(window0.records[2].get("sum(duration)"), 21.0, 7)
        self.assertAlmostEqual(window0.records[2].get("avg(duration)"), 10.5, 7)
        self.assertEqual(window0.records[2].get("distinctcount(city)"), 1)

        self.assertEqual(window0.records[3].get("country"), "US")
        self.assertEqual(window0.records[3].get("count()"), 6)
        self.assertEqual(window0.records[3].get("sum(score)"), 155)
        self.assertAlmostEqual(window0.records[3].get("avg(score)"), 155 / 6, 7)
        self.assertAlmostEqual(window0.records[3].get("sum(duration)"), 362, 7)
        self.assertAlmostEqual(window0.records[3].get("avg(duration)"), 362 / 6, 7)
        self.assertEqual(window0.records[3].get("distinctcount(city)"), 4)

    def test_tumbling_with_group_pattern_b_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "country",
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "avg(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "avg(duration)",
                    "distinctcount(city)",
                )
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(
                    self.TIMESTAMP - bios.time.minutes(1),
                    bios.time.minutes(4),
                    bios.time.minutes(1),
                )
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)
            self.assertEqual(len(result.data_windows), 2)
            self._verify_tumbling_with_group_pattern_b_first_window(result.data_windows)

            window1 = result.data_windows[1]
            self.assertEqual(len(window1.records), 3)
            self.assertEqual(window1.records[0].get("country"), "Australia")
            self.assertEqual(window1.records[0].get("count()"), 4)
            self.assertEqual(window1.records[0].get("sum(score)"), 93)
            self.assertEqual(window1.records[0].get("min(score)"), 11)
            self.assertEqual(window1.records[0].get("max(score)"), 51)
            self.assertAlmostEqual(window1.records[0].get("sum(duration)"), 82.9, 7)
            self.assertAlmostEqual(window1.records[0].get("min(duration)"), 11.2, 7)
            self.assertAlmostEqual(window1.records[0].get("max(duration)"), 38.2, 7)
            self.assertEqual(window1.records[0].get("distinctcount(city)"), 3)
            self.assertEqual(window1.records[1].get("country"), "Japan")
            self.assertEqual(window1.records[1].get("count()"), 2)
            self.assertEqual(window1.records[1].get("distinctcount(city)"), 2)
            self.assertEqual(window1.records[2].get("country"), "US")
            self.assertEqual(window1.records[2].get("count()"), 3)
            self.assertEqual(window1.records[2].get("distinctcount(city)"), 3)

    def test_tumbling_with_group_pattern_b_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            statement = (
                bios.isql()
                .select(
                    "country",
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "avg(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "avg(duration)",
                    "distinctcount(city)",
                )
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(
                    self._snap(self.TIMESTAMP - bios.time.minutes(1), step),
                    bios.time.minutes(4),
                    step,
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)
            self.assertEqual(len(result.data_windows), 2)
            self._verify_tumbling_with_group_pattern_b_first_window(result.data_windows)

            window1 = result.data_windows[1]
            self.assertEqual(len(window1.records), 5)
            self.assertEqual(window1.records[0].get("country"), "Australia")
            self.assertEqual(window1.records[0].get("count()"), 5)
            self.assertEqual(window1.records[0].get("sum(score)"), 106)
            self.assertEqual(window1.records[0].get("min(score)"), 11)
            self.assertEqual(window1.records[0].get("max(score)"), 51)
            self.assertAlmostEqual(window1.records[0].get("avg(score)"), 106 / 5, 7)
            self.assertAlmostEqual(window1.records[0].get("sum(duration)"), 87.4, 7)
            self.assertAlmostEqual(window1.records[0].get("min(duration)"), 4.5, 7)
            self.assertAlmostEqual(window1.records[0].get("max(duration)"), 38.2, 7)
            self.assertAlmostEqual(window1.records[0].get("avg(duration)"), 87.4 / 5, 7)
            self.assertEqual(window1.records[0].get("distinctcount(city)"), 3)

            self.assertEqual(window1.records[1].get("country"), "France")
            self.assertEqual(window1.records[1].get("count()"), 2)
            self.assertEqual(window1.records[1].get("sum(score)"), 26)
            self.assertAlmostEqual(window1.records[1].get("avg(score)"), 13, 7)
            self.assertAlmostEqual(window1.records[1].get("sum(duration)"), 242.6, 7)
            self.assertAlmostEqual(window1.records[1].get("avg(duration)"), 121.3, 7)
            self.assertEqual(window1.records[1].get("distinctcount(city)"), 2)

            self.assertEqual(window1.records[2].get("country"), "India")
            self.assertEqual(window1.records[2].get("count()"), 2)
            self.assertEqual(window1.records[2].get("sum(score)"), 87)
            self.assertAlmostEqual(window1.records[2].get("avg(score)"), 43.5, 7)
            self.assertAlmostEqual(window1.records[2].get("sum(duration)"), 120, 7)
            self.assertAlmostEqual(window1.records[2].get("avg(duration)"), 60, 7)
            self.assertEqual(window1.records[2].get("distinctcount(city)"), 1)

            self.assertEqual(window1.records[3].get("country"), "Japan")
            self.assertEqual(window1.records[3].get("count()"), 3)
            self.assertEqual(window1.records[3].get("sum(score)"), 166)
            self.assertAlmostEqual(window1.records[3].get("avg(score)"), 166 / 3, 7)
            self.assertAlmostEqual(window1.records[3].get("sum(duration)"), 207.8, 7)
            self.assertAlmostEqual(window1.records[3].get("avg(duration)"), 207.8 / 3, 7)
            self.assertEqual(window1.records[3].get("distinctcount(city)"), 3)

            self.assertEqual(window1.records[4].get("country"), "US")
            self.assertEqual(window1.records[4].get("count()"), 5)
            self.assertEqual(window1.records[4].get("sum(score)"), 177)
            self.assertAlmostEqual(window1.records[4].get("avg(score)"), 177 / 5, 7)
            self.assertAlmostEqual(window1.records[4].get("sum(duration)"), 250.2, 7)
            self.assertAlmostEqual(window1.records[4].get("avg(duration)"), 250.2 / 5, 7)
            self.assertEqual(window1.records[4].get("distinctcount(city)"), 4)

    def test_hopping_without_group_pattern_a_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("count()")
                .from_signal(self.SIGNAL_NAME)
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(self.TIMESTAMP, bios.time.minutes(6), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 19)
            self.assertEqual(result.data_windows[1].records[0].get("count()"), 19)
            self.assertEqual(len(result.data_windows[2].records), 0)

    def test_hopping_without_group_pattern_a_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            statement = (
                bios.isql()
                .select("count()")
                .from_signal(self.SIGNAL_NAME)
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(self._snap(self.TIMESTAMP, step), bios.time.minutes(6), step)
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 19)
            self.assertEqual(result.data_windows[1].records[0].get("count()"), 27)
            self.assertEqual(result.data_windows[2].records[0].get("count()"), 8)

    def test_hopping_without_group_pattern_b_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("count()")
                .from_signal(self.SIGNAL_NAME)
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(
                    self.TIMESTAMP - bios.time.minutes(1),
                    bios.time.minutes(6),
                    bios.time.minutes(1),
                )
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 10)
            self.assertEqual(result.data_windows[1].records[0].get("count()"), 19)
            self.assertEqual(result.data_windows[2].records[0].get("count()"), 9)

    def test_hopping_without_group_pattern_b_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            statement = (
                bios.isql()
                .select("count()")
                .from_signal(self.SIGNAL_NAME)
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(
                    self._snap(self.TIMESTAMP - bios.time.minutes(1), step),
                    bios.time.minutes(6),
                    step,
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            self.assertEqual(result.data_windows[0].records[0].get("count()"), 10)
            self.assertEqual(result.data_windows[1].records[0].get("count()"), 27)
            self.assertEqual(result.data_windows[2].records[0].get("count()"), 17)

    def test_hopping_with_group_pattern_a_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("country", "count()")
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(self.TIMESTAMP, bios.time.minutes(6), bios.time.minutes(1))
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            data_window0 = result.data_windows[0]
            self.assertEqual(len(data_window0.records), 4)
            self.assertEqual(data_window0.records[0].get("country"), "Australia")
            self.assertEqual(data_window0.records[0].get("count()"), 5)
            data_window1 = result.data_windows[1]
            self.assertEqual(len(data_window1.records), 4)
            self.assertEqual(data_window1.records[0].get("country"), "Australia")
            self.assertEqual(data_window1.records[0].get("count()"), 5)

            self.assertEqual(len(result.data_windows[2].records), 0)

    def test_hopping_with_group_pattern_a_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            statement = (
                bios.isql()
                .select("country", "count()")
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(self._snap(self.TIMESTAMP, step), bios.time.minutes(6), step)
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            data_window0 = result.data_windows[0]
            self.assertEqual(len(data_window0.records), 4)
            self.assertEqual(data_window0.records[0].get("country"), "Australia")
            self.assertEqual(data_window0.records[0].get("count()"), 5)
            data_window1 = result.data_windows[1]
            self.assertEqual(len(data_window1.records), 5)
            self.assertEqual(data_window1.records[0].get("country"), "Australia")
            self.assertEqual(data_window1.records[0].get("count()"), 6)
            data_window2 = result.data_windows[2]
            self.assertEqual(len(data_window2.records), 5)
            self.assertEqual(data_window2.records[0].get("country"), "Australia")
            self.assertEqual(data_window2.records[0].get("count()"), 1)

    def test_hopping_with_group_pattern_b_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select("country", "count()")
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(
                    self.TIMESTAMP - bios.time.minutes(1),
                    bios.time.minutes(6),
                    bios.time.minutes(1),
                )
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            data_window0 = result.data_windows[0]
            self.assertEqual(len(data_window0.records), 4)
            self.assertEqual(data_window0.records[0].get("country"), "Australia")
            self.assertEqual(data_window0.records[0].get("count()"), 1)
            data_window1 = result.data_windows[1]
            self.assertEqual(len(data_window1.records), 4)
            self.assertEqual(data_window1.records[0].get("country"), "Australia")
            self.assertEqual(data_window1.records[0].get("count()"), 5)
            data_window2 = result.data_windows[2]
            self.assertEqual(len(data_window2.records), 3)
            self.assertEqual(data_window2.records[0].get("country"), "Australia")
            self.assertEqual(data_window2.records[0].get("count()"), 4)

    def test_hopping_with_group_pattern_b_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            step = bios.time.minutes(1)
            statement = (
                bios.isql()
                .select("country", "count()")
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .hopping_window(bios.time.minutes(2), 2)
                .time_range(
                    self._snap(self.TIMESTAMP - bios.time.minutes(1), step),
                    bios.time.minutes(6),
                    step,
                )
                .on_the_fly()
                .build()
            )
            result = admin.execute(statement)
            # pprint.pprint(result.data_windows)

            self.assertEqual(len(result.data_windows), 3)
            data_window0 = result.data_windows[0]
            self.assertEqual(len(data_window0.records), 4)
            self.assertEqual(data_window0.records[0].get("country"), "Australia")
            self.assertEqual(data_window0.records[0].get("count()"), 1)
            data_window1 = result.data_windows[1]
            self.assertEqual(len(data_window1.records), 5)
            self.assertEqual(data_window1.records[0].get("country"), "Australia")
            self.assertEqual(data_window1.records[0].get("count()"), 6)
            data_window2 = result.data_windows[2]
            self.assertEqual(len(data_window2.records), 5)
            self.assertEqual(data_window2.records[0].get("country"), "Australia")
            self.assertEqual(data_window2.records[0].get("count()"), 5)

    # Negative cases ####################################################

    def test_tumbling_without_group_no_features_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                )
                .from_signal(self.SIGNAL_NAME_NO_FEATURES)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_tumbling_without_group_no_features_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                )
                .from_signal(self.SIGNAL_NAME_NO_FEATURES)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .on_the_fly()
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_tumbling_with_group_no_features_default(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                )
                .from_signal(self.SIGNAL_NAME_NO_FEATURES)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_tumbling_with_group_no_features_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                )
                .from_signal(self.SIGNAL_NAME_NO_FEATURES)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .on_the_fly()
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_tumbling_without_group_sketches_on_the_fly(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "median(duration)",
                )
                .from_signal(self.SIGNAL_NAME)
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .on_the_fly()
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
            self.assertRegex(
                error_context.exception.message,
                ".*Invalid request: Not supported: "
                "Aggregate MEDIAN is requested in on-the-fly mode.*",
            )

    def test_tumbling_with_group_sketches(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as admin:
            statement = (
                bios.isql()
                .select(
                    "count()",
                    "sum(score)",
                    "min(score)",
                    "max(score)",
                    "sum(duration)",
                    "min(duration)",
                    "max(duration)",
                    "median(duration)",
                )
                .from_signal(self.SIGNAL_NAME)
                .group_by(["country"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(self.TIMESTAMP, bios.time.minutes(4), bios.time.minutes(1))
                .on_the_fly()
                .build()
            )
            with self.assertRaises(ServiceError) as error_context:
                admin.execute(statement)
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
            self.assertRegex(
                error_context.exception.message,
                ".*Invalid request: Data Sketches do not support this query yet*",
            )

    def _snap(self, timestamp, step):
        return int(timestamp / step) * step


if __name__ == "__main__":
    pytest.main(sys.argv)
