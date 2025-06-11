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
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios.models import Metric
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url

from tutils import get_next_checkpoint


class TestSummarizeWithFilters(unittest.TestCase):
    ROLLUP_INTERVAL_SECONDS = 60
    ROLLUP_INTERVAL_MINUTES = int(ROLLUP_INTERVAL_SECONDS / 60)

    SIGNAL_NAME = "summarizeWithFilterTest"
    SIGNAL_CONFIG = {
        "signalName": SIGNAL_NAME,
        "missingAttributePolicy": "Reject",
        "attributes": [
            {"attributeName": "product", "type": "String"},
            {"attributeName": "quantity", "type": "Integer"},
            {"attributeName": "phone", "type": "Integer"},
            {"attributeName": "name", "type": "String"},
        ],
        "postStorageStage": {
            "features": [
                {
                    "featureName": "view_by_phone_product",
                    "dimensions": ["phone", "product"],
                    "attributes": ["quantity"],
                    "featureInterval": 60000,
                },
                {
                    "featureName": "view_by_name_product",
                    "dimensions": ["name", "product"],
                    "attributes": ["quantity"],
                    "featureInterval": 60000,
                },
            ]
        },
    }

    ADMIN_USER = admin_user + "@" + TENANT_NAME

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.INTERVAL = 60000

        admin = bios.login(ep_url(), cls.ADMIN_USER, admin_pass)
        try:
            admin.delete_signal(cls.SIGNAL_NAME)
        except ServiceError:
            pass
        admin.create_signal(cls.SIGNAL_CONFIG)

        # wait if near to upper end of hour
        postprocess_interval = cls.INTERVAL * 2 / 1000
        now = time.time()
        next_checkpoint = get_next_checkpoint(now, postprocess_interval)
        print(
            "now: {}, next_checktpoint: {}".format(
                cls._ts2date(now), cls._ts2date(next_checkpoint)
            )
        )
        eta = next_checkpoint - now
        if eta < postprocess_interval / 2 + 10:
            print("will sleep until", cls._ts2date(eta + 10 + time.time()))
            time.sleep(eta + 10)
        print("now:", cls._ts2date(time.time()))
        print("inserting initial events")

        # ingest a cluster of events to the stream
        initial_events = [
            "mobile,10,9991234567,jack",
            "laptop,20,9991234566,paul",
            "smarttv,30,9991234565,nancy",
            "battery,15,9991234569,paul",
            "tablet,40,9991234566,paul",
            "tv,50,9991234565,nancy",
            "tablet,60,9991234564,nick",
            "tv,15,9991234569,anne",
            "battery,15,9991234569,paul",
            "tv,45,9991234563,eddy",
            "mobile,80,9991234566,paul",
            "battery,15,9991234569,anne",
            "smarttv,90,9991234562,gary",
        ]
        statement = bios.isql().insert().into(cls.SIGNAL_NAME).csv_bulk(initial_events).build()
        resp = admin.execute(statement)

        cls.start = resp.records[0].timestamp
        # cls.start = 1630998260943
        print(f"Start={cls.start}")

        # wait for rollup to complete
        cls._skip_interval(cls.start, interval=cls.INTERVAL)

        print("now:", cls._ts2date(time.time()))
        print("inserting second events")
        # ingest another cluster of events to the stream
        second_events = [
            "smarttv,3,8881234565,michael",
            "smarttv,2,8881234566,oliver",
            "laptop,1,9991234565,nancy",
            "smartphone,5,9991234569,anne",
            "mobile,8,9991234563,eddy",
            "battery,2,8881234565,michael",
            "mobile,3,8881234566,oliver",
            "battery,3,8881234565,june",
            "smarttv,4,9991234566,paul",
            "smartphone,7,9991234567,jack",
        ]
        statement2 = bios.isql().insert().into(cls.SIGNAL_NAME).csv_bulk(second_events).build()
        resp2 = admin.execute(statement2)

        cls.end = resp2.records[-1].timestamp
        # cls.end = 1630998320987
        print(f"End={cls.end}")

        # wait for rollup to complete
        sleep_until = int((cls.end + cls.INTERVAL - 1) / cls.INTERVAL) * cls.INTERVAL / 1000 + 20
        print(f"will sleep until {cls._ts2date(sleep_until)}")
        sleep_time = sleep_until - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)

        admin.close()

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def _skip_interval(cls, origin, interval=None):
        """Method to sleep until the next rollup interval.

        The method sleeps until the next rollup window from the origin time

        Args:
            origin (int): Origin milliseconds.
            interval (int): Interval milliseconds to skip.  The default interval is used
                if not speicfied.
        """
        if not interval:
            interval = 60000
        sleep_time = (origin + interval) / 1000 - time.time()
        time.sleep(sleep_time)

    def setUp(self):
        self.client = bios.login(ep_url(), f"{extract_user}@{TENANT_NAME}", extract_pass)

    def tearDown(self):
        self.client.close()

    def _summarize_checkpoint_floor(self, timestamp, interval):
        return int(timestamp / interval) * interval

    def _dump(self, method, summarize_timestamp, res):
        """Dump test data in case of failure

        Args:
            method (obj): test method
            summarize_timestamp (int): Time when the summarize operation happened
            res (obj): Summarize response
        """
        print("failed:", method.__doc__)
        print("extract time={}", self._ts2date(summarize_timestamp))
        print(
            "start={} ({}), end={} ({})".format(
                self._ts2date(self.start / 1000),
                self.start,
                self._ts2date(self.end / 1000),
                self.end,
            )
        )
        pprint.pprint(res)

    @classmethod
    def _ts2date(self, timestamp):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

    def test_no_filter(self):
        # adjust the time to lower boundary of interval
        interval = 60000
        start_time = self.start
        delta = interval * 2

        statement = (
            bios.isql()
            .select(
                "product", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .group_by(["product"])
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)

        summarize_timestamp = time.time()

        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 6)
            res_entries0 = {x.get("product"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get("battery").get("cnt"), 3)
            self.assertEqual(res_entries0.get("battery").get("qty"), 45)
            self.assertEqual(res_entries0.get("laptop").get("cnt"), 1)
            self.assertEqual(res_entries0.get("laptop").get("qty"), 20)
            self.assertEqual(res_entries0.get("mobile").get("cnt"), 2)
            self.assertEqual(res_entries0.get("mobile").get("qty"), 90)
            self.assertEqual(res_entries0.get("smarttv").get("cnt"), 2)
            self.assertEqual(res_entries0.get("smarttv").get("qty"), 120)
            self.assertEqual(res_entries0.get("tablet").get("cnt"), 2)
            self.assertEqual(res_entries0.get("tablet").get("qty"), 100)
            self.assertEqual(res_entries0.get("tv").get("cnt"), 3)
            self.assertEqual(res_entries0.get("tv").get("qty"), 110)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 5)
            res_entries1 = {x.get("product"): x for x in result.data_windows[1].records}
            self.assertEqual(res_entries1.get("battery").get("cnt"), 2)
            self.assertEqual(res_entries1.get("battery").get("qty"), 5)
            self.assertEqual(res_entries1.get("laptop").get("cnt"), 1)
            self.assertEqual(res_entries1.get("laptop").get("qty"), 1)
            self.assertEqual(res_entries1.get("mobile").get("cnt"), 2)
            self.assertEqual(res_entries1.get("mobile").get("qty"), 11)
            self.assertEqual(res_entries1.get("smartphone").get("cnt"), 2)
            self.assertEqual(res_entries1.get("smartphone").get("qty"), 12)
            self.assertEqual(res_entries1.get("smarttv").get("cnt"), 3)
            self.assertEqual(res_entries1.get("smarttv").get("qty"), 9)
        except Exception:
            self._dump(self.test_no_filter, summarize_timestamp, result)
            raise

    def test_filter_by_phone_equality_without_group(self):
        """5277-001-a One-dimensional single-value filter without grouping"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone = 9991234566")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)
            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )

            self.assertEqual(len(result.data_windows[0].records), 1)
            self.assertEqual(result.data_windows[0].records[0].get("cnt"), 3)
            self.assertEqual(result.data_windows[0].records[0].get("qty"), 140)

            self.assertEqual(len(result.data_windows[1].records), 1)
            self.assertEqual(result.data_windows[1].records[0].get("cnt"), 1)
            self.assertEqual(result.data_windows[1].records[0].get("qty"), 4)
        except Exception:
            self._dump(
                self.test_filter_by_phone_equality_without_group,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_equality_with_group(self):
        """5277-001-b One-dimensional single-value filter group by product (secondary index)"""
        # adjust start time
        interval = 60000
        start_time = self.start
        delta = interval * 2

        statement = (
            bios.isql()
            .select(
                "product", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .group_by(["product"])
            .where("phone = 9991234569")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 2)
            res_entries0 = {x.get("product"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get("battery").get("cnt"), 3)
            self.assertEqual(res_entries0.get("battery").get("qty"), 45)
            self.assertEqual(res_entries0.get("tv").get("cnt"), 1)
            self.assertEqual(res_entries0.get("tv").get("qty"), 15)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            res_entries1 = {x.get("product"): x for x in result.data_windows[1].records}
            self.assertEqual(res_entries1.get("smartphone").get("cnt"), 1)
            self.assertEqual(res_entries1.get("smartphone").get("qty"), 5)
        except Exception:
            self._dump(self.test_filter_by_phone_equality_with_group, summarize_timestamp, result)
            raise

    def test_filter_by_phone_equality_without_match_partially(self):
        """5277-001-c One-dimensional single-value filter without grouping"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone = 8881234566")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 0)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)

            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 2)
            self.assertEqual(entry1.get("qty"), 5)
        except Exception:
            self._dump(
                self.test_filter_by_phone_equality_without_match_partially,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_equality_without_match_at_all(self):
        """5277-001-d One-dimensional single-value filter without grouping"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone = 7891234566")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 0)
            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 0)
        except Exception:
            self._dump(
                self.test_filter_by_phone_equality_without_match_at_all,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_multi_equality_without_group(self):
        """5277-002-a One-dimensional multi-value filter witnout group"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)"), Metric("count()"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone IN (9991234564, 9991234566, 8881234566)")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)

            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("count()"), 4)
            self.assertEqual(entry0.get("sum(quantity)"), 200)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)

            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("count()"), 3)
            self.assertEqual(entry1.get("sum(quantity)"), 9)
        except Exception:
            self._dump(
                self.test_filter_by_phone_multi_equality_without_group,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_multi_equality_with_group(self):
        """5277-002-b One-dimensional multi-value filter group by secondary index (product)"""
        # adjust start time
        interval = 60000
        start_time = self.start
        delta = interval * 2

        statement = (
            bios.isql()
            .select(
                "product", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .group_by(["product"])
            .where("phone IN (9991234569, 8881234565, 9991234567)")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 3)
            res_entries0 = {x.get("product"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get("battery").get("cnt"), 3)
            self.assertEqual(res_entries0.get("battery").get("qty"), 45)
            self.assertEqual(res_entries0.get("mobile").get("cnt"), 1)
            self.assertEqual(res_entries0.get("mobile").get("qty"), 10)
            self.assertEqual(res_entries0.get("tv").get("cnt"), 1)
            self.assertEqual(res_entries0.get("tv").get("qty"), 15)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 3)
            res_entries1 = {x.get("product"): x for x in result.data_windows[1].records}
            self.assertEqual(res_entries1.get("battery").get("cnt"), 2)
            self.assertEqual(res_entries1.get("battery").get("qty"), 5)
            self.assertEqual(res_entries1.get("smartphone").get("cnt"), 2)
            self.assertEqual(res_entries1.get("smartphone").get("qty"), 12)
            self.assertEqual(res_entries1.get("smarttv").get("cnt"), 1)
            self.assertEqual(res_entries1.get("smarttv").get("qty"), 3)
        except Exception:
            self._dump(
                self.test_filter_by_phone_multi_equality_with_group,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_multi_equality_with_group2(self):
        """5277-002-c One-dimensional multi-value filter group by primary index (phone)"""
        # adjust start time
        interval = 60000
        start_time = self.start
        delta = interval * 2

        statement = (
            bios.isql()
            .select(
                "phone", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .group_by(["phone"])
            .where("phone IN (9991234569, 777, 8881234565, 9991234567)")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 2)
            res_entries0 = {x.get("phone"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get(9991234567).get("cnt"), 1)
            self.assertEqual(res_entries0.get(9991234567).get("qty"), 10)
            self.assertEqual(res_entries0.get(9991234569).get("cnt"), 4)
            self.assertEqual(res_entries0.get(9991234569).get("qty"), 60)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 3)
            res_entries1 = {x.get("phone"): x for x in result.data_windows[1].records}
            self.assertEqual(res_entries1.get(8881234565).get("cnt"), 3)
            self.assertEqual(res_entries1.get(8881234565).get("qty"), 8)
            self.assertEqual(res_entries1.get(9991234567).get("cnt"), 1)
            self.assertEqual(res_entries1.get(9991234567).get("qty"), 7)
            self.assertEqual(res_entries1.get(9991234569).get("cnt"), 1)
            self.assertEqual(res_entries1.get(9991234569).get("qty"), 5)
        except Exception:
            self._dump(
                self.test_filter_by_phone_multi_equality_with_group2,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_multi_equality_with_group3(self):
        """5277-002-d One-dimensional multi-value filter group by non-index (name)
        --- not feasible
        """
        # adjust start time
        interval = 60000
        start_time = self.start
        delta = interval * 2
        try:
            statement = (
                bios.isql()
                .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
                .from_signal(self.SIGNAL_NAME)
                .group_by(["name"])
                .where("phone IN (9991234569, 777, 8881234565, 9991234567)")
                .tumbling_window(interval)
                .time_range(start_time, delta, interval)
                .build()
            )
            self.client.execute(statement)
            self.fail("exception is expected")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)

    def test_filter_by_phone_single_comparator_without_group(self):
        """5277-003-a One-dimensional single-value comparator witnout group"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)"), Metric("count()"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone <= 9991234563")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)

            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("count()"), 2)
            self.assertEqual(entry0.get("sum(quantity)"), 135)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)

            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("count()"), 6)
            self.assertEqual(entry1.get("sum(quantity)"), 21)
        except Exception:
            self._dump(
                self.test_filter_by_phone_single_comparator_without_group,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_single_comparator_with_group(self):
        """5277-003-b One-dimensional single-value comparator group by secondary index (product)"""
        # adjust start time
        interval = 60000
        start_time = self.start
        delta = interval * 2
        statement = (
            bios.isql()
            .select(
                "product", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .group_by(["product"])
            .where("phone <= 9991234563")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 2)
            res_entries0 = {x.get("product"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get("smarttv").get("cnt"), 1)
            self.assertEqual(res_entries0.get("smarttv").get("qty"), 90)
            self.assertEqual(res_entries0.get("tv").get("cnt"), 1)
            self.assertEqual(res_entries0.get("tv").get("qty"), 45)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 3)
            res_entries1 = {x.get("product"): x for x in result.data_windows[1].records}
            self.assertEqual(res_entries1.get("battery").get("cnt"), 2)
            self.assertEqual(res_entries1.get("battery").get("qty"), 5)
            self.assertEqual(res_entries1.get("mobile").get("cnt"), 2)
            self.assertEqual(res_entries1.get("mobile").get("qty"), 11)
            self.assertEqual(res_entries1.get("smarttv").get("cnt"), 2)
            self.assertEqual(res_entries1.get("smarttv").get("qty"), 5)
        except Exception:
            self._dump(
                self.test_filter_by_phone_single_comparator_with_group,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_single_comparator_with_group2(self):
        """5277-003-c One-dimensional single-value comparator group by primary index (phone)"""
        # adjust start time
        interval = 60000
        start_time = self.start
        delta = interval * 2
        statement = (
            bios.isql()
            .select(
                "phone", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .group_by(["phone"])
            .where("phone <= 9991234563")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, interval)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 2)
            res_entries0 = {x.get("phone"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get(9991234562).get("cnt"), 1)
            self.assertEqual(res_entries0.get(9991234562).get("qty"), 90)
            self.assertEqual(res_entries0.get(9991234563).get("cnt"), 1)
            self.assertEqual(res_entries0.get(9991234563).get("qty"), 45)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 3)
            res_entries1 = {x.get("phone"): x for x in result.data_windows[1].records}
            self.assertEqual(res_entries1.get(8881234565).get("cnt"), 3)
            self.assertEqual(res_entries1.get(8881234565).get("qty"), 8)
            self.assertEqual(res_entries1.get(8881234566).get("cnt"), 2)
            self.assertEqual(res_entries1.get(8881234566).get("qty"), 5)
            self.assertEqual(res_entries1.get(9991234563).get("cnt"), 1)
            self.assertEqual(res_entries1.get(9991234563).get("qty"), 8)
        except Exception:
            self._dump(
                self.test_filter_by_phone_single_comparator_with_group2,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_phone_multi_comparators(self):
        """5277-004 One-dimensional multi-value comparators witnout group"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)"), Metric("count()"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone > 8881234565 AND phone < 9991234567")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("count()"), 8)
            self.assertEqual(entry0.get("sum(quantity)"), 415)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("count()"), 5)
            self.assertEqual(entry1.get("sum(quantity)"), 18)
        except Exception:
            self._dump(self.test_filter_by_phone_multi_comparators, summarize_timestamp, result)
            raise

    def test_filter_for_secondary_index(self):
        """5277-005 One-dimensional single-value filter against an index secondary key (product)"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)"), Metric("count()"))
            .from_signal(self.SIGNAL_NAME)
            .where("product = 'mobile'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("count()"), 2)
            self.assertEqual(entry0.get("sum(quantity)"), 90)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("count()"), 2)
            self.assertEqual(entry1.get("sum(quantity)"), 11)
        except Exception:
            self._dump(self.test_filter_for_secondary_index, summarize_timestamp, result)
            raise

    def test_filter_single_comparator_for_secondary_index(self):
        """5277-007 One-dimensional single-value comparator against an index secondary key
        (product)
        """
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)"), Metric("count()"))
            .from_signal(self.SIGNAL_NAME)
            .where("product > 'smarttv'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("count()"), 5)
            self.assertEqual(entry0.get("sum(quantity)"), 210)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 0)
        except Exception:
            self._dump(
                self.test_filter_single_comparator_for_secondary_index,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_by_name_equality(self):
        """5277-008-a One-dimensional single-value filter for string primary index"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("name = 'paul'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        # res = self.client.summarize(
        #     start_time,
        #     delta,
        #     interval,
        #     aggregates=[Sum("quantity", "qty"), Count(output_as="cnt")],
        #     filter="name = 'paul'",
        # )
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 5)
            self.assertEqual(entry0.get("qty"), 170)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 1)
            self.assertEqual(entry1.get("qty"), 4)
        except Exception:
            self._dump(self.test_filter_by_name_equality, summarize_timestamp, result)
            raise

    def test_filter_by_name_equality_partially_mismatch(self):
        """5277-008-b One-dimensional single-value filter for string primary index,
        no match in the second period"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("name = 'gary'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 1)
            self.assertEqual(entry0.get("qty"), 90)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 0)
        except Exception:
            self._dump(self.test_filter_by_name_equality, summarize_timestamp, result)
            raise

    def test_filter_by_name_multi_equality(self):
        """5277-009 One-dimensional multi-value filter for string primary index"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        summarize_filter = "name IN ('nick', 'michael', 'gary', 'anne', 'oliver')"
        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 4)
            self.assertEqual(entry0.get("qty"), 180)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 5)
            self.assertEqual(entry1.get("qty"), 15)
        except Exception:
            self._dump(self.test_filter_by_name_multi_equality, summarize_timestamp, result)
            raise

    def test_filter_by_name_single_comparator(self):
        """5277-010 One-dimensional single comparator for string primary index"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("name < 'n'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 5)
            self.assertEqual(entry0.get("qty"), 175)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 6)
            self.assertEqual(entry1.get("qty"), 28)
        except Exception:
            self._dump(self.test_filter_by_name_single_comparator, summarize_timestamp, result)
            raise

    def test_filter_by_name_multi_comparators(self):
        """5277-011 One-dimensional multi comparators for string primary index"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("name > 'michael' AND name <= 'paul'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 8)
            self.assertEqual(entry0.get("qty"), 310)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 4)
            self.assertEqual(entry1.get("qty"), 10)
        except Exception:
            self._dump(self.test_filter_by_name_multi_comparators, summarize_timestamp, result)
            raise

    def test_filter_by_quantity_single_equality(self):
        """5277-012 One-dimensional single equality for non-indexed attribute (quantity)"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("quantity = 15")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.client.execute(statement)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_filter_by_quantity_multi_equality(self):
        """5277-013 One-dimensional multi equality for non-indexed attribute (quantity)
        -- not feasible
        """
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        try:
            statement = (
                bios.isql()
                .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
                .from_signal(self.SIGNAL_NAME)
                .where("quantity IN (15, 20, 5, 89)")
                .tumbling_window(interval)
                .time_range(start_time, delta, interval)
                .build()
            )
            self.client.execute(statement)
            self.fail("exception is expected")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)

    def test_filter_by_quantity_single_comparator(self):
        """5277-014 One-dimensional single comparator for non-indexed attribute (quantity)"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("quantity >= 5 AND quantity < 30")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.client.execute(statement)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_filter_two_dimension_single_equality(self):
        """5277-015 Two-dimensional single equality filter for primary and secondary indexes
        (phone & product)
        """
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where("phone = 8881234565 AND product = 'battery'")
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 0)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 2)
            self.assertEqual(entry1.get("qty"), 5)
        except Exception:
            self._dump(self.test_filter_two_dimension_single_equality, summarize_timestamp, result)
            raise

    def test_filter_two_dimension_multi_equality(self):
        """5277-016 Two-dimensional multi equality filter for primary and secondary indexes
        (phone & product)
        """
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        summarize_filter = (
            "phone IN (8881234565, 9991234569) AND product IN ('battery', 'smarttv')"
        )
        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 3)
            self.assertEqual(entry0.get("qty"), 45)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 3)
            self.assertEqual(entry1.get("qty"), 8)
        except Exception:
            self._dump(self.test_filter_two_dimension_multi_equality, summarize_timestamp, result)
            raise

    def test_filter_two_dimension_single_equality_and_comparator(self):
        """5277-017 Two-dimensional multi equality filter for primary index and comparator for
        secondary index
        """
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        summarize_filter = "phone IN (8881234565, 9991234566) AND product > 'laptop'"
        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 2)
            self.assertEqual(entry0.get("qty"), 120)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 2)
            self.assertEqual(entry1.get("qty"), 7)
        except Exception:
            self._dump(
                self.test_filter_two_dimension_single_equality_and_comparator,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_two_dimension_comparator_and_equality(self):
        """5277-018 Two-dimensional; comparator for primary index and equality for
        secondary index"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        summarize_filter = "phone < 9991234566 AND product = 'smarttv'"
        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 2)
            self.assertEqual(entry0.get("qty"), 120)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 2)
            self.assertEqual(entry1.get("qty"), 5)
        except Exception:
            self._dump(
                self.test_filter_two_dimension_comparator_and_equality,
                summarize_timestamp,
                result,
            )
            raise

    # @unittest.skip("BB-1351 expected behavior has been changed")
    def test_filter_two_dimension_comparator_and_multi_equality(self):
        """5277-019 Two-dimensional; comparator for primary index and multi equality for secondary
        index
        """
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        summarize_filter = "phone < 9991234566 AND product IN ('battery', 'smarttv')"
        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            entry0 = result.data_windows[0].records[0]
            self.assertEqual(entry0.get("cnt"), 2)
            self.assertEqual(entry0.get("qty"), 120)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + interval
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            entry1 = result.data_windows[1].records[0]
            self.assertEqual(entry1.get("cnt"), 4)
            self.assertEqual(entry1.get("qty"), 10)
        except Exception:
            self._dump(
                self.test_filter_two_dimension_comparator_and_equality,
                summarize_timestamp,
                result,
            )
            raise

    def test_filter_two_dimensional_equalities_for_non_index(self):
        """5277-020 Two-dimensional; equality and comparator for non index"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2

        summarize_filter = "phone = 9991234566 AND quantity < 50"
        statement = (
            bios.isql()
            .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .tumbling_window(interval)
            .time_range(start_time, delta, interval)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.client.execute(statement)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_filter_combine_multi_intervals(self):
        """5277-022 One-dimensional; multi-equality, longer interval"""
        # adjust start time
        interval = 120000
        start_time = self._summarize_checkpoint_floor(self.start, 60000)
        delta = interval

        summarize_filter = "phone IN (9991234566, 8881234566, 991234564)"
        statement = (
            bios.isql()
            .select(
                "phone", Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt")
            )
            .from_signal(self.SIGNAL_NAME)
            .where(summarize_filter)
            .group_by(["phone"])
            .tumbling_window(interval)
            .time_range(start_time, delta, 60000)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = start_time

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 2)
            res_entries0 = {x.get("phone"): x for x in result.data_windows[0].records}
            self.assertEqual(res_entries0.get(8881234566).get("cnt"), 2)
            self.assertEqual(res_entries0.get(8881234566).get("qty"), 5)
            self.assertEqual(res_entries0.get(9991234566).get("cnt"), 4)
            self.assertEqual(res_entries0.get(9991234566).get("qty"), 144)
        except Exception:
            self._dump(self.test_filter_combine_multi_intervals, summarize_timestamp, result)
            raise

    def test_filter_syntax_error(self):
        """5277-023 Negative; filter syntax error"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2
        try:
            summarize_filter = "phone == 9991234566"
            statement = (
                bios.isql()
                .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
                .from_signal(self.SIGNAL_NAME)
                .where(summarize_filter)
                .tumbling_window(interval)
                .time_range(start_time, delta, 60000)
                .build()
            )
            self.client.execute(statement)
            self.fail("exception is expected")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)

    def test_filter_invalid_value(self):
        """5277-024 Negative; filter invalid value error for invalid value specification"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2
        try:
            summarize_filter = "phone = '9991234566'"
            statement = (
                bios.isql()
                .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
                .from_signal(self.SIGNAL_NAME)
                .where(summarize_filter)
                .tumbling_window(interval)
                .time_range(start_time, delta, 60000)
                .build()
            )
            self.client.execute(statement)
            self.fail("exception is expected")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)

    def test_filter_invalid_value2(self):
        """5277-025 Negative; filter invalid value error for invalid value specification"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2
        try:
            summarize_filter = "name = 1234"
            statement = (
                bios.isql()
                .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
                .from_signal(self.SIGNAL_NAME)
                .where(summarize_filter)
                .tumbling_window(interval)
                .time_range(start_time, delta, 60000)
                .build()
            )
            self.client.execute(statement)
            self.fail("exception is expected")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)

    def test_filter_for_nonexisting_attribute(self):
        """5277-026 Negative; filter for non-existing attribute"""
        # adjust start time
        interval = 60000
        start_time = self._summarize_checkpoint_floor(self.start, interval)
        delta = interval * 2
        try:
            summarize_filter = "name = '1234' AND nosuch = 'abc'"
            statement = (
                bios.isql()
                .select(Metric("sum(quantity)").set_as("qty"), Metric("count()").set_as("cnt"))
                .from_signal(self.SIGNAL_NAME)
                .where(summarize_filter)
                .tumbling_window(interval)
                .time_range(start_time, delta, 60000)
                .build()
            )
            self.client.execute(statement)
            self.fail("exception is expected")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)


if __name__ == "__main__":
    pytest.main(sys.argv)
