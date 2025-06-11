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

import logging
import os
import sys
import time
import unittest
from pprint import pprint

import bios
import pytest
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_signal, setup_tenant_config, try_delete_signal
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_rollup_intervals
from tutils import b64encStr, get_next_checkpoint, skip_interval

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

INTERVAL_MS = get_rollup_intervals()
SIGNAL_NAME = "signal_rollup_alltypes"
SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "intAttribute", "type": "Integer"},
        {"attributeName": "intAttribute1", "type": "Integer"},
        {"attributeName": "stringAttribute", "type": "String"},
        {"attributeName": "doubleAttribute", "type": "Decimal"},
        {"attributeName": "doubleAttribute1", "type": "Decimal"},
        {"attributeName": "booleanAttribute", "type": "Boolean"},
        {
            "attributeName": "enumAttribute",
            "type": "String",
            "allowedValues": [
                "enum_const_1",
                "enum_const_2",
                "enum_const_3",
                "enum_const_4",
            ],
        },
        {"attributeName": "blobAttribute", "type": "Blob"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "groupByStringInt",
                "dimensions": ["stringAttribute", "intAttribute"],
                "attributes": ["intAttribute1", "doubleAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByEnumDouble",
                "dimensions": ["enumAttribute", "doubleAttribute"],
                "attributes": ["intAttribute1", "doubleAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByBoolean",
                "dimensions": ["booleanAttribute"],
                "attributes": ["intAttribute1", "doubleAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByBlob",
                "dimensions": ["blobAttribute"],
                "attributes": ["intAttribute1", "doubleAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByString",
                "dimensions": ["stringAttribute"],
                "attributes": [],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByEmpty",
                "dimensions": [],
                "attributes": [],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByEmptyInt",
                "dimensions": [],
                "attributes": ["intAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "groupByEmptyIntDouble",
                "dimensions": [],
                "attributes": ["intAttribute1", "doubleAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "distinctManyGroupNoAttribute",
                "dimensions": ["stringAttribute", "enumAttribute", "booleanAttribute"],
                "attributes": [],
                "featureInterval": INTERVAL_MS,
            },
        ]
    },
}


class TestRollupAlltypes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_signal(cls.session, SIGNAL_CONFIG)

    @classmethod
    def tearDownClass(cls):
        try_delete_signal(cls.session, SIGNAL_NAME)
        cls.session.close()

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    @classmethod
    def upsert_into_context(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().upsert().into(stream).csv_bulk(records).build()
        cls.session.execute(request)

    @classmethod
    def adjust_start_time(cls, interval=None):
        """Method to adjust start time of the first ingest."""
        now = time.time()
        if not interval:
            interval = 60
        checkpoint = get_next_checkpoint(now, interval)
        if checkpoint - now < 20:
            time_to_sleep = checkpoint - now + 5
            print(f"adjust_start_time(): sleeping for {time_to_sleep} seconds")
            time.sleep(time_to_sleep)

    @classmethod
    def get_window_start_time(cls, start, interval=None):
        """Calculates the window start time for a rollup.

        Args:
            start (int): Timestamp in milliseconds
            interval (int): Rollup interval in milliseconds

        Returns: int: The rollup timestamp
        """
        if interval is None:
            interval = 60000
        return int(start / interval) * interval

    def test(self):
        self.adjust_start_time()
        initial_events = [
            "1,111,tom,8.11,11.11,false,enum_const_1," + b64encStr("blobData1"),
            "1,222,tom,8.11,22.22,false,enum_const_1," + b64encStr("blobData1"),
            "1,333,tom,8.11,33.33,false,enum_const_1," + b64encStr("blobData1"),
            "2,444,jerry,8.22,44.44,false,enum_const_2," + b64encStr("blobData2"),
            "2,555,jerry,8.22,55.55,false,enum_const_2," + b64encStr("blobData2"),
            "3,666,ram,8.33,66.66,false,enum_const_3," + b64encStr("blobData3"),
        ]
        start1 = self.insert_into_signal(SIGNAL_NAME, initial_events)
        skip_interval(start1)

        second_events = [
            "1,111,tom,8.11,11.11,false,enum_const_1," + b64encStr("blobData1"),
            "1,222,tom,8.11,22.22,false,enum_const_1," + b64encStr("blobData1"),
            "1,333,tom,8.11,33.33,false,enum_const_1," + b64encStr("blobData1"),
            "2,444,jerry,8.22,44.44,false,enum_const_2," + b64encStr("blobData2"),
            "2,555,jerry,8.22,55.55,false,enum_const_2," + b64encStr("blobData2"),
            "3,666,ram,8.33,66.66,false,enum_const_3," + b64encStr("blobData3"),
        ]
        start2 = self.insert_into_signal(SIGNAL_NAME, second_events)
        skip_interval(start2)
        time.sleep(10)

        select_request = (
            bios.isql()
            .select(
                "stringAttribute",
                "intAttribute",
                "count()",
                "sum(intAttribute1)",
                "min(intAttribute1)",
                "max(intAttribute1)",
                "sum(doubleAttribute1)",
                "min(doubleAttribute1)",
                "max(doubleAttribute1)",
            )
            .from_signal(SIGNAL_NAME)
            .group_by(["stringAttribute", "intAttribute"])
            .tumbling_window(bios.time.minutes(1))
            .time_range(start1, bios.time.minutes(2), bios.time.minutes(1))
            .build()
        )

        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        pprint(resp)
        sys.stdout.flush()
        self.assertIsNotNone(resp.get_data_windows())
        self.assertEqual(len(resp.get_data_windows()), 2)

        for i, window in enumerate(resp.get_data_windows()):
            if i == 0:
                win_beg_time = self.get_window_start_time(start1)
            else:
                win_beg_time = self.get_window_start_time(start2)
            self.assertEqual(window.window_begin_time, win_beg_time)
            self.assertEqual(len(window.records), 3)

            expected = {
                "stringAttribute": ["jerry", "ram", "tom"],
                "intAttribute": [2, 3, 1],
                "count()": [2, 1, 3],
                "sum(intAttribute1)": [999, 666, 666],
                "min(intAttribute1)": [444, 666, 111],
                "max(intAttribute1)": [555, 666, 333],
                "sum(doubleAttribute1)": [99.99, 66.66, 66.66],
                "min(doubleAttribute1)": [44.44, 66.66, 11.11],
                "max(doubleAttribute1)": [55.55, 66.66, 33.33],
            }
            for j, record in enumerate(window.records):
                for key, value in expected.items():
                    self.assertEqual(record.get(key), value[j])

        select_request = (
            bios.isql()
            .select(
                "enumAttribute",
                "doubleAttribute",
                "count()",
                "sum(intAttribute1)",
                "min(intAttribute1)",
                "max(intAttribute1)",
                "sum(doubleAttribute1)",
                "min(doubleAttribute1)",
                "max(doubleAttribute1)",
            )
            .from_signal(SIGNAL_NAME)
            .group_by(["enumAttribute", "doubleAttribute"])
            .tumbling_window(bios.time.minutes(1))
            .time_range(start1, bios.time.minutes(1), bios.time.minutes(1))
            .build()
        )

        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        pprint(resp)
        sys.stdout.flush()
        self.assertIsNotNone(resp.get_data_windows())
        self.assertEqual(len(resp.get_data_windows()), 1)
        window = resp.get_data_windows()[0]
        self.assertEqual(window.window_begin_time, self.get_window_start_time(start1))
        self.assertEqual(len(window.records), 3)
        expected = {
            "enumAttribute": ["enum_const_1", "enum_const_2", "enum_const_3"],
            "doubleAttribute": [8.11, 8.22, 8.33],
            "count()": [3, 2, 1],
            "sum(intAttribute1)": [666, 999, 666],
            "min(intAttribute1)": [111, 444, 666],
            "max(intAttribute1)": [333, 555, 666],
            "sum(doubleAttribute1)": [66.66, 99.99, 66.66],
            "min(doubleAttribute1)": [11.11, 44.44, 66.66],
            "max(doubleAttribute1)": [33.33, 55.55, 66.66],
        }
        for i, record in enumerate(window.records):
            for key, value in expected.items():
                self.assertEqual(record.get(key), value[i])

        select_request = (
            bios.isql()
            .select(
                "booleanAttribute",
                "count()",
                "sum(intAttribute1)",
                "min(intAttribute1)",
                "max(intAttribute1)",
                "min(doubleAttribute1)",
                "max(doubleAttribute1)",
            )
            .from_signal(SIGNAL_NAME)
            .group_by(["booleanAttribute"])
            .tumbling_window(bios.time.minutes(1))
            .time_range(start2, bios.time.minutes(1), bios.time.minutes(1))
            .build()
        )

        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        pprint(resp)
        sys.stdout.flush()
        self.assertIsNotNone(resp.get_data_windows())
        self.assertEqual(len(resp.get_data_windows()), 1)
        window = resp.get_data_windows()[0]
        self.assertEqual(window.window_begin_time, self.get_window_start_time(start2))
        self.assertEqual(len(window.records), 1)
        expected = {
            "booleanAttribute": [False],
            "count()": [6],
            "sum(intAttribute1)": [2331],
            "min(intAttribute1)": [111],
            "max(intAttribute1)": [666],
            "min(doubleAttribute1)": [11.11],
            "max(doubleAttribute1)": [66.66],
        }
        record = window.records[0]
        for key, value in expected.items():
            self.assertEqual(record.get(key), value[0])

        # BIOS-5210 - Enable me after fixing
        # select_request = (
        #     bios.isql()
        #     .select(
        #         "blobAttribute",
        #         "count()",
        #         "sum(intAttribute1)",
        #         "min(intAttribute1)",
        #         "max(intAttribute1)",
        #         "sum(doubleAttribute1)",
        #         "min(doubleAttribute1)",
        #         "max(doubleAttribute1)",
        #     )
        #     .from_signal(SIGNAL_NAME)
        #     .group_by(["blobAttribute"])
        #     .tumbling_window(bios.time.minutes(1))
        #     .time_range(start1, bios.time.minutes(1), bios.time.minutes(1))
        #     .build()
        # )

        # resp = self.session.execute(select_request)
        # self.assertIsNotNone(resp)
        # pprint(resp)
        # sys.stdout.flush()
        # self.assertIsNotNone(resp.get_data_windows())
        # self.assertEqual(len(resp.get_data_windows()), 1)
        # window = resp.get_data_windows()[0]
        # self.assertEqual(window.window_begin_time, self.get_window_start_time(start1))
        # self.assertEqual(len(window.records), 1)
        # expected = {
        #     "blobAttribute": ["blobData1", "blobData2", "blobData3"],
        #     "count()": [3, 2, 1],
        #     "sum(intAttribute1)": [666, 999, 666],
        #     "min(intAttribute1)": [111, 444, 666],
        #     "max(intAttribute1)": [333, 555, 666],
        #     "sum(doubleAttribute1)": [66.66, 666],
        #     "min(doubleAttribute1)": [11.11, 44.44, 66.66],
        #     "max(doubleAttribute1)": [33.33, 55.55, 66.66],
        # }
        # record = window.records[0]
        # for key, value in expected.items():
        #     self.assertEqual(record.get(key), value[0])

        select_request = (
            bios.isql()
            .select("stringAttribute", "count()")
            .from_signal(SIGNAL_NAME)
            .group_by(["stringAttribute"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(start1, bios.time.minutes(2), bios.time.minutes(1))
            .build()
        )

        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        pprint(resp)
        sys.stdout.flush()
        self.assertIsNotNone(resp.get_data_windows())
        self.assertEqual(len(resp.get_data_windows()), 1)
        window = resp.get_data_windows()[0]
        self.assertEqual(window.window_begin_time, self.get_window_start_time(start1))
        self.assertEqual(len(window.records), 3)
        expected = {"stringAttribute": ["jerry", "ram", "tom"], "count()": [4, 2, 6]}
        record = window.records[0]
        for key, value in expected.items():
            self.assertEqual(record.get(key), value[0])


if __name__ == "__main__":
    pytest.main(sys.argv)
