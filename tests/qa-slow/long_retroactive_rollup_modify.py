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
from tutils import get_next_checkpoint, skip_interval

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

INTERVAL_MS = get_rollup_intervals()
SIGNAL_NAME = "retroactive_rollup_modify"
SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "distance", "type": "Integer"},
        {"attributeName": "count", "type": "Integer"},
        {"attributeName": "number_str", "type": "String"},
    ],
}
SIGNAL_CONFIG_SECOND = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "distance", "type": "Integer"},
        {"attributeName": "time", "type": "Integer", "default": 0},
        {"attributeName": "count", "type": "Integer"},
        {"attributeName": "number", "type": "Integer", "default": -1},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "view1",
                "dimensions": ["key"],
                "attributes": ["time"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "view2",
                "dimensions": ["key"],
                "attributes": ["number"],
                "featureInterval": INTERVAL_MS,
            },
        ]
    },
}
SIGNAL_CONFIG_THIRD = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "distance", "type": "Integer"},
        {"attributeName": "time", "type": "Integer", "default": 0},
        {"attributeName": "count", "type": "Integer"},
        {"attributeName": "number", "type": "Integer", "default": -1},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "view2",
                "dimensions": ["key"],
                "attributes": ["distance"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "view3",
                "dimensions": ["key"],
                "attributes": ["number"],
                "featureInterval": INTERVAL_MS,
            },
        ]
    },
}


class TestRetroactiveRollupModify(unittest.TestCase):
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

    def test(self):
        self.adjust_start_time()

        initial_events = [
            "key_A,10,1,999",
            "key_B,20,2,888",
            "key_C,30,3,777",
            "key_B,40,4,666",
            "key_B,50,5,555",
            "key_A,60,6,444",
            "key_C,70,7,333",
            "key_A,80,8,222",
            "key_B,90,9,111",
        ]
        start1 = self.insert_into_signal(SIGNAL_NAME, initial_events)

        # ingest another cluster of events in the next rollup window
        skip_interval(start1)
        second_events = [
            "key_X,900,90,111",
            "key_Y,800,80,222",
            "key_Y,700,70,333",
            "key_Z,600,60,444",
            "key_X,500,50,555",
            "key_X,400,40,666",
            "key_X,300,30,777",
            "key_Z,200,20,888",
        ]
        start2 = self.insert_into_signal(SIGNAL_NAME, second_events)

        # sleep until the next rollup window
        skip_interval(start2)

        # add the rollup by modifying the stream
        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG_SECOND)

        # then ingest the third cluster of events.
        third_events = [
            "key_D,1,60,100,11",
            "key_E,2,50,200,22",
            "key_F,3,40,300,33",
            "key_D,4,30,400,44",
            "key_E,5,20,500,55",
            "key_F,6,10,600,66",
        ]
        start3 = self.insert_into_signal(SIGNAL_NAME, third_events)

        # sleep again and ingest
        skip_interval(start3)

        # Adding another rollup with different view
        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG_THIRD)

        fourth_events = [
            "key_U,1000,808080,10,9",
            "key_V,2000,707070,20,8",
            "key_W,3000,606060,30,7",
            "key_U,4000,505050,40,6",
            "key_U,5000,404040,50,5",
            "key_W,6000,303030,60,4",
            "key_W,7000,202020,70,3",
            "key_W,8000,101010,80,2",
        ]
        start4 = self.insert_into_signal(SIGNAL_NAME, fourth_events)

        # wait for rollups to happen
        skip_interval(start4)
        time.sleep(10)

        # verify
        expected1 = {
            "key": [
                "key_A",
                "key_B",
                "key_C",
                "key_D",
                "key_E",
                "key_F",
                "key_U",
                "key_V",
                "key_W",
                "key_X",
                "key_Y",
                "key_Z",
            ],
            "count()": [3, 4, 2, 2, 2, 2, 3, 1, 4, 4, 2, 2],
            "sum(distance)": [150, 200, 100, 5, 7, 9, 10000, 2000, 24000, 2100, 1500, 800],
            "min(distance)": [10, 20, 30, 1, 2, 3, 1000, 2000, 3000, 300, 700, 200],
            "max(distance)": [80, 90, 70, 4, 5, 6, 5000, 2000, 8000, 900, 800, 600],
        }

        select_request1 = (
            bios.isql()
            .select("key", "count()", "sum(distance)", "min(distance)", "max(distance)")
            .from_signal(SIGNAL_NAME)
            .group_by(["key"])
            .tumbling_window(bios.time.minutes(4))
            .time_range(start1, bios.time.minutes(4), bios.time.minutes(1))
            .build()
        )

        resp1 = self.session.execute(select_request1)
        self.assertIsNotNone(resp1)
        pprint(resp1)
        sys.stdout.flush()
        self.assertIsNotNone(resp1.get_data_windows())
        self.assertEqual(len(resp1.get_data_windows()), 1)
        data_window1 = resp1.get_data_windows()[0]
        self.assertEqual(data_window1.window_begin_time, self.get_window_start_time(start1))
        self.assertEqual(len(data_window1.records), 12)

        for i, record in enumerate(data_window1.records):
            for key, value in expected1.items():
                self.assertEqual(record.get(key), value[i])

        expected2 = {
            "key": ["key_D", "key_E", "key_F", "key_U", "key_V", "key_W"],
            "count()": [2, 2, 2, 3, 1, 4],
            "min(number)": [11, 22, 33, 5, 8, 2],
            "max(number)": [44, 55, 66, 9, 8, 7],
            "sum(number)": [55, 77, 99, 20, 8, 16],
        }

        select_request2 = (
            bios.isql()
            .select("key", "count()", "sum(number)", "min(number)", "max(number)")
            .from_signal(SIGNAL_NAME)
            .group_by(["key"])
            .tumbling_window(bios.time.minutes(4))
            .time_range(start1, bios.time.minutes(4), bios.time.minutes(1))
            .build()
        )

        resp2 = self.session.execute(select_request2)
        self.assertIsNotNone(resp2)
        pprint(resp2)
        sys.stdout.flush()
        self.assertIsNotNone(resp2.get_data_windows())
        self.assertEqual(len(resp2.get_data_windows()), 1)
        data_window2 = resp2.get_data_windows()[0]
        self.assertEqual(data_window2.window_begin_time, self.get_window_start_time(start1))
        self.assertEqual(len(data_window2.records), 6)

        for i, record in enumerate(data_window2.records):
            for key, value in expected2.items():
                self.assertEqual(record.get(key), value[i])


if __name__ == "__main__":
    pytest.main(sys.argv)
