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
SIGNAL_NAME = "retroactive_rollup_add_multiple"
SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "value", "type": "Integer"},
        {"attributeName": "value1", "type": "Integer"},
        {"attributeName": "value2", "type": "Integer"},
    ],
}

SIGNAL_CONFIG_ONE_ROLLUP = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "value", "type": "Integer"},
        {"attributeName": "value1", "type": "Integer"},
        {"attributeName": "value2", "type": "Integer"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "view1",
                "dimensions": ["key"],
                "attributes": ["value"],
                "featureInterval": INTERVAL_MS,
            }
        ]
    },
}

SIGNAL_CONFIG_TWO_ROLLUP = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "value", "type": "Integer"},
        {"attributeName": "value1", "type": "Integer"},
        {"attributeName": "value2", "type": "Integer"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "view1",
                "dimensions": ["key"],
                "attributes": ["value"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "view2",
                "dimensions": ["value1"],
                "attributes": ["value2"],
                "featureInterval": INTERVAL_MS,
            },
        ]
    },
}


class TestRetroactiveRollupAddMultiple(unittest.TestCase):
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
            "key_A,10,60,100",
            "key_B,20,80,300",
            "key_C,30,60,700",
            "key_B,40,100,190",
            "key_B,50,80,192",
            "key_A,60,100,675",
            "key_C,70,60,564",
            "key_A,80,100,800",
            "key_B,90,80,893",
        ]
        start1 = self.insert_into_signal(SIGNAL_NAME, initial_events)

        # ingest another cluster of events in the next rollup window
        skip_interval(start1)
        second_events = [
            "key_X,900,9,80",
            "key_Y,800,70,100",
            "key_Y,700,9,105",
            "key_Z,600,16,30",
            "key_X,500,16,50",
            "key_X,400,9,100",
            "key_X,300,16,90",
            "key_Z,200,9,10",
        ]
        start2 = self.insert_into_signal(SIGNAL_NAME, second_events)

        # sleep until the next rollup window
        skip_interval(start2)

        # add the rollup by modifying the stream
        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG_ONE_ROLLUP)

        # Adding another rollup with different view
        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG_TWO_ROLLUP)

        # then ingest the third cluster of events.
        third_events = [
            "key_D,1,45,90",
            "key_E,2,50,100",
            "key_F,3,60,120",
            "key_D,4,50,200",
            "key_E,5,45,180",
            "key_F,6,45,360",
        ]
        start3 = self.insert_into_signal(SIGNAL_NAME, third_events)

        # sleep again and ingest
        skip_interval(start3)

        fourth_events = [
            "key_U,1000,2,4",
            "key_V,2000,2,40",
            "key_W,3000,3,100",
            "key_U,4000,4,90",
            "key_U,5000,5,100",
            "key_W,6000,3,200",
            "key_W,7000,5,60",
            "key_W,8000,4,80",
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
            "sum(value)": [150, 200, 100, 5, 7, 9, 10000, 2000, 24000, 2100, 1500, 800],
            "min(value)": [10, 20, 30, 1, 2, 3, 1000, 2000, 3000, 300, 700, 200],
            "max(value)": [80, 90, 70, 4, 5, 6, 5000, 2000, 8000, 900, 800, 600],
        }

        select_request1 = (
            bios.isql()
            .select("key", "count()", "sum(value)", "min(value)", "max(value)")
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
            "value1": [2, 3, 4, 5, 9, 16, 45, 50, 60, 70, 80, 100],
            "count()": [2, 2, 2, 2, 4, 3, 3, 2, 4, 1, 3, 3],
            "min(value2)": [4, 100, 80, 60, 10, 30, 90, 100, 100, 100, 192, 190],
            "max(value2)": [40, 200, 90, 100, 105, 90, 360, 200, 700, 100, 893, 800],
            "sum(value2)": [44, 300, 170, 160, 295, 170, 630, 300, 1484, 100, 1385, 1665],
        }

        select_request2 = (
            bios.isql()
            .select("value1", "count()", "sum(value2)", "min(value2)", "max(value2)")
            .from_signal(SIGNAL_NAME)
            .group_by(["value1"])
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
        self.assertEqual(len(data_window2.records), 12)

        for i, record in enumerate(data_window2.records):
            for key, value in expected2.items():
                self.assertEqual(record.get(key), value[i])


if __name__ == "__main__":
    pytest.main(sys.argv)
