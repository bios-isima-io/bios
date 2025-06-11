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

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_signal, setup_tenant_config, try_delete_signal
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_rollup_intervals
from tutils import get_next_checkpoint, skip_interval

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

INTERVAL_MS = get_rollup_intervals()
SIGNAL_NAME = "retroactive_rollup_remove"
SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "value", "type": "Integer"},
    ],
}

SIGNAL_CONFIG_MODIFIED = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "key", "type": "string"},
        {"attributeName": "value", "type": "Integer"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "view_basic",
                "dimensions": ["key"],
                "attributes": ["value"],
                "featureInterval": INTERVAL_MS,
            }
        ]
    },
}


class TestRetroactiveRollupRemove(unittest.TestCase):
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

        # ingest a cluster of events to the original stream
        initial_events = [
            "key_A,10",
            "key_B,20",
            "key_C,30",
            "key_B,40",
            "key_B,50",
            "key_A,60",
            "key_C,70",
            "key_A,80",
            "key_B,90",
        ]
        start1 = self.insert_into_signal(SIGNAL_NAME, initial_events)

        # ingest another cluster of events in the next rollup window
        skip_interval(start1)
        second_events = [
            "key_X,900",
            "key_Y,800",
            "key_Y,700",
            "key_Z,600",
            "key_X,500",
            "key_X,400",
            "key_X,300",
            "key_Z,200",
        ]
        start2 = self.insert_into_signal(SIGNAL_NAME, second_events)

        # sleep until the next rollup window
        skip_interval(start2)

        # add the rollup by updating the signal
        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG_MODIFIED)

        # insert the third cluster of events.
        third_events = [
            "key_D,1",
            "key_E,2",
            "key_F,3",
            "key_D,4",
            "key_E,5",
            "key_F,6",
        ]
        start3 = self.insert_into_signal(SIGNAL_NAME, third_events)

        # sleep again and ingest
        skip_interval(start3)

        # insert the fourth cluster of events.
        fourth_events = [
            "key_U,1000",
            "key_V,2000",
            "key_W,3000",
            "key_U,4000",
            "key_U,5000",
            "key_W,6000",
            "key_W,7000",
            "key_W,8000",
        ]
        start4 = self.insert_into_signal(SIGNAL_NAME, fourth_events)

        # Add the rollup back
        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG)

        # wait for rollups to happen
        skip_interval(start4)
        time.sleep(10)

        # verify
        select_request = (
            bios.isql()
            .select("key", "count()", "sum(value)", "min(value)", "max(value)")
            .from_signal(SIGNAL_NAME)
            .group_by(["key"])
            .tumbling_window(bios.time.minutes(4))
            .time_range(start1, bios.time.minutes(4), bios.time.minutes(1))
            .build()
        )

        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*No suitable features found for the specified windowed query*",
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
