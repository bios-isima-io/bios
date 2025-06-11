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
from setup_common import (
    setup_context,
    setup_signal,
    setup_tenant_config,
    try_delete_context,
    try_delete_signal,
)
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_rollup_intervals
from tutils import skip_interval

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

INTERVAL_MS = get_rollup_intervals()
CONTEXT_NAME = "context_add_rollup"
CONTEXT_CONFIG = {
    "contextName": CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "primaryKey": ["intAttribute"],
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
    ],
}

SIGNAL_NAME = "signal_add_rollup"
SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "intAttribute", "type": "Integer"},
        {"attributeName": "testUpdateAttribType1", "type": "Integer"},
        {"attributeName": "testUpdateAttribType2", "type": "Decimal"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "strict_pp",
                "foreignKey": ["intAttribute"],
                "missingLookupPolicy": "Reject",
                "contextName": CONTEXT_NAME,
                "contextAttributes": [
                    {"attributeName": "intAttribute1"},
                    {"attributeName": "stringAttribute"},
                    {"attributeName": "doubleAttribute"},
                    {"attributeName": "doubleAttribute1"},
                    {"attributeName": "booleanAttribute"},
                    {"attributeName": "enumAttribute"},
                ],
            }
        ]
    },
    "postStorageStage": {
        "features": [
            {
                "featureName": "view3",
                "dimensions": ["stringAttribute", "intAttribute1"],
                "attributes": ["intAttribute", "testUpdateAttribType1", "testUpdateAttribType2"],
                "featureInterval": INTERVAL_MS,
            }
        ]
    },
}

SIGNAL_CONFIG_MODIFIED = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "intAttribute", "type": "Integer"},
        {"attributeName": "testUpdateAttribType1", "type": "Integer"},
        {"attributeName": "testUpdateNewAttrib", "type": "Integer", "default": 0},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "strict_pp",
                "foreignKey": ["intAttribute"],
                "missingLookupPolicy": "Reject",
                "contextName": CONTEXT_NAME,
                "contextAttributes": [
                    {"attributeName": "intAttribute1"},
                    {"attributeName": "stringAttribute"},
                    {"attributeName": "doubleAttribute"},
                    {"attributeName": "doubleAttribute1"},
                    {"attributeName": "booleanAttribute"},
                    {"attributeName": "enumAttribute"},
                ],
            }
        ]
    },
    "postStorageStage": {
        "features": [
            {
                "featureName": "view3",
                "dimensions": ["stringAttribute", "intAttribute1"],
                "attributes": ["intAttribute", "testUpdateAttribType1"],
                "featureInterval": INTERVAL_MS,
            },
            {
                "featureName": "view4",
                "dimensions": ["stringAttribute"],
                "attributes": [
                    "intAttribute",
                    "testUpdateAttribType1",
                    "testUpdateNewAttrib",
                ],
                "featureInterval": INTERVAL_MS,
            },
        ]
    },
}


class TestRollupAddRollup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_context(cls.session, CONTEXT_CONFIG)
        setup_signal(cls.session, SIGNAL_CONFIG)

    @classmethod
    def tearDownClass(cls):
        try_delete_signal(cls.session, SIGNAL_NAME)
        try_delete_context(cls.session, CONTEXT_NAME)
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
        now = bios.time.now()
        current_window_start_time = self.get_window_start_time(now)
        if now - current_window_start_time > 45000:
            print("Sleeping for 16 seconds to avoid risk of hitting window boundary")
            time.sleep(16)

        # Setup ############################################################
        context_entries = [
            "251,1,tom,8.11,11.11,false,enum_const_1",
            "252,1,tom,8.11,22.22,false,enum_const_1",
            "253,1,tom,8.11,33.33,false,enum_const_1",
            "254,2,jerry,8.22,44.44,false,enum_const_2",
            "255,2,jerry,8.22,55.55,false,enum_const_2",
            "256,3,ram,8.33,66.66,false,enum_const_3",
        ]
        self.upsert_into_context(CONTEXT_NAME, context_entries)

        initial_events = [
            "251,1,15.00036268111",
            "252,2,15.00036268222",
            "253,3,15.00036268333",
            "254,4,15.00036268444",
            "255,5,15.00036268555",
            "256,6,15.00036268666",
        ]
        start1 = self.insert_into_signal(SIGNAL_NAME, initial_events)

        self.session.update_signal(SIGNAL_NAME, SIGNAL_CONFIG_MODIFIED)

        second_events = [
            "251,1500036268111,1",
            "252,1500036268222,2",
            "253,1500036268333,3",
            "254,1500036268444,4",
            "255,1500036268555,5",
            "256,1500036268666,6",
        ]

        start2 = self.insert_into_signal(SIGNAL_NAME, second_events)

        skip_interval(start2)

        select_request = (
            bios.isql()
            .select(
                "stringAttribute",
                "count()",
                "sum(intAttribute)",
                "min(intAttribute)",
                "max(intAttribute)",
                "sum(testUpdateAttribType1)",
                "min(testUpdateAttribType1)",
                "max(testUpdateAttribType1)",
                "sum(testUpdateNewAttrib)",
                "min(testUpdateNewAttrib)",
                "max(testUpdateNewAttrib)",
            )
            .from_signal(SIGNAL_NAME)
            .group_by(["stringAttribute"])
            .tumbling_window(bios.time.minutes(1))
            .time_range(start1, bios.time.minutes(1), bios.time.minutes(1))
            .build()
        )

        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertIsNotNone(resp.get_data_windows())
        self.assertEqual(len(resp.get_data_windows()), 1)
        data_window = resp.get_data_windows()[0]
        self.assertEqual(data_window.window_begin_time, self.get_window_start_time(start1))
        self.assertEqual(len(data_window.records), 3)

        expected = {
            "stringAttribute": ["jerry", "ram", "tom"],
            "count()": [4, 2, 6],
            "sum(intAttribute)": [1018, 512, 1512],
            "min(intAttribute)": [254, 256, 251],
            "max(intAttribute)": [255, 256, 253],
            "sum(testUpdateAttribType1)": [3000072537008, 1500036268672, 4500108804672],
            "min(testUpdateAttribType1)": [4, 6, 1],
            "max(testUpdateAttribType1)": [1500036268555, 1500036268666, 1500036268333],
            "sum(testUpdateNewAttrib)": [9, 6, 6],
            "min(testUpdateNewAttrib)": [0, 0, 0],
            "max(testUpdateNewAttrib)": [5, 6, 3],
        }

        for i, record in enumerate(data_window.records):
            pprint(f"record {i} = {record}")
            sys.stdout.flush()
            for key, value in expected.items():
                self.assertEqual(record.get(key), value[i])


if __name__ == "__main__":
    pytest.main(sys.argv)
