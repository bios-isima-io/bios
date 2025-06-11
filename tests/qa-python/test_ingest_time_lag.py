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

import base64
import copy
import json
import logging
import os
import time
import unittest

from bios import ServiceError, ErrorCode
import bios
from bios.isql_request import ISqlRequest
from bios_tutils import get_minimum_signal, get_all_types_signal
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "ingestTimeLagTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


class BiosIngestTimeLagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.SIGNAL = {
            "signalName": "signal",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "one", "type": "string"},
                {
                    "attributeName": "eventTime",
                    "type": "integer",
                    "tags": {
                        "category": "quantity",
                        "kind": "timestamp",
                        "unit": "UnixMillisecond",
                    },
                },
            ],
            "enrich": {
                "ingestTimeLag": [
                    {
                        "ingestTimeLagName": "lagSinceEventTime",
                        "attribute": "eventTime",
                        "as": "lag",
                    }
                ]
            },
        }

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    def tearDown(self):
        self.session.close()

    def test_fundamental(self):
        test_name = "fundamental"
        self.verify_lag_calculation(test_name)

    def test_input_second_output_no_tags(self):
        test_name = "inputSecondOutputNoTags"
        self.verify_lag_calculation(test_name, reference_unit="UnixSecond")

    def test_input_milliseconds_output_seconds(self):
        test_name = "inputMillisecondOutputSecond"
        self.verify_lag_calculation(test_name, output_unit="Second")

    def test_input_seconds_output_seconds(self):
        test_name = "inputSecondOutputSecond"
        self.verify_lag_calculation(test_name, reference_unit="UnixSecond", output_unit="Second")

    def test_input_milliseconds_output_minutes(self):
        test_name = "inputMillisecondOutputMinute"
        self.verify_lag_calculation(test_name, output_unit="Minute", fill_in=100.1)

    def test_input_milliseconds_output_hours(self):
        test_name = "inputMillisecondOutputHour"
        self.verify_lag_calculation(test_name, output_unit="Hour", fill_in=0)

    def test_input_milliseconds_output_days(self):
        test_name = "inputMillisecondOutputDay"
        self.verify_lag_calculation(test_name, output_unit="Day", fill_in=-1)

    def test_input_milliseconds_output_weeks(self):
        test_name = "inputMillisecondOutputWeek"
        self.verify_lag_calculation(test_name, output_unit="Week")

    def test_input_seconds_output_seconds_high_lag_limit(self):
        test_name = "HighLagLimit"
        self.verify_lag_calculation(
            test_name,
            reference_unit="UnixSecond",
            output_unit="Second",
            fill_in=100.1,
            introduced_lag_ms=8650000000,
            expected_lag_ms=100100,
        )

    def test_input_seconds_output_seconds_negative_lag_limit(self):
        test_name = "NegativeLagLimit"
        self.verify_lag_calculation(
            test_name,
            reference_unit="UnixSecond",
            output_unit="Second",
            fill_in=100.1,
            introduced_lag_ms=-3700000,
            expected_lag_ms=100100,
        )

    def test_input_seconds_output_seconds_negative_lag(self):
        test_name = "NegativeLag"
        self.verify_lag_calculation(
            test_name,
            reference_unit="UnixSecond",
            output_unit="Second",
            fill_in=100.1,
            introduced_lag_ms=-20000,
            expected_lag_ms=-20000,
        )

    def verify_lag_calculation(
        self,
        test_name,
        reference_unit=None,
        output_unit=None,
        fill_in=None,
        introduced_lag_ms=None,
        expected_lag_ms=None,
    ):
        """Generic verification method to test ingestTimeLag calculation"""
        # set up the test signal
        signal_name = f"{test_name}Signal"
        signal = copy.deepcopy(self.SIGNAL)
        input_millis = True
        output_factor = 1
        if reference_unit:
            signal["attributes"][1]["tags"]["unit"] = reference_unit
            input_millis = reference_unit.lower() == "unixmillisecond"
        if output_unit:
            signal["enrich"]["ingestTimeLag"][0]["tags"] = {
                "category": "Quantity",
                "kind": "duration",
                "unit": output_unit,
            }
            if output_unit.lower() == "millisecond":
                output_factor = 1
            elif output_unit.lower() == "second":
                output_factor = 1000
            elif output_unit.lower() == "minute":
                output_factor = 1000 * 60
            elif output_unit.lower() == "hour":
                output_factor = 1000 * 60 * 60
            elif output_unit.lower() == "day":
                output_factor = 1000 * 60 * 60 * 24
            elif output_unit.lower() == "week":
                output_factor = 1000 * 60 * 60 * 24 * 7
        if fill_in:
            signal["enrich"]["ingestTimeLag"][0]["fillIn"] = fill_in
        signal["signalName"] = signal_name
        self.session.create_signal(signal)

        # insert with time lag calculation
        input_factor = 1000 if input_millis else 1
        introduced_lag_ms = introduced_lag_ms or 10500
        event_time = int((time.time() - introduced_lag_ms / 1000.0) * input_factor)
        insert_record = self.session.execute(
            bios.isql().insert().into(signal_name).csv(f"abc,{event_time}").build()
        ).records[0]
        self.assertGreater(insert_record.timestamp, 0)

        # extract the result and verify the ingestTimeLag enrichment
        response = self.session.execute(
            bios.isql()
            .select()
            .from_signal(signal_name)
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        # print(response.get_data_windows()[0])
        records = response.get_data_windows()[0].records
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].get("one"), "abc")
        self.assertEqual(records[0].get("eventTime"), event_time)
        lag = records[0].get("lag")
        print(f"{test_name}: {lag}")
        lag_ms = lag * output_factor
        expected_lag_ms = expected_lag_ms or 10500
        self.assertGreaterEqual(lag_ms, expected_lag_ms)
        self.assertLess(lag_ms, expected_lag_ms + 2000)


if __name__ == "__main__":
    unittest.main()
