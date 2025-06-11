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

import bios
import math
import time
import unittest

from bios import ServiceError
from setup_common import read_file
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_user, sadmin_pass, admin_user, admin_pass

TEST_TENANT_NAME = "quantilesTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

QUANTILES_SIGNAL_NAME = "quantilesSignal"
QUANTILES_SIGNAL_BIOS = "../resources/quantiles-signal-bios.json"


class QuantilesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            session.create_signal(read_file(QUANTILES_SIGNAL_BIOS))

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        self.assertIsNotNone(self.session)

    def tearDown(self):
        self.session.close()

    @staticmethod
    def sleep_until(sleep_until):
        sleep_time = sleep_until - time.time()
        print(f"sleeping for {sleep_time} seconds until {sleep_until}")
        time.sleep(sleep_time)

    def verify(self, quantiles, metric, percentile):
        allowed_error_fraction = 0.05
        a_actual = quantiles.get(f"{metric}(a)")
        a_expected = self.limit * percentile / 100.0
        a_error = a_actual - a_expected
        a_error_fraction = math.fabs(a_error / self.limit)
        if a_error_fraction > allowed_error_fraction:
            self.error_message += (
                "\n" + f"{metric}(a) = {a_actual} but expected = {a_expected},"
                f" error fraction = {a_error_fraction}"
            )
            self.errors_found = True

        b_actual = quantiles.get(f"{metric}(b)")
        b_expected = self.limit * percentile / 100.0 / 1000.0
        b_error = b_actual - b_expected
        b_error_fraction = math.fabs(b_error / (self.limit / 1000.0))
        if b_error_fraction > allowed_error_fraction:
            self.error_message += (
                "\n" + f"{metric}(b) = {b_actual} but expected = {b_expected},"
                f" error fraction = {b_error_fraction}"
            )
            self.errors_found = True

        c_actual = quantiles.get(f"{metric}(c)")
        c_expected = self.limit * percentile * 10.0
        c_error = c_actual - c_expected
        c_error_fraction = math.fabs(c_error / (self.limit * 1000))
        if c_error_fraction > allowed_error_fraction:
            self.error_message += (
                "\n" + f"{metric}(c) = {c_actual} but expected = {c_expected},"
                f" error fraction = {c_error_fraction}"
            )
            self.errors_found = True

    def test_quantiles_across_windows(self):
        # This test failed inexplicably: "Stream not found: quantilesTest.quantilesSignal".
        # Adding a sleep to help with Cassandra settling down.
        time.sleep(10)
        signal = self.session.get_signal(QUANTILES_SIGNAL_NAME)
        self.assertEqual(signal["signalName"], QUANTILES_SIGNAL_NAME)
        first_event = True
        self.limit = 100000
        offset1 = int(self.limit / 3)
        offset2 = int(7 * self.limit / 11)
        for i in range(self.limit):
            a = i + 1
            b = ((i + offset1) % self.limit) + 1
            b = b / 1000.0
            c = ((i + offset2) % self.limit) + 1
            c = c * 1000
            statement = (
                bios.isql().insert().into(QUANTILES_SIGNAL_NAME).csv(f"{a},{b},{c}").build()
            )
            resp = self.session.execute(statement).records[0]
            if first_event:
                first_event_timestamp = resp.timestamp
                first_event = False

        # Sleep for enough time to finish creating sketches for the events ingested above.
        end_time = math.ceil(time.time() / 15) * 15 + 14.0
        self.sleep_until(end_time)
        # For the select query, start before the first event, and ensure start time is not
        # aligned with a 5-minute boundary, so that the sketch picked is not the 5-minute sketch.
        safe_start_time = int(math.floor(first_event_timestamp / 300000)) * 300000 - 15000

        statement = (
            bios.isql()
            .select(
                "p0_01(a)",
                "p0_1(a)",
                "p1(a)",
                "p10(a)",
                "p25(a)",
                "p50(a)",
                "p75(a)",
                "p90(a)",
                "p99(a)",
                "p99_9(a)",
                "p99_99(a)",
                "p0_01(b)",
                "p0_1(b)",
                "p1(b)",
                "p10(b)",
                "p25(b)",
                "p50(b)",
                "p75(b)",
                "p90(b)",
                "p99(b)",
                "p99_9(b)",
                "p99_99(b)",
                "p0_01(c)",
                "p0_1(c)",
                "p1(c)",
                "p10(c)",
                "p25(c)",
                "p50(c)",
                "p75(c)",
                "p90(c)",
                "p99(c)",
                "p99_9(c)",
                "p99_99(c)",
            )
            .from_signal(QUANTILES_SIGNAL_NAME)
            .tumbling_window(bios.time.hours(1))
            .time_range(safe_start_time, bios.time.hours(1), bios.time.seconds(15))
            .build()
        )
        result = self.session.execute(statement)
        self.errors_found = False
        self.error_message = "Errors:"
        try:
            quantiles = result.data_windows[0].records[0]
            self.verify(quantiles, "p0_01", 0.01)
            self.verify(quantiles, "p0_1", 0.1)
            self.verify(quantiles, "p1", 1)
            self.verify(quantiles, "p10", 10)
            self.verify(quantiles, "p25", 25)
            self.verify(quantiles, "p50", 50)
            self.verify(quantiles, "p75", 75)
            self.verify(quantiles, "p90", 90)
            self.verify(quantiles, "p99", 99)
            self.verify(quantiles, "p99_9", 99.9)
            self.verify(quantiles, "p99_99", 99.99)
            if self.errors_found:
                print(self.error_message)
                print()
                print(result)
                print()
                print(f"First event timestamp: {first_event_timestamp}")
                print(f"Safe start time: {safe_start_time}")
                self.fail("Actual quantiles values not within expected error bounds.")
        except (IndexError, KeyError, TypeError):
            print(result)
            print()
            print(f"First event timestamp: {first_event_timestamp}")
            print(f"Safe start time: {safe_start_time}")
            self.fail("Expected members not present in select result.")


if __name__ == "__main__":
    unittest.main()
