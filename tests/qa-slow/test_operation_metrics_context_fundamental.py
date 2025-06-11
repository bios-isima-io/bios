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

import pprint
import random
import sys
import time

import bios
import pytest
from bios import ServiceError
from setup_common import setup_context
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsContextFundamental(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()

        cls.CONTEXT_SIMPLE_NAME = "gdpOfCountry"
        cls.CONTEXT_SIMPLE_CONFIG = {
            "contextName": cls.CONTEXT_SIMPLE_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "country", "type": "String"},
                {"attributeName": "gdp", "type": "Decimal"},
            ],
            "primaryKey": ["country"],
        }

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            setup_context(admin, cls.CONTEXT_SIMPLE_CONFIG)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def test_context_fundamental(self):
        """Test metrics for a context"""
        app_name = "test_context_simple"
        with bios.login(
            ep_url(),
            self.ADMIN_USER,
            admin_pass,
            app_name=app_name,
            app_type=bios.models.AppType.REALTIME,
        ) as admin:
            start = bios.time.now()

            # Build the insert statements against the test context
            entries_invalid = [
                f"USA,{random.random() * 10}",
                f"Canada,{random.random() * 10}",
                f"Germany,{random.random() * 10}",
                f"Japan,{random.random() * 10}",
                "Peru,Unavailable",
                f"India,{random.random() * 10}",
            ]
            value_usa = random.random() * 10
            value_canada = random.random() * 10
            value_china = random.random() * 10
            value_japan = random.random() * 10
            value_india = random.random() * 10
            entries = [
                f"USA,{value_usa}",
                f"Canada,{value_canada}",
                f"China,{value_china}",
                f"Japan,{value_japan}",
                f"India,{value_india}",
            ]
            upsert_statement_invalid = (
                bios.isql()
                .upsert()
                .into(self.CONTEXT_SIMPLE_NAME)
                .csv_bulk(entries_invalid)
                .build()
            )
            upsert_statement_wrong_context = (
                bios.isql().upsert().into("nonExisting").csv("abc").build()
            )
            upsert_statement = (
                bios.isql().upsert().into(self.CONTEXT_SIMPLE_NAME).csv_bulk(entries).build()
            )

            # execute the upsert statements
            try:
                admin.execute(upsert_statement_invalid)
            except ServiceError:
                pass
            try:
                admin.execute(upsert_statement_wrong_context)
            except ServiceError:
                pass
            admin.execute(upsert_statement)

            # Build the select statement for the test context
            select_statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_SIMPLE_NAME)
                .where(
                    keys=[
                        ["India"],
                        # ["Germany"], # TODO Uncomment this after fixing the bug BIOS-4014
                        ["Japan"],
                        ["Italy"],
                        ["USA"],
                    ]
                )
                .build()
            )
            select_wrong_context = (
                bios.isql().select().from_context("abcde").where(keys=[["x"]]).build()
            )
            try:
                admin.execute(select_wrong_context)
            except ServiceError:
                pass
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            self.assertEqual(len(records), 3)
            self.assertAlmostEqual(records[0].get("gdp"), value_india, 0.001)
            # self.assertAlmostEqual(records[1].get("gdp"), 4.0, 0.001)
            self.assertAlmostEqual(records[1].get("gdp"), value_japan, 0.001)
            self.assertAlmostEqual(records[2].get("gdp"), value_usa, 0.001)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Get the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            self.assertEqual(len(usage_metrics.keys()), 2)

            # Verify the usage metrics
            entry_insert = usage_metrics.get(
                (
                    self.CONTEXT_SIMPLE_NAME,
                    "UPSERT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertEqual(entry_insert[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_insert[self.USG_NUM_WRITES], 11)
            self.assertEqual(entry_insert[self.USG_NUM_READS], 0)
            self.assertEqual(entry_insert[self.USG_NUM_VALIDATION_ERRORS], 1)

            entry_select = usage_metrics.get(
                (
                    self.CONTEXT_SIMPLE_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 3)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

            # verify the operation metrics
            entry_insert = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.CONTEXT_SIMPLE_NAME,
                    "UPSERT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_insert)
            self.assertEqual(entry_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_insert[self.OP_NUM_WRITES], 11)
            self.assertGreater(entry_insert[self.OP_BYTES_WRITTEN], 0)
            self.assertEqual(entry_insert[self.OP_NUM_READS], 0)
            self.assertEqual(entry_insert[self.OP_BYTES_READ], 0)
            self.assertGreaterEqual(entry_insert[self.OP_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_insert[self.OP_LATENCY_MAX], entry_insert[self.OP_LATENCY_MIN]
            )
            self.assertGreaterEqual(
                entry_insert[self.OP_LATENCY_SUM], entry_insert[self.OP_LATENCY_MAX]
            )
            self.assertEqual(entry_insert[self.OP_NUM_VALIDATION_ERRORS], 1)
            self.assertEqual(entry_insert[self.OP_NUM_TRANSIENT_ERRORS], 0)
            self.assertEqual(entry_insert[self.OP_NUM_STORAGE_ACCESSES], 1)
            self.assertGreaterEqual(entry_insert[self.OP_STORAGE_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_insert[self.OP_STORAGE_LATENCY_MAX],
                entry_insert[self.OP_STORAGE_LATENCY_MIN],
            )
            self.assertGreaterEqual(
                entry_insert[self.OP_STORAGE_LATENCY_SUM],
                entry_insert[self.OP_STORAGE_LATENCY_MAX],
            )

            entry_select = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.CONTEXT_SIMPLE_NAME,
                    "SELECT_CONTEXT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_select)
            self.assertEqual(entry_select[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.OP_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.OP_BYTES_WRITTEN], 0)
            self.assertEqual(entry_select[self.OP_NUM_READS], 3)
            self.assertGreater(entry_select[self.OP_BYTES_READ], 0)
            self.assertGreaterEqual(entry_select[self.OP_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_select[self.OP_LATENCY_MAX], entry_select[self.OP_LATENCY_MIN]
            )
            self.assertGreaterEqual(
                entry_select[self.OP_LATENCY_SUM], entry_select[self.OP_LATENCY_MAX]
            )
            self.assertEqual(entry_select[self.OP_NUM_VALIDATION_ERRORS], 0)
            self.assertEqual(entry_select[self.OP_NUM_TRANSIENT_ERRORS], 0)
            self.assertEqual(entry_select[self.OP_NUM_STORAGE_ACCESSES], 1)
            self.assertGreaterEqual(entry_select[self.OP_STORAGE_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_select[self.OP_STORAGE_LATENCY_MAX],
                entry_select[self.OP_STORAGE_LATENCY_MIN],
            )
            self.assertGreaterEqual(
                entry_select[self.OP_STORAGE_LATENCY_SUM],
                entry_select[self.OP_STORAGE_LATENCY_MAX],
            )

            # for non validation errors
            entry_failed_insert = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    "nonExisting",
                    "UPSERT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_failed_insert)
            self.assertEqual(entry_failed_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 0)
            self.assertEqual(entry_failed_insert[self.OP_NUM_WRITES], 0)
            self.assertEqual(entry_failed_insert[self.OP_BYTES_WRITTEN], 0)
            self.assertEqual(entry_failed_insert[self.OP_NUM_READS], 0)
            self.assertEqual(entry_failed_insert[self.OP_BYTES_READ], 0)
            self.assertEqual(entry_failed_insert[self.OP_LATENCY_MIN], 0)
            self.assertEqual(entry_failed_insert[self.OP_LATENCY_MAX], 0)
            self.assertEqual(entry_failed_insert[self.OP_LATENCY_SUM], 0)
            self.assertEqual(entry_failed_insert[self.OP_NUM_VALIDATION_ERRORS], 0)
            self.assertEqual(entry_failed_insert[self.OP_NUM_TRANSIENT_ERRORS], 1)
            self.assertEqual(entry_failed_insert[self.OP_NUM_STORAGE_ACCESSES], 0)
            self.assertEqual(entry_failed_insert[self.OP_STORAGE_LATENCY_MIN], 0)
            self.assertEqual(entry_failed_insert[self.OP_STORAGE_LATENCY_MAX], 0)
            self.assertEqual(entry_failed_insert[self.OP_STORAGE_LATENCY_SUM], 0)

            entry_failed_bulk_insert = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    "abcde",
                    "SELECT_CONTEXT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_failed_bulk_insert)
            self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_WRITES], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_BYTES_WRITTEN], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_READS], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_BYTES_READ], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_LATENCY_MIN], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_LATENCY_MAX], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_LATENCY_SUM], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_VALIDATION_ERRORS], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_TRANSIENT_ERRORS], 1)
            self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_STORAGE_ACCESSES], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_STORAGE_LATENCY_MIN], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_STORAGE_LATENCY_MAX], 0)
            self.assertEqual(entry_failed_bulk_insert[self.OP_STORAGE_LATENCY_SUM], 0)


if __name__ == "__main__":
    pytest.main(sys.argv)
