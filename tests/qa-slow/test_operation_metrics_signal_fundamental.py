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

import json
import pprint
import random
import string
import sys
import time
import unittest
from typing import Dict, List, Tuple

import bios
import pytest
from bios import ErrorCode, ServiceError, rollback_on_failure
from setup_common import setup_signal
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsSignalFundamental(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()
        cls.INTERVAL = 60000

        cls.SIGNAL_SIMPLE_NAME = "simpleSignal"
        cls.SIGNAL_SIMPLE_CONFIG = {
            "signalName": cls.SIGNAL_SIMPLE_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "country", "type": "String"},
                {"attributeName": "state", "type": "String"},
                {"attributeName": "population", "type": "Integer"},
            ],
        }

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            setup_signal(admin, cls.SIGNAL_SIMPLE_CONFIG)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _summarize_checkpoint_floor(self, timestamp, interval):
        return int(timestamp / interval) * interval

    def test_signal_fundamental(self):
        expected = {
            "inserted_records": 10,
            "insert_bulk_storage_accesses": 6,
        }
        self._run_test_signal_fundamental(False, expected)

    def test_signal_fundamental_atomic(self):
        expected = {
            "inserted_records": 7,
            "insert_bulk_storage_accesses": 1,
        }
        self._run_test_signal_fundamental(True, expected)

    def _run_test_signal_fundamental(self, atomic, expected):
        """Test metrics for a signal without enrichments and features"""
        app_name = "test_signal_simple"
        with bios.login(
            ep_url(),
            self.ADMIN_USER,
            admin_pass,
            app_name=app_name,
            app_type=bios.models.AppType.REALTIME,
        ) as admin:
            # Build the insert statements against the test signal
            simple_signals = [
                "USA,CA,12345",
                "Canada,BC,2233",
                "US,AZ,wrong",
                "Japan,Tokyo,5432",
                "India,Goa,9876",
            ]
            insert_statements = [
                bios.isql().insert().into(self.SIGNAL_SIMPLE_NAME.lower()).csv(signal).build()
                for signal in simple_signals
            ]
            insert_statement_wrong_signal = (
                bios.isql().insert().into("nonExisting").csv("abc").build()
            )
            simple_bulk_signals_partial_failure = [
                "USA,OR,31893",
                "USA,CO,invalid",
                "USA,WA,29873",
                "Japan,Kagoshima,751",
            ]
            insert_bulk_statement_partial_failure = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_SIMPLE_NAME.lower())
                .csv_bulk(simple_bulk_signals_partial_failure)
                .build()
            )
            simple_bulk_signals = ["USA,OR,31894", "USA,WA,29874", "Japan,Kagoshima,752"]
            insert_bulk_statement_wrong_signal = (
                bios.isql()
                .insert()
                .into("xxabckla")
                .csv_bulk(simple_bulk_signals_partial_failure)
                .build()
            )
            insert_bulk_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_SIMPLE_NAME)
                .csv_bulk(simple_bulk_signals)
                .build()
            )

            # execute insert statements
            insert_responses = []
            for statement in insert_statements:
                try:
                    insert_responses.append(
                        admin.execute(rollback_on_failure(statement) if atomic else statement)
                    )
                except ServiceError:
                    pass
            try:
                admin.execute(
                    rollback_on_failure(insert_statement_wrong_signal)
                    if atomic
                    else insert_statement_wrong_signal
                )
            except ServiceError:
                pass
            try:
                admin.execute(
                    rollback_on_failure(insert_bulk_statement_partial_failure)
                    if atomic
                    else insert_bulk_statement_partial_failure
                )
            except ServiceError:
                pass
            try:
                admin.execute(
                    rollback_on_failure(insert_bulk_statement_wrong_signal)
                    if atomic
                    else insert_bulk_statement_wrong_signal
                )
            except ServiceError:
                pass
            bulk_insert_response = admin.execute(
                rollback_on_failure(insert_bulk_statement) if atomic else insert_bulk_statement
            )

            start = insert_responses[0].records[0].timestamp
            end = bulk_insert_response.records[-1].timestamp

            # Build the select statement for the test signal
            select_statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_SIMPLE_NAME.lower())
                .time_range(start, end - start + 1)
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            self.assertEqual(len(records), expected["inserted_records"])

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Get the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())

            # Verify the usage metrics
            self.assertEqual(len(usage_metrics.keys()), 2)

            entry_insert = usage_metrics.get(
                (self.SIGNAL_SIMPLE_NAME, "INSERT", app_name, "Realtime")
            )
            self.assertIsNotNone(entry_insert)
            self.assertEqual(entry_insert[self.USG_NUM_SUCCESSFUL_OPERATIONS], 5)
            self.assertEqual(entry_insert[self.USG_NUM_WRITES], 12)
            self.assertEqual(entry_insert[self.USG_NUM_READS], 0)
            self.assertEqual(entry_insert[self.USG_NUM_VALIDATION_ERRORS], 2)

            entry_select = usage_metrics.get(
                (self.SIGNAL_SIMPLE_NAME, "SELECT", app_name, "Realtime")
            )
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], expected["inserted_records"])
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

            # Verify the operations metrics
            entry_insert = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.SIGNAL_SIMPLE_NAME,
                    "INSERT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_insert)
            self.assertEqual(entry_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 4)
            self.assertEqual(entry_insert[self.OP_NUM_WRITES], 5)
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
            self.assertEqual(entry_insert[self.OP_NUM_STORAGE_ACCESSES], 4)
            self.assertGreaterEqual(entry_insert[self.OP_STORAGE_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_insert[self.OP_STORAGE_LATENCY_MAX],
                entry_insert[self.OP_STORAGE_LATENCY_MIN],
            )
            self.assertGreaterEqual(
                entry_insert[self.OP_STORAGE_LATENCY_SUM],
                entry_insert[self.OP_STORAGE_LATENCY_MAX],
            )

            entry_insert_bulk = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.SIGNAL_SIMPLE_NAME,
                    "INSERT_BULK",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_insert_bulk)
            self.assertEqual(entry_insert_bulk[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_insert_bulk[self.OP_NUM_WRITES], 7)
            self.assertGreater(entry_insert_bulk[self.OP_BYTES_WRITTEN], 0)
            self.assertEqual(entry_insert_bulk[self.OP_NUM_READS], 0)
            self.assertEqual(entry_insert_bulk[self.OP_BYTES_READ], 0)
            self.assertGreaterEqual(entry_insert_bulk[self.OP_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_insert_bulk[self.OP_LATENCY_MAX], entry_insert_bulk[self.OP_LATENCY_MIN]
            )
            self.assertGreaterEqual(
                entry_insert_bulk[self.OP_LATENCY_SUM], entry_insert_bulk[self.OP_LATENCY_MAX]
            )
            self.assertEqual(entry_insert_bulk[self.OP_NUM_VALIDATION_ERRORS], 1)
            self.assertEqual(entry_insert_bulk[self.OP_NUM_TRANSIENT_ERRORS], 0)
            self.assertEqual(
                entry_insert_bulk[self.OP_NUM_STORAGE_ACCESSES],
                expected["insert_bulk_storage_accesses"],
            )
            self.assertGreaterEqual(entry_insert_bulk[self.OP_STORAGE_LATENCY_MIN], 0)
            self.assertGreaterEqual(
                entry_insert_bulk[self.OP_STORAGE_LATENCY_MAX],
                entry_insert_bulk[self.OP_STORAGE_LATENCY_MIN],
            )
            self.assertGreaterEqual(
                entry_insert_bulk[self.OP_STORAGE_LATENCY_SUM],
                entry_insert_bulk[self.OP_STORAGE_LATENCY_MAX],
            )

            entry_select = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.SIGNAL_SIMPLE_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertIsNotNone(entry_select)
            self.assertEqual(entry_select[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.OP_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.OP_BYTES_WRITTEN], 0)
            self.assertEqual(entry_select[self.OP_NUM_READS], expected["inserted_records"])
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
                    "INSERT",
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
                    "xxabckla",
                    "INSERT_BULK",
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
