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
import sys
import time

import bios
import pytest
from bios import ServiceError
from setup_common import setup_context, setup_signal
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsInternal(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()
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
            setup_signal(admin, cls.SIGNAL_SIMPLE_CONFIG)
            setup_context(admin, cls.CONTEXT_SIMPLE_CONFIG)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def test_internal_user(self):
        """Access from an internal user should not appear in the usage signal"""
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
        insert_statement_wrong_signal = bios.isql().insert().into("nonExisting").csv("abc").build()
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
                insert_responses.append(self.SUPPORT.execute(statement))
            except ServiceError:
                pass
        try:
            self.SUPPORT.execute(insert_statement_wrong_signal)
        except ServiceError:
            pass
        try:
            self.SUPPORT.execute(insert_bulk_statement_partial_failure)
        except ServiceError:
            pass
        try:
            self.SUPPORT.execute(insert_bulk_statement_wrong_signal)
        except ServiceError:
            pass
        bulk_insert_response = self.SUPPORT.execute(insert_bulk_statement)

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
        select_response = self.SUPPORT.execute(select_statement)
        records = select_response.to_dict()
        self.assertEqual(len(records), 10)

        ##############################

        # Build the insert statements against the test context
        entries_invalid = [
            "USA,1.0",
            "Canada,2.0",
            "Germany,4.0",
            "Japan,4.0",
            "Peru,Unavailable",
            "India,5.0",
        ]
        entries = [
            "USA,20.49",
            "Canada,1.71",
            "China,13.4",
            "Japan,4.97",
            "India,2.72",
        ]
        upsert_statement_invalid = (
            bios.isql().upsert().into(self.CONTEXT_SIMPLE_NAME).csv_bulk(entries_invalid).build()
        )
        upsert_statement_wrong_context = (
            bios.isql().upsert().into("nonExisting").csv("abc").build()
        )
        upsert_statement = (
            bios.isql().upsert().into(self.CONTEXT_SIMPLE_NAME).csv_bulk(entries).build()
        )

        # execute the upsert statements
        try:
            self.SUPPORT.execute(upsert_statement_invalid)
        except ServiceError:
            pass
        try:
            self.SUPPORT.execute(upsert_statement_wrong_context)
        except ServiceError:
            pass
        self.SUPPORT.execute(upsert_statement)

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
            self.SUPPORT.execute(select_wrong_context)
        except ServiceError:
            pass
        select_response = self.SUPPORT.execute(select_statement)
        records = select_response.to_dict()
        pprint.pprint(records)
        self.assertEqual(len(records), 3)
        self.assertAlmostEqual(records[0].get("gdp"), 2.72, 0.001)
        # self.assertAlmostEqual(records[1].get("gdp"), 4.0, 0.001)
        self.assertAlmostEqual(records[1].get("gdp"), 4.97, 0.001)
        self.assertAlmostEqual(records[2].get("gdp"), 20.49, 0.001)

        ###################

        # Sleep until the metrics get populated
        self._skip_interval(bios.time.now(), 35000)

        # Get the metrics
        usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())

        # Verify nothing is recorded in the usage metrics
        assert len(usage_metrics.keys()) == 0

        # Verify the operations metrics
        pprint.pprint(operation_metrics, width=200)
        assert len(operation_metrics) >= 10

        key = (self.TENANT_NAME, self.SIGNAL_SIMPLE_NAME, "INSERT", "support", "Internal")
        entry_insert = operation_metrics.get(key)
        self.assertIsNotNone(entry_insert)
        self.assertEqual(entry_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 4)

        key = (self.TENANT_NAME, self.SIGNAL_SIMPLE_NAME, "INSERT_BULK", "support", "Internal")
        entry_insert_bulk = operation_metrics.get(key)
        self.assertIsNotNone(entry_insert_bulk)
        self.assertEqual(entry_insert_bulk[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)

        key = (self.TENANT_NAME, self.SIGNAL_SIMPLE_NAME, "SELECT", "support", "Internal")
        entry_select = operation_metrics.get(key)
        self.assertIsNotNone(entry_select)
        self.assertEqual(entry_select[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)

        # for non validation errors
        key = (self.TENANT_NAME, "nonExisting", "INSERT", "support", "Internal")
        entry_failed_insert = operation_metrics.get(key)
        self.assertIsNotNone(entry_failed_insert)
        self.assertEqual(entry_failed_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 0)
        self.assertEqual(entry_failed_insert[self.OP_NUM_VALIDATION_ERRORS], 0)
        self.assertEqual(entry_failed_insert[self.OP_NUM_TRANSIENT_ERRORS], 1)

        key = (self.TENANT_NAME, "xxabckla", "INSERT_BULK", "support", "Internal")
        entry_failed_bulk_insert = operation_metrics.get(key)
        self.assertIsNotNone(entry_failed_bulk_insert)
        self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 0)
        self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_VALIDATION_ERRORS], 0)
        self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_TRANSIENT_ERRORS], 1)

        key = (self.TENANT_NAME, self.CONTEXT_SIMPLE_NAME, "SELECT_CONTEXT", "support", "Internal")
        entry_select = operation_metrics.get(key)
        self.assertIsNotNone(entry_select)
        self.assertEqual(entry_select[self.OP_NUM_SUCCESSFUL_OPERATIONS], 1)

        key = (self.TENANT_NAME, "nonExisting", "UPSERT", "support", "Internal")
        entry_failed_insert = operation_metrics.get(key)
        self.assertIsNotNone(entry_failed_insert)
        self.assertEqual(entry_failed_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 0)
        self.assertEqual(entry_failed_insert[self.OP_NUM_VALIDATION_ERRORS], 0)
        self.assertEqual(entry_failed_insert[self.OP_NUM_TRANSIENT_ERRORS], 1)

        key = (self.TENANT_NAME, "abcde", "SELECT_CONTEXT", "support", "Internal")
        entry_failed_bulk_insert = operation_metrics.get(key)
        self.assertIsNotNone(entry_failed_bulk_insert)
        self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS], 0)
        self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_VALIDATION_ERRORS], 0)
        self.assertEqual(entry_failed_bulk_insert[self.OP_NUM_TRANSIENT_ERRORS], 1)


if __name__ == "__main__":
    pytest.main(sys.argv)
