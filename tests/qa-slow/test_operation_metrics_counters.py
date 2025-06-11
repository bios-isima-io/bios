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
import string
import sys
import time

import bios
import pytest
from setup_common import setup_context, setup_signal
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsCounters(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()

        cls.INTERVAL = 5000

        cls.CONTEXT_SNAPSHOTS_NAME = "updates_byCountryState"
        cls.CONTEXT_SNAPSHOTS = {
            "contextName": cls.CONTEXT_SNAPSHOTS_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "country", "type": "String"},
                {"attributeName": "state", "type": "String"},
                {"attributeName": "timestamp", "type": "Integer"},
                {"attributeName": "population", "type": "Decimal"},
            ],
            "primaryKey": ["country", "state"],
        }

        cls.SIGNAL_UPDATES_NAME = "updates"
        cls.SIGNAL_UPDATES = {
            "signalName": cls.SIGNAL_UPDATES_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "country", "type": "String"},
                {"attributeName": "state", "type": "String"},
                {
                    "attributeName": "operation",
                    "type": "String",
                    "allowedValues": ["set", "change"],
                },
                {"attributeName": "population", "type": "Decimal"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "byCountryState",
                        "dimensions": ["country", "state", "operation"],
                        "attributes": ["population"],
                        "featureInterval": cls.INTERVAL,
                        "materializedAs": "AccumulatingCount",
                        "indexOnInsert": True,
                    }
                ]
            },
        }

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            setup_context(admin, cls.CONTEXT_SNAPSHOTS)
            setup_signal(admin, cls.SIGNAL_UPDATES)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def test_counters(self):
        """Test metrics for a signal with a LastN feature as context"""
        app_name = "test_counters"
        with bios.login(
            ep_url(),
            self.ADMIN_USER,
            admin_pass,
            app_name=app_name,
            app_type=bios.models.AppType.REALTIME,
        ) as admin:
            # Build the insert statements against the test signal
            simple_signals = [
                "USA,California,set,10000",
                "USA,California,change,1000",
                "USA,Oregon,change,1100",
                "Japan,Tokyo,set,100000",
                "Japan,Kanagawa,change,55000",
            ]
            insert_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_UPDATES_NAME)
                .csv_bulk(simple_signals)
                .build()
            )

            start = bios.time.now()
            admin.execute(insert_statement)

            # try counter recency fetch
            select_statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_SNAPSHOTS_NAME)
                .where(keys=[["Japan", "Tokyo"], ["USA", "California"]])
                .on_the_fly()
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            assert len(records) == 2

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            pprint.pprint(operation_metrics)
            pprint.pprint(usage_metrics)
            assert len(usage_metrics.keys()) == 2

            entry_insert = usage_metrics.get(
                (
                    self.SIGNAL_UPDATES_NAME,
                    "INSERT",
                    app_name,
                    "Realtime",
                )
            )
            assert entry_insert is not None
            assert entry_insert[self.USG_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert entry_insert[self.USG_NUM_WRITES] == 5
            assert entry_insert[self.USG_NUM_READS] == 0
            assert entry_insert[self.USG_NUM_VALIDATION_ERRORS] == 0

            entry_select = usage_metrics.get(
                (
                    self.SIGNAL_UPDATES_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            has_entry_select = entry_select is not None
            if has_entry_select:
                assert entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS] == 1
                assert entry_select[self.USG_NUM_WRITES] == 0
                assert entry_select[self.USG_NUM_READS] == 3
                assert entry_select[self.USG_NUM_VALIDATION_ERRORS] == 0

            if not has_entry_select:
                entry_select = usage_metrics.get(
                    (
                        self.CONTEXT_SNAPSHOTS_NAME,
                        "SELECT",
                        app_name,
                        "Realtime",
                    )
                )
                has_entry_select = entry_select is not None
                assert entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS] == 1
                assert entry_select[self.USG_NUM_WRITES] == 0
                assert entry_select[self.USG_NUM_READS] == 2
                assert entry_select[self.USG_NUM_VALIDATION_ERRORS] == 0

            system_entry_select_context = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.CONTEXT_SNAPSHOTS_NAME,
                    "SELECT_CONTEXT",
                    app_name,
                    "Realtime",
                )
            )
            assert system_entry_select_context is not None
            assert system_entry_select_context[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert system_entry_select_context[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert system_entry_select_context[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert system_entry_select_context[self.OP_NUM_WRITES] == 0
            context_num_reads = system_entry_select_context[self.OP_NUM_READS]

            system_entry_select_signal = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.SIGNAL_UPDATES_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            assert system_entry_select_signal is not None
            assert system_entry_select_signal[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert system_entry_select_signal[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert system_entry_select_signal[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert system_entry_select_signal[self.OP_NUM_WRITES] == 0
            signal_num_reads = system_entry_select_signal[self.OP_NUM_READS]

            assert (context_num_reads == 0 and signal_num_reads == 3) or (
                context_num_reads == 2 and signal_num_reads == 0
            )

            # try performance fetch
            start = bios.time.now()
            select_statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_SNAPSHOTS_NAME)
                .where(keys=[["Japan", "Kanagawa"], ["USA", "California"]])
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            assert len(records) == 2

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics again
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            pprint.pprint(operation_metrics)
            pprint.pprint(usage_metrics)
            assert len(usage_metrics.keys()) == 1
            key = (
                self.CONTEXT_SNAPSHOTS_NAME,
                "SELECT",
                app_name,
                "Realtime",
            )
            entry_select = usage_metrics.get(key)
            assert entry_select is not None
            assert entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert entry_select[self.USG_NUM_WRITES] == 0
            assert entry_select[self.USG_NUM_READS] == 2
            assert entry_select[self.USG_NUM_VALIDATION_ERRORS] == 0

            system_entry_select_context = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.CONTEXT_SNAPSHOTS_NAME,
                    "SELECT_CONTEXT",
                    app_name,
                    "Realtime",
                )
            )
            assert system_entry_select_context is not None
            assert system_entry_select_context[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert system_entry_select_context[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert system_entry_select_context[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert system_entry_select_context[self.OP_NUM_WRITES] == 0
            assert system_entry_select_context[self.OP_NUM_READS] == 2

            system_entry_select_signal = operation_metrics.get(
                (
                    self.TENANT_NAME,
                    self.SIGNAL_UPDATES_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            assert system_entry_select_signal is None


if __name__ == "__main__":
    pytest.main(sys.argv)
