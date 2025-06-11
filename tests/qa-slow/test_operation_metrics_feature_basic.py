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

import sys
import time

import bios
import pytest
from setup_common import setup_signal
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsFeature(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()

        cls.INTERVAL = 60000

        cls.SIGNAL_WITH_FEATURE_NAME = "signalWithFeature"
        cls.SIGNAL_WITH_FEATURE_CONFIG = {
            "signalName": cls.SIGNAL_WITH_FEATURE_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "country", "type": "String"},
                {"attributeName": "state", "type": "String"},
                {"attributeName": "population", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "byCountryState",
                        "dimensions": ["country", "state"],
                        "attributes": ["population"],
                        "featureInterval": cls.INTERVAL,
                    }
                ]
            },
        }

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            setup_signal(admin, cls.SIGNAL_WITH_FEATURE_CONFIG)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def test_feature_basic(self):
        """Test metrics for a signal with a feature"""
        app_name = "test_feature_basic"
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
                "USA,AZ,5155",
                "Japan,Tokyo,5432",
                "India,Goa,9876",
            ]
            insert_statements = [
                bios.isql().insert().into(self.SIGNAL_WITH_FEATURE_NAME).csv(signal).build()
                for signal in simple_signals
            ]
            simple_bulk_signals = ["USA,OR,31893", "USA,WA,29873", "Japan,Kagoshima,751"]
            insert_bulk_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_WITH_FEATURE_NAME)
                .csv_bulk(simple_bulk_signals)
                .build()
            )

            # execute insert statements
            insert_responses = [admin.execute(statement) for statement in insert_statements]
            bulk_insert_response = admin.execute(insert_bulk_statement)

            start = insert_responses[0].records[0].timestamp
            bulk_insert_response.records[-1].timestamp

            # Wait for the next rollup
            self._skip_interval(start, self.INTERVAL * 2)

            # Build the select statement for the test signal
            select_statement = (
                bios.isql()
                .select("max(population)")
                .from_signal(self.SIGNAL_WITH_FEATURE_NAME.lower())
                .group_by("country")
                .tumbling_window(self.INTERVAL)
                .time_range(start, self.INTERVAL)
                .build()
            )
            select_response = admin.execute(select_statement)
            data_window = select_response.data_windows[0]
            self.assertEqual(len(data_window.records), 4)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            self.assertEqual(len(usage_metrics.keys()), 2)

            entry_insert = usage_metrics.get(
                (
                    self.SIGNAL_WITH_FEATURE_NAME,
                    "INSERT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertEqual(entry_insert[self.USG_NUM_SUCCESSFUL_OPERATIONS], 6)
            self.assertEqual(entry_insert[self.USG_NUM_WRITES], 8)
            self.assertEqual(entry_insert[self.USG_NUM_READS], 0)
            self.assertEqual(entry_insert[self.USG_NUM_VALIDATION_ERRORS], 0)

            entry_select = usage_metrics.get(
                (
                    self.SIGNAL_WITH_FEATURE_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 4)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)


if __name__ == "__main__":
    pytest.main(sys.argv)
