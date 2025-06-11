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


class TestOperationMetricsLastN(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()

        cls.INTERVAL = 60000

        cls.CONTEXT_LAST_N_NAME = "signalWithLastNFac_last15Visits"
        cls.CONTEXT_LAST_N_CONFIG = {
            "contextName": cls.CONTEXT_LAST_N_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "userName", "type": "String"},
                {"attributeName": "visits", "type": "String"},
            ],
            "primaryKey": ["userName"],
        }

        cls.SIGNAL_WITH_LAST_N_FAC_NAME = "signalWithLastNFac"
        cls.SIGNAL_WITH_LAST_N_FAC_CONFIG = {
            "signalName": cls.SIGNAL_WITH_LAST_N_FAC_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "userId", "type": "String"},
                {"attributeName": "purpose", "type": "String"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "last15Visits",
                        "dimensions": ["userId"],
                        "attributes": ["purpose"],
                        "dataSketches": ["LastN"],
                        "items": 15,
                        "featureInterval": cls.INTERVAL,
                    }
                ]
            },
        }

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            setup_context(admin, cls.CONTEXT_LAST_N_CONFIG)
            setup_signal(admin, cls.SIGNAL_WITH_LAST_N_FAC_CONFIG)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def test_last_n_feature_as_context(self):
        """Test metrics for a signal with a LastN feature as context"""
        app_name = "test_last_n_fac"
        with bios.login(
            ep_url(),
            self.ADMIN_USER,
            admin_pass,
            app_name=app_name,
            app_type=bios.models.AppType.REALTIME,
        ) as admin:
            # Build the insert statements against the test signal
            simple_signals = [
                "user_1,firstVisit",
                "user_2,firstVisit",
                "user_3,regularVisit",
                "user_4,renewal",
                "user_2,regularVisit",
            ]
            insert_statements = [
                bios.isql().insert().into(self.SIGNAL_WITH_LAST_N_FAC_NAME).csv(signal).build()
                for signal in simple_signals
            ]
            simple_bulk_signals = ["user_4,regularVisit", "user_2,payment", "user_3,renewal"]
            insert_bulk_statement = (
                bios.isql()
                .insert()
                .into(self.SIGNAL_WITH_LAST_N_FAC_NAME)
                .csv_bulk(simple_bulk_signals)
                .build()
            )

            # execute insert statements
            insert_responses = [admin.execute(statement) for statement in insert_statements]
            bulk_insert_response = admin.execute(insert_bulk_statement)

            start = insert_responses[0].records[0].timestamp
            end = bulk_insert_response.records[-1].timestamp

            # Wait for the next rollup
            self._skip_interval(start, self.INTERVAL * 2)

            # Build the select statement for the test context
            select_statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_LAST_N_NAME.lower())
                .where(keys=[["user_1"], ["user_2"], ["user_3"], ["user_4"], ["user_5"]])
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            self.assertEqual(len(records), 4)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            self.assertEqual(len(usage_metrics.keys()), 2)

            entry_insert = usage_metrics.get(
                (
                    self.SIGNAL_WITH_LAST_N_FAC_NAME,
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
                    self.CONTEXT_LAST_N_NAME,
                    "SELECT",
                    app_name,
                    "Realtime",
                )
            )
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 4)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

    @classmethod
    def _generate_random_string(cls, length):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for _ in range(length))


if __name__ == "__main__":
    pytest.main(sys.argv)
