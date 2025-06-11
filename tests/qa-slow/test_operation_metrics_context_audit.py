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
from setup_common import setup_context
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsContextAudit(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()

        cls.CONTEXT_WITH_AUDIT_NAME = "contextWithAudit"
        cls.CONTEXT_WITH_AUDIT_CONFIG = {
            "contextName": cls.CONTEXT_WITH_AUDIT_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "primaryKey": ["product"],
            "auditEnabled": True,
        }

        cls.SIGNAL_AUDIT_NAME = "auditContextWithAudit"

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            setup_context(admin, cls.CONTEXT_WITH_AUDIT_CONFIG)

        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    @classmethod
    def _generate_random_string(cls, length):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for _ in range(length))

    def test_context_audit(self):
        """Test metrics for a context with audit enabled"""
        app_name = "test_context_with_audit"
        with bios.login(
            ep_url(),
            self.ADMIN_USER,
            admin_pass,
            app_name=app_name,
            app_type=bios.models.AppType.REALTIME,
        ) as admin:
            # phase 1: initial insertions #############################

            print("*** Putting context entries")
            start = bios.time.now()

            # Build the insert statements against the test signal
            product_1 = "product_1_" + self._generate_random_string(8)
            product_2 = "product_2_" + self._generate_random_string(8)
            product_3 = "product_3_" + self._generate_random_string(8)
            product_4 = "product_4_" + self._generate_random_string(8)
            entries = [f"{product_1},10", f"{product_2},20", f"{product_3},30", f"{product_4},40"]
            statement = (
                bios.isql().upsert().into(self.CONTEXT_WITH_AUDIT_NAME).csv_bulk(entries).build()
            )

            # execute the upsert statement
            admin.execute(statement)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 10000)

            # Wait for the next rollup
            # self._skip_interval(start, self.INTERVAL * 2)

            # Build the select statement for the test context
            select_statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_AUDIT_NAME)
                .time_range(start - 1000, bios.time.now() + 1000 - start)
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
            print("operation metrics:")
            pprint.pprint(operation_metrics, width=300)
            self.assertEqual(len(usage_metrics.keys()), 2)

            context_name = self.CONTEXT_WITH_AUDIT_NAME
            key_upsert_usage = (context_name, "UPSERT", app_name, "Realtime")
            usage_upsert = usage_metrics.get(key_upsert_usage)
            self.assertEqual(usage_upsert[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(usage_upsert[self.USG_NUM_WRITES], 4)
            self.assertEqual(usage_upsert[self.USG_NUM_READS], 0)
            self.assertEqual(usage_upsert[self.USG_NUM_VALIDATION_ERRORS], 0)

            key_upsert_ops = (self.TENANT_NAME, context_name, "UPSERT", app_name, "Realtime")
            ops_upsert = operation_metrics.get(key_upsert_ops)
            assert ops_upsert is not None
            assert ops_upsert[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_upsert[self.OP_NUM_WRITES] == 4
            assert ops_upsert[self.OP_BYTES_WRITTEN] > 0
            assert ops_upsert[self.OP_NUM_READS] == 0
            assert ops_upsert[self.OP_BYTES_READ] == 0
            assert ops_upsert[self.OP_LATENCY_MIN] > 0
            assert ops_upsert[self.OP_LATENCY_MAX] >= ops_upsert[self.OP_LATENCY_MIN]
            assert ops_upsert[self.OP_LATENCY_SUM] >= ops_upsert[self.OP_LATENCY_MAX]
            assert ops_upsert[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_upsert[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_upsert[self.OP_NUM_STORAGE_ACCESSES] == 1

            audit_signal = self.SIGNAL_AUDIT_NAME
            key_insert_ops = (self.TENANT_NAME, audit_signal, "INSERT_AUDIT", app_name, "Realtime")
            ops_insert = operation_metrics.get(key_insert_ops)
            assert ops_insert is not None
            assert ops_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_insert[self.OP_NUM_WRITES] == 4
            # assert ops_insert[self.OP_BYTES_WRITTEN] > 0
            assert ops_insert[self.OP_NUM_READS] == 0
            assert ops_insert[self.OP_BYTES_READ] == 0
            assert ops_insert[self.OP_LATENCY_MIN] > 0
            assert ops_insert[self.OP_LATENCY_MAX] >= ops_insert[self.OP_LATENCY_MIN]
            assert ops_insert[self.OP_LATENCY_SUM] >= ops_insert[self.OP_LATENCY_MAX]
            assert ops_insert[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_insert[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_insert[self.OP_NUM_STORAGE_ACCESSES] == 0

            key_select_usage = (audit_signal, "SELECT", app_name, "Realtime")
            entry_select = usage_metrics.get(key_select_usage)
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 4)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

            key_select_ops = (self.TENANT_NAME, audit_signal, "SELECT", app_name, "Realtime")
            ops_select = operation_metrics.get(key_select_ops)
            assert ops_select is not None
            assert ops_select[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_select[self.OP_NUM_WRITES] == 0
            assert ops_select[self.OP_BYTES_WRITTEN] == 0
            assert ops_select[self.OP_NUM_READS] == 4
            assert ops_select[self.OP_BYTES_READ] > 0
            assert ops_select[self.OP_LATENCY_MIN] > 0
            assert ops_select[self.OP_LATENCY_MAX] >= ops_select[self.OP_LATENCY_MIN]
            assert ops_select[self.OP_LATENCY_SUM] >= ops_select[self.OP_LATENCY_MAX]
            assert ops_select[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_select[self.OP_NUM_TRANSIENT_ERRORS] == 0

            # phase 2: overwriting #####################################

            print("*** Overwriting context entries")
            start = bios.time.now()

            # partially update the entries
            product_5 = "product_5_" + self._generate_random_string(8)
            entries = [f"{product_2},20", f"{product_3},20", f"{product_4},40", f"{product_5},50"]
            statement = (
                bios.isql().upsert().into(self.CONTEXT_WITH_AUDIT_NAME).csv_bulk(entries).build()
            )
            admin.execute(statement)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 10000)

            # Wait for the next rollup
            # self._skip_interval(start, self.INTERVAL * 2)

            # Build the select statement for the test context
            select_statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_AUDIT_NAME)
                .time_range(start - 1000, bios.time.now() + 1000 - start)
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            self.assertEqual(len(records), 2)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            print("operation metrics:")
            pprint.pprint(operation_metrics, width=300)
            self.assertEqual(len(usage_metrics.keys()), 2)

            entry_upsert = usage_metrics.get(key_upsert_usage)
            self.assertEqual(entry_upsert[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_upsert[self.USG_NUM_WRITES], 4)
            self.assertEqual(entry_upsert[self.USG_NUM_READS], 0)
            self.assertEqual(entry_upsert[self.USG_NUM_VALIDATION_ERRORS], 0)

            ops_upsert = operation_metrics.get(key_upsert_ops)
            assert ops_upsert is not None
            assert ops_upsert[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_upsert[self.OP_NUM_WRITES] == 4
            assert ops_upsert[self.OP_BYTES_WRITTEN] > 0
            assert ops_upsert[self.OP_NUM_READS] == 0
            assert ops_upsert[self.OP_BYTES_READ] == 0
            assert ops_upsert[self.OP_LATENCY_MIN] > 0
            assert ops_upsert[self.OP_LATENCY_MAX] >= ops_upsert[self.OP_LATENCY_MIN]
            assert ops_upsert[self.OP_LATENCY_SUM] >= ops_upsert[self.OP_LATENCY_MAX]
            assert ops_upsert[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_upsert[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_upsert[self.OP_NUM_STORAGE_ACCESSES] == 1

            ops_insert = operation_metrics.get(key_insert_ops)
            assert ops_insert is not None
            assert ops_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_insert[self.OP_NUM_WRITES] == 2
            # assert ops_insert[self.OP_BYTES_WRITTEN] > 0
            assert ops_insert[self.OP_NUM_READS] == 0
            assert ops_insert[self.OP_BYTES_READ] == 0
            assert ops_insert[self.OP_LATENCY_MIN] > 0
            assert ops_insert[self.OP_LATENCY_MAX] >= ops_insert[self.OP_LATENCY_MIN]
            assert ops_insert[self.OP_LATENCY_SUM] >= ops_insert[self.OP_LATENCY_MAX]
            assert ops_insert[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_insert[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_insert[self.OP_NUM_STORAGE_ACCESSES] == 0

            entry_select = usage_metrics.get(key_select_usage)
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 2)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

            ops_select = operation_metrics.get(key_select_ops)
            assert ops_select is not None
            assert ops_select[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_select[self.OP_NUM_WRITES] == 0
            assert ops_select[self.OP_BYTES_WRITTEN] == 0
            assert ops_select[self.OP_NUM_READS] == 2
            assert ops_select[self.OP_BYTES_READ] > 0
            assert ops_select[self.OP_LATENCY_MIN] > 0
            assert ops_select[self.OP_LATENCY_MAX] >= ops_select[self.OP_LATENCY_MIN]
            assert ops_select[self.OP_LATENCY_SUM] >= ops_select[self.OP_LATENCY_MAX]
            assert ops_select[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_select[self.OP_NUM_TRANSIENT_ERRORS] == 0

            # phase 3: updating ########################################

            print("*** Updating a context entry")
            start = bios.time.now()

            # update an entry
            statement = (
                bios.isql()
                .update(self.CONTEXT_WITH_AUDIT_NAME)
                .set({"price": 123})
                .where(key=[product_1])
                .build()
            )
            admin.execute(statement)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 10000)

            # Wait for the next rollup
            # self._skip_interval(start, self.INTERVAL * 2)

            # Build the select statement for the test context
            select_statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_AUDIT_NAME)
                .time_range(start - 1000, bios.time.now() + 1000 - start)
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            self.assertEqual(len(records), 1)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            print("operation metrics:")
            pprint.pprint(operation_metrics, width=300)
            self.assertEqual(len(usage_metrics.keys()), 2)

            key_update_usage = (context_name, "UPDATE", app_name, "Realtime")
            entry_update = usage_metrics.get(key_update_usage)
            self.assertEqual(entry_update[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_update[self.USG_NUM_WRITES], 1)
            self.assertEqual(entry_update[self.USG_NUM_READS], 1)
            self.assertEqual(entry_update[self.USG_NUM_VALIDATION_ERRORS], 0)

            key_update_ops = (self.TENANT_NAME, context_name, "UPDATE", app_name, "Realtime")
            ops_update = operation_metrics.get(key_update_ops)
            assert ops_update is not None
            assert ops_update[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_update[self.OP_NUM_WRITES] == 1
            assert ops_update[self.OP_BYTES_WRITTEN] > 0
            assert ops_update[self.OP_NUM_READS] == 1
            # assert ops_update[self.OP_BYTES_READ] > 0
            assert ops_update[self.OP_LATENCY_MIN] > 0
            assert ops_update[self.OP_LATENCY_MAX] >= ops_update[self.OP_LATENCY_MIN]
            assert ops_update[self.OP_LATENCY_SUM] >= ops_update[self.OP_LATENCY_MAX]
            assert ops_update[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_update[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_update[self.OP_NUM_STORAGE_ACCESSES] == 1

            ops_insert = operation_metrics.get(key_insert_ops)
            assert ops_insert is not None
            assert ops_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_insert[self.OP_NUM_WRITES] == 1
            # assert ops_insert[self.OP_BYTES_WRITTEN] > 0
            assert ops_insert[self.OP_NUM_READS] == 0
            assert ops_insert[self.OP_BYTES_READ] == 0
            assert ops_insert[self.OP_LATENCY_MIN] > 0
            assert ops_insert[self.OP_LATENCY_MAX] >= ops_insert[self.OP_LATENCY_MIN]
            assert ops_insert[self.OP_LATENCY_SUM] >= ops_insert[self.OP_LATENCY_MAX]
            assert ops_insert[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_insert[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_insert[self.OP_NUM_STORAGE_ACCESSES] == 0

            entry_select = usage_metrics.get(key_select_usage)
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 1)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

            ops_select = operation_metrics.get(key_select_ops)
            assert ops_select is not None
            assert ops_select[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_select[self.OP_NUM_WRITES] == 0
            assert ops_select[self.OP_BYTES_WRITTEN] == 0
            assert ops_select[self.OP_NUM_READS] == 1
            # assert ops_select[self.OP_BYTES_READ] > 0
            assert ops_select[self.OP_LATENCY_MIN] > 0
            assert ops_select[self.OP_LATENCY_MAX] >= ops_select[self.OP_LATENCY_MIN]
            assert ops_select[self.OP_LATENCY_SUM] >= ops_select[self.OP_LATENCY_MAX]
            assert ops_select[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_select[self.OP_NUM_TRANSIENT_ERRORS] == 0

            # phase 4: deleting ########################################

            print("*** Deleting a context entries")
            start = bios.time.now()

            # update an entry
            statement = (
                bios.isql()
                .delete()
                .from_context(self.CONTEXT_WITH_AUDIT_NAME)
                .where(keys=[[product_1], [product_3]])
                .build()
            )
            admin.execute(statement)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 10000)

            # Wait for the next rollup
            # self._skip_interval(start, self.INTERVAL * 2)

            # Build the select statement for the test context
            select_statement = (
                bios.isql()
                .select()
                .from_signal(self.SIGNAL_AUDIT_NAME)
                .time_range(start - 1000, bios.time.now() + 1000 - start)
                .build()
            )
            select_response = admin.execute(select_statement)
            records = select_response.to_dict()
            pprint.pprint(records)
            self.assertEqual(len(records), 2)

            # Sleep until the metrics get populated
            self._skip_interval(bios.time.now(), 35000)

            # Verify the metrics
            usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())
            print("operation metrics:")
            pprint.pprint(operation_metrics, width=300)
            self.assertEqual(len(usage_metrics.keys()), 2)

            key_delete_usage = (context_name, "DELETE", app_name, "Realtime")
            entry_delete = usage_metrics.get(key_delete_usage)
            self.assertEqual(entry_delete[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_delete[self.USG_NUM_WRITES], 2)
            self.assertEqual(entry_delete[self.USG_NUM_READS], 0)
            self.assertEqual(entry_delete[self.USG_NUM_VALIDATION_ERRORS], 0)

            key_delete_ops = (self.TENANT_NAME, context_name, "DELETE", app_name, "Realtime")
            ops_delete = operation_metrics.get(key_delete_ops)
            assert ops_delete is not None
            assert ops_delete[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_delete[self.OP_NUM_WRITES] == 2
            assert ops_delete[self.OP_BYTES_WRITTEN] == 0
            assert ops_delete[self.OP_NUM_READS] == 0
            assert ops_delete[self.OP_BYTES_READ] == 0
            assert ops_delete[self.OP_LATENCY_MIN] > 0
            assert ops_delete[self.OP_LATENCY_MAX] >= ops_delete[self.OP_LATENCY_MIN]
            assert ops_delete[self.OP_LATENCY_SUM] >= ops_delete[self.OP_LATENCY_MAX]
            assert ops_delete[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_delete[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_update[self.OP_NUM_STORAGE_ACCESSES] == 1

            ops_insert = operation_metrics.get(key_insert_ops)
            assert ops_insert is not None
            assert ops_insert[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_insert[self.OP_NUM_WRITES] == 2
            # assert ops_insert[self.OP_BYTES_WRITTEN] > 0
            assert ops_insert[self.OP_NUM_READS] == 0
            assert ops_insert[self.OP_BYTES_READ] == 0
            assert ops_insert[self.OP_LATENCY_MIN] > 0
            assert ops_insert[self.OP_LATENCY_MAX] >= ops_insert[self.OP_LATENCY_MIN]
            assert ops_insert[self.OP_LATENCY_SUM] >= ops_insert[self.OP_LATENCY_MAX]
            assert ops_insert[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_insert[self.OP_NUM_TRANSIENT_ERRORS] == 0
            assert ops_insert[self.OP_NUM_STORAGE_ACCESSES] == 0

            entry_select = usage_metrics.get(key_select_usage)
            self.assertEqual(entry_select[self.USG_NUM_SUCCESSFUL_OPERATIONS], 1)
            self.assertEqual(entry_select[self.USG_NUM_WRITES], 0)
            self.assertEqual(entry_select[self.USG_NUM_READS], 2)
            self.assertEqual(entry_select[self.USG_NUM_VALIDATION_ERRORS], 0)

            ops_select = operation_metrics.get(key_select_ops)
            assert ops_select is not None
            assert ops_select[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
            assert ops_select[self.OP_NUM_WRITES] == 0
            assert ops_select[self.OP_BYTES_WRITTEN] == 0
            assert ops_select[self.OP_NUM_READS] == 2
            assert ops_select[self.OP_BYTES_READ] > 0
            assert ops_select[self.OP_LATENCY_MIN] > 0
            assert ops_select[self.OP_LATENCY_MAX] >= ops_select[self.OP_LATENCY_MIN]
            assert ops_select[self.OP_LATENCY_SUM] >= ops_select[self.OP_LATENCY_MAX]
            assert ops_select[self.OP_NUM_VALIDATION_ERRORS] == 0
            assert ops_select[self.OP_NUM_TRANSIENT_ERRORS] == 0


if __name__ == "__main__":
    pytest.main(sys.argv)
