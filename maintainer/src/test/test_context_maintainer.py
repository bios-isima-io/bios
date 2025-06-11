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

import heapq
import unittest

from dbdozer.context_maintainer import ContextMaintenanceStatus


class ContextMaintainerTest(unittest.TestCase):
    def test_status_initial_loading(self):
        src = {
            "tenant_name": "testTenant",
            "context_name": "testContext",
        }
        status = ContextMaintenanceStatus(src, 600)
        self.assertEqual(status.tenant_name, "testTenant")
        self.assertEqual(status.context_name, "testContext")
        self.assertEqual(status.last_finished, 0)
        self.assertEqual(status.next_maintenance_time, 600)
        self.assertIsNone(status.keyspace_name)
        self.assertIsNone(status.table_name)

        dumped = status.to_dict()
        self.assertEqual(dumped.get("tenant_name"), "testTenant")
        self.assertEqual(dumped.get("context_name"), "testContext")
        self.assertEqual(dumped.get("last_finished"), 0)
        self.assertIsNone(dumped.get("next_maintenance_time"))
        self.assertIsNone(dumped.get("keyspace_name"))
        self.assertIsNone(dumped.get("table_name"))
        self.assertEqual(status.next_maintenance_time, 600)

    def test_status_loading_with_keyspace_and_table(self):
        src = {
            "tenant_name": "testTenant",
            "context_name": "testContext",
            "last_finished": 1234567,
            "keyspace_name": "theKeyspaceName",
            "table_name": "theTableName",
        }
        status = ContextMaintenanceStatus(src, 600)
        self.assertEqual(status.tenant_name, "testTenant")
        self.assertEqual(status.context_name, "testContext")
        self.assertEqual(status.last_finished, 1234567)
        self.assertEqual(status.next_maintenance_time, 1234567 + 600)
        self.assertEqual(status.keyspace_name, "theKeyspaceName")
        self.assertEqual(status.table_name, "theTableName")

        dumped = status.to_dict()
        self.assertEqual(dumped, src)

    def test_priority_queue(self):
        items = []
        heapq.heappush(
            items,
            ContextMaintenanceStatus(
                {
                    "tenant_name": "testTenant1",
                    "context_name": "testContext1",
                    "last_finished": 1000000,
                },
                600,
            ),
        )
        heapq.heappush(
            items,
            ContextMaintenanceStatus(
                {
                    "tenant_name": "testTenant2",
                    "context_name": "testContext2",
                    "last_finished": 1000001,
                },
                600,
            ),
        )
        heapq.heappush(
            items,
            ContextMaintenanceStatus(
                {
                    "tenant_name": "testTenant3",
                    "context_name": "testContext3",
                    "last_finished": 1000000,
                },
                500,
            ),
        )
        self.assertEqual(items[0].context_name, "testContext3")
        heapq.heappop(items)
        second = heapq.heappop(items)
        self.assertEqual(second.context_name, "testContext1")
        third = heapq.heappop(items)
        self.assertEqual(third.context_name, "testContext2")


if __name__ == "__main__":
    unittest.main()
