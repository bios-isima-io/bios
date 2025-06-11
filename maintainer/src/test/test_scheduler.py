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
import unittest

from dbdozer.scheduler import DozerStatusRecorder, KeyspaceMaintenanceProgressItem


class TestProgress(unittest.TestCase):
    """Tests progress classes"""

    def test_maintenance_progress_item(self):
        recorded_entry = {
            "last_backup_time": 1654197535,
            "last_finished": 1654197535,
            "last_incr_backup_time": 1653730366,
            "last_started": 1654197482,
            "status": "done",
        }
        item = KeyspaceMaintenanceProgressItem(recorded_entry)
        self.assertEqual(item.last_started, 1654197482)
        self.assertEqual(item.last_finished, 1654197535)
        self.assertEqual(item.last_backup_time, 1654197535)
        self.assertEqual(item.last_incr_backup_time, 1653730366)
        self.assertEqual(item.status, "done")

        partially_filled_entry = {
            "last_finished": 1654197535,
            "last_started": 1654197482,
            "status": "done",
        }
        item2 = KeyspaceMaintenanceProgressItem(partially_filled_entry)
        self.assertEqual(item2.last_started, 1654197482)
        self.assertEqual(item2.last_finished, 1654197535)
        self.assertIsNone(item2.last_backup_time)
        self.assertIsNone(item2.last_incr_backup_time)
        self.assertEqual(item2.status, "done")

        print(f"{json.dumps(item2.to_dict(), indent=2)}")

    @unittest.skip("this is not actually testing")
    def test_loading_progress(self):
        status_data = {
            "system_auth": {
                "last_finished": 1654195711,
                "last_started": 1654188858,
                "status": "done",
            },
            "system_distributed": {
                "last_finished": 1654196398,
                "last_started": 1654195711,
                "status": "done",
            },
            "system_traces": {
                "last_finished": 1654196423,
                "last_started": 1654196398,
                "status": "done",
            },
            "tfos_admin": {
                "last_backup_time": 1654196555,
                "last_finished": 1654196555,
                "last_incr_backup_time": 1653729639,
                "last_started": 1654196423,
                "status": "done",
            },
            "tfos_bi_meta": {
                "last_backup_time": 1654197535,
                "last_finished": 1654197535,
                "last_incr_backup_time": 1653730366,
                "last_started": 1654197482,
                "status": "done",
            },
            "tfos_d_0d9930f000d1347cbbbac64f3498fe70": {
                "last_backup_time": 1654206290,
                "last_finished": 0,
                "last_incr_backup_time": 1653759760,
                "last_started": 1654198095,
                "status": "done",
            },
            "tfos_d_0efe78e88f35367db2aacce867f57e5b": {
                "last_finished": 1654215784,
                "last_started": 1654214925,
                "status": "done",
            },
        }
        scheduler = DozerStatusRecorder()
        scheduler.load_keyspace_statuses(status_data)

        aaa = scheduler.to_dict()
        print(json.dumps(aaa, indent=2))


if __name__ == "__main__":
    unittest.main()
