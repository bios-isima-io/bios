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
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from operation_metrics_test_base import OperationMetricsTestBase


class TestOperationMetricsLogin(OperationMetricsTestBase):
    @classmethod
    def setUpClass(cls):
        cls.setup_common()
        print("--- Sleep for 35 seconds to wait for metrics for initialization ops being flushed")
        time.sleep(35)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_common()

    def test_login(self):
        """Test metrics of login operations"""

        start = bios.time.now()
        app_name = "test_login"
        with bios.login(
            ep_url(),
            self.ADMIN_USER,
            admin_pass,
            app_name=app_name,
            app_type=bios.models.AppType.REALTIME,
        ) as admin:
            # do whatever
            admin.get_signals()

        # Sleep until the metrics get populated
        self._skip_interval(bios.time.now(), 35000)

        # Get the metrics
        usage_metrics, operation_metrics = self._get_metrics(start, bios.time.now())

        print("USAGE")
        pprint.pprint(usage_metrics)
        # Verify usage signal does not include login record
        assert len(usage_metrics) == 0

        print("OPERATIONS")
        pprint.pprint(operation_metrics)

        entry_login = operation_metrics.get(
            (
                self.TENANT_NAME,
                "",
                "LOGIN",
                app_name,
                "Realtime",
            )
        )

        assert entry_login is not None
        assert entry_login[self.OP_NUM_SUCCESSFUL_OPERATIONS] == 1
        assert entry_login[self.OP_NUM_WRITES] == 0
        assert entry_login[self.OP_BYTES_WRITTEN] == 0
        assert entry_login[self.OP_NUM_READS] == 0
        assert entry_login[self.OP_BYTES_READ] > 0
        assert entry_login[self.OP_LATENCY_MIN] > 0
        assert entry_login[self.OP_LATENCY_MAX] >= entry_login[self.OP_LATENCY_MIN]
        assert entry_login[self.OP_LATENCY_SUM] >= entry_login[self.OP_LATENCY_MAX]
        assert entry_login[self.OP_NUM_VALIDATION_ERRORS] == 0
        assert entry_login[self.OP_NUM_TRANSIENT_ERRORS] == 0


if __name__ == "__main__":
    pytest.main(sys.argv)
