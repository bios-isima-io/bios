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
from bios.models import Metric
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user, support_pass, support_user

# from tutils import RollupTestHelper, summarize_checkpoint_floor, get_next_checkpoint


class OperationMetricsTestBase(unittest.TestCase):
    USG_NUM_SUCCESSFUL_OPERATIONS = 0
    USG_NUM_WRITES = 1
    USG_NUM_VALIDATION_ERRORS = 2
    USG_NUM_READS = 3

    OP_NUM_SUCCESSFUL_OPERATIONS = 0
    OP_NUM_VALIDATION_ERRORS = 1
    OP_NUM_TRANSIENT_ERRORS = 2
    OP_NUM_WRITES = 3
    OP_BYTES_WRITTEN = 4
    OP_NUM_READS = 5
    OP_BYTES_READ = 6
    OP_LATENCY_MIN = 7
    OP_LATENCY_MAX = 8
    OP_LATENCY_SUM = 9
    OP_NUM_STORAGE_ACCESSES = 10
    OP_STORAGE_LATENCY_MIN = 11
    OP_STORAGE_LATENCY_MAX = 12
    OP_STORAGE_LATENCY_SUM = 13

    @classmethod
    def setup_common(cls):
        cls.TENANT_NAME = f"operationMetricsTest{random.randint(1,65536)}"
        print("Tenant name is " + cls.TENANT_NAME)

        cls.ADMIN_USER = f"{admin_user}@{cls.TENANT_NAME}"
        cls.SUPPORT_USER = f"{support_user}+{cls.TENANT_NAME}@isima.io"

        cls.SADMIN = bios.login(ep_url(), sadmin_user, sadmin_pass)
        setup_tenant_config(clear_tenant=True, tenant_name=cls.TENANT_NAME)

        # we keep a session with the support user to be used for fetching _usage signal
        sleep_time = 5
        for _ in range(3):
            try:
                cls.SUPPORT = bios.login(
                    ep_url(),
                    cls.SUPPORT_USER,
                    support_pass,
                    app_name="support",
                    app_type=bios.models.AppType.REALTIME,
                )
                break
            except ServiceError as err:
                if err.error_code != ErrorCode.UNAUTHORIZED:
                    raise
                # Creating users may delay?
                time.sleep(sleep_time)
                sleep_time *= 2

    @classmethod
    def teardown_common(cls):
        cls.SUPPORT.close()
        cls.SADMIN.close()

    @classmethod
    def _ts2date(self, timestamp):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

    @classmethod
    def _skip_interval(cls, origin, interval=None):
        """Method to sleep until the next rollup interval.

        The method sleeps until the next rollup window from the origin time

        Args:
            origin (int): Origin milliseconds.
            interval (int): Interval milliseconds to skip.  The default interval is used
                if not speicfied.
        """
        if not interval:
            interval = 60000
        sleep_time = (origin + interval) / 1000 - time.time()
        if sleep_time > 0:
            print(f"-- Sleeping until {cls._ts2date((origin + interval) / 1000)}")
            time.sleep(sleep_time)

    def _get_metrics(
        self, start: int, end: int, signal_name: str = None
    ) -> Tuple[
        Dict[Tuple[str, str, str, str], Tuple[int, int, int, int]],
        Dict[Tuple[str, str, str, str, str], List[int]],
    ]:
        """Fetches metrics signals, returns a tuple of maps (for _usage and _operations) of
           group key and tuple of following dictionaries
        for _usage :
         A key is tuple of following
          - stream
          - request
          - appName
          - appType
         A value is tuple of following:
          - successCount
          - numWrites
          - numValidationErrors
          - numReads
        for _operations :
         A key is tuple of following
          - tenant
          - stream
          - request
          - appName
          - appType
         A value is tuple of following:
          - successCount
          - numValidationErrors
          - numTransientErrors
          - numWrites
          - bytesWritten
          - numReads
          - bytesRead
          - latencyMin
          - latencyMax
          - latencySum
          - numStorageAccesses
          - storageLatencyMin
          - storageLatencyMax
          - storageLatencySum
        """
        statement_ongoing = bios.isql().select().from_signal("_usage")
        if signal_name:
            statement_ongoing = statement_ongoing.where(f"stream = '{signal_name}'")
        statement = statement_ongoing.time_range(start, end - start).build()
        usage_response = self.SUPPORT.execute(statement)
        usage_metrics = self._parse_usage_metrics(usage_response.to_dict())

        filter_terms = [f"tenant = '{self.TENANT_NAME}'"]
        if signal_name:
            filter_terms.append(f"stream = '{signal_name}'")
        operations_statement = (
            bios.isql()
            .select()
            .from_signal("_operations")
            .where(" AND ".join(filter_terms))
            .time_range(start, end - start)
            .build()
        )
        operations_response = self.SADMIN.execute(operations_statement)
        operations_metrics = self._parse_operations_metrics(operations_response.to_dict())

        return usage_metrics, operations_metrics

    def _parse_usage_metrics(self, records: dict) -> dict:
        pprint.pprint(records)
        metrics = {}
        for record in records:
            key = (
                record.get("stream"),
                record.get("request"),
                record.get("appName"),
                record.get("appType"),
            )
            entry = metrics.setdefault(key, (0, 0, 0, 0))
            new_entry = (
                entry[self.USG_NUM_SUCCESSFUL_OPERATIONS] + record.get("numSuccessfulOperations"),
                entry[self.USG_NUM_WRITES] + record.get("numWrites"),
                entry[self.USG_NUM_VALIDATION_ERRORS] + record.get("numValidationErrors"),
                entry[self.USG_NUM_READS] + record.get("numReads"),
            )
            metrics[key] = new_entry
        return metrics

    def _parse_operations_metrics(self, records: dict) -> dict:
        # pprint.pprint(records)
        metrics = {}
        for record in records:
            key = (
                record.get("tenant"),
                record.get("stream"),
                record.get("request"),
                record.get("appName"),
                record.get("appType"),
            )
            entry = metrics.setdefault(key, [0] * 14)
            entry[self.OP_NUM_SUCCESSFUL_OPERATIONS] += record.get("numSuccessfulOperations")
            entry[self.OP_NUM_VALIDATION_ERRORS] += record.get("numValidationErrors")
            entry[self.OP_NUM_TRANSIENT_ERRORS] += record.get("numTransientErrors")
            entry[self.OP_NUM_WRITES] += record.get("numWrites")
            entry[self.OP_BYTES_WRITTEN] += record.get("bytesWritten")
            entry[self.OP_NUM_READS] += record.get("numReads")
            entry[self.OP_BYTES_READ] += record.get("bytesRead")
            entry[self.OP_LATENCY_MIN] += record.get("latencyMin")
            entry[self.OP_LATENCY_MAX] += record.get("latencyMax")
            entry[self.OP_LATENCY_SUM] += record.get("latencySum")
            entry[self.OP_NUM_STORAGE_ACCESSES] += record.get("numStorageAccesses")
            entry[self.OP_STORAGE_LATENCY_MIN] += record.get("storageLatencyMin")
            entry[self.OP_STORAGE_LATENCY_MAX] += record.get("storageLatencyMax")
            entry[self.OP_STORAGE_LATENCY_SUM] += record.get("storageLatencySum")
        return metrics


if __name__ == "__main__":
    pytest.main(sys.argv)
