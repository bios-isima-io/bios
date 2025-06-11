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
import random
import sys
import threading
import time
import unittest
from pprint import pprint

import bios
import pytest
from bios import ServiceError
from bios.errors import ErrorCode
from bios.isql_request import ISqlRequest
from bios.models.app_type import AppType
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as ep
from tsetup import sadmin_pass, sadmin_user

SIGNAL_JSON_FILE = "../resources/bios-signal-schema.json"
CONTEXT_JSON_FILE = "../resources/CustomerContext.json"


class BiosClientMetricsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = f"biosClientMetricsTest{random.randint(1,65536)}"
        cls.ADMIN_USER = admin_user + "@" + cls.TENANT_NAME
        cls.EXTRACT_USER = extract_user + "@" + cls.TENANT_NAME

        with open(SIGNAL_JSON_FILE, "r") as json_file:
            cls.LOADED_SIGNAL = json.loads(json_file.read())
        with open(CONTEXT_JSON_FILE, "r") as json_file:
            cls.LOADED_CONTEXT = json.loads(json_file.read())
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(cls.TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": cls.TENANT_NAME})

    def setUp(self):
        self.session = bios.login(
            ep_url(), self.ADMIN_USER, admin_pass, "biosClientMetricsTest", AppType.REALTIME
        )
        self.signal_name = self.LOADED_SIGNAL["signalName"]
        result1 = self.session.create_signal(self.LOADED_SIGNAL)
        self.assertIsNotNone(result1)
        self.context_name = self.LOADED_CONTEXT["contextName"]
        result2 = self.session.create_context(self.LOADED_CONTEXT)
        self.assertIsNotNone(result2)
        time.sleep(10)

    def tearDown(self):
        self.session.delete_signal(self.signal_name)
        self.session.delete_context(self.context_name)
        self.session.close()

    def insert_bulk_into_signal(self, num_records=20):
        req_bulk = []
        for i in range(num_records):
            req_bulk.append("{0},2,3,0.1,0.5,test,MALE,IN,MH,8080".format(i))
        resp = self.session.execute(
            ISqlRequest().insert().into(self.signal_name).csv_bulk(req_bulk).build()
        )
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.records), num_records)

    def upsert_into_context(self, num_records=20):
        req_bulk = []
        for i in range(num_records):
            req_bulk.append("{0},false,false,true,true,false,false".format(i))
        self.session.execute(
            ISqlRequest().upsert().into(self.context_name).csv_bulk(req_bulk).build()
        )

    def test_client_log_permission_denied(self):
        extract_sess = bios.login(ep_url(), self.EXTRACT_USER, extract_pass)
        try:
            extract_sess.execute(
                ISqlRequest().insert().into("_clientLog").csv_bulk(["1,2,whatever"]).build()
            )
            self.fail("Should always raise an exception")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.FORBIDDEN)
            self.assertEqual(
                err.message,
                "Permission denied: Operation not permitted for user;"
                f" email=extract@{self.TENANT_NAME.lower()}, roles=[EXTRACT];"
                f" tenant={self.TENANT_NAME}",
            )
        extract_sess.close()

    def test_client_log_sanity(self):
        try:
            self.session.execute(
                ISqlRequest().insert().into("_clientLog").csv("1,2,whatever").build()
            )
            self.fail("Should always raise an exception")
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.NO_SUCH_STREAM)
            self.assertIn("Stream not found", err.message)

    def test_get_signal(self):
        signal = self.session.get_signal("_clientMetrics")
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], "_clientMetrics")

    def test_with_inserts(self):
        t1 = bios.time.now()
        for i in range(20):
            resp = self.session.execute(
                ISqlRequest()
                .insert()
                .into(self.signal_name)
                .csv("1,2,3,0.1,0.5,test,MALE,IN,MH,8080")
                .build()
            )
            self.assertIsNotNone(resp)
            self.assertEqual(len(resp.records), 1)
        time.sleep(60)
        t2 = bios.time.now()

        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.signal_name}' AND request = 'INSERT'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp2)
        self.assertEqual(len(resp2.data_windows), 1)
        pprint(resp2.data_windows[0])

        num_successful_ops = 0

        for record in resp2.data_windows[0].records:
            self.assertEqual(record.get("stream"), self.signal_name)
            self.assertEqual(record.get("request"), "INSERT")
            self.assertEqual(record.get("appName"), "biosClientMetricsTest")
            self.assertEqual(record.get("appType"), "Realtime")
            self.assertEqual(record.get("numFailedOperations"), 0)
            num_successful_ops = num_successful_ops + record.get("numSuccessfulOperations")

            self.assertGreater(record.get("latencySum"), 0)
            self.assertGreater(record.get("latencyMin"), 0)
            self.assertGreater(record.get("latencyMax"), 0)

            self.assertGreater(record.get("latencyInternalSum"), 0)
            self.assertGreater(record.get("latencyInternalMin"), 0)
            self.assertGreater(record.get("latencyInternalMax"), 0)

            self.assertGreaterEqual(record.get("latencySum"), record.get("latencyInternalSum"))
            latencyAvg = record.get("latencyInternalSum") / record.get("numSuccessfulOperations")
            latencyInternalAvg = record.get("latencyInternalSum") / record.get(
                "numSuccessfulOperations"
            )
            self.assertLessEqual(latencyAvg - latencyInternalAvg, 10 * 1000)

            self.assertGreater(record.get("numWrites"), 0)
            self.assertEqual(record.get("numThreads"), 1)

        self.assertEqual(num_successful_ops, 20)

    def test_with_insert_bulk(self):
        t1 = bios.time.now()
        self.insert_bulk_into_signal()
        time.sleep(60)
        t2 = bios.time.now()

        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.signal_name}' AND request = 'INSERT'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp2)
        self.assertEqual(len(resp2.data_windows), 1)
        pprint(resp2.data_windows[0])
        self.assertEqual(len(resp2.data_windows[0].records), 1)
        record = resp2.data_windows[0].records[0]

        self.assertEqual(record.get("stream"), self.signal_name)
        self.assertEqual(record.get("request"), "INSERT")
        self.assertEqual(record.get("appName"), "biosClientMetricsTest")
        self.assertEqual(record.get("appType"), "Realtime")
        self.assertEqual(record.get("numSuccessfulOperations"), 1)
        self.assertEqual(record.get("numFailedOperations"), 0)

        self.assertGreater(record.get("latencySum"), 0)
        self.assertGreater(record.get("latencyMin"), 0)
        self.assertGreater(record.get("latencyMax"), 0)

        self.assertGreater(record.get("latencyInternalSum"), 0)
        self.assertGreater(record.get("latencyInternalMin"), 0)
        self.assertGreater(record.get("latencyInternalMax"), 0)

        self.assertGreaterEqual(record.get("latencySum"), record.get("latencyInternalSum"))
        latencyAvg = record.get("latencyInternalSum") / record.get("numSuccessfulOperations")
        latencyInternalAvg = record.get("latencyInternalSum") / record.get(
            "numSuccessfulOperations"
        )
        self.assertLessEqual(latencyAvg - latencyInternalAvg, 10 * 1000)

        self.assertEqual(record.get("numWrites"), 20)
        self.assertEqual(record.get("numThreads"), 1)

    def test_with_select(self):
        t1 = bios.time.now()
        self.insert_bulk_into_signal()
        time.sleep(10)
        t2 = bios.time.now()
        resp1 = self.session.execute(
            ISqlRequest().select().from_signal(self.signal_name).time_range(t2, t1 - t2).build()
        )
        self.assertIsNotNone(resp1)
        self.assertEqual(len(resp1.data_windows), 1)
        self.assertEqual(len(resp1.data_windows[0].records), 20)
        time.sleep(60)
        t3 = bios.time.now()

        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.signal_name}' AND request = 'SELECT'")
            .time_range(t3, t1 - t3)
            .build()
        )
        num_reads = 0
        for window in resp2.data_windows:
            for record in window.records:
                num_reads = num_reads + record.get("numReads")
                self.assertEqual(record.get("numWrites"), 0)
        self.assertEqual(num_reads, 20)

    def test_with_select_context(self):
        t1 = bios.time.now()
        self.upsert_into_context()
        time.sleep(10)

        resp1 = self.session.execute(
            ISqlRequest()
            .select()
            .from_context(self.context_name)
            .where(keys=[["1"], ["2"], ["3"], ["4"], ["5"]])
            .build()
        )
        self.assertIsNotNone(resp1)
        self.assertEqual(len(resp1.get_records()), 5)

        time.sleep(60)
        t3 = bios.time.now()
        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.context_name}' AND request = 'SELECT'")
            .time_range(t3, t1 - t3)
            .build()
        )
        num_reads = 0
        for window in resp2.data_windows:
            for record in window.records:
                num_reads = num_reads + record.get("numReads")
                self.assertEqual(record.get("numWrites"), 0)
        self.assertEqual(num_reads, 5)

    def test_system_tenant_with_insert_bulk(self):
        t1 = bios.time.now()
        self.insert_bulk_into_signal()
        time.sleep(60)
        t2 = bios.time.now()

        resp1 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.signal_name}'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp1)
        self.assertEqual(len(resp1.data_windows), 1)
        self.assertEqual(len(resp1.data_windows[0].records), 1)
        record1 = resp1.data_windows[0].records[0]
        self.assertEqual(record1.get("stream"), self.signal_name)
        self.assertEqual(record1.get("request"), "INSERT")
        self.assertEqual(record1.get("appName"), "biosClientMetricsTest")
        self.assertEqual(record1.get("appType"), "Realtime")
        self.assertEqual(record1.get("numSuccessfulOperations"), 1)
        self.assertEqual(record1.get("numFailedOperations"), 0)
        self.assertEqual(record1.get("numThreads"), 1)

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            resp2 = sadmin.execute(
                ISqlRequest()
                .select()
                .from_signal("_allClientMetrics")
                .where(f"tenant = '{self.TENANT_NAME}' AND stream = '{self.signal_name}'")
                .time_range(t2, t1 - t2)
                .build()
            )
            self.assertIsNotNone(resp2)
            self.assertEqual(len(resp2.data_windows), 1)
            self.assertEqual(len(resp2.data_windows[0].records), 1)
            pprint(resp2.data_windows[0])
            record2 = resp2.data_windows[0].records[0]

            self.assertEqual(record2.get("tenant"), self.TENANT_NAME)
            self.assertEqual(record2.get("stream"), self.signal_name)
            self.assertEqual(record2.get("request"), "INSERT_BULK")
            self.assertEqual(record2.get("appName"), "biosClientMetricsTest")
            self.assertEqual(record2.get("appType"), "Realtime")
            self.assertEqual(record2.get("numSuccessfulOperations"), 1)
            self.assertEqual(record2.get("numFailedOperations"), 0)

            self.assertEqual(record2.get("latencySum"), record1.get("latencySum"))
            self.assertEqual(record2.get("latencyMin"), record1.get("latencyMin"))
            self.assertEqual(record2.get("latencyMax"), record1.get("latencyMax"))

            self.assertEqual(record2.get("latencyInternalSum"), record1.get("latencyInternalSum"))
            self.assertEqual(record2.get("latencyInternalMin"), record1.get("latencyInternalMin"))
            self.assertEqual(record2.get("latencyInternalMax"), record1.get("latencyInternalMax"))

            self.assertEqual(record2.get("numQosRetryConsidered"), 0)
            self.assertEqual(record2.get("numQosRetrySent"), 0)
            self.assertEqual(record2.get("numQosRetryResponseUsed"), 0)

    def test_system_tenant_select_context(self):
        t1 = bios.time.now()
        self.upsert_into_context()
        time.sleep(60)
        t2 = bios.time.now()

        resp1 = self.session.execute(
            ISqlRequest()
            .select()
            .from_context(self.context_name)
            .where(keys=[["1"], ["2"], ["3"], ["4"], ["5"]])
            .build()
        )
        self.assertIsNotNone(resp1)
        self.assertEqual(len(resp1.get_records()), 5)

        time.sleep(60)
        t3 = bios.time.now()
        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.context_name}' AND request = 'SELECT'")
            .time_range(t3, t1 - t3)
            .build()
        )
        num_reads = 0
        for window in resp2.data_windows:
            for record in window.records:
                self.assertEqual(record.get("request"), "SELECT")
                num_reads = num_reads + record.get("numReads")
                self.assertEqual(record.get("numWrites"), 0)
        self.assertEqual(num_reads, 5)

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            resp3 = sadmin.execute(
                ISqlRequest()
                .select()
                .from_signal("_allClientMetrics")
                .where(
                    f"tenant = '{self.TENANT_NAME}' AND "
                    f"stream = '{self.context_name}' AND "
                    f"request = 'SELECT_CONTEXT'"
                )
                .time_range(t3, t1 - t3)
                .build()
            )
            num_reads2 = 0
            for window in resp3.data_windows:
                pprint(window)
                for record in window.records:
                    self.assertEqual(record.get("request"), "SELECT_CONTEXT")
                    num_reads2 = num_reads2 + record.get("numReads")
                    self.assertEqual(record.get("numWrites"), 0)
                    self.assertEqual(record.get("numThreads"), 1)
            self.assertEqual(num_reads2, 5)

    def test_num_threads(self):
        bios.enable_threads()
        self.addCleanup(bios.disable_threads)
        multi_session = bios.login(
            ep_url(), self.ADMIN_USER, admin_pass, "biosClientMetricsTest_multi", AppType.REALTIME
        )
        self.addCleanup(multi_session.close)
        t1 = bios.time.now()

        def insert_one_event():
            resp = multi_session.execute(
                ISqlRequest()
                .insert()
                .into(self.signal_name)
                .csv("1,2,3,0.1,0.5,test,MALE,IN,MH,8080")
                .build()
            )
            self.assertIsNotNone(resp)
            self.assertEqual(len(resp.records), 1)

        thread1 = threading.Thread(target=insert_one_event)
        thread2 = threading.Thread(target=insert_one_event)
        thread3 = threading.Thread(target=insert_one_event)
        thread4 = threading.Thread(target=insert_one_event)

        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()

        time.sleep(60)
        t2 = bios.time.now()

        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.signal_name}' AND request = 'INSERT'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp2)
        self.assertEqual(len(resp2.data_windows), 1)
        pprint(resp2.data_windows[0])

        max_num_threads = 0

        for record in resp2.data_windows[0].records:
            max_num_threads = max(record.get("numThreads"), max_num_threads)

        print("max numThreads:{}".format(max_num_threads))
        self.assertGreater(max_num_threads, 1)

    def test_stream_name(self):
        t1 = bios.time.now()
        req_bulk = []
        req_bulk.append("{},Request,AppName,,,,,,,,,,,,,".format(self.context_name.lower()))
        req_bulk.append("{},Request,AppName,,,,,,,,,,,,,".format(self.signal_name.upper()))
        resp = self.session.execute(
            ISqlRequest().insert().into("_clientMetrics").csv_bulk(req_bulk).build()
        )
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.records), 2)
        time.sleep(10)
        t2 = bios.time.now()

        resp2 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.context_name}' AND request = 'Request'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp2)
        self.assertEqual(len(resp2.data_windows), 1)
        pprint(resp2.data_windows[0])
        self.assertEqual(len(resp2.data_windows[0].records), 1)
        record = resp2.data_windows[0].records[0]
        self.assertEqual(record.get("stream"), self.context_name)

        resp3 = self.session.execute(
            ISqlRequest()
            .select()
            .from_signal("_clientMetrics")
            .where(f"stream = '{self.signal_name}' AND request = 'Request'")
            .time_range(t2, t1 - t2)
            .build()
        )
        self.assertIsNotNone(resp3)
        self.assertEqual(len(resp3.data_windows), 1)
        pprint(resp3.data_windows[0])
        self.assertEqual(len(resp3.data_windows[0].records), 1)
        record = resp3.data_windows[0].records[0]
        self.assertEqual(record.get("stream"), self.signal_name)


if __name__ == "__main__":
    pytest.main(sys.argv)
