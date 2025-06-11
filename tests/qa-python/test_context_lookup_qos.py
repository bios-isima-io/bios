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
import logging
import os
import pprint
import sys
import time
import unittest

import bios
import pytest
from bios import ServiceError
from bios.models import ContextRecords
from bios.models.app_type import AppType
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from setup_common import setup_context, setup_tenant_config

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosContextLookupQosTest"

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
STORE_CONTEXT = {
    "contextName": "lookupQosContext",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "id",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "zipCode", "type": "Integer", "default": -1},
        {"attributeName": "address", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["id"],
}


@unittest.skipUnless(
    os.getenv("MULTI_AZ_DEPLOYMENT", default="no") == "yes", "This test requires multi AZ setup"
)
class BiosContextLookupQosTest(unittest.TestCase):
    """Test cases to test the context multi_get lookup QoS feature"""

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        try:
            cls.sadmin.delete_tenant(TEST_TENANT_NAME)
        except ServiceError:
            pass
        cls.context_name = STORE_CONTEXT["contextName"]
        cls.sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})
        cls.sadmin.set_property(
            "tenant." + TEST_TENANT_NAME + ".prop.lookupQosThresholdMillis", "200"
        )
        cls.sadmin.set_property("prop.test.analysis.MultiGetDelayContextName", cls.context_name)
        connection_type = os.environ.get("BIOS_CONNECTION_TYPE")
        print(f"BIOS_CONNECTION_TYPE={connection_type}")
        cache_update_millis = cls.sadmin.get_property("prop.sharedPropertiesCacheExpiryMillis")
        print(f"cache update millis={cache_update_millis}")
        upstream = json.loads(cls.sadmin.get_property("upstream") or "{}")
        print("upstream=", end="")
        pprint.pprint(upstream)
        print("sleeping for 10 seconds till sharedProperties cache is invalidated")
        time.sleep(10)
        cls.session = bios.login(
            ep_url(), ADMIN_USER, admin_pass, app_name="QosTest", app_type=AppType.REALTIME
        )
        setup_context(cls.session, STORE_CONTEXT)

    @classmethod
    def tearDownClass(cls):
        cls.session.delete_context(cls.context_name)
        cls.session.close()
        cls.sadmin.set_property(
            "tenant." + TEST_TENANT_NAME + ".prop.lookupQosThresholdMillis", ""
        )
        cls.sadmin.set_property("prop.lookupQosThresholdMillis", "0")
        cls.sadmin.set_property("prop.test.analysis.MultiGetDelayContextName", "")
        print("sleeping for 10 seconds till sharedProperties cache is invalidated")
        time.sleep(10)
        try:
            cls.sadmin.delete_tenant(TEST_TENANT_NAME)
        except ServiceError:
            pass
        cls.sadmin.close()

    def upsert_entries(self, entries, context_name):
        """Upserts records to the context"""
        insert_request = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
        self.session.execute(insert_request)

    def delete_entries(self, keys, context_name):
        """Upserts records matching list of keys from a context"""
        delete_request = bios.isql().delete().from_context(context_name).where(keys=keys).build()
        self.session.execute(delete_request)

    def test_bios_multi_get_context_entries(self):
        """Verify that lookupQos feature works and request gets completed within stipulated time"""
        t1 = bios.time.now()
        self.upsert_entries(["0,10,rcb", "1,11,vk"], self.context_name)
        self.addCleanup(self.delete_entries, [[0], [1]], self.context_name)

        select_request1 = (
            bios.isql().select().from_context(self.context_name).where(keys=[[0]]).build()
        )
        select_request2 = (
            bios.isql().select().from_context(self.context_name).where(keys=[[1]]).build()
        )

        sleep_time = 1.0
        for _ in range(20):
            start_time = time.perf_counter()
            reply = self.session.multi_execute(select_request1, select_request2)
            elapsed = time.perf_counter() - start_time
            print(f"elapsed={elapsed}")
            if 0.2 <= elapsed < 1:
                break
            time.sleep(sleep_time)
        print(f"multi execute got completed in {elapsed}s")

        # context select requests are delayed by 1 second in analysis node.
        # c-sdk should forward this request to another node and
        #   it should complete before analysis node returns.
        self.assertLess(elapsed, 1.0)
        self.assertGreaterEqual(elapsed, 0.2)

        self.assertIsInstance(reply, list)
        self.assertEqual(len(reply), 2)
        self.assertIsInstance(reply[0], ContextRecords)
        self.assertIsInstance(reply[1], ContextRecords)
        records0 = reply[0].get_records()
        self.assertEqual(len(records0), 1)
        record00 = records0[0]
        self.assertEqual(record00.get_primary_key(), [0])
        self.assertEqual(record00.get("id"), 0)
        self.assertEqual(record00.get("zipCode"), 10)
        self.assertEqual(record00.get("address"), "rcb")
        records1 = reply[1].get_records()
        self.assertEqual(len(records1), 1)
        record10 = records1[0]
        self.assertEqual(record10.get_primary_key(), [1])
        self.assertEqual(record10.get("id"), 1)
        self.assertEqual(record10.get("zipCode"), 11)
        self.assertEqual(record10.get("address"), "vk")

        print("sleeping for 60 seconds")
        time.sleep(60)
        t2 = bios.time.now()
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            resp2 = sadmin.execute(
                bios.isql()
                .select()
                .from_signal("_allClientMetrics")
                .where(f"tenant = '{TEST_TENANT_NAME}' AND stream = ''")
                .time_range(t2, t1 - t2)
                .build()
            )
            self.assertIsNotNone(resp2)
            self.assertEqual(len(resp2.data_windows), 1)
            num_qos_retry_considered = 0
            num_qos_retry_sent = 0
            num_qos_retry_response_used = 0
            for record in resp2.data_windows[0].records:
                self.assertEqual(record.get("tenant"), TEST_TENANT_NAME)
                num_qos_retry_considered += record.get("numQosRetryConsidered")
                num_qos_retry_sent += record.get("numQosRetrySent")
                num_qos_retry_response_used += record.get("numQosRetryResponseUsed")
                print(
                    f"--  {record.get('request')} {record.get('appName')}"
                    f" {record.get('numSuccessfulOperations')} {record.get('numReads')}"
                    f" {record.get('numWrites')} QoS: {num_qos_retry_considered}"
                    f" {num_qos_retry_sent} {num_qos_retry_response_used}"
                )

            self.assertGreaterEqual(num_qos_retry_considered, 1)
            self.assertGreaterEqual(num_qos_retry_sent, 1)
            self.assertGreaterEqual(num_qos_retry_response_used, 1)


if __name__ == "__main__":
    pytest.main(sys.argv)
