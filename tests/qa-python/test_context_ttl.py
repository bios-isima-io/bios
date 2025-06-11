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

import copy
import time
import unittest

import bios
from bios import ServiceError
from bios import ErrorCode

from setup_common import (
    BIOS_QA_COMMON_ADMIN_USER as ADMIN_USER,
    setup_tenant_config,
)
from tsetup import get_endpoint_url as ep_url
from tsetup import admin_pass, sadmin_user, sadmin_pass


TTL_CONTEXT_NAME = "ttl_context"
TTL_CONTEXT = {
    "contextName": TTL_CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "integer"},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key"],
    "ttl": 30000,
}


@unittest.skip("Not possible to test anymore in the regular QA framework")
class ContextTtlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        cls.prevMode = cls.sadmin.get_property("prop.contextMaintenanceMode")
        cls.prevInt1 = cls.sadmin.get_property("prop.contextMaintenanceInterval")
        cls.prevInt2 = cls.sadmin.get_property("prop.contextMaintenanceRevisitIntervalSeconds")
        print(f"prop.contextMaintenanceMode: {cls.prevMode}")
        print(f"prop.contextMaintenanceInterval: {cls.prevInt1}")
        print(f"prop.contextMaintenanceRevisitIntervalSeconds: {cls.prevInt2}")
        if cls.prevMode is None or cls.prevMode == "":
            cls.prevMode = "DRY_RUN"
        if cls.prevInt1 is None or cls.prevInt1 == "":
            cls.prevInt1 = "300000"
        if cls.prevInt2 is None or cls.prevInt2 == "":
            cls.prevInt2 = "86400"

        cls.sadmin.set_property("prop.contextMaintenanceMode", "ENABLED")
        cls.sadmin.set_property("prop.contextMaintenanceInterval", "10000")
        cls.sadmin.set_property("prop.contextMaintenanceRevisitIntervalSeconds", "10")

    @classmethod
    def tearDownClass(cls):
        cls.sadmin.set_property("prop.contextMaintenanceMode", cls.prevMode)
        cls.sadmin.set_property("prop.contextMaintenanceInterval", cls.prevInt1)
        cls.sadmin.set_property("prop.contextMaintenanceRevisitIntervalSeconds", cls.prevInt2)
        cls.sadmin.close()

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        self.assertIsNotNone(self.session)
        try:
            self.session.delete_context(TTL_CONTEXT_NAME)
        except ServiceError:
            pass

    def tearDown(self):
        try:
            self.session.delete_context(TTL_CONTEXT_NAME)
        except ServiceError:
            pass
        self.session.close()

    def test_context_ttl_config_crud(self):
        self.session.create_context(TTL_CONTEXT)
        ctx = copy.deepcopy(TTL_CONTEXT)
        ctx["ttl"] = 1000
        result = self.session.update_context(TTL_CONTEXT_NAME, ctx)
        print(result)
        self.assertEqual(result["ttl"], 1000)

        del ctx["ttl"]
        result = self.session.update_context(TTL_CONTEXT_NAME, ctx)
        print(result)
        self.assertNotIn("ttl", result)

        ctx["ttl"] = 9999
        result = self.session.update_context(TTL_CONTEXT_NAME, ctx)
        print(result)
        self.assertEqual(result["ttl"], 9999)

        ctx["ttl"] = 55555
        result = self.session.update_context(TTL_CONTEXT_NAME, ctx)
        print(result)
        self.assertEqual(result["ttl"], 55555)

        with self.assertRaises(ServiceError) as error_context:
            result = self.session.update_context(TTL_CONTEXT_NAME, ctx)
            print(result)
            print(error_context)
        print(repr(error_context.exception))
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

        self.session.delete_context(TTL_CONTEXT_NAME)

    def test_context_ttl_entry_deletion(self):
        self.session.create_context(TTL_CONTEXT)
        upsert_request = (
            bios.isql().upsert().into(TTL_CONTEXT_NAME).csv_bulk(["0,abc", "1,def"]).build()
        )
        self.session.execute(upsert_request)

        select_request = (
            bios.isql().select().from_context(TTL_CONTEXT_NAME).where(keys=[[0], [1]]).build()
        )
        reply = self.session.execute(select_request)
        records = reply.get_records()
        self.assertEqual(len(records), 2)

        # Keep updating one entry and do not touch the other entry.
        # Verify that the touched entry survives and the other one is auto-deleted.
        entry_deleted = False
        for i in range(60):
            upsert_request2 = (
                bios.isql()
                .upsert()
                .into(TTL_CONTEXT_NAME)
                .csv_bulk([f"1,repeated_upsert_{i}"])
                .build()
            )
            self.session.execute(upsert_request2)
            reply = self.session.execute(select_request)
            records = reply.get_records()
            print(records)
            if len(records) < 2:
                self.assertEqual(records[0].get("the_key"), 1)
                entry_deleted = True
                break
            time.sleep(10)

        self.assertTrue(entry_deleted)
        # Wait for some more time and verify that the touched entry does survive.
        for i in range(2):
            time.sleep(10)
            reply = self.session.execute(select_request)
            records = reply.get_records()
            print(records)
            self.assertEqual(records[0].get("the_key"), 1)


if __name__ == "__main__":
    unittest.main()
