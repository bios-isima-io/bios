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
import logging as logger
import pprint
import time
import unittest

import bios
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from session_pool import SessionPool
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

PRODUCT_ID = "productId"
PRODUCT_ID_COLLECTION = "productIdCollection"

CONTEXT_ATC = {
    "contextName": "addToCart_ProductIdByObjectId",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "objectId", "type": "String"},
        {"attributeName": "productIdCollection", "type": "String"},
    ],
    "primaryKey": ["objectId"],
}

CONTEXT_OTC = {
    "contextName": "otcPdpSignal_productIdbyObjectId",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "objectId", "type": "String"},
        {"attributeName": "productIdCollection", "type": "String"},
    ],
    "primaryKey": ["objectId"],
    "dataSynthesisStatus": "Disabled",
}


class SessionExpirationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        try:
            with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
                setup_context(admin, CONTEXT_ATC)
                setup_context(admin, CONTEXT_OTC)

                thirty_seconds = "30000"
                with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
                    sadmin.set_property("session_expiry", thirty_seconds)

            cls.pool = SessionPool(ep_url(), ADMIN_USER, admin_pass)

        except ServiceError:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("session_expiry", "")
        cls.pool.close()

    def test(self):
        # Try to fetch pool items more than capacity
        items = []
        for i in range(5):
            items.append(self.pool.get_session())
        for item in items:
            self.pool.return_session(item)

        # keep sending requests for 60 seconds
        for i in range(60):
            time.sleep(1)
            print(f"send request #{i}")

            statement1 = (
                bios.isql()
                .upsert()
                .into("otcPdpSignal_productIdbyObjectId")
                .csv(f"{i},hello{i * 10}")
                .build()
            )

            statement2 = (
                bios.isql()
                .upsert()
                .into("addToCart_productIdbyObjectId")
                .csv(f"{i},hello{i * 10}")
                .build()
            )

            item = self.pool.get_session()
            print(f"item #{item.id}")
            item.session.execute(statement1)
            item.session.execute(statement2)
            self.pool.return_session(item)

        # Sleep for 45 seconds; Session should expire
        time.sleep(45)

        item = self.pool.get_session()
        with self.assertRaises(ServiceError) as ec:
            item.session.get_signals()
        self.assertEqual(ec.exception.error_code, ErrorCode.SESSION_EXPIRED)
        message = ec.exception.message
        self.assertTrue(message.startswith("Session expired"), message)

        # Retry to verify the client not crashing
        with self.assertRaises(ServiceError) as ec:
            item.session.get_signals()
        self.assertEqual(ec.exception.error_code, ErrorCode.SESSION_EXPIRED)
        message = ec.exception.message
        self.assertTrue(message.startswith("Session expired"), message)

        self.pool.return_session(item)

        # Verify that reactivated sessions are reusable
        for i in range(20):
            item = self.pool.get_session()
            otc_req = (
                bios.isql()
                .select(PRODUCT_ID_COLLECTION)
                .from_context("otcPdpSignal_productIdbyObjectId")
                .where(keys=[[i]])
                .build()
            )
            atc_req = (
                bios.isql()
                .select(PRODUCT_ID_COLLECTION)
                .from_context("addToCart_ProductIdByObjectId")
                .where(keys=[[i]])
                .build()
            )
            result1 = self.execute(item, otc_req)
            result2 = self.execute(item, atc_req)
            self.assertEqual(result1.to_dict()[0].get("productIdCollection"), f"hello{i * 10}")
            self.assertEqual(result2.to_dict()[0].get("productIdCollection"), f"hello{i * 10}")
            self.pool.return_session(item)

    def execute(self, item, statement):
        """Executes an iSQL statement, re-activates the session if expired, returns response"""
        num_trials = 3
        while True:
            num_trials -= 1
            try:
                return item.session.execute(statement)
            except ServiceError as err:
                if err.error_code == ErrorCode.SESSION_EXPIRED and num_trials > 0:
                    logger.info("Session expired, creating a new one")
                    item.activate_session()
                else:
                    raise


if __name__ == "__main__":
    unittest.main()
