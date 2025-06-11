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
import time
import unittest

import bios
from bios import ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

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


class SessionEventLoopTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            setup_context(admin, CONTEXT_ATC)
            setup_context(admin, CONTEXT_OTC)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            try:
                admin.delete_context(CONTEXT_ATC.get("contextName"))
            except ServiceError:
                pass
            try:
                admin.delete_context(CONTEXT_OTC.get("contextName"))
            except ServiceError:
                pass

    def test(self):
        admin_sess1 = bios.login(ep_url(), ADMIN_USER, admin_pass)
        admin_sess2 = bios.login(ep_url(), ADMIN_USER, admin_pass)

        statement1 = (
            bios.isql().upsert().into("otcPdpSignal_productIdbyObjectId").csv("1,hello10").build()
        )

        statement2 = (
            bios.isql().upsert().into("addToCart_productIdbyObjectId").csv("2,hello20").build()
        )

        admin_sess1.execute(statement1)
        admin_sess2.execute(statement2)
        context = admin_sess2.get_context("otcPdpSignal_productIdbyObjectId")
        self.assertIsNotNone(context)
        self.assertEqual(context["contextName"], "otcPdpSignal_productIdbyObjectId")

        # Sleep for 15 seconds; Session should expire
        time.sleep(5)

        admin_sess1.close()

        time.sleep(5)

        admin_sess2.execute(statement1)
        admin_sess2.execute(statement2)
        context = admin_sess2.get_context("otcPdpSignal_productIdbyObjectId")
        self.assertIsNotNone(context)
        self.assertEqual(context["contextName"], "otcPdpSignal_productIdbyObjectId")

        time.sleep(5)
        admin_sess2.close()


if __name__ == "__main__":
    unittest.main()
