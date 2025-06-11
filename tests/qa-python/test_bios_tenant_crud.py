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

from bios import ServiceError, ErrorCode
import bios
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_user, sadmin_pass, admin_user, admin_pass


TEST_TENANT_NAME = "bios_tenant_crud"
EXISTING_TENANT = "bios_existing_tenant"
EXISTING_ADMIN_USER = admin_user + "@" + EXISTING_TENANT


class BiosTenantCrudTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(EXISTING_TENANT)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": EXISTING_TENANT})

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.session = bios.login(ep_url(), sadmin_user, sadmin_pass)

    def tearDown(self):
        self.session.close()

    def test_get_existing_tenant_by_sadmin(self):
        with self.assertRaises(ServiceError) as cm:
            self.session.get_tenant(EXISTING_TENANT)
            error = cm.exception
            self.assertEqual(error.error_code, ErrorCode.FORBIDDEN)

    def test_get_existing_tenant_by_admin(self):
        with bios.login(ep_url(), EXISTING_ADMIN_USER, admin_pass) as admin:
            tenant = admin.get_tenant(EXISTING_TENANT)
            self.assertEqual(tenant["tenantName"], EXISTING_TENANT)
            # Test the get_tenant() API without tenant parameter
            tenant2 = admin.get_tenant()
            self.assertEqual(tenant2, tenant)

    def test_get_tenants(self):
        self.assertIsNotNone(self.session)
        tenants = self.session.list_tenants()
        self.assertIsNotNone(tenants)
        self.assertGreaterEqual(len(tenants), 1)
        self.assertIn("_system", tenants)
        self.assertIn(EXISTING_TENANT, tenants)

    def test_get_tenants_with_options(self):
        tenants = self.session.list_tenants(names=[EXISTING_TENANT])
        self.assertIsNotNone(tenants)
        self.assertGreaterEqual(len(tenants), 1)
        self.assertNotIn("_system", tenants)
        self.assertIn(EXISTING_TENANT, tenants)

    def test_create_get_delete_tenant(self):
        self.session.create_tenant({"tenantName": TEST_TENANT_NAME})
        tenants = self.session.list_tenants()
        self.assertIn(TEST_TENANT_NAME, tenants)

        # wait for the DB being propagated
        time.sleep(1)

        with bios.login(ep_url(), admin_user + "@" + TEST_TENANT_NAME, admin_pass) as admin:
            tenant = admin.get_tenant(TEST_TENANT_NAME)
            self.assertEqual(tenant["tenantName"], TEST_TENANT_NAME)

        self.session.delete_tenant(TEST_TENANT_NAME)
        tenants = self.session.list_tenants()
        self.assertNotIn(TEST_TENANT_NAME, tenants)

    def test_list_tenants_by_admin(self):
        with bios.login(ep_url(), EXISTING_ADMIN_USER, admin_pass) as admin:
            with self.assertRaises(ServiceError) as cm:
                admin.list_tenants()
                error = cm.exception
                self.assertEqual(error.error_code, ErrorCode.FORBIDDEN)

    def test_create_tenant_by_admin(self):
        with bios.login(ep_url(), EXISTING_ADMIN_USER, admin_pass) as admin:
            with self.assertRaises(ServiceError) as cm:
                admin.create_tenant({"tenantName": "youCant"})
                error = cm.exception
                self.assertEqual(error.error_code, ErrorCode.FORBIDDEN)

    def test_delete_tenant_by_admin(self):
        with bios.login(ep_url(), EXISTING_ADMIN_USER, admin_pass) as admin:
            with self.assertRaises(ServiceError) as cm:
                admin.delete_tenant(EXISTING_TENANT)
                error = cm.exception
                self.assertEqual(error.error_code, ErrorCode.FORBIDDEN)


if __name__ == "__main__":
    unittest.main()
