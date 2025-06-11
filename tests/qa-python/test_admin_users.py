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

import logging
import os
import sys
import unittest

import bios
import pytest
from bios import ServiceError
from bios.errors import ErrorCode
from setup_common import setup_signal, setup_tenant_config, try_delete_signal
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import ingest_user, sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TENANT_NAME = "adminUserTest"
NEIGHBOR_TENANT_NAME = "neighbor"
ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

SIGNAL = {
    "signalName": "biosAdminUsersSignal",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "stringAttribute", "type": "String"}],
}


class AdminUserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config(tenant_name=TENANT_NAME, clear_tenant=True)
        setup_tenant_config(tenant_name=NEIGHBOR_TENANT_NAME, clear_tenant=True)
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_signal(cls.session, SIGNAL)
        cls.superadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        cls.superadmin.create_user(
            bios.User(
                email="sadmin@isima.io",
                full_name="sadmin test user",
                password="sadmin@123",
                tenant_name="/",
                roles=["SystemAdmin", "TenantAdmin"],
            )
        )
        cls.sadmin = bios.login(ep_url(), "sadmin@isima.io", "sadmin@123")

    @classmethod
    def tearDownClass(cls):
        try_delete_signal(cls.session, SIGNAL["signalName"])
        cls.session.close()
        cls.sadmin.close()
        cls.superadmin.delete_user("sadmin@isima.io")
        cls.superadmin.close()

    def test_sadmin_change_password_self(self):
        sess1 = bios.login(ep_url(), "sadmin@isima.io", "sadmin@123")
        sess1.change_password("sadmin@123", "test@123")
        sess1.close()

        sess2 = bios.login(ep_url(), "sadmin@isima.io", "test@123")
        sess2.close()

    def test_change_password_other_user(self):
        self.sadmin.create_user(
            bios.User(
                email="testuser@isima.io",
                full_name="test user",
                password="testuser@123",
                tenant_name="/",
                roles=["Report"],
            )
        )
        self.addCleanup(self.sadmin.delete_user, "testuser@isima.io")
        self.sadmin.change_password("testuser@123", "testUser", "testuser@isima.io")
        session = bios.login(ep_url(), "testuser@isima.io", "testUser")
        session.close()

    def test_change_password_by_system_admin(self):
        ingest_user_email = f"{ingest_user}@{TENANT_NAME}"
        self.sadmin.change_password(email=ingest_user_email, new_password="newPassword")
        with bios.login(ep_url(), ingest_user_email, "newPassword"):
            pass

    def test_change_password_invalid_user(self):
        with self.assertRaises(ServiceError) as error_context:
            self.sadmin.change_password("sadmin@123", "test@123", "invalidUser@NoTenant.com")
        self.assertEqual(error_context.exception.error_code, ErrorCode.UNAUTHORIZED)

    def test_change_password_of_user_in_another_tenant(self):
        neighbor_user = f"{admin_user}@{NEIGHBOR_TENANT_NAME}"
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            with self.assertRaises(ServiceError) as error_context:
                session.change_password(email=neighbor_user, new_password="newPasswd")
            self.assertEqual(error_context.exception.error_code, ErrorCode.FORBIDDEN)

    def test_reset_password_invalid_user(self):
        with self.assertRaises(ServiceError) as error_context:
            self.sadmin.change_password("sadmin@123", "test@123", "invalidUser@NoTenant.com")
        self.assertEqual(error_context.exception.error_code, ErrorCode.UNAUTHORIZED)


if __name__ == "__main__":
    pytest.main(sys.argv)
