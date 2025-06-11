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

import unittest

import pytest
import sys

from bios import ServiceError, ErrorCode
import bios
from tsetup import (
    ingest_user,
    ingest_pass,
    extract_user,
    extract_pass,
    mlengineer_user,
    mlengineer_pass,
    admin_pass,
    admin_user,
    sadmin_pass,
    sadmin_user,
)
from tsetup import get_endpoint_url as ep_url


class ResetPasswordTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
        cls.TENANT_NAME = "resetPasswordTest"
        cls.TENANT_NAME2 = "resetPasswordTest2"
        cls.ADMIN_USER = admin_user + "@" + cls.TENANT_NAME

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(cls.TENANT_NAME)
            except ServiceError:
                pass
            try:
                sadmin.delete_tenant(cls.TENANT_NAME2)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": cls.TENANT_NAME})
            sadmin.create_tenant({"tenantName": cls.TENANT_NAME2})

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(cls.TENANT_NAME)
            except ServiceError:
                pass
            try:
                sadmin.delete_tenant(cls.TENANT_NAME2)
            except ServiceError:
                pass

    def test_change_password_ingest(self):
        self.verify_self_password_reset(ingest_user, ingest_pass, "Test@123!")

    def test_change_password_extract(self):
        self.verify_self_password_reset(extract_user, extract_pass, "as;dlfkajsfawaepo232n)*(*&")

    def test_change_password_mle(self):
        self.verify_self_password_reset(mlengineer_user, mlengineer_pass, "hello world")

    def test_change_password_admin(self):
        self.verify_self_password_reset(admin_user, admin_pass, "adminadmin", True)

    def test_negative_missing_current_password(self):
        with bios.login(ep_url(), f"{ingest_user}@{self.TENANT_NAME}", ingest_pass) as session:
            with self.assertRaises(ServiceError) as ec:
                session.change_password(new_password="newpass")
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            self.assertEqual(
                ec.exception.message, "Permission denied: Current password must be set"
            )
            with self.assertRaises(ServiceError) as ec:
                session.change_password(
                    new_password="newpass", email=f"{ingest_user}@{self.TENANT_NAME}"
                )
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            self.assertEqual(
                ec.exception.message, "Permission denied: Current password must be set"
            )

    def test_cross_password_reset(self):
        tenant_name = self.TENANT_NAME
        new_password = "@#$%&;alksdjfasdfp009823"
        with bios.login(ep_url(), f"{admin_user}@{tenant_name}", admin_pass) as session:
            self.run_cross_user_password_reset(session, ingest_user, ingest_pass, new_password)
            self.run_cross_user_password_reset(session, extract_user, extract_pass, new_password)
            self.run_cross_user_password_reset(
                session, mlengineer_user, mlengineer_pass, new_password
            )
            # User in different tenant cannot be changed
            with self.assertRaises(ServiceError) as ec:
                session.change_password(
                    email=f"ingest@{self.TENANT_NAME2}", new_password=new_password
                )
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            self.assertEqual(
                ec.exception.message,
                "Permission denied: "
                f"Not allowed to change password of user ingest@{self.TENANT_NAME2}",
            )

    def run_cross_user_password_reset(
        self, session, user_name: str, old_password: str, new_password: str
    ):
        tenant_name = self.TENANT_NAME
        session.change_password(email=f"{user_name}@{tenant_name}", new_password=new_password)
        with bios.login(ep_url(), f"{user_name}@{tenant_name}", new_password) as another:
            another.change_password(current_password=new_password, new_password=old_password)

    def verify_self_password_reset(
        self,
        user_name: str,
        old_password: str,
        new_password: str,
        cross_user_allowed: bool = False,
    ):
        with bios.login(ep_url(), f"{user_name}@{self.TENANT_NAME}", old_password) as session:
            # reset password from old to new
            session.change_password(current_password=old_password, new_password=new_password)
            # resetting twice wouldn't work
            with self.assertRaises(ServiceError) as ec:
                session.change_password(current_password=old_password, new_password=new_password)
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            self.assertEqual(
                ec.exception.message,
                "Permission denied: Current password does not match",
            )
        # verify the password has changed
        with self.assertRaises(ServiceError) as ec:
            bios.login(ep_url(), f"{user_name}@{self.TENANT_NAME}", old_password)
        self.assertEqual(ec.exception.error_code, ErrorCode.UNAUTHORIZED)
        with bios.login(ep_url(), f"{user_name}@{self.TENANT_NAME}", new_password) as session:
            # reverting the password; reset password must accept the new password
            with self.assertRaises(ServiceError) as ec:
                session.change_password(current_password=old_password, new_password=new_password)
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            session.change_password(current_password=new_password, new_password=old_password)
            # cross-user password reset
            if not cross_user_allowed:
                with self.assertRaises(ServiceError) as ec:
                    if user_name != ingest_user:
                        email = f"{ingest_user}@{self.TENANT_NAME}"
                    else:
                        email = f"{extract_user}@{self.TENANT_NAME}"
                    session.change_password(new_password=new_password, email=email)
                self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
                self.assertEqual(
                    ec.exception.message,
                    "Permission denied: TenantAdmin role is required"
                    " to reset password of another user",
                )


if __name__ == "__main__":
    pytest.main(sys.argv)
