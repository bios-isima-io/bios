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
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user


class UsersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
        cls.ADMIN_USER = admin_user + "@" + TENANT_NAME
        setup_tenant_config()

        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)

        # These email adresses are used by the test cases commonly
        cls.email1 = f"support+{TENANT_NAME}@isima.io"
        cls.email2 = f"ingest+bios@{TENANT_NAME}"
        cls.email3 = "test+_system@isima.io"
        cls.email4 = "test2+_system@isima.io"
        cls.email5 = "userman+{TENANT_NAME}@isima.io"

    @classmethod
    def tearDownClass(cls):
        cls.sadmin.close()

    def tearDown(self):
        try:
            self.sadmin.delete_user(self.email1)
        except ServiceError:  # it's OK
            pass
        try:
            self.sadmin.delete_user(self.email2)
        except ServiceError:
            pass
        try:
            self.sadmin.delete_user(self.email3)
        except ServiceError:
            pass
        try:
            self.sadmin.delete_user(self.email4)
        except ServiceError:
            pass

    def test_normal_cases(self):
        # Verify user creations
        time0 = bios.time.now()
        user1 = self.sadmin.create_user(
            bios.User(
                email=self.email1,
                full_name=f"{TENANT_NAME} Support User",
                password="secret",
                tenant_name=TENANT_NAME,
                roles=["TenantAdmin", "Report"],
            )
        )
        self.assertEqual(user1.get("email"), self.email1.lower())
        self.assertEqual(user1.get("fullName"), f"{TENANT_NAME} Support User")
        self.assertEqual(user1.get("tenantName"), TENANT_NAME)
        self.assertEqual(user1.get("roles"), ["TenantAdmin", "Report"])
        self.assertIsNone(user1.get("password"))
        self.assertIsNotNone(user1.get("createTimestamp"))
        self.assertIsNotNone(user1.get("modifyTimestamp"))

        user2 = self.sadmin.create_user(
            bios.User(
                email=self.email2,
                full_name="配信用ユーザ",
                password="secret",
                tenant_name=TENANT_NAME,
                roles=["Ingest"],
            )
        )
        self.assertEqual(user2.get("email"), self.email2.lower())
        self.assertEqual(user2.get("fullName"), "配信用ユーザ")
        self.assertEqual(user2.get("tenantName"), TENANT_NAME)
        self.assertEqual(user2.get("roles"), ["Ingest"])
        self.assertIsNone(user2.get("password"))
        self.assertIsNotNone(user2.get("createTimestamp"))
        self.assertIsNotNone(user2.get("modifyTimestamp"))

        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            users = admin.get_users()
            users_summary = [
                {
                    "email": user.get("email"),
                    "roles": user.get("roles"),
                    "fullName": user.get("fullName"),
                    "status": user.get("status"),
                }
                for user in users.get("users")
            ]
            self.assertTrue(
                {
                    "email": self.email1.lower(),
                    "roles": ["TenantAdmin", "Report"],
                    "fullName": f"{TENANT_NAME} Support User",
                    "status": "Active",
                }
                in users_summary
            )
            self.assertTrue(
                {
                    "email": self.email2.lower(),
                    "roles": ["Ingest"],
                    "fullName": "配信用ユーザ",
                    "status": "Active",
                }
                in users_summary
            )

        # check audit logs
        time.sleep(1)  # to ensure the audit log being delivered
        now = bios.time.now()
        statement = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where("operation='Create User'")
            .time_range(now, time0 - now - 1)
            .build()
        )
        audit_logs = self.sadmin.execute(statement).to_dict()
        self.assertEqual(len(audit_logs), 2)
        self.assertEqual(audit_logs[0].get("status"), "SUCCESS")
        self.assertEqual(audit_logs[1].get("status"), "SUCCESS")

        # System admin should be able to get any users by email.
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            users = sadmin.get_users(emails=[self.email1, self.email2])
            users_summary = [
                {
                    "email": user.get("email"),
                    "roles": user.get("roles"),
                    "fullName": user.get("fullName"),
                    "status": user.get("status"),
                }
                for user in users.get("users")
            ]
            self.assertTrue(
                {
                    "email": self.email1.lower(),
                    "roles": ["TenantAdmin", "Report"],
                    "fullName": f"{TENANT_NAME} Support User",
                    "status": "Active",
                }
                in users_summary
            )
            self.assertTrue(
                {
                    "email": self.email2.lower(),
                    "roles": ["Ingest"],
                    "fullName": "配信用ユーザ",
                    "status": "Active",
                }
                in users_summary,
                json.dumps(users, indent=2),
            )

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.modify_user(user2.get("userId"), roles=["Ingest", "Internal"])
                modified = sadmin.get_users(emails=[user2.get("email")])
                self.assertEqual(modified.get("users")[0].get("roles"), ["Ingest", "Internal"])
            finally:
                sadmin.modify_user(user2.get("userId"), roles=["Ingest"])

        # Verify user modification
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            time0 = bios.time.now()

            with self.assertRaises(ServiceError) as err_ctx:
                admin.modify_user(user2.get("userId"), roles=["Ingest", "Internal"])
            self.assertEqual(err_ctx.exception.error_code, ErrorCode.FORBIDDEN)

            user2b = admin.modify_user(
                user2.get("userId"), full_name="For extractions", roles=["Extract", "Report"]
            )
            self.assertEqual(user2b.get("tenantName"), TENANT_NAME)
            self.assertEqual(user2b.get("email"), self.email2.lower())
            self.assertEqual(user2b.get("roles"), ["Extract", "Report"])
            self.assertEqual(user2b.get("fullName"), "For extractions")
            self.assertEqual(user2b.get("status"), "Active")
            self.assertEqual(user2b.get("userId"), user2.get("userId"))
            self.assertEqual(user2b.get("createTimestamp"), user2.get("createTimestamp"))
            self.assertGreater(user2b.get("modifyTimestamp"), user2.get("modifyTimestamp"))
            users = admin.get_users()
            users_summary = [
                {
                    "email": user.get("email"),
                    "roles": user.get("roles"),
                    "fullName": user.get("fullName"),
                    "status": user.get("status"),
                }
                for user in users.get("users")
            ]
            self.assertTrue(
                {
                    "email": self.email2.lower(),
                    "roles": ["Extract", "Report"],
                    "fullName": "For extractions",
                    "status": "Active",
                }
                in users_summary
            )

            # super admin also can modify a user
            user2c = self.sadmin.modify_user(user2.get("userId"), status="Suspended")
            self.assertEqual(user2c.get("tenantName"), TENANT_NAME)
            self.assertEqual(user2c.get("email"), self.email2.lower())
            self.assertEqual(user2c.get("roles"), ["Extract", "Report"])
            self.assertEqual(user2c.get("fullName"), "For extractions")
            self.assertEqual(user2c.get("status"), "Suspended")
            self.assertEqual(user2c.get("userId"), user2.get("userId"))
            self.assertEqual(user2c.get("createTimestamp"), user2.get("createTimestamp"))
            self.assertGreater(user2c.get("modifyTimestamp"), user2b.get("modifyTimestamp"))

            time.sleep(1)  # to ensure the audit log being delivered
            now = bios.time.now()
            statement = (
                bios.isql()
                .select()
                .from_signal("audit_log")
                .where("operation='Modify User'")
                .time_range(now, time0 - now - 1)
                .build()
            )
            audit_logs = self.sadmin.execute(statement).to_dict()
            self.assertEqual(len(audit_logs), 2)
            self.assertEqual(audit_logs[0].get("status"), "SUCCESS")
            self.assertEqual(audit_logs[1].get("status"), "SUCCESS")

        # Verify user deletions
        time0 = bios.time.now()
        self.sadmin.delete_user(self.email1)
        self.sadmin.delete_user(self.email2)
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            users = admin.get_users()
            user_emails = {user.get("email") for user in users.get("users")}
            self.assertFalse(self.email1.lower() in user_emails)
            self.assertFalse(self.email2.lower() in user_emails)

        # check audit logs
        time.sleep(1)  # to ensure the audit log being delivered
        now = bios.time.now()
        statement = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where("operation='Delete User'")
            .time_range(now, time0 - now - 1)
            .build()
        )
        audit_logs = self.sadmin.execute(statement).to_dict()
        self.assertEqual(len(audit_logs), 2)
        self.assertEqual(audit_logs[0].get("status"), "SUCCESS")
        self.assertEqual(audit_logs[1].get("status"), "SUCCESS")

    def test_user_create_conflict_and_double_deletion(self):
        time0 = bios.time.now()
        self.sadmin.create_user(
            bios.User(
                email=self.email1,
                full_name=f"{TENANT_NAME} Support User",
                password="secret",
                tenant_name=TENANT_NAME,
                roles=["TenantAdmin", "Report"],
            )
        )
        # This would be user conflict
        with self.assertRaises(ServiceError) as err_ctx:
            self.sadmin.create_user(
                bios.User(
                    email=self.email1,
                    full_name=f"{TENANT_NAME} Support User",
                    password="secret",
                    tenant_name=TENANT_NAME,
                    roles=["TenantAdmin", "Report"],
                )
            )
        self.assertEqual(err_ctx.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)
        self.assertEqual(err_ctx.exception.message, f"User already exists: {self.email1}")

        # check audit logs
        time.sleep(1)  # to ensure the audit log being delivered
        now = bios.time.now()
        statement = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where("operation='Create User'")
            .time_range(now, time0 - now - 1)
            .build()
        )
        audit_logs = self.sadmin.execute(statement).to_dict()
        self.assertEqual(len(audit_logs), 2)
        self.assertEqual(audit_logs[0].get("status"), "SUCCESS")
        self.assertEqual(audit_logs[1].get("status"), "FAILURE")

        time0 = bios.time.now()
        self.sadmin.delete_user(self.email1)
        # This would be double deletion
        with self.assertRaises(ServiceError) as err_ctx:
            self.sadmin.delete_user(self.email1)
        self.assertEqual(err_ctx.exception.error_code, ErrorCode.NOT_FOUND)
        self.assertEqual(err_ctx.exception.message, f"No such user: {self.email1}")

    def test_invalid_tenant(self):
        with self.assertRaises(ServiceError) as err_ctx:
            self.sadmin.create_user(
                bios.User(
                    email=self.email1,
                    full_name=f"{TENANT_NAME} Support User",
                    password="secret",
                    tenant_name="noSuchTenant",
                    roles=["TenantAdmin", "Report"],
                )
            )
        self.assertEqual(err_ctx.exception.error_code, ErrorCode.NO_SUCH_TENANT)
        self.assertEqual(err_ctx.exception.message, "Tenant not found: noSuchTenant")

    def test_called_by_non_sysadmin(self):
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            time0 = bios.time.now()
            with self.assertRaises(ServiceError) as err_ctx:
                admin.create_user(
                    bios.User(
                        email=self.email1,
                        full_name=f"{TENANT_NAME} Support User",
                        password="secret",
                        tenant_name=TENANT_NAME + "x",
                        roles=["TenantAdmin", "Report"],
                    )
                )
                self.assertEqual(err_ctx.exception.error_code, ErrorCode.FORBIDDEN)
                self.assertEqual(err_ctx.exception.message, "Permission denied")

                # check the audit log
                time.sleep(1)  # to ensure the audit log being delivered
                now = bios.time.now()
                statement = (
                    bios.isql()
                    .select()
                    .from_signal("audit_log")
                    .where("operation='Create User'")
                    .time_range(now, time0 - now - 1)
                    .build()
                )
                audit_logs = self.sadmin.execute(statement).to_dict()
                self.assertEqual(len(audit_logs), 1)
                self.assertEqual(audit_logs[0].get("status"), "FAILURE")

                created_user = admin.create_user(
                    bios.User(
                        email=self.email5,
                        full_name=f"{TENANT_NAME} User Manager",
                        password="secret",
                        tenant_name=TENANT_NAME,
                        roles=["TenantAdmin", "Report"],
                    )
                )

            time0 = bios.time.now()
            with self.assertRaises(ServiceError) as err_ctx:
                admin.delete_user("no_such_user@example.com")
                self.assertEqual(err_ctx.exception.error_code, ErrorCode.NOT_FOUND)
                self.assertEqual(err_ctx.exception.message, "Permission denied")

                # check the audit log
                time.sleep(1)  # to ensure the audit log being delivered
                now = bios.time.now()
                statement = (
                    bios.isql()
                    .select()
                    .from_signal("audit_log")
                    .where("operation='Delete User'")
                    .time_range(now, time0 - now - 1)
                    .build()
                )
                audit_logs = self.sadmin.execute(statement).to_dict()
                self.assertEqual(len(audit_logs), 1)
                self.assertEqual(audit_logs[0].get("status"), "FAILURE")

                admin.delete_user(user_id=created_user.get("userId"))

    def test_get_users(self):
        # Verify user creations
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            users = admin.get_users()
            users_summary = [
                {
                    "email": user.get("email"),
                    "roles": user.get("roles"),
                    "fullName": user.get("fullName"),
                    "status": user.get("status"),
                }
                for user in users.get("users")
            ]
            self.assertTrue(
                {
                    "email": f"admin+report@{TENANT_NAME.lower()}",
                    "roles": ["TenantAdmin", "Report"],
                    "fullName": f"Tenant {TENANT_NAME} Administrator + Report",
                    "status": "Active",
                }
                in users_summary
            )

    def test_get_users_system_tenant(self):
        password = "vZufvdcdqegjh5"
        user3 = self.sadmin.create_user(
            bios.User(
                email=self.email3,
                full_name=f"System tenant test user 1",
                password=password,
                tenant_name="_system",
                roles=["TenantAdmin", "Report"],
            )
        )
        user_id_email3 = user3.get("userId")
        with bios.login(ep_url(), self.email3, password) as session:
            tenant = session.get_tenant()
            assert tenant.get("tenantName") == "_system"

        # for backward compatibility
        user4 = self.sadmin.create_user(
            bios.User(
                email=self.email4,
                full_name="System tenant test user 2",
                password=password,
                tenant_name="/",
                roles=["TenantAdmin", "Report"],
            )
        )
        user_id_email4 = user4.get("userId")
        with bios.login(ep_url(), self.email4, password) as session:
            tenant = session.get_tenant()
            assert tenant.get("tenantName") == "_system"

        users = self.sadmin.get_users()
        emails = {user.get("email") for user in users.get("users")}

        assert self.email3 in emails
        assert self.email4 in emails

        self.sadmin.modify_user(user_id_email3, roles=["Ingest"])
        self.sadmin.modify_user(user_id_email4, roles=["Extract"])

        users2 = self.sadmin.get_users()
        modified = {user.get("email"): user.get("roles") for user in users2.get("users")}

        assert modified.get(self.email3) == ["Ingest"]
        assert modified.get(self.email4) == ["Extract"]

    def test_app_master_normal(self):
        email = "apps@isima.io"
        password = "XZvOPv0LiL"
        try:
            self.sadmin.create_user(
                bios.User(
                    email=email,
                    full_name="App master",
                    password=password,
                    tenant_name=TENANT_NAME,
                    roles=["AppMaster"],
                )
            )

            with bios.login(ep_url(), email, password) as session:
                tenant = session.get_tenant()
                assert tenant.get("tenantName") == TENANT_NAME
        finally:
            self.sadmin.delete_user(email)

    def test_modify_user_to_app_master(self):
        email = "regulat_to_apps@isima.io"
        password = "VEL82BxyXM"
        try:
            self.sadmin.create_user(
                bios.User(
                    email=email,
                    full_name="App master",
                    password=password,
                    tenant_name=TENANT_NAME,
                    roles=["Extract"],
                )
            )

            with bios.login(ep_url(), email, password) as session:
                tenant = session.get_tenant()
                assert tenant.get("tenantName") == TENANT_NAME

            with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
                users = admin.get_users([email])
                assert len(users.get("users")) == 1
                assert users.get("users")[0].get("roles") == ["Extract"]
                user_id = users.get("users")[0].get("userId")
                # regular tenant admin cannot change a user to AppMaster
                with pytest.raises(ServiceError) as err_ctx:
                    admin.modify_user(user_id, roles=["AppMaster"])
                assert err_ctx.value.error_code == ErrorCode.FORBIDDEN

                self.sadmin.modify_user(user_id, roles=["AppMaster"])
                users = admin.get_users([email])
                assert len(users.get("users")) == 1
                assert users.get("users")[0].get("roles") == ["AppMaster"]
        finally:
            self.sadmin.delete_user(email=email)

    def test_app_master_negative(self):
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            email = "apps-bad@isima.io"
            password = "O2xYvAalcP"
            with pytest.raises(ServiceError) as err_ctx:
                admin.create_user(
                    bios.User(
                        email=email,
                        full_name="App master",
                        password=password,
                        tenant_name=TENANT_NAME,
                        roles=["AppMaster"],
                    )
                )
            assert err_ctx.value.error_code == ErrorCode.FORBIDDEN
            assert err_ctx.value.message.startswith(
                "User role not allowed: Only System Admin users can create an AppMaster user"
            )


if __name__ == "__main__":
    pytest.main(sys.argv)
