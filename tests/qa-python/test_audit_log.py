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

import pytest

import bios
from bios import ServiceError

from tsetup import extract_user, extract_pass
from tsetup import sadmin_user, sadmin_pass, admin_user, admin_pass
from tsetup import get_endpoint_url as ep_url


class TestAuditLog(unittest.TestCase):
    # constants
    TENANT_NAME = "audit_tenant"
    TENANT_CONFIG = {"tenantName": TENANT_NAME}
    SIGNAL_CONFIG = {
        "signalName": "audit_stream",
        "missingAttributePolicy": "Reject",
        "attributes": [{"attributeName": "name", "type": "string"}],
    }
    UPDATED_CONFIG = {
        "signalName": "audit_stream",
        "missingAttributePolicy": "Reject",
        "attributes": [
            {"attributeName": "name", "type": "String"},
            {"attributeName": "id", "type": "Integer", "default": 0},
        ],
    }

    def setUp(self):
        self.super_admin = None

    def tearDown(self):
        if self.super_admin:
            self.try_delete_tenant(self.TENANT_NAME)
            self.super_admin.close()

    def try_delete_tenant(self, tenant):
        try:
            self.super_admin.delete_tenant(tenant)
        except ServiceError:
            pass

    def test_add_tenant_log(self):
        #
        # Create a new tenant
        #
        self.super_admin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        start_time = bios.time.now()
        self.super_admin.create_tenant(self.TENANT_CONFIG)
        delta = bios.time.now() - start_time + 10

        #
        # Verify tenant added is audited.
        #
        extract_session = bios.login(ep_url(), f"{extract_user}@_system", extract_pass)
        statement_create_tenant = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where("operation = 'Add Tenant' AND request = 'audit_tenant' and status = 'SUCCESS'")
            .time_range(start_time, delta)
            .build()
        )

        #
        # Verify that audit is logged for failed tenant creation
        #
        start_create_tenant_2 = bios.time.now()
        try:
            self.super_admin.create_tenant(self.TENANT_CONFIG)
        except ServiceError:
            pass
        delta = bios.time.now() - start_time + 10

        statement_failed_create_tenant = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where("operation = 'Add Tenant' AND request = 'audit_tenant' AND status = 'CONFLICT'")
            .time_range(start_create_tenant_2, 10000)
            .build()
        )

        #
        # Verrify Admin login is audited
        #
        start_admin_login = bios.time.now()
        admin_session = bios.login(ep_url(), f"{admin_user}@audit_tenant", admin_pass)

        statement_admin_login = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where(
                "operation = 'Login' AND user = 'admin@audit_tenant' AND tenant = 'audit_tenant'"
            )
            .time_range(start_admin_login, 10000)
            .build()
        )

        #
        # Verify creating stream config is audited
        #
        start_add_stream = bios.time.now()
        admin_session.create_signal(self.SIGNAL_CONFIG)

        statement_add_stream = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where(
                "operation = 'Add Signal Config' AND user = 'admin@audit_tenant'"
                " AND tenant = 'audit_tenant'"
            )
            .time_range(start_add_stream, 10000)
            .build()
        )

        #
        # Verify updating stream config is audited
        #
        start_update_stream = bios.time.now()
        admin_session.update_signal(self.SIGNAL_CONFIG.get("signalName"), self.UPDATED_CONFIG)

        statement_update_signal = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where(
                "operation = 'Update Signal Config' AND user = 'admin@audit_tenant'"
                " AND tenant = 'audit_tenant'"
            )
            .time_range(start_update_stream, 10000)
            .build()
        )

        #
        # Verify deleting stream config is audited
        #
        start_delete_signal = bios.time.now()
        admin_session.delete_signal(self.SIGNAL_CONFIG.get("signalName"))

        statement_delete_signal = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where(
                "operation = 'Delete Signal Config' AND user = 'admin@audit_tenant'"
                " AND tenant = 'audit_tenant'"
            )
            .time_range(start_delete_signal, 10000)
            .build()
        )

        #
        # Verify deleting tenant config is audited
        #
        start_delete_tenant = bios.time.now()
        self.super_admin.delete_tenant("audit_tenant")

        statement_delete_tenant = (
            bios.isql()
            .select()
            .from_signal("audit_log")
            .where("operation = 'Delete Tenant' AND request = 'audit_tenant'")
            .time_range(start_delete_tenant, 5000)
            .build()
        )

        for _ in range(10):
            last_result = extract_session.execute(statement_delete_tenant).to_dict()
            if last_result:
                break
            time.sleep(1)

        results = extract_session.multi_execute(
            statement_create_tenant,
            statement_failed_create_tenant,
            statement_admin_login,
            statement_add_stream,
            statement_update_signal,
            statement_delete_signal,
            statement_delete_tenant,
        )
        for index, result in enumerate(results):
            entries = result.to_dict()
            message = f"result[{index}]: {json.dumps(entries)}"
            assert len(entries) == 1, message

        extract_session.close()
        admin_session.close()


if __name__ == "__main__":
    pytest.main(sys.argv)
