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

import bios
import os
import unittest

from bios import ServiceError, ErrorCode
from setup_common import read_file
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as endpoint
from tsetup import sadmin_user, sadmin_pass, admin_user, admin_pass


TEST_TENANT_NAME = "deleteDependencyTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

CONTEXT1 = "../resources/CustomerContext.json"
CONTEXT2 = "../resources/ElementContext.json"
CONTEXT3 = "../resources/ItemContext.json"
SIGNAL = "../resources/ImpressionSignal.json"
CONTEXT3_NAME = "itemContext"


class DeleteDependencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        self.assertIsNotNone(self.session)

    def tearDown(self):
        self.session.close()

    def test_delete_dependency(self):
        self.session.create_context(read_file(CONTEXT1))
        self.session.create_context(read_file(CONTEXT2))
        self.session.create_context(read_file(CONTEXT3))
        try:
            self.session.create_signal(read_file(SIGNAL))
        except ServiceError as err:
            self.assertEqual(err.error_code, ErrorCode.BAD_INPUT)
        self.session.delete_context(CONTEXT3_NAME)


if __name__ == "__main__":
    unittest.main()
