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
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


class BiosAppName(unittest.TestCase):
    """Test cases to test bios app name validation"""

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

    def test_bios_valid_app_names(self):
        session = bios.login(ep_url(), ADMIN_USER, admin_pass, app_name="valid-app_name")
        session.close()
        session = bios.login(ep_url(), sadmin_user, sadmin_pass, app_name="_valid-app_name2")
        session.close()

    def try_invalid_app_name(self, app_name):
        print(app_name)
        with self.assertRaises(ServiceError) as context:
            bios.login(ep_url(), ADMIN_USER, admin_pass, app_name=app_name)
        print(context.exception.message)
        self.assertEqual(context.exception.error_code, ErrorCode.BAD_INPUT)

        with self.assertRaises(ServiceError) as context:
            bios.login(ep_url(), sadmin_user, sadmin_pass, app_name=app_name)
        print(context.exception.message)
        self.assertEqual(context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_bios_invalid_app_names(self):
        self.try_invalid_app_name("has spaces in the name")
        self.try_invalid_app_name("has, commas, in, the, name")
        self.try_invalid_app_name("'abc")
        self.try_invalid_app_name("exclamation!")
        self.try_invalid_app_name("plus+")


if __name__ == "__main__":
    pytest.main(sys.argv)
