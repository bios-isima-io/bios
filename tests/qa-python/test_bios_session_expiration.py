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
import time
import unittest
import pytest

import bios
from bios import ServiceError
from bios.errors import ErrorCode

from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME,
    setup_tenant_config,
)
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


# This test mirrors sessionExpiration.test.js
# The reason we need in both JS and Python is that Python SDK uses CSDK, whereas
# JS SDK does not. We need to test session expiration in both code paths.
class BiosSessionExpiration(unittest.TestCase):
    """Test cases to test bios app name validation"""

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            session.set_property("session_expiry", "15000")

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            session.set_property("session_expiry", "")

    def test_session_timeout(self):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            time.sleep(18)
            with self.assertRaises(ServiceError) as context:
                session.get_signals()
            assert context.exception.error_code == ErrorCode.SESSION_EXPIRED
            assert "Session expired" in context.exception.message

    def test_session_renewal(self):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            for _ in range(7):
                time.sleep(3)
                session.get_signals()


if __name__ == "__main__":
    pytest.main(sys.argv)
