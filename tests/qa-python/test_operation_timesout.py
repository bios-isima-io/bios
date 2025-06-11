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
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios._csdk import CSdkOperationId
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


class SessionExpirationTest(unittest.TestCase):
    def test(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass, timeout=2) as session:
            try:
                session.session.simple_method(CSdkOperationId.TEST_BIOS, data=['{"delay":5000}'])
                self.fail("Exception is expected")
            except ServiceError as err:
                self.assertEqual(err.error_code, ErrorCode.TIMEOUT)


if __name__ == "__main__":
    pytest.main(sys.argv)
