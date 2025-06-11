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

import os
import unittest
import urllib.error
import urllib.request

import bios
from bios import ServiceError, ErrorCode
from tsetup import sadmin_pass, sadmin_user


# TODO: We should setup the server in the environment. This depends on a
# third test environment
ENDPOINT = "https://load.tieredfractals.com"


def endpoint_reachable():
    try:
        with urllib.request.urlopen(ENDPOINT) as f:
            f.read(1)
        return True
    except urllib.error.URLError:
        return False


@unittest.skipUnless(endpoint_reachable(), f"Endpoint {ENDPOINT} is unreachable")
class ExternalConnectionTest(unittest.TestCase):
    """This test case verifies external site access from SDK.
    The SDK uses system-installed CA certificates for TLS negotiation to access external site,
    which is qa.tieredfractals.com in this test case. The test passes when the SDK connects to
    the external server successfully. The case also tries a few simple operations, failing them
    is fine unless it's SERVER_CONNECTION_FAILURE or SERVER_CHANNEL_ERROR. Otherwise are likely
    to be caused by any server side issue, and it is out of scope of this test.
    """

    @classmethod
    def setUpClass(cls):
        cls.env_name = "BIOS_CONNECTION_TYPE"
        cls.connection_type = os.environ.get(cls.env_name)
        if cls.connection_type:
            del os.environ[cls.env_name]

    @classmethod
    def tearDownClass(cls):
        if cls.connection_type:
            os.environ[cls.env_name] = cls.connection_type

    def test_crud_system_signal_by_sysadmin(self):
        try:
            with bios.login(ENDPOINT, sadmin_user, sadmin_pass) as sadmin:
                tenants = sadmin.list_tenants()
                self.assertGreaterEqual(len(tenants), 1)
        except ServiceError as err:
            # TODO(BIOS-1804): Is it better to allow only 5xx errors?
            self.assertNotEqual(err.error_code, ErrorCode.SERVER_CONNECTION_FAILURE)
            self.assertNotEqual(err.error_code, ErrorCode.SERVER_CHANNEL_ERROR)


if __name__ == "__main__":
    unittest.main()
