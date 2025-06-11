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
import unittest

import pytest

from bios import ServiceError
import bios
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as endpoint
from tsetup import sadmin_user, sadmin_pass, admin_user, admin_pass


TEST_TENANT_NAME = "bios_login_test"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
SIGNAL_MINIMUM = {
    "signalName": "minimum",
    "missingAttributePolicy": "reject",
    "attributes": [{"attributeName": "one", "type": "string"}],
}

SIGNAL_MAXIMUM = {
    "signalName": "maximum",
    "missingAttributePolicy": "reject",
    "attributes": [{"attributeName": "one", "type": "string"}],
}


class BiosLoginTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        with bios.login(ep_url(), f"{admin_user}@{TEST_TENANT_NAME}", admin_pass) as admin:
            admin.create_signal(SIGNAL_MINIMUM)
            admin.create_signal(SIGNAL_MAXIMUM)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def test_invalid_login(self):
        with self.assertRaises(ServiceError) as context:
            bios.login(ep_url(), "uperadmin", sadmin_pass)
            self.assertTrue("Authentication failed", context.exception)

    def test_valid_login(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            self.assertIsNotNone(session)

    def test_get_signals(self):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            self.assertIsNotNone(session)
            signals = session.get_signals()
            self.assertIsNotNone(signals)
            self.assertEqual(len(signals), 2)
            self.assertEqual(
                set(map(lambda entry: entry["signalName"], signals)),
                set(["minimum", "maximum"]),
            )

    def test_get_system_signals(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            self.assertIsNotNone(session)
            signals = session.get_signals(include_internal=True, detail=True)
            self.assertIsNotNone(signals)

    def test_get_signals_with_name_filter(self):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            self.assertIsNotNone(session)

            signals = session.get_signals(names=["minimum"])
            self.assertIsNotNone(signals)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signalName"], "minimum")

            signals = session.get_signals(names=["maximum"])
            self.assertIsNotNone(signals)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signalName"], "maximum")

            minimum = session.get_signal("minimum")
            self.assertIsNotNone(minimum)
            self.assertEqual(minimum["signalName"], "minimum")

    def test_get_signals_with_non_existent_signal_raises_exception(self):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            self.assertIsNotNone(session)
            with self.assertRaises(ServiceError) as context:
                session.get_signals(names=["mum"])
                self.assertTrue("No such signal", context.exception)


if __name__ == "__main__":
    pytest.main(sys.argv)
