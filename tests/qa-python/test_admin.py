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
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TENANT_NAME

VALID_NAMES = [
    "weather_data",
    "abcdefghijklmnopqrstuvwxyz",
    "ZYXWVUTSRQPONMLKJIHGFEDCBA",
    "1234567890123456789012345678901234567890",
    "x234567890123456789012345678901234567890",
    "ends_with_underscore_",
    "system",
    "config",
    "tenant",
    "tenants",
    "stream",
    "streams",
    "m",
]

INVALID_NAMES = [
    "",
    "_weather_data_",
    "x12345678901234567890123456789012345678901234567890123456789012345678901234567890",
    "$bad",
    "bad$bad",
    "bad$",
    "this-is-bad-too",
]


class AdminTest(unittest.TestCase):
    """Test cases that were ported from tfos-sdk to test extract operations"""

    @classmethod
    def setUpClass(cls):
        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.sadmin.close()

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_recreate_stream_with_new_attribute(self):
        signal = {
            "signalName": "student",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "firstName", "type": "String"},
                {"attributeName": "lastName", "type": "String"},
            ],
        }

        self.addCleanup(self.session.delete_signal, "student")
        self.session.create_signal(signal)
        start = self.insert_into_signal("student", "10,Richard,Owen")
        select_request = (
            bios.isql()
            .select()
            .from_signal("student")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("rollNo"), 10)
        self.assertEqual(record.get("firstName"), "Richard")
        self.assertEqual(record.get("lastName"), "Owen")

        self.session.delete_signal("student")

        signal_new = signal.copy()
        signal_new["attributes"].append({"attributeName": "score", "type": "Integer"})

        self.session.create_signal(signal_new)
        start = self.insert_into_signal("student", "10,Richard,Owen,90")
        select_request = (
            bios.isql()
            .select()
            .from_signal("student")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("rollNo"), 10)
        self.assertEqual(record.get("firstName"), "Richard")
        self.assertEqual(record.get("lastName"), "Owen")
        self.assertEqual(record.get("score"), 90)

    def test_create_stream_duplicate_attribute(self):
        signal = {
            "signalName": "studentDuplicateAttribute",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "firstName", "type": "String"},
                {"attributeName": "firstName", "type": "String"},
            ],
        }

        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(error_context.exception.message, ".*Attribute name conflict;.*")

    def test_tenant_names(self):
        for name in VALID_NAMES:
            self.addCleanup(self.sadmin.delete_tenant, name)
            self.sadmin.create_tenant({"tenantName": name})

        for name in INVALID_NAMES:
            with self.assertRaises(ServiceError) as error_context:
                self.sadmin.create_tenant({"tenantName": name})
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_stream_names(self):
        for name in VALID_NAMES:
            self.session.create_signal(
                {
                    "signalName": name,
                    "missingAttributePolicy": "Reject",
                    "attributes": [{"attributeName": "num", "type": "Integer"}],
                }
            )
            self.session.delete_signal(name)

        for name in INVALID_NAMES:
            with self.assertRaises(ServiceError) as error_context:
                self.session.create_signal(
                    {
                        "signalName": name,
                        "missingAttributePolicy": "Reject",
                        "attributes": [{"attributeName": "num", "type": "Integer"}],
                    }
                )
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_attr_names(self):
        for name in VALID_NAMES:
            self.session.create_signal(
                {
                    "signalName": "valid_attr_signal",
                    "missingAttributePolicy": "Reject",
                    "attributes": [{"attributeName": name, "type": "Integer"}],
                }
            )
            self.session.delete_signal("valid_attr_signal")

        for name in INVALID_NAMES:
            with self.assertRaises(ServiceError) as error_context:
                self.session.create_signal(
                    {
                        "signalName": "invalid_attr_signal",
                        "missingAttributePolicy": "Reject",
                        "attributes": [{"attributeName": name, "type": "Integer"}],
                    }
                )
            self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_signal_add_attribute_without_default(self):
        self.addCleanup(self.session.delete_signal, "student1")
        signal = {
            "signalName": "student1",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "firstName", "type": "String"},
                {"attributeName": "lastName", "type": "String"},
            ],
        }

        self.session.create_signal(signal)
        signal["attributes"].append({"attributeName": "score", "type": "Integer"})

        with self.assertRaises(ServiceError) as error_context:
            self.session.update_signal("student1", signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*Default value must be configured for an adding attribute.*",
        )

    def test_context_add_attribute_without_default(self):
        self.addCleanup(self.session.delete_context, "studentInfo1")
        context = {
            "contextName": "studentInfo1",
            "missingAttributePolicy": "Reject",
            "primaryKey": ["rollNo"],
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "first", "type": "String"},
                {"attributeName": "last", "type": "String"},
            ],
        }

        self.session.create_context(context)
        context["attributes"].append({"attributeName": "score", "type": "Integer"})

        with self.assertRaises(ServiceError) as error_context:
            self.session.update_context("studentInfo1", context)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*Default value must be configured for an adding attribute.*",
        )

    def test_signal_delete_attribute(self):
        self.addCleanup(self.session.delete_signal, "student2")
        signal = {
            "signalName": "student2",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "firstName", "type": "String"},
                {"attributeName": "lastName", "type": "String"},
            ],
        }
        self.session.create_signal(signal)
        signal["attributes"].pop(1)
        self.session.update_signal(signal["signalName"], signal)

    def test_context_delete_attribute(self):
        self.addCleanup(self.session.delete_context, "studentInfo2")
        context = {
            "contextName": "studentInfo2",
            "missingAttributePolicy": "Reject",
            "primaryKey": ["rollNo"],
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "first", "type": "String"},
                {"attributeName": "last", "type": "String"},
            ],
        }

        self.session.create_context(context)
        context["attributes"].pop(1)
        self.session.update_context(context["contextName"], context)

    def test_context_delete_primary_key(self):
        self.addCleanup(self.session.delete_context, "studentInfo3")
        context = {
            "contextName": "studentInfo3",
            "missingAttributePolicy": "Reject",
            "primaryKey": ["rollNo"],
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "first", "type": "String"},
                {"attributeName": "last", "type": "String"},
            ],
        }

        self.session.create_context(context)
        context["attributes"].pop(0)
        context["primaryKey"] = ["first"]
        with self.assertRaises(ServiceError) as error_context:
            self.session.update_context(context["contextName"], context)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            "Constraint violation: Primary key attributes may not be modified*",
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
