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
import pytest
from bios import ServiceError
import bios
from bios.errors import ErrorCode
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosMiscTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


class MiscTest(unittest.TestCase):
    """Test cases that were ported from tfos-sdk to test extract operations"""

    @classmethod
    def setUpClass(cls):
        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        try:
            cls.sadmin.delete_tenant(TEST_TENANT_NAME)
        except ServiceError:
            pass
        cls.sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        try:
            cls.sadmin.delete_tenant(TEST_TENANT_NAME)
        except ServiceError:
            pass
        cls.sadmin.close()

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_sorting_after_new_attr(self):
        """
        Test to verify sorting should be successful when newly added
        attribute is the sorting key.
        """
        stream1 = {
            "signalName": "stream1",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "key", "type": "String"},
            ],
        }

        self.session.create_signal(stream1)
        ev_timestamp = self.insert_into_signal("stream1", ["Bill"])

        stream1_new = {
            "signalName": "stream1",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "key", "type": "String"},
                {
                    "attributeName": "value",
                    "type": "Integer",
                    "missingAttributePolicy": "StoreDefaultValue",
                    "default": 0,
                },
            ],
        }

        self.session.update_signal("stream1", stream1_new)
        self.insert_into_signal("stream1", ["dolly,198", "alley,197", "zen,199"])
        select_request = (
            bios.isql()
            .select()
            .from_signal("stream1")
            .order_by("value")
            .time_range(ev_timestamp, bios.time.now() - ev_timestamp)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0].get("value"), 0)
        self.assertEqual(records[1].get("value"), 197)
        self.assertEqual(records[2].get("value"), 198)
        self.assertEqual(records[3].get("value"), 199)

    def test_context_enrich_attr_change(self):
        """
        Test should fail when the context attribute datatype is changed
        but the same attribute is unchanged in signal stream.
        """
        context = {
            "contextName": "studentInfo",
            "missingAttributePolicy": "Reject",
            "primaryKey": ["rollNo"],
            "attributes": [
                {"attributeName": "rollNo", "type": "Integer"},
                {"attributeName": "first", "type": "String"},
                {"attributeName": "last", "type": "String"},
            ],
        }

        signal = {
            "signalName": "student",
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "rollNo", "type": "Integer"}],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "studentDetails",
                        "foreignKey": ["rollNo"],
                        "missingLookupPolicy": "StoreFillInValue",
                        "contextName": "studentInfo",
                        "contextAttributes": [
                            {"attributeName": "first", "fillIn": "unknown"},
                            {"attributeName": "last", "fillIn": "unknown"},
                        ],
                    },
                ]
            },
        }

        self.session.create_context(context)
        self.session.create_signal(signal)
        context["attributes"][0]["type"] = "String"
        with self.assertRaises(ServiceError) as error:
            self.session.update_context("studentInfo", context)
        self.assertEqual(error.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error.exception.message,
            "Constraint violation:"
            r" Type of foreignKey\[0\] must match the type of the context's primary key.*",
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
