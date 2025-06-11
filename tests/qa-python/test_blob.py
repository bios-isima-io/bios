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
from tutils import b64enc


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosBlobTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

# constants
BLOB1 = bytearray.fromhex("feedfacecafebeef")
BLOB2 = bytearray.fromhex("31a571cf1a54")
BLOB3 = b"hello world"
BLOB4 = bytearray.fromhex("aeb7b476c82011e8a8d5f2801f1b9fd1")

CONTEXT_CONFIG = [
    {
        "contextName": "blob_context_1",
        "missingAttributePolicy": "StoreDefaultValue",
        "primaryKey": ["blobAttribute1"],
        "attributes": [
            {
                "attributeName": "blobAttribute1",
                "type": "Blob",
                "default": b64enc(BLOB1),
            },
            {"attributeName": "intAttribute1", "type": "Integer", "default": 123},
            {
                "attributeName": "blobAttribute_extra",
                "type": "Blob",
                "default": b64enc(BLOB2),
            },
        ],
    },
    {
        "contextName": "blob_context_2",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["blobAttribute2"],
        "attributes": [
            {"attributeName": "blobAttribute2", "type": "Blob"},
            {"attributeName": "intAttribute2", "type": "Integer"},
        ],
    },
    {
        "contextName": "blob_context_3",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["blobAttribute3"],
        "attributes": [
            {
                "attributeName": "blobAttribute3",
                "type": "Blob",
                "default": b64enc(BLOB3),
            },
            {"attributeName": "intAttribute3", "type": "Integer", "default": 1234},
        ],
    },
    {
        "contextName": "blob_context_4",
        "missingAttributePolicy": "StoreDefaultValue",
        "primaryKey": ["blobAttribute4"],
        "attributes": [
            {
                "attributeName": "blobAttribute4",
                "type": "Blob",
                "missingAttributePolicy": "Reject",
            },
            {
                "attributeName": "intAttribute4",
                "type": "Integer",
                "missingAttributePolicy": "Reject",
            },
        ],
    },
]
SIGNAL = {
    "signalName": "blob_signal_data",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "intAttribute", "type": "Integer", "default": 0},
        {"attributeName": "blobAttribute1", "type": "Blob", "default": b64enc(b"455")},
    ],
}


class AdminTest(unittest.TestCase):
    """Test cases that were ported from tfos-sdk to test extract operations"""

    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_create_context_map_default(self):
        self.addCleanup(self.session.delete_context, CONTEXT_CONFIG[0]["contextName"])
        self.session.create_context(CONTEXT_CONFIG[0])

    def test_create_context_map_reject(self):
        self.addCleanup(self.session.delete_context, CONTEXT_CONFIG[1]["contextName"])
        self.session.create_context(CONTEXT_CONFIG[1])

    def test_create_context_attribute_map_default(self):
        self.addCleanup(self.session.delete_context, CONTEXT_CONFIG[2]["contextName"])
        self.session.create_context(CONTEXT_CONFIG[2])

    def test_create_context_attribute_map_reject(self):
        self.addCleanup(self.session.delete_context, CONTEXT_CONFIG[3]["contextName"])
        self.session.create_context(CONTEXT_CONFIG[3])

    def test_create_signal_attribute_map_default(self):
        self.addCleanup(self.session.delete_signal, SIGNAL["signalName"])
        self.session.create_signal(SIGNAL)

    def test_create_context_primary_key_default_empty(self):
        context_config = {
            "contextName": "blob_context_pk_empty",
            "missingAttributePolicy": "StoreDefaultValue",
            "primaryKey": ["blobAttribute1"],
            "attributes": [
                {"attributeName": "blobAttribute1", "type": "Blob", "default": ""},
                {
                    "attributeName": "blobAttribute2",
                    "type": "Blob",
                    "default": b64enc(BLOB4),
                },
            ],
        }

        with self.assertRaises(ServiceError) as err_context:
            self.session.create_context(context_config)
        self.assertEqual(err_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            err_context.exception.message,
            ".*Constraint violation: Default value of primary key may not be empty.*",
        )

    def test_create_context_default_empty(self):
        context_config = {
            "contextName": "blob_context_empty",
            "missingAttributePolicy": "StoreDefaultValue",
            "primaryKey": ["blobAttribute1"],
            "attributes": [
                {"attributeName": "blobAttribute1", "type": "Blob", "default": b64enc(BLOB4)},
                {
                    "attributeName": "blobAttribute2",
                    "type": "Blob",
                    "default": "",
                },
            ],
        }
        self.addCleanup(self.session.delete_context, context_config["contextName"])
        self.session.create_context(context_config)

    def test_blob_values(self):
        self.addCleanup(self.session.delete_signal, SIGNAL["signalName"])
        self.session.create_signal(SIGNAL)
        start = self.insert_into_signal(
            SIGNAL["signalName"],
            [
                "67," + b64enc(b"77777"),
                "16," + b64enc(b"[1,2,'hj']"),
                "1," + b64enc(b"65.22"),
                "44," + b64enc(b"99999999999999999999"),
                "9," + b64enc(b"2017-09-10"),
                "90," + b64enc(b"10.44.55.77"),
                "6," + b64enc(b"9d184dc0-3da0-4057-93cb-f879cb2a9caa"),
                "10," + b64enc(b"{1.0, 'Hello', (1, 2, 3)}"),
            ],
        )
        select_request = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        events = resp.get_data_windows()[0].records
        self.assertEqual(len(events), 8)
        self.assertEqual(events[0].get("blobAttribute1"), b"77777")
        self.assertEqual(events[1].get("blobAttribute1"), b"[1,2,'hj']")
        self.assertEqual(events[2].get("blobAttribute1"), b"65.22")
        self.assertEqual(events[3].get("blobAttribute1"), b"99999999999999999999")
        self.assertEqual(events[4].get("blobAttribute1"), b"2017-09-10")
        self.assertEqual(events[5].get("blobAttribute1"), b"10.44.55.77")
        self.assertEqual(
            events[6].get("blobAttribute1"),
            b"9d184dc0-3da0-4057-93cb-f879cb2a9caa",
        )
        self.assertEqual(events[7].get("blobAttribute1"), b"{1.0, 'Hello', (1, 2, 3)}")

    @unittest.skip("BIOS-5093")
    def test_order_by_blob(self):
        self.addCleanup(self.session.delete_signal, SIGNAL["signalName"])
        self.session.create_signal(SIGNAL)
        start = self.insert_into_signal(
            SIGNAL["signalName"],
            [
                "67," + b64enc(b"77777"),
                "16," + b64enc(b"[1,2,'hj']"),
                "1," + b64enc(b"65.22"),
                "44," + b64enc(b"99999999999999999999"),
                "9," + b64enc(b"2017-09-10"),
                "90," + b64enc(b"10.44.55.77"),
                "6," + b64enc(b"9d184dc0-3da0-4057-93cb-f879cb2a9caa"),
                "10," + b64enc(b"{1.0, 'Hello', (1, 2, 3)}"),
            ],
        )
        select_request = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .order_by("blobAttribute1")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)


if __name__ == "__main__":
    pytest.main(sys.argv)
