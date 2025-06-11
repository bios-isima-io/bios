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
import bios
from bios import ServiceError
from bios import ErrorCode
from tutils import b64enc
from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME,
    setup_context,
    setup_signal,
    try_delete_signal,
    try_delete_context,
    setup_tenant_config,
)
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

CONTEXT_CONFIG = {
    "contextName": "context_update_entry_test",
    "missingAttributePolicy": "Reject",
    "primaryKey": ["enumdt"],
    "attributes": [
        {
            "attributeName": "enumdt",
            "type": "String",
            "allowedValues": ["2BHK", "4BHK", "9BHK", "1BHK", "3BHK", "5BHK"],
        },
        {"attributeName": "intdt", "type": "Integer"},
        {"attributeName": "stringdt", "type": "String"},
        {"attributeName": "booleandt", "type": "Boolean"},
        {"attributeName": "doubledt", "type": "Decimal"},
        {
            "attributeName": "enumdt2",
            "type": "String",
            "allowedValues": ["hall", "kitchen", "Bedroom", "Balcony"],
        },
        {"attributeName": "blobdt", "type": "blob"},
    ],
}

SIGNAL_CONFIG = {
    "signalName": "signal_update_entry_test",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "enumdt1",
            "type": "String",
            "allowedValues": ["2BHK", "4BHK", "9BHK", "1BHK", "3BHK", "5BHK"],
        }
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "context_merge",
                "foreignKey": ["enumdt1"],
                "missingLookupPolicy": "Reject",
                "contextName": "context_update_entry_test",
                "contextAttributes": [
                    {"attributeName": "intdt"},
                    {"attributeName": "stringdt"},
                    {"attributeName": "booleandt"},
                    {"attributeName": "doubledt"},
                    {"attributeName": "enumdt2"},
                    {"attributeName": "blobdt"},
                ],
            }
        ]
    },
}


class ContextUpdateEntryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_context(cls.session, CONTEXT_CONFIG)
        setup_signal(cls.session, SIGNAL_CONFIG)

        cls.upsert_entries(
            [
                "9BHK,23,abc,True,19.34,Balcony," + b64enc(b"123123"),
                "3BHK,45,hjd,False,7.8,hall," + b64enc(b"hello"),
                "5BHK,78,jkl,False,0.9,kitchen," + b64enc(b"abcabc"),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        try_delete_signal(cls.session, SIGNAL_CONFIG["signalName"])
        try_delete_context(cls.session, CONTEXT_CONFIG["contextName"])
        cls.session.close()

    @classmethod
    def select_entries(cls, keys):
        select_request = (
            bios.isql()
            .select()
            .from_context(CONTEXT_CONFIG["contextName"])
            .where(keys=keys)
            .build()
        )
        return cls.session.execute(select_request).to_dict()

    @classmethod
    def update_entry(cls, key, values):
        update_request = (
            bios.isql().update(CONTEXT_CONFIG["contextName"]).set(values).where(key=key).build()
        )
        cls.session.execute(update_request)

    @classmethod
    def upsert_entries(cls, entries):
        if not isinstance(entries, list):
            entries = [entries]
        upsert_request = (
            bios.isql().upsert().into(CONTEXT_CONFIG["contextName"]).csv_bulk(entries).build()
        )
        cls.session.execute(upsert_request)

    @classmethod
    def delete_entries(cls, keys):
        delete_request = (
            bios.isql()
            .delete()
            .from_context(CONTEXT_CONFIG["contextName"])
            .where(keys=keys)
            .build()
        )
        cls.session.execute(delete_request)

    @classmethod
    def insert(cls, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(SIGNAL_CONFIG["signalName"]).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_update_context_entries(self):
        self.update_entry(
            ["5BHK"],
            {
                "stringdt": "abc",
                "booleandt": True,
                "enumdt2": "hall",
                "doubledt": 10.5,
                "blobdt": b64enc(b"hello"),
            },
        )
        records = self.select_entries([["5BHK"]])
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["enumdt"], "5BHK")
        self.assertEqual(record["intdt"], 78)
        self.assertEqual(record["stringdt"], "abc")
        self.assertEqual(record["booleandt"], True)
        self.assertEqual(record["enumdt2"], "hall")
        self.assertEqual(record["doubledt"], 10.5)
        self.assertEqual(record["blobdt"], b64enc(b"hello"))

    def test_update_context_entries_multiple(self):
        self.update_entry(
            ["5BHK"],
            {
                "stringdt": "what",
                "intdt": 878,
                "booleandt": True,
                "enumdt2": "hall",
                "doubledt": 18.56,
                "blobdt": b64enc(b"No way"),
            },
        )

        self.update_entry(
            ["5BHK"],
            {
                "stringdt": "abc",
                "booleandt": True,
                "enumdt2": "hall",
                "doubledt": 10.5,
                "blobdt": b64enc(b"hello"),
            },
        )
        records = self.select_entries([["5BHK"]])
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["enumdt"], "5BHK")
        self.assertEqual(record["intdt"], 878)
        self.assertEqual(record["stringdt"], "abc")
        self.assertEqual(record["booleandt"], True)
        self.assertEqual(record["enumdt2"], "hall")
        self.assertEqual(record["doubledt"], 10.5)
        self.assertEqual(record["blobdt"], b64enc(b"hello"))

    def test_enrich_with_updated_entries(self):
        self.update_entry(
            ["3BHK"],
            {
                "stringdt": "iii",
                "intdt": 79,
                "booleandt": True,
                "enumdt2": "Balcony",
                "doubledt": 1.5,
                "blobdt": b64enc(b"uihg"),
            },
        )
        timestamp = self.insert("3BHK")
        select = (
            bios.isql()
            .select()
            .from_signal(SIGNAL_CONFIG["signalName"])
            .time_range(timestamp, bios.time.now() - timestamp)
            .build()
        )
        resp = self.session.execute(select)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("enumdt1"), "3BHK")
        self.assertEqual(record.get("intdt"), 79)
        self.assertEqual(record.get("stringdt"), "iii")
        self.assertEqual(record.get("booleandt"), True)
        self.assertEqual(record.get("doubledt"), 1.5)
        self.assertEqual(record.get("enumdt2"), "Balcony")
        # TODO(BIOS-5169)
        # self.assertEqual(record.get("blobdt"), b64enc(b"uihg"))

    def test_update_invalid_key(self):
        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(
                ["1BHK"],
                {
                    "intdt": 8780,
                },
            )
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message, "Invalid primary key: No such primary key*"
        )

    def test_update_invalid_dtype(self):
        self.upsert_entries(["9BHK,23,abc,True,19.34,Balcony," + b64enc(b"123123")])

        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(["9BHK"], {"intdt": "543z"})
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            "Invalid value syntax: Input must be a numeric string*",
        )

        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(["9BHK"], {"booleandt": "cool"})
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(["9BHK"], {"blobdt": 0})
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_update_invalid_value(self):
        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(["9BHK"], {"intdt_invalid": "543"})
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(error_context.exception.message, "Invalid request: No such attribute*")

    def test_update_primary_key(self):
        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(["9BHK"], {"enumdt": "2BHK"})
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message, "Invalid request: Primary key may not be modified*"
        )

    @unittest.skip("BIOS-5170")
    def test_update_none_value(self):
        with self.assertRaises(ServiceError) as error_context:
            self.update_entry(["9BHK"], {"intdt": None})
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_update_with_same_value(self):
        self.update_entry(
            ["5BHK"],
            {
                "stringdt": "abc",
                "booleandt": True,
                "enumdt2": "hall",
                "doubledt": 10.5,
                "blobdt": b64enc(b"hello"),
            },
        )
        self.update_entry(
            ["5BHK"],
            {
                "stringdt": "abc",
                "booleandt": True,
                "enumdt2": "hall",
                "doubledt": 10.5,
                "blobdt": b64enc(b"hello"),
            },
        )
        records = self.select_entries([["5BHK"]])
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["enumdt"], "5BHK")
        self.assertEqual(record["intdt"], 878)
        self.assertEqual(record["stringdt"], "abc")
        self.assertEqual(record["booleandt"], True)
        self.assertEqual(record["enumdt2"], "hall")
        self.assertEqual(record["doubledt"], 10.5)
        self.assertEqual(record["blobdt"], b64enc(b"hello"))


if __name__ == "__main__":
    pytest.main(sys.argv)
