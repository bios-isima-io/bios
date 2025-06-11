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
import math
import unittest
import pytest
import bios
from bios import ErrorCode, ServiceError
from bios.models.metric import Metric
from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME,
    setup_context,
    setup_signal,
    setup_tenant_config,
    try_delete_context,
    try_delete_signal,
)
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tutils import b64enc

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

CONTEXTS = [
    {
        "contextName": "missing_lookup_context_1",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["ctx1Attribute"],
        "attributes": [
            {"attributeName": "ctx1Attribute", "type": "String"},
            {"attributeName": "pp1_Attribute", "type": "String"},
        ],
    },
    {
        "contextName": "missing_lookup_context_2",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["intAttribute"],
        "attributes": [
            {"attributeName": "intAttribute", "type": "Integer"},
            {"attributeName": "pp2_Attribute", "type": "Integer"},
        ],
    },
    {
        "contextName": "missing_lookup_context_3",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["doubleAttribute"],
        "attributes": [
            {"attributeName": "doubleAttribute", "type": "Decimal"},
            {"attributeName": "pp3_Attribute", "type": "Decimal"},
        ],
    },
    {
        "contextName": "missing_lookup_context_4",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["booleanAttribute"],
        "attributes": [
            {"attributeName": "booleanAttribute", "type": "Boolean"},
            {"attributeName": "pp4_Attribute", "type": "Boolean"},
        ],
    },
    {
        "contextName": "missing_lookup_context_5",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["blobAttribute"],
        "attributes": [
            {"attributeName": "blobAttribute", "type": "Blob"},
            {"attributeName": "pp5_Attribute", "type": "Blob"},
        ],
    },
]


class MissingLookupPolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        for _, stream in enumerate(CONTEXTS):
            setup_context(cls.session, stream)

    @classmethod
    def tearDownClass(cls):
        for _, stream in enumerate(CONTEXTS):
            try_delete_context(cls.session, stream["contextName"])
        cls.session.close()

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    @classmethod
    def upsert_into_context(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().upsert().into(stream).csv_bulk(records).build()
        cls.session.execute(request)

    def test_mlp_set_to_reject(self):
        signal_config = {
            "signalName": "alltypes_mlp_reject",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "intAttribute", "type": "Integer"},
                {"attributeName": "doubleAttribute", "type": "Decimal"},
                {"attributeName": "booleanAttribute", "type": "Boolean"},
                {"attributeName": "blobAttribute", "type": "Blob"},
            ],
            "enrich": {
                "missingLookupPolicy": "Reject",
                "enrichments": [
                    {
                        "enrichmentName": "join_string",
                        "foreignKey": ["stringAttribute"],
                        "contextName": "missing_lookup_context_1",
                        "contextAttributes": [{"attributeName": "pp1_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_int",
                        "foreignKey": ["intAttribute"],
                        "contextName": "missing_lookup_context_2",
                        "contextAttributes": [{"attributeName": "pp2_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_double",
                        "foreignKey": ["doubleAttribute"],
                        "contextName": "missing_lookup_context_3",
                        "contextAttributes": [{"attributeName": "pp3_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_boolean",
                        "foreignKey": ["booleanAttribute"],
                        "contextName": "missing_lookup_context_4",
                        "contextAttributes": [{"attributeName": "pp4_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_blob",
                        "foreignKey": ["blobAttribute"],
                        "contextName": "missing_lookup_context_5",
                        "contextAttributes": [{"attributeName": "pp5_Attribute"}],
                    },
                ],
            },
        }
        self.session.create_signal(signal_config)
        self.addCleanup(self.session.delete_signal, signal_config["signalName"])
        self.upsert_into_context("missing_lookup_context_1", "hjk,str")
        self.upsert_into_context("missing_lookup_context_2", "24,35")
        self.upsert_into_context("missing_lookup_context_3", "45.7,89.2")
        self.upsert_into_context("missing_lookup_context_4", "False,True")
        self.upsert_into_context(
            "missing_lookup_context_5",
            b64enc(bytearray.fromhex("900dca75")) + "," + b64enc(bytearray.fromhex("900dd095")),
        )
        start = self.insert_into_signal(
            "alltypes_mlp_reject", "hjk,24,45.7,False," + b64enc(bytearray.fromhex("900dca75"))
        )
        select_request = (
            bios.isql()
            .select()
            .from_signal("alltypes_mlp_reject")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("pp1_Attribute"), "str")
        self.assertEqual(record.get("pp2_Attribute"), 35)
        self.assertEqual(record.get("pp3_Attribute"), 89.2)
        self.assertEqual(record.get("pp4_Attribute"), True)
        self.assertEqual(record.get("pp5_Attribute"), bytearray.fromhex("900dd095"))

    def test_mlp_set_to_storefillin(self):
        signal_config = {
            "signalName": "alltypes_mlp_storefillin",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "intAttribute", "type": "Integer"},
                {"attributeName": "doubleAttribute", "type": "Decimal"},
                {"attributeName": "booleanAttribute", "type": "Boolean"},
            ],
            "enrich": {
                "missingLookupPolicy": "StoreFillInValue",
                "enrichments": [
                    {
                        "enrichmentName": "join_string",
                        "foreignKey": ["stringAttribute"],
                        "contextName": "missing_lookup_context_1",
                        "contextAttributes": [
                            {"attributeName": "pp1_Attribute", "fillIn": "empty"}
                        ],
                    },
                    {
                        "enrichmentName": "join_int",
                        "foreignKey": ["intAttribute"],
                        "contextName": "missing_lookup_context_2",
                        "contextAttributes": [{"attributeName": "pp2_Attribute", "fillIn": -1}],
                    },
                    {
                        "enrichmentName": "join_double",
                        "foreignKey": ["doubleAttribute"],
                        "contextName": "missing_lookup_context_3",
                        "contextAttributes": [
                            {"attributeName": "pp3_Attribute", "fillIn": math.pi}
                        ],
                    },
                    {
                        "enrichmentName": "join_boolean",
                        "foreignKey": ["booleanAttribute"],
                        "contextName": "missing_lookup_context_4",
                        "contextAttributes": [{"attributeName": "pp4_Attribute", "fillIn": False}],
                    },
                ],
            },
        }
        self.session.create_signal(signal_config)
        self.addCleanup(self.session.delete_signal, signal_config["signalName"])
        start = self.insert_into_signal("alltypes_mlp_storefillin", "sdgaas,645,127.54,True")
        select_request = (
            bios.isql()
            .select()
            .from_signal("alltypes_mlp_storefillin")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("pp1_Attribute"), "empty")
        self.assertEqual(record.get("pp2_Attribute"), -1)
        self.assertEqual(record.get("pp3_Attribute"), math.pi)
        self.assertEqual(record.get("pp4_Attribute"), False)

    def test_mlp_set_to_reject_failure_case(self):
        signal_config = {
            "signalName": "alltypes_mlp_reject",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "intAttribute", "type": "Integer"},
                {"attributeName": "doubleAttribute", "type": "Decimal"},
                {"attributeName": "booleanAttribute", "type": "Boolean"},
                {"attributeName": "blobAttribute", "type": "Blob"},
            ],
            "enrich": {
                "missingLookupPolicy": "Reject",
                "enrichments": [
                    {
                        "enrichmentName": "join_string",
                        "foreignKey": ["stringAttribute"],
                        "contextName": "missing_lookup_context_1",
                        "contextAttributes": [{"attributeName": "pp1_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_int",
                        "foreignKey": ["intAttribute"],
                        "contextName": "missing_lookup_context_2",
                        "contextAttributes": [{"attributeName": "pp2_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_double",
                        "foreignKey": ["doubleAttribute"],
                        "contextName": "missing_lookup_context_3",
                        "contextAttributes": [{"attributeName": "pp3_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_boolean",
                        "foreignKey": ["booleanAttribute"],
                        "contextName": "missing_lookup_context_4",
                        "contextAttributes": [{"attributeName": "pp4_Attribute"}],
                    },
                    {
                        "enrichmentName": "join_blob",
                        "foreignKey": ["blobAttribute"],
                        "contextName": "missing_lookup_context_5",
                        "contextAttributes": [{"attributeName": "pp5_Attribute"}],
                    },
                ],
            },
        }
        self.session.create_signal(signal_config)
        self.addCleanup(self.session.delete_signal, signal_config["signalName"])
        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal(
                "alltypes_mlp_reject", "sdgaas,645,127.54,True," + b64enc(b"_SomeUnknownBlob")
            )
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message, "Missed to lookup context with a foreign key*"
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
