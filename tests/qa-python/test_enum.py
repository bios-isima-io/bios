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
from bios.models.metric import Metric
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import (
    setup_context,
    setup_signal,
    setup_tenant_config,
    try_delete_context,
    try_delete_signal,
)
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

SIGNALS = [
    {
        "signalName": "EnumTestSignal1",
        "missingAttributePolicy": "Reject",
        "attributes": [
            {"attributeName": "sr", "type": "Integer"},
            {
                "attributeName": "flatType",
                "type": "String",
                "allowedValues": ["1BHK", "2BHK", "4BHK", "5BHK", "6BHK", "7BHK"],
            },
            {
                "attributeName": "kitchen",
                "type": "String",
                "allowedValues": ["fridge", "stove", "cooker", "oven", "microwave"],
            },
        ],
    },
    {
        "signalName": "EnumTestEnumStrict",
        "missingAttributePolicy": "Reject",
        "attributes": [
            {
                "attributeName": "enumAttr1",
                "type": "String",
                "allowedValues": ["2BHK", "4BHK", "9BHK", "1BHK"],
            },
            {
                "attributeName": "enumAttr2",
                "type": "String",
                "allowedValues": ["kitchen", "Rooms", "Balcony"],
            },
        ],
        "enrich": {
            "enrichments": [
                {
                    "enrichmentName": "ppl1",
                    "foreignKey": ["enumAttr1"],
                    "missingLookupPolicy": "Reject",
                    "contextName": "EnumTestContext1",
                    "contextAttributes": [{"attributeName": "sr_no1", "as": "final1"}],
                },
                {
                    "enrichmentName": "ppl2",
                    "foreignKey": ["enumAttr2"],
                    "missingLookupPolicy": "Reject",
                    "contextName": "EnumTestContext2",
                    "contextAttributes": [{"attributeName": "sr_no2", "as": "final2"}],
                },
            ],
        },
    },
    {
        "signalName": "EnumTestEnumDefault",
        "missingAttributePolicy": "Reject",
        "attributes": [
            {
                "attributeName": "enumAttr1",
                "type": "String",
                "allowedValues": ["2BHK", "4BHK", "9BHK", "1BHK"],
            }
        ],
        "enrich": {
            "enrichments": [
                {
                    "enrichmentName": "ppl",
                    "foreignKey": ["enumAttr1"],
                    "missingLookupPolicy": "StoreFillInValue",
                    "contextName": "EnumTestContext1",
                    "contextAttributes": [
                        {"attributeName": "sr_no1", "as": "final1", "fillIn": 123},
                        {"attributeName": "listenum", "as": "final3", "fillIn": "abc"},
                    ],
                }
            ],
        },
    },
]

CONTEXTS = [
    {
        "contextName": "EnumTestContext1",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["enumAttribute1"],
        "attributes": [
            {"attributeName": "sr_no1", "type": "Integer"},
            {"attributeName": "listenum", "type": "String", "allowedValues": ["abc", "uij"]},
            {
                "attributeName": "enumAttribute1",
                "type": "String",
                "allowedValues": ["2BHK", "4BHK", "9BHK", "1BHK"],
            },
        ],
    },
    {
        "contextName": "EnumTestContext2",
        "missingAttributePolicy": "Reject",
        "primaryKey": ["enumAttribute2"],
        "attributes": [
            {"attributeName": "sr_no2", "type": "Integer"},
            {
                "attributeName": "enumAttribute2",
                "type": "String",
                "allowedValues": ["kitchen", "Rooms", "Balcony"],
            },
        ],
    },
]


class EnumTest(unittest.TestCase):
    """Test select enum operations"""

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        for _, stream in enumerate(CONTEXTS):
            setup_context(cls.session, stream)
        for _, stream in enumerate(SIGNALS):
            setup_signal(cls.session, stream)

    @classmethod
    def tearDownClass(cls):
        for _, stream in enumerate(SIGNALS):
            try_delete_signal(cls.session, stream["signalName"])
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

    def test_order_by_enum(self):
        records = ["34,5BHK,fridge", "67,1BHK,microwave", "90,4BHK,stove"]
        start = self.insert_into_signal("EnumTestSignal1", records)

        request = (
            bios.isql()
            .select()
            .from_signal("EnumTestSignal1")
            .order_by("flatType", case_sensitive=True)
            .time_range(start, bios.time.seconds(5))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0].get("flatType"), "1BHK")
        self.assertEqual(records[0].get("sr"), 67)
        self.assertEqual(records[1].get("flatType"), "4BHK")
        self.assertEqual(records[1].get("sr"), 90)
        self.assertEqual(records[2].get("flatType"), "5BHK")
        self.assertEqual(records[2].get("sr"), 34)

    def test_group_by_enum(self):
        records = [
            "10,2BHK,microwave",
            "767,6BHK,microwave",
            "767,6BHK,cooker",
            "909,7BHK,stove",
            "10,2BHK,fridge",
            "88,5BHK,cooker",
        ]
        start = self.insert_into_signal("EnumTestSignal1", records)

        request = (
            bios.isql()
            .select(Metric("count()").set_as("numAmenities"))
            .from_signal("EnumTestSignal1")
            .group_by("kitchen")
            .order_by("kitchen")
            .time_range(start, bios.time.seconds(5))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0].get("kitchen"), "fridge")
        self.assertEqual(records[0].get("numAmenities"), 1)
        self.assertEqual(records[1].get("kitchen"), "stove")
        self.assertEqual(records[1].get("numAmenities"), 1)
        self.assertEqual(records[2].get("kitchen"), "cooker")
        self.assertEqual(records[2].get("numAmenities"), 2)
        self.assertEqual(records[3].get("kitchen"), "microwave")
        self.assertEqual(records[3].get("numAmenities"), 2)

    def test_join_with_mlp_set_to_strict(self):
        self.upsert_into_context("EnumTestContext1", ["40,abc,2BHK"])
        self.upsert_into_context("EnumTestContext2", ["100,Balcony"])
        start = self.insert_into_signal("EnumTestEnumStrict", ["2BHK,Balcony"])
        select_statement = (
            bios.isql()
            .select()
            .from_signal("EnumTestEnumStrict")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_statement)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("enumAttr1"), "2BHK")
        self.assertEqual(record.get("final1"), 40)
        self.assertEqual(record.get("enumAttr2"), "Balcony")
        self.assertEqual(record.get("final2"), 100)

    def test_join_with_missing_context_enum_entries_when_mlp_set_to_strict(self):
        with self.assertRaises(bios.ServiceError) as error_context:
            self.insert_into_signal("EnumTestEnumStrict", ["4BHK,Rooms"])
        self.assertEqual(error_context.exception.error_code, bios.errors.ErrorCode.BAD_INPUT)

    def test_join_when_mlp_set_to_use_default(self):
        self.upsert_into_context("EnumTestContext1", ["40,uij,2BHK"])

        start = self.insert_into_signal("EnumTestEnumDefault", ["2BHK"])
        select_statement = (
            bios.isql()
            .select()
            .from_signal("EnumTestEnumDefault")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_statement)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("enumAttr1"), "2BHK")
        self.assertEqual(record.get("final1"), 40)
        self.assertEqual(record.get("final3"), "uij")

    def test_join_when_mlp_set_to_use_default2(self):
        self.upsert_into_context("EnumTestContext1", ["440,uij,9BHK"])
        start = self.insert_into_signal("EnumTestEnumDefault", ["4BHK"])
        select_statement = (
            bios.isql()
            .select()
            .from_signal("EnumTestEnumDefault")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_statement)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("enumAttr1"), "4BHK")
        self.assertEqual(record.get("final1"), 123)
        self.assertEqual(record.get("final3"), "abc")


if __name__ == "__main__":
    pytest.main(sys.argv)
