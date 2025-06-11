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

import copy
import logging
import os
import pprint
import sys
import unittest
from copy import deepcopy

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user
from tutils import b64enc

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import (
    setup_context,
    setup_signal,
    setup_tenant_config,
    try_delete_context,
    try_delete_signal,
)

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

CONTEXT = {
    "contextName": "AdminEnrichTestContext",
    "missingAttributePolicy": "Reject",
    "primaryKey": ["ctx1Attribute"],
    "attributes": [
        {"attributeName": "ctx1Attribute", "type": "String"},
        {"attributeName": "pp1Attribute", "type": "Integer"},
        {"attributeName": "pp2Attribute", "type": "Decimal"},
    ],
}
SIGNAL = {
    "signalName": "AdminEnrichTestSignal",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "stringAttribute", "type": "String"}],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "ppl",
                "foreignKey": ["stringAttribute"],
                "missingLookupPolicy": "StoreFillInValue",
                "contextName": "AdminEnrichTestContext",
                "contextAttributes": [{"attributeName": "pp1Attribute", "fillIn": -1}],
            }
        ]
    },
}

CONTEXT_MD = {
    "contextName": "adminEnrichTestContextMd",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "primaryOne", "type": "String", "allowedValues": ["AAA", "BBB", "CCC"]},
        {"attributeName": "primaryTwo", "type": "String"},
        {"attributeName": "primaryThree", "type": "Integer"},
        {"attributeName": "primaryFour", "type": "Decimal"},
        {"attributeName": "primaryFive", "type": "Boolean"},
        {"attributeName": "primarySix", "type": "Blob"},
        {"attributeName": "pp1Attribute", "type": "Integer"},
        {"attributeName": "pp2Attribute", "type": "Decimal"},
    ],
    "primaryKey": [
        "primaryOne",
        "primaryTwo",
        "primaryThree",
        "primaryFour",
        "primaryFive",
        "primarySix",
    ],
}
SIGNAL_MD = {
    "signalName": "adminEnrichTestSignalMd",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "foreignOne", "type": "String", "allowedValues": ["AAA", "BBB", "CCC"]},
        {"attributeName": "foreignTwo", "type": "String"},
        {"attributeName": "foreignThree", "type": "Integer"},
        {"attributeName": "foreignFour", "type": "Decimal"},
        {"attributeName": "foreignFive", "type": "Boolean"},
        {"attributeName": "foreignSix", "type": "Blob"},
        {"attributeName": "extra", "type": "String"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "byAllTypes",
                "foreignKey": [
                    "foreignOne",
                    "foreignTwo",
                    "foreignThree",
                    "foreignFour",
                    "foreignFive",
                    "foreignSix",
                ],
                "missingLookupPolicy": "StoreFillInValue",
                "contextName": CONTEXT_MD["contextName"],
                "contextAttributes": [{"attributeName": "pp1Attribute", "fillIn": -1}],
            }
        ]
    },
}

# blob values
BLOB1 = bytearray.fromhex("ba53ba11f007ba11")
BLOB2 = bytearray.fromhex("31a571cf1a54")
BLOB3 = b"hello world"
BLOB4 = bytearray.fromhex("aeb7b476c82011e8a8d5f2801f1b9fd1")


class AdminEnrichTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        try_delete_signal(cls.session, SIGNAL["signalName"])
        setup_context(cls.session, CONTEXT)
        setup_signal(cls.session, SIGNAL)
        try_delete_signal(cls.session, SIGNAL_MD["signalName"])
        setup_context(cls.session, CONTEXT_MD)

        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        cls.created_signals = []

    @classmethod
    def tearDownClass(cls):
        try_delete_signal(cls.session, SIGNAL["signalName"])
        try_delete_context(cls.session, CONTEXT["contextName"])
        cls.session.close()
        cls.sadmin.close()

    def tearDown(self):
        for signal_name in self.created_signals:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                # ok
                pass
        self.created_signals.clear()

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

    def test_select_after_updating_signal(self):
        """
        This test tests various combinations of adding and removing attributes to signal as well as
        its enriched attributes.
        """
        context = {
            "contextName": "AdminEnrichTestContext1",
            "missingAttributePolicy": "Reject",
            "primaryKey": ["ctx1Attribute"],
            "attributes": [
                {"attributeName": "ctx1Attribute", "type": "String"},
                {"attributeName": "pp1Attribute", "type": "Integer"},
                {"attributeName": "pp2Attribute", "type": "Decimal"},
            ],
        }
        signal = {
            "signalName": "AdminEnrichTestSignal1",
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "stringAttribute", "type": "String"}],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "ppl",
                        "foreignKey": ["stringAttribute"],
                        "missingLookupPolicy": "Reject",
                        "contextName": "AdminEnrichTestContext1",
                        "contextAttributes": [{"attributeName": "pp1Attribute"}],
                    }
                ],
            },
        }

        self.session.create_context(context)
        self.addCleanup(self.session.delete_context, context["contextName"])
        self.session.create_signal(signal)
        self.addCleanup(self.session.delete_signal, signal["signalName"])

        self.upsert_into_context(
            context["contextName"],
            [
                "dummy,123,788888888888888888",
                "hnm,786,99999999999999999",
                "xcv,345,66666666666666666666666666666",
                "xcer,8,94555555555555555555555555555",
            ],
        )

        start = self.insert_into_signal(signal["signalName"], "dummy")

        signal2 = deepcopy(signal)
        signal2["attributes"].append(
            {"attributeName": "AddedAttr", "type": "Decimal", "default": 67.45}
        )
        signal2["enrich"]["enrichments"][0]["contextAttributes"].append(
            {"attributeName": "pp2Attribute", "fillIn": -1}
        )
        self.session.update_signal(signal["signalName"], signal2)
        self.insert_into_signal(signal["signalName"], "hnm,12.3")

        select_request = (
            bios.isql()
            .select()
            .from_signal(signal["signalName"])
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].get("stringAttribute"), "dummy")
        self.assertEqual(records[0].get("pp1Attribute"), 123)
        self.assertEqual(records[0].get("AddedAttr"), 67.45)
        self.assertEqual(records[0].get("pp2Attribute"), pytest.approx(-1.0))

        self.assertEqual(records[1].get("stringAttribute"), "hnm")
        self.assertEqual(records[1].get("pp1Attribute"), 786)
        self.assertEqual(records[1].get("AddedAttr"), 12.3)
        self.assertEqual(records[1].get("pp2Attribute"), pytest.approx(99999999999999999))

        signal3 = deepcopy(signal)
        signal3["attributes"].append(
            {"attributeName": "AddedAttr", "type": "Decimal", "default": 67}
        )
        self.session.update_signal(signal["signalName"], signal3)
        self.insert_into_signal(signal["signalName"], "xcer,100")

        self.session.update_signal(signal["signalName"], signal)
        self.insert_into_signal(signal["signalName"], "xcv")

        select_request = (
            bios.isql()
            .select()
            .from_signal(signal["signalName"])
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0].get("stringAttribute"), "dummy")
        self.assertEqual(records[0].get("pp1Attribute"), 123)
        self.assertIsNone(records[0].get("AddedAttr"))
        self.assertIsNone(records[0].get("pp2Attribute"))

        self.assertEqual(records[1].get("stringAttribute"), "hnm")
        self.assertEqual(records[1].get("pp1Attribute"), 786)
        self.assertIsNone(records[1].get("AddedAttr"))
        self.assertIsNone(records[1].get("pp2Attribute"))

        self.assertEqual(records[2].get("stringAttribute"), "xcer")
        self.assertEqual(records[2].get("pp1Attribute"), 8)
        self.assertIsNone(records[2].get("AddedAttr"))
        self.assertIsNone(records[2].get("pp2Attribute"))

        self.assertEqual(records[3].get("stringAttribute"), "xcv")
        self.assertEqual(records[3].get("pp1Attribute"), 345)
        self.assertIsNone(records[3].get("AddedAttr"))
        self.assertIsNone(records[3].get("pp2Attribute"))

    def test_enrich_after_deleting_context_entries(self):
        self.upsert_into_context(
            CONTEXT["contextName"],
            [
                "dummy,878,788888888888888888",
                "hnm,786,99999999999999999",
                "xcv,345,66666666666666666666666666666",
                "xcer,8,94555555555555555555555555555",
            ],
        )

        start = self.insert_into_signal(SIGNAL["signalName"], "dummy")

        delete_request = (
            bios.isql()
            .delete()
            .from_context(CONTEXT["contextName"])
            .where(keys=[["dummy"]])
            .build()
        )
        self.session.execute(delete_request)

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
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].get("stringAttribute"), "dummy")
        self.assertEqual(records[0].get("pp1Attribute"), 878)

    def test_enrich_deleted_values(self):
        self.upsert_into_context(CONTEXT["contextName"], ["invalid_val,1000,788888888888888888"])
        delete_request = (
            bios.isql()
            .delete()
            .from_context(CONTEXT["contextName"])
            .where(keys=[["invalid_val"]])
            .build()
        )
        self.session.execute(delete_request)

        start = self.insert_into_signal(SIGNAL["signalName"], "invalid_val")
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
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].get("stringAttribute"), "invalid_val")
        self.assertEqual(records[0].get("pp1Attribute"), -1)

    def test_enrich_duplicate_entries(self):
        self.upsert_into_context(CONTEXT["contextName"], ["dup,1001,-1.99"])
        start = self.insert_into_signal(SIGNAL["signalName"], "dup")

        self.upsert_into_context(CONTEXT["contextName"], ["dup,1002,1.99"])
        self.insert_into_signal(SIGNAL["signalName"], "dup")
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
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].get("stringAttribute"), "dup")
        self.assertEqual(records[0].get("pp1Attribute"), 1001)
        self.assertEqual(records[1].get("stringAttribute"), "dup")
        self.assertEqual(records[1].get("pp1Attribute"), 1002)

    def test_md_fundamental(self):
        """Test basic enrichment behavior using a composite foreign key"""
        signal = copy.deepcopy(SIGNAL_MD)
        signal_name = signal.get("signalName")
        setup_signal(self.session, signal)
        self.created_signals.append(signal_name)
        self.verify_md_enrich(signal_name, signal)

    def test_md_reverse_attributes(self):
        """Enrichment is doable regardless the signal attribute order as long as the
        foreign key elements are listed in correct order."""
        signal_name = SIGNAL_MD["signalName"] + "Reverse"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        signal["attributes"].reverse()
        pprint.pprint(signal)
        setup_signal(self.session, signal)
        self.created_signals.append(signal_name)
        self.verify_md_enrich(signal_name, signal)

    def test_md_retroactive_enrichment(self):
        """Test retroactive enrichment using a composite foreign key"""
        # create initial and second signal configs
        signal_name = SIGNAL_MD["signalName"] + "Retroactive"
        signal_initial = copy.deepcopy(SIGNAL_MD)
        signal_initial["signalName"] = signal_name

        signal_second = copy.deepcopy(signal_initial)
        del signal_initial["enrich"]

        setup_signal(self.session, signal_initial)
        self.created_signals.append(signal_name)

        self.session.update_signal(signal_name, signal_second)

        self.verify_md_enrich(signal_name, signal_second)

    def test_md_different_foreign_key_cases(self):
        """Foreign key element that has different cases from the attribute name should be fixed
        on creation"""
        signal_name = SIGNAL_MD["signalName"] + "FKey"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        signal["enrich"]["enrichments"][0]["foreignKey"] = [
            element.upper() for element in foreign_key
        ]
        pprint.pprint(signal)
        setup_signal(self.session, signal)
        self.created_signals.append(signal_name)
        created = self.session.get_signal(signal_name)
        pprint.pprint(signal)
        assert (
            created["enrich"]["enrichments"][0]["foreignKey"]
            == SIGNAL_MD["enrich"]["enrichments"][0]["foreignKey"]
        )

        self.verify_md_enrich(signal_name, created)

    def test_md_different_foreign_key_cases_update(self):
        """Foreign key element that has different cases from the attribute name should be fixed
        on update"""
        signal_name = SIGNAL_MD["signalName"] + "FKey"
        signal_second = copy.deepcopy(SIGNAL_MD)
        signal_second["signalName"] = signal_name
        foreign_key = signal_second["enrich"]["enrichments"][0]["foreignKey"]
        signal_second["enrich"]["enrichments"][0]["foreignKey"] = [
            element.upper() for element in foreign_key
        ]

        signal_first = copy.deepcopy(signal_second)
        del signal_first["enrich"]

        setup_signal(self.session, signal_first)
        self.created_signals.append(signal_name)
        self.session.update_signal(signal_name, signal_second)
        updated = self.session.get_signal(signal_name)
        pprint.pprint(updated)
        assert (
            updated["enrich"]["enrichments"][0]["foreignKey"]
            == SIGNAL_MD["enrich"]["enrichments"][0]["foreignKey"]
        )

        self.verify_md_enrich(signal_name, updated)

    def test_md_different_remote_context_cases(self):
        """Foreign key element that has different cases from the attribute name should be fixed
        on creation"""
        signal_name = SIGNAL_MD["signalName"] + "RCtxCase"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        remote_context = signal["enrich"]["enrichments"][0]["contextName"]
        signal["enrich"]["enrichments"][0]["contextName"] = remote_context.upper()
        pprint.pprint(signal)
        setup_signal(self.session, signal)
        self.created_signals.append(signal_name)
        created = self.session.get_signal(signal_name)
        pprint.pprint(created)
        assert (
            created["enrich"]["enrichments"][0]["contextName"]
            == SIGNAL_MD["enrich"]["enrichments"][0]["contextName"]
        )

        self.verify_md_enrich(signal_name, created)

    def test_md_different_context_attribute_cases(self):
        """The cases should be fixed to be the same with the one in the remote context"""
        signal_name = SIGNAL_MD["signalName"] + "CAttr"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        attr = signal["enrich"]["enrichments"][0]["contextAttributes"][0]
        attr["attributeName"] = attr["attributeName"].upper()
        pprint.pprint(signal)
        setup_signal(self.session, signal)
        self.created_signals.append(signal_name)
        created = self.session.get_signal(signal_name)
        pprint.pprint(created)
        assert created["enrich"] == SIGNAL_MD["enrich"]

        self.verify_md_enrich(signal_name, created)

    def verify_md_enrich(self, signal_name, signal_config):
        """Common method to verify configured enrichment behavior"""
        primary_keys = [
            ["BBB", "hello", 123, 4.56, True, BLOB1],
            ["BBB", "hello", 123, 4.56, True, BLOB2],
            ["CCC", "world", 7910, 5.18, False, BLOB3],
            ["AAA", "hi", 99283, 3.14, False, BLOB4],
        ]
        primary_text_keys = [
            [key[0], key[1], str(key[2]), str(key[3]), str(key[4]), b64enc(key[5])]
            for key in primary_keys
        ]

        ctx_texts = []
        for index, key in enumerate(primary_text_keys):
            values = key.copy()
            values.extend([str((index + 1) * 100), str((index + 1) * 100 + 0.1)])
            ctx_texts.append(",".join(values))
        upsert_statement = (
            bios.isql().upsert().into(CONTEXT_MD.get("contextName")).csv_bulk(ctx_texts).build()
        )
        self.session.execute(upsert_statement)

        # make insert statement
        attribute_values = []
        for key in primary_text_keys:
            entry = {"extra": "xxxxx"}
            for index, key_element in enumerate(
                signal_config["enrich"]["enrichments"][0]["foreignKey"]
            ):
                entry[key_element] = key[index]
            attribute_values.append(entry)
        signal_texts = []
        for entry in attribute_values:
            values = []
            for attribute in signal_config["attributes"]:
                values.append(entry.get(attribute["attributeName"]))
            signal_texts.append(",".join(values))
        print(signal_texts)
        insert_statement = bios.isql().insert().into(signal_name).csv_bulk(signal_texts).build()
        start_time = bios.time.now()
        self.session.execute(insert_statement)
        end_time = bios.time.now()

        # verify insertion
        select_statement = (
            bios.isql()
            .select()
            .from_signal(signal_name)
            .time_range(start_time, end_time - start_time)
            .build()
        )
        response = self.session.execute(select_statement)
        records = response.get_data_windows()[0].get_records()
        assert len(records) == 4
        for index, record in enumerate(records):
            expected_value = (index + 1) * 100
            assert record.get("pp1Attribute") == expected_value

    def test_md_foreign_key_missing(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "NoFKey"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        del signal["enrich"]["enrichments"][0]["foreignKey"]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_foreign_key_empty(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "FKeyEmpty"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["foreignKey"] = []
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_foreign_key_shorter_than_necessary(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "FKeyShort"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        signal["enrich"]["enrichments"][0]["foreignKey"] = foreign_key[:-1]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_foreign_key_longer_than_necessary(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "FKeyLong"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        foreign_key.append("extra")
        signal["enrich"]["enrichments"][0]["foreignKey"] = foreign_key
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_foreign_key_type_mismatch(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "Type"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        foreign_key[5] = "extra"
        signal["enrich"]["enrichments"][0]["foreignKey"] = foreign_key
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_include_none_in_foreign_key(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "FKeyHasNone"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        foreign_key[3] = None
        signal["enrich"]["enrichments"][0]["foreignKey"] = foreign_key
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_include_empty_in_foreign_key(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "FKeyHasEmpty"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        foreign_key[4] = ""
        signal["enrich"]["enrichments"][0]["foreignKey"] = foreign_key
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_foreign_key_not_found_in_signal(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "DanglingFKey"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        foreign_key = signal["enrich"]["enrichments"][0]["foreignKey"]
        foreign_key[2] = "wrong"
        signal["enrich"]["enrichments"][0]["foreignKey"] = foreign_key
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_enrichment_name_missing(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "NoEName"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        del signal["enrich"]["enrichments"][0]["enrichmentName"]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_remote_context_missing(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "CtxMissing"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        del signal["enrich"]["enrichments"][0]["contextName"]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_remote_context_not_found(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "CtxNotFound"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["contextName"] = "mercury"
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_no_missing_lookup_policy(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "MlpMissing"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        del signal["enrich"]["enrichments"][0]["missingLookupPolicy"]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_wrong_missing_lookup_policy(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "BadMlp"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["missingLookupPolicy"] = "InvalidValue"
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_no_enrichment_attributes(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "NoAttr"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        del signal["enrich"]["enrichments"][0]["contextAttributes"]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_empty_enrichment_attributes(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "EmptyAttr"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["contextAttributes"] = []
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_enrichment_attribute_name_missing(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "EmptyAttr"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        del signal["enrich"]["enrichments"][0]["contextAttributes"][0]["attributeName"]
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_md_enriched_attribute_name_conflict(self):
        """Negative"""
        signal_name = SIGNAL_MD["signalName"] + "EmptyAttr"
        signal = copy.deepcopy(SIGNAL_MD)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["contextAttributes"][0]["as"] = "extra"
        pprint.pprint(signal)
        with pytest.raises(ServiceError) as exc_info:
            self.session.create_signal(signal)
            self.created_signals.append(signal_name)
        print(exc_info.value.message)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT


if __name__ == "__main__":
    pytest.main(sys.argv)
