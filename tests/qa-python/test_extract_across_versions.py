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
import pprint
import sys
import unittest

import pytest

import bios
from bios import ServiceError, ErrorCode

from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME,
    setup_signal,
    setup_tenant_config,
)
from tsetup import get_endpoint_url as ep_url
from tsetup import (
    sadmin_user,
    sadmin_pass,
    admin_user,
    admin_pass,
    ingest_user,
    ingest_pass,
    extract_user,
    extract_pass,
)


class TestExtractAcrossVersion(unittest.TestCase):
    # constants
    TENANT = "biosPythonQA"
    ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.created_signals = []
        cls.admin_session = bios.login(ep_url(), cls.ADMIN_USER, admin_pass)

        # Setup signal with added attribute
        cls.signal_name_add_attribute = "signalWithAddedAttribute"
        cls.created_signals.append(cls.signal_name_add_attribute)
        signal = {
            "signalName": cls.signal_name_add_attribute,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        try:
            cls.admin_session.delete_signal(cls.signal_name_add_attribute)
        except ServiceError:
            pass
        cls.admin_session.create_signal(signal)

        statement = (
            bios.isql()
            .insert()
            .into(cls.signal_name_add_attribute)
            .csv_bulk(
                [
                    "morale,902",
                    "hardship,89",
                    "medal,890182",
                ]
            )
            .build()
        )
        resp = cls.admin_session.execute(statement)
        updated = {
            "signalName": cls.signal_name_add_attribute,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
                {"attributeName": "new", "type": "Integer", "default": 7543},
            ],
        }
        cls.admin_session.update_signal(cls.signal_name_add_attribute, updated)
        statement2 = (
            bios.isql()
            .insert()
            .into(cls.signal_name_add_attribute)
            .csv_bulk(
                [
                    "bnc,890182,89",
                    "blow,902,7543",
                    "motionless,904,7600",
                ]
            )
            .build()
        )
        cls.admin_session.execute(statement2)
        cls.start_add_attribute = resp.records[0].timestamp

        # Setup signal with modified attribute
        cls.signal_name_modify_attribute = "signalWithModifiedAttribute"
        cls.created_signals.append(cls.signal_name_modify_attribute)
        signal = {
            "signalName": cls.signal_name_modify_attribute,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
                {
                    "attributeName": "category",
                    "type": "String",
                    "allowedValues": ["ONE", "TWO", "THREE"],
                },
            ],
        }
        try:
            cls.admin_session.delete_signal(cls.signal_name_modify_attribute)
        except ServiceError:
            pass
        cls.admin_session.create_signal(signal)

        statement = (
            bios.isql()
            .insert()
            .into(cls.signal_name_modify_attribute)
            .csv_bulk(
                [
                    "layout,902,TWO",
                    "contrast,89,ONE",
                    "garlic,890182,ONE",
                ]
            )
            .build()
        )
        resp = cls.admin_session.execute(statement)
        updated = {
            "signalName": cls.signal_name_modify_attribute,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "String"},
                {"attributeName": "category", "type": "String"},
            ],
        }
        cls.admin_session.update_signal(cls.signal_name_modify_attribute, updated)
        statement2 = (
            bios.isql()
            .insert()
            .into(cls.signal_name_modify_attribute)
            .csv_bulk(
                [
                    "motorcycle,890183,TWO",
                    "show,902,FOUR",
                    "shout,902x,THREE",
                ]
            )
            .build()
        )
        cls.admin_session.execute(statement2)
        cls.start_modify_attribute = resp.records[0].timestamp

    @classmethod
    def tearDownClass(cls):
        for signal in cls.created_signals:
            try:
                cls.admin_session.delete_signal(signal)
            except ServiceError:
                pass
        cls.admin_session.close()

    def test_extract_across_versions(self):
        """TC-ID : dp_ext_ver_05, dp_ext_ver_06
        This test is to verify events ingested before and after stream updation
        should be extracted successfully
        """
        signal_name = "dp_ext_ver_05"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "id", "type": "Integer"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.create_signal(signal)
        insert_statement = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(["123,78", "564444,100", "9065,2000", "785,13000", "903,90222"])
            .build()
        )
        insert_response = self.admin_session.execute(insert_statement)

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "id", "type": "Decimal"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)
        insert_statement2 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(["12345.6,4568", "3324.8,23", "15,100", "-139.22,90222", "555.55,12367"])
            .build()
        )
        self.admin_session.execute(insert_statement2)

        updated2 = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "id", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.update_signal(signal_name, updated2)
        statement3 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "2017-10-14T12:44:28.574Z,109",
                    "2017-06-14T12:44:28.574Z,801",
                    "2017-10-14T12:44:28.574Z,892",
                ]
            )
            .build()
        )
        self.admin_session.execute(statement3)

        start = insert_response.records[0].timestamp
        extract_statement = (
            bios.isql()
            .select()
            .from_signal(signal_name)
            .time_range(start, bios.time.now() - start)
            .build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        # pprint.pprint(records)
        try:
            self.assertEqual(len(records), 13)
            self.assertEqual(records[0].get("id"), "123")
            self.assertEqual(records[1].get("id"), "564444")
            self.assertEqual(records[2].get("id"), "9065")
            self.assertEqual(records[3].get("id"), "785")
            self.assertEqual(records[4].get("id"), "903")
            self.assertEqual(records[5].get("id"), "12345.6")
            self.assertEqual(records[6].get("id"), "3324.8")
            self.assertEqual(records[7].get("id"), "15.0")
            self.assertEqual(records[8].get("id"), "-139.22")
            self.assertEqual(records[9].get("id"), "555.55")
            self.assertEqual(records[10].get("id"), "2017-10-14T12:44:28.574Z")
            self.assertEqual(records[11].get("id"), "2017-06-14T12:44:28.574Z")
            self.assertEqual(records[12].get("id"), "2017-10-14T12:44:28.574Z")
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    def test_extract_across_versions_on_adding_attribute(self):
        """TC-ID : dp_ext_ver_04a
        This test is to verify events ingested before and after adding attribute
        should be extracted successfully
        """
        signal_name = "dp_ext_ver_04a"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        try:
            self.admin_session.delete_signal(signal_name)
        except ServiceError:
            pass
        self.admin_session.create_signal(signal)
        insert_statement1 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "ask,333333378",
                    "Animalsdd,444444444100",
                    "cccAmphibians,777777772000",
                    "irds,55513788888000",
                    "qua,7777902226666666",
                ]
            )
            .build()
        )
        resp = self.admin_session.execute(insert_statement1)

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
                {"attributeName": "new", "type": "Decimal", "default": 67.5},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)

        insert_statement2 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "tyrrrr,3333390000002,10.4",
                    "klaa,92222233333333,90.45",
                ]
            )
            .build()
        )
        self.admin_session.execute(insert_statement2)

        start = resp.records[0].timestamp
        delta = bios.time.now() - start
        extract_statement = (
            bios.isql().select().from_signal(signal_name).time_range(start, delta).build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        try:
            self.assertEqual(len(records), 7)
            self.assertEqual(records[0].get("stringAttribute"), "ask")
            self.assertEqual(records[0].get("value"), 333333378)
            self.assertAlmostEqual(records[0].get("new"), 67.5)
            self.assertEqual(records[1].get("stringAttribute"), "Animalsdd")
            self.assertEqual(records[1].get("value"), 444444444100)
            self.assertAlmostEqual(records[1].get("new"), 67.5)
            self.assertEqual(records[2].get("stringAttribute"), "cccAmphibians")
            self.assertEqual(records[2].get("value"), 777777772000)
            self.assertAlmostEqual(records[2].get("new"), 67.5)
            self.assertEqual(records[3].get("stringAttribute"), "irds")
            self.assertEqual(records[3].get("value"), 55513788888000)
            self.assertAlmostEqual(records[3].get("new"), 67.5)
            self.assertEqual(records[4].get("stringAttribute"), "qua")
            self.assertEqual(records[4].get("value"), 7777902226666666)
            self.assertAlmostEqual(records[4].get("new"), 67.5)
            self.assertEqual(records[5].get("stringAttribute"), "tyrrrr")
            self.assertEqual(records[5].get("value"), 3333390000002)
            self.assertAlmostEqual(records[5].get("new"), 10.4)
            self.assertEqual(records[6].get("stringAttribute"), "klaa")
            self.assertEqual(records[6].get("value"), 92222233333333)
            self.assertEqual(records[6].get("new"), 90.45)
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    def test_extract_across_versions_on_removing_attribute(self):
        """TC-ID : dp_ext_ver_04b
        This test is to verify events ingested before and after removing attribute
        should be extracted successfully
        """
        signal_name = "dp_ext_ver_04b"
        self.created_signals.append(signal_name)
        stream = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        try:
            self.admin_session.delete_signal(signal_name)
        except ServiceError:
            pass
        self.admin_session.create_signal(stream)

        insert_statement1 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "appoint,3333333333378",
                    "university,4444444444444100",
                    "salad,77777777772000",
                    "hearing,55555513788888000",
                    "bedroom,77777902226666666",
                ]
            )
            .build()
        )
        resp = self.admin_session.execute(insert_statement1)

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "string"},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)
        insert_statement2 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "estate",
                    "chemistry",
                    "audience",
                ]
            )
            .build()
        )
        self.admin_session.execute(insert_statement2)

        start = resp.records[0].timestamp
        delta = bios.time.now() - start
        extract_statement = (
            bios.isql().select().from_signal(signal_name).time_range(start, delta).build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        try:
            self.assertEqual(len(records), 8)
            self.assertEqual(records[0].get("stringAttribute"), "appoint")
            self.assertIsNone(records[0].get("value"))
            self.assertEqual(records[1].get("stringAttribute"), "university")
            self.assertIsNone(records[1].get("value"))
            self.assertEqual(records[2].get("stringAttribute"), "salad")
            self.assertIsNone(records[2].get("value"))
            self.assertEqual(records[3].get("stringAttribute"), "hearing")
            self.assertIsNone(records[3].get("value"))
            self.assertEqual(records[4].get("stringAttribute"), "bedroom")
            self.assertIsNone(records[4].get("value"))
            self.assertEqual(records[5].get("stringAttribute"), "estate")
            self.assertEqual(records[7].get("stringAttribute"), "audience")
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    def test_extract_on_extracting_filter_with_unchanged_attribute(self):
        """TC-ID : dp_ext_ver_13
        This test is to verify events extracted successfully when specify filer
        condition for unchanged attribute
        """
        signal_name = "dp_ext_ver_13"
        self.created_signals.append(signal_name)
        stream = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        try:
            self.admin_session.delete_signal(signal_name)
        except ServiceError:
            pass
        self.admin_session.create_signal(stream)
        insert_statement1 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "guest,3333333378",
                    "meat,444444444100",
                    "trainer,7777777772000",
                    "introduction,55513788888000",
                    "skill,777902226666666",
                ]
            )
            .build()
        )
        resp = self.admin_session.execute(insert_statement1)
        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "string"},
                {"attributeName": "value", "type": "Integer"},
                {"attributeName": "new", "type": "Integer", "default": 902},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)

        insert_statement2 = (
            bios.isql().insert().into(signal_name).csv("nmvbff,9044444446666,675").build()
        )
        self.admin_session.execute(insert_statement2)
        start = resp.records[0].timestamp
        delta = bios.time.now() - start
        extract_statement = (
            bios.isql()
            .select()
            .from_signal(signal_name)
            .where("value = 55513788888000")
            .time_range(start, delta)
            .build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        try:
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["value"], 55513788888000)
            self.assertEqual(records[0]["stringAttribute"], "introduction")
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    def test_extract_on_extracting_filter_with_deleted_attribute(self):
        """TC-ID : dp_ext_ver_14
        This test is to verify events extraction should fail when filter
        condition has deleted attribute value
        """
        signal_name = "dp_ext_ver_14"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "string"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.create_signal(signal)

        insert_statement1 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "topic,33333333378",
                    "meat,3444444444444100",
                    "hearing,7777777772000",
                ]
            )
            .build()
        )
        resp = self.admin_session.execute(insert_statement1)
        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "stringAttribute", "type": "string"}],
        }
        self.admin_session.update_signal(signal_name, updated)

        insert_statement2 = bios.isql().insert().into(signal_name).csv("bnc").build()
        self.admin_session.execute(insert_statement2)

        caught = None
        try:
            start = resp.records[0].timestamp
            delta = bios.time.now() - start
            extract_statement = (
                bios.isql()
                .select()
                .from_signal(signal_name)
                .where("value = 33333333378")
                .time_range(start, delta)
                .build()
            )
            self.admin_session.execute(extract_statement)
            self.fail("Exception is expected")
        except ServiceError as err:
            caught = err
        self.assertRegex(
            caught.message, ".*Invalid filter: Attribute 'value' is not in stream attributes.*"
        )
        self.assertEqual(caught.error_code, ErrorCode.BAD_INPUT)

    def test_filter_added_attribute_eq_match_new(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new = 89")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "bnc")

    def test_filter_added_attribute_lt_match_new(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new < 7543")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "bnc")

    def test_filter_added_attribute_lt_match_both(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new < 7544")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0].get("stringAttribute"), "morale")
        self.assertEqual(result[1].get("stringAttribute"), "hardship")
        self.assertEqual(result[2].get("stringAttribute"), "medal")
        self.assertEqual(result[3].get("stringAttribute"), "bnc")
        self.assertEqual(result[4].get("stringAttribute"), "blow")

    def test_filter_added_attribute_le_match_new(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new <= 7542")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "bnc")

    def test_filter_added_attribute_le_match_both1(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new <= 7543")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0].get("stringAttribute"), "morale")
        self.assertEqual(result[1].get("stringAttribute"), "hardship")
        self.assertEqual(result[2].get("stringAttribute"), "medal")
        self.assertEqual(result[3].get("stringAttribute"), "bnc")
        self.assertEqual(result[4].get("stringAttribute"), "blow")

    def test_filter_added_attribute_le_match_both2(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new <= 7544")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0].get("stringAttribute"), "morale")
        self.assertEqual(result[1].get("stringAttribute"), "hardship")
        self.assertEqual(result[2].get("stringAttribute"), "medal")
        self.assertEqual(result[3].get("stringAttribute"), "bnc")
        self.assertEqual(result[4].get("stringAttribute"), "blow")

    def test_filter_added_attribute_gt_match_new(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new > 7543")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "motionless")

    def test_filter_added_attribute_gt_match_both(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new > 7542")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0].get("stringAttribute"), "morale")
        self.assertEqual(result[1].get("stringAttribute"), "hardship")
        self.assertEqual(result[2].get("stringAttribute"), "medal")
        self.assertEqual(result[3].get("stringAttribute"), "blow")
        self.assertEqual(result[4].get("stringAttribute"), "motionless")

    def test_filter_added_attribute_ge_match_new(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new >= 7544")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "motionless")

    def test_filter_added_attribute_ge_match_both1(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new >= 7543")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0].get("stringAttribute"), "morale")
        self.assertEqual(result[1].get("stringAttribute"), "hardship")
        self.assertEqual(result[2].get("stringAttribute"), "medal")
        self.assertEqual(result[3].get("stringAttribute"), "blow")
        self.assertEqual(result[4].get("stringAttribute"), "motionless")

    def test_filter_added_attribute_ge_match_both2(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new >= 7542")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0].get("stringAttribute"), "morale")
        self.assertEqual(result[1].get("stringAttribute"), "hardship")
        self.assertEqual(result[2].get("stringAttribute"), "medal")
        self.assertEqual(result[3].get("stringAttribute"), "blow")
        self.assertEqual(result[4].get("stringAttribute"), "motionless")

    def test_filter_added_attribute_eq_match_default(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement2 = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new = 7543")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result2 = self.admin_session.execute(select_statement2).to_dict()
        # pprint.pprint(result2)
        self.assertEqual(result2[0].get("stringAttribute"), "morale")
        self.assertEqual(result2[1].get("stringAttribute"), "hardship")
        self.assertEqual(result2[2].get("stringAttribute"), "medal")
        self.assertEqual(result2[3].get("stringAttribute"), "blow")

    def test_filter_against_added_attribute_no_match(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement3 = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("new = 7544")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result3 = self.admin_session.execute(select_statement3).to_dict()
        self.assertEqual(len(result3), 0)

    def test_attribute_added_signal_with_filter_for_existing_attribute(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement4 = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("value = 890182")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result4 = self.admin_session.execute(select_statement4).to_dict()
        # pprint.pprint(result4)
        self.assertEqual(len(result4), 2)
        self.assertEqual(result4[0].get("stringAttribute"), "medal")
        self.assertEqual(result4[1].get("stringAttribute"), "bnc")

    def test_attribute_added_signal_complex_filter(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement5 = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("value = 890182 AND new = 89")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result5 = self.admin_session.execute(select_statement5).to_dict()
        # pprint.pprint(result5)
        self.assertEqual(len(result5), 1)
        self.assertEqual(result5[0].get("stringAttribute"), "bnc")

    def test_attribute_added_signal_complex_filter2(self):
        delta = bios.time.now() - self.start_add_attribute
        select_statement6 = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_add_attribute)
            .where("value = 902 AND new = 7543")
            .time_range(self.start_add_attribute, delta)
            .build()
        )
        result6 = self.admin_session.execute(select_statement6).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result6), 2)
        self.assertEqual(result6[0].get("stringAttribute"), "morale")
        self.assertEqual(result6[1].get("stringAttribute"), "blow")

    def test_attribute_modified_signal_with_filter_match_both(self):
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("value = '902'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].get("stringAttribute"), "layout")
        self.assertEqual(result[1].get("stringAttribute"), "show")

    def test_attribute_modified_signal_with_filter_match_old(self):
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("value = '89'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "contrast")

    def test_attribute_modified_signal_with_filter_match_old_enum(self):
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("category = 'ONE'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].get("stringAttribute"), "contrast")
        self.assertEqual(result[1].get("stringAttribute"), "garlic")

    def test_attribute_modified_signal_with_filter_match_new(self):
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("value = '890182'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "garlic")

    def test_attribute_modified_signal_with_filter_match_new_enum(self):
        """old: enum -> new: string, the filter value is in the old enum range"""
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("category = 'THREE'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "shout")

    def test_attribute_modified_signal_with_filter_match_new_enum2(self):
        """old: enum -> new: string, the filter value is out of the old enum range"""
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("category = 'FOUR'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "show")

    def test_attribute_modified_signal_with_filter_match_new_string(self):
        delta = bios.time.now() - self.start_modify_attribute
        select_statement = (
            bios.isql()
            .select()
            .from_signal(self.signal_name_modify_attribute)
            .where("value = '902x'")
            .time_range(self.start_modify_attribute, delta)
            .build()
        )
        result = self.admin_session.execute(select_statement).to_dict()
        # pprint.pprint(result6)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].get("stringAttribute"), "shout")

    def test_extract_across_versions_with_sort_on_adding_attribute(self):
        """TC-ID : dp_ext_ver_18
        This test is to verify events ingested before and after adding attribute
        should be extracted successfully and sorted as per sorting key
        """
        signal_name = "dp_ext_ver_18"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.create_signal(signal)

        insert_statement1 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "virtue,33333333333378",
                    "Animalsdd,3444444444444444100",
                    "cccAmphibians,7777777772000",
                    "irds,5555555513788888000",
                    "qua,77777902226666666",
                ]
            )
            .build()
        )
        resp = self.admin_session.execute(insert_statement1)

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
                {"attributeName": "new", "type": "Decimal", "default": 78.4},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)

        insert_statement2 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "tyrrrr,333333333300002,10.4",
                    "klaa,9022222222333333,90.45",
                ]
            )
            .build()
        )
        self.admin_session.execute(insert_statement2)

        start = resp.records[0].timestamp
        delta = bios.time.now() - start
        extract_statement = (
            bios.isql()
            .select()
            .from_signal(signal_name)
            .order_by("new", reverse=True)
            .time_range(start, delta)
            .build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        try:
            self.assertEqual(len(records), 7)
            self.assertEqual(records[0]["stringAttribute"], "klaa")
            self.assertEqual(records[0]["new"], 90.45)
            self.assertEqual(records[1]["stringAttribute"], "virtue")
            self.assertEqual(records[1]["new"], 78.4)
            self.assertEqual(records[2]["stringAttribute"], "Animalsdd")
            self.assertEqual(records[2]["new"], 78.4)
            self.assertEqual(records[3]["stringAttribute"], "cccAmphibians")
            self.assertEqual(records[3]["new"], 78.4)
            self.assertEqual(records[4]["stringAttribute"], "irds")
            self.assertEqual(records[4]["new"], 78.4)
            self.assertEqual(records[5]["stringAttribute"], "qua")
            self.assertEqual(records[5]["new"], 78.4)
            self.assertEqual(records[6]["stringAttribute"], "tyrrrr")
            self.assertEqual(records[6]["new"], 10.4)
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    # def test_extract_across_versions_with_distinct_on_adding_attribute(self):
    #     """TC-ID : dp_ext_ver_19
    #     This test is to verify events ingested before and after adding attribute
    #     should be distinctly extracted as perdistinct key
    #     """
    #     signal_name = "dp_ext_ver_19"
    #     self.created_signals.append(signal_name)
    #     signal = {
    #         "signalName": signal_name,
    #         "missingAttributePolicy": "Reject",
    #         "attributes": [
    #             {"attributeName": "stringAttribute", "type": "String"},
    #             {"attributeName": "value", "type": "Integer"},
    #         ],
    #     }
    #     self.admin_session.create_signal(signal)

    #     insert_statement1 = bios.isql().insert().into(signal_name).csv_bulk([
    #         "circumstance,33333333333378",
    #         "Animalsdd,344444100",
    #         "cccAmphibians,7777777772000",
    #         "irds,555555513788888000",
    #         "qua,777777772226666666",
    #         ]).build()
    #     resp = self.admin_session.execute(insert_statement1)

    #     updated = {
    #         "signalName": signal_name,
    #         "missingAttributePolicy": "Reject",
    #         "attributes": [
    #             {"attributeName": "stringAttribute", "type": "String"},
    #             {"attributeName": "value", "type": "Integer"},
    #             {"attributeName": "new", "type": "Integer", "default": 99},
    #         ],
    #     }
    #     self.admin_session.update_signal(signal_name, updated)

    #     insert_statement2 = bios.isql().insert().into(signal_name).csv_bulk([
    #         "tyrrrr,3333333390000002,10",
    #         "klaa,902222233333333,90",
    #         ]).build()

    #     start = resp.records[0].timestamp
    #     delta = bios.time.now() - start
    #     extract_statement = bios.isql().select("distinct(new)").from_signal(signal_name).
    #     events = extractor1.extract(
    #         start,
    #         delta,
    #         view=Distinct("new"),
    #         attributes=["new", Count(output_as="count")],
    #     )

    #     try:
    #         self.assertEqual(len(events), 3)
    #         count_per_rec = {}
    #         for i in events:
    #             count_per_rec[i.get("new")] = i.get("count")
    #         self.assertEqual(count_per_rec.get(10), 1)
    #         self.assertEqual(count_per_rec.get(90), 1)
    #         self.assertEqual(count_per_rec.get(99), 5)
    #     except Exception:
    #         pprint.PrettyPrinter().pprint(events)
    #         raise

    def test_extract_across_versions_with_group_on_modifying_attribute(self):
        """TC-ID : dp_ext_ver_21
        This test is to verify events ingested before and after adding attribute
        should be grouped and extracted as per group key
        """
        signal_name = "dp_ext_ver_21"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "enumAttribute",
                    "type": "String",
                    "allowedValues": ["Birds", "Animals", "Reptiles", "amphibians"],
                },
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.create_signal(signal)

        resp = self.admin_session.execute(
            bios.isql().insert().into(signal_name).csv("Reptiles,788").build()
        )

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "enumAttribute",
                    "type": "String",
                    "allowedValues": ["Birds", "Animals", "Reptiles", "amphibians"],
                },
                {"attributeName": "value", "type": "String"},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)

        insert_statement = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "amphibians,7888888888888888888888888888",
                    "Reptiles,7888888888888888888888888888",
                    "Birds,98234444444444444444444444424444",
                ]
            )
            .build()
        )
        self.admin_session.execute(insert_statement)

        start = resp.records[0].timestamp
        delta = bios.time.now() - start
        extract_statement = (
            bios.isql()
            .select("value", "count()")
            .from_signal(signal_name)
            .group_by(["value"])
            .time_range(start, delta)
            .build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        self.assertEqual(len(records), 3)
        count_per_rec = {}
        for i in records:
            count_per_rec[i.get("value")] = i.get("count()")
        try:
            self.assertEqual(count_per_rec.get("788"), 1)
            self.assertEqual(count_per_rec.get("7888888888888888888888888888"), 2)
            self.assertEqual(count_per_rec.get("98234444444444444444444444424444"), 1)
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    def test_extract_across_versions_on_modifying_enum_to_str(self):
        """TC-ID : bug-1336
        This test is to verify events ingested before and after adding attribute
        should be extracted successfully
        """
        signal_name = "bug1336"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "enumAttribute",
                    "type": "String",
                    "allowedValues": ["MALE", "FEMALE"],
                },
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.create_signal(signal)

        insert_statement1 = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                [
                    "MALE,12",
                    "FEMALE,90",
                ]
            )
            .build()
        )
        resp = self.admin_session.execute(insert_statement1)

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "enumAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        self.admin_session.update_signal(signal_name, updated)

        self.admin_session.execute(bios.isql().insert().into(signal_name).csv("io,50").build())

        start = resp.records[0].timestamp
        delta = bios.time.now() - start
        extract_statement = (
            bios.isql().select().from_signal(signal_name).time_range(start, delta).build()
        )
        records = self.admin_session.execute(extract_statement).to_dict()
        try:
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0].get("enumAttribute"), "MALE")
            self.assertEqual(records[1].get("enumAttribute"), "FEMALE")
            self.assertEqual(records[2].get("enumAttribute"), "io")
        except Exception:
            pprint.PrettyPrinter().pprint(records)
            raise

    def test_extract_across_versions_on_modifying_str_to_enum(self):
        """changing a signal attribute type from string to enum is prohibited"""
        signal_name = "bug1336a"
        self.created_signals.append(signal_name)
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "enumAttribute", "type": "String"},
                {"attributeName": "value", "type": "Integer"},
            ],
        }
        setup_signal(self.admin_session, signal)

        updated = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "enumAttribute",
                    "type": "String",
                    "allowedValues": ["MALE", "FEMALE"],
                    "default": "FEMALE",
                },
                {"attributeName": "value", "type": "Integer"},
            ],
        }

        with pytest.raises(ServiceError) as excinfo:
            self.admin_session.update_signal(signal_name, updated)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert "Constraint violation: Unsupported attribute type transition;" in error.message


if __name__ == "__main__":
    pytest.main(sys.argv)
