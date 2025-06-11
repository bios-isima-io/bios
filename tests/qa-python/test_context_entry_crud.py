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
import random
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios.models import AttributeType, ContextRecords
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
STORE_CONTEXT = {
    "contextName": "storeContext",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "storeId",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "zipCode", "type": "Integer", "default": -1},
        {"attributeName": "address", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["storeId"],
}

CONTEXT_WITH_VERSION_ATTRIBUTE = {
    "contextName": "versionAttributeContext",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "integer"},
        {"attributeName": "the_version", "type": "integer"},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key"],
    "versionAttribute": "the_version",
}

CONTEXT_WITH_STRING_KEY = {
    "contextName": "stringKeyContext",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "objectId", "type": "String"},
        {"attributeName": "projectCode", "type": "Integer"},
        {"attributeName": "title", "type": "String"},
    ],
    "primaryKey": ["objectId"],
}

CONTEXT_INT_AND_STRING = {
    "contextName": "intAndString",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "integer"},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key"],
}

CONTEXT_PRIMARY_KEYS = {
    "contextName": "primaryKeysTest",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "integer"},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key"],
}

CONTEXT_INT_AND_DECIMAL = {
    "contextName": "intAndDecimal",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "integer"},
        {"attributeName": "the_value", "type": "decimal"},
    ],
    "primaryKey": ["the_key"],
}


class BiosContextEntryCrudTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_context(cls.session, STORE_CONTEXT)
        setup_context(cls.session, CONTEXT_WITH_VERSION_ATTRIBUTE)
        setup_context(cls.session, CONTEXT_WITH_STRING_KEY)
        setup_context(cls.session, CONTEXT_INT_AND_STRING)
        setup_context(cls.session, CONTEXT_PRIMARY_KEYS)
        setup_context(cls.session, CONTEXT_INT_AND_DECIMAL)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_bios_context_crud(self):
        upsert_statement = (
            bios.isql()
            .upsert()
            .into(STORE_CONTEXT["contextName"])
            .csv_bulk(["0,10,rcb", "1,11,vk"])
            .build()
        )
        self.session.execute(upsert_statement)

        select_request = (
            bios.isql()
            .select()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0], [1]])
            .build()
        )

        reply = self.session.execute(select_request)
        self.assertIsInstance(reply, ContextRecords)

        records = reply.get_records()
        self.assertEqual(len(records), 2)
        record0 = records[0]
        self.assertEqual(record0.get_primary_key(), [0])
        self.assertEqual(record0.get("storeId"), 0)
        self.assertEqual(record0.get("zipCode"), 10)
        self.assertEqual(record0.get("address"), "rcb")
        record1 = records[1]
        self.assertEqual(record1.get_primary_key(), [1])
        self.assertEqual(record1.get("storeId"), 1)
        self.assertEqual(record1.get("zipCode"), 11)
        self.assertEqual(record1.get("address"), "vk")

        self.assertEqual(record0.get_type("storeId"), AttributeType.INTEGER)
        self.assertEqual(record0.get_type("zipCode"), AttributeType.INTEGER)
        self.assertEqual(record0.get_type("address"), AttributeType.STRING)

        # WARNING: fetching ContextRecords._response is not a right way to read
        # response and shoulnd't be used for testing. These assertions are kept
        # to make sure that there is no behavior change to existing applications
        response = reply._response
        self.assertEqual(response["contentRepresentation"], "UNTYPED")
        self.assertEqual(response["primaryKey"][0], "storeId")
        self.assertEqual(len(response["entries"]), 2)
        self.assertEqual(len(response["entries"][0]["attributes"]), 3)
        self.assertEqual(len(response["entries"][1]["attributes"]), 3)
        self.assertEqual(response["entries"][0]["attributes"][0], 0)
        self.assertEqual(response["entries"][0]["attributes"][1], 10)
        self.assertEqual(response["entries"][0]["attributes"][2], "rcb")
        self.assertEqual(response["entries"][1]["attributes"][0], 1)
        self.assertEqual(response["entries"][1]["attributes"][1], 11)
        self.assertEqual(response["entries"][1]["attributes"][2], "vk")

        self.assertEqual(records[0].get_timestamp(), response["entries"][0]["timestamp"])
        self.assertEqual(records[1].get_timestamp(), response["entries"][1]["timestamp"])

        request = (
            bios.isql()
            .update(STORE_CONTEXT["contextName"])
            .set({"zipCode": 14, "address": "CSK"})
            .where(key=[0])
            .build()
        )
        self.session.execute(request)

        select_statement = (
            bios.isql()
            .select()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0], [1]])
            .build()
        )
        reply = self.session.execute(select_statement)

        records = reply.get_records()
        self.assertEqual(len(records), 2)
        record0 = records[0]
        self.assertEqual(record0.get_primary_key(), [0])
        self.assertEqual(record0.get("storeId"), 0)
        self.assertEqual(record0.get("zipCode"), 14)
        self.assertEqual(record0.get("address"), "CSK")
        record1 = records[1]
        self.assertEqual(record1.get_primary_key(), [1])
        self.assertEqual(record1.get("storeId"), 1)
        self.assertEqual(record1.get("zipCode"), 11)
        self.assertEqual(record1.get("address"), "vk")

        # WARNING: fetching ContextRecords._response is not a right way to read
        # response and shoulnd't be used for testing. These assertions are kept
        # to make sure that there is no behavior change to existing applications
        response = reply._response
        self.assertEqual(response["contentRepresentation"], "UNTYPED")
        self.assertEqual(response["primaryKey"][0], "storeId")
        self.assertEqual(len(response["entries"]), 2)
        self.assertEqual(len(response["entries"][0]["attributes"]), 3)
        self.assertEqual(len(response["entries"][1]["attributes"]), 3)
        self.assertEqual(response["entries"][0]["attributes"][0], 0)
        self.assertEqual(response["entries"][0]["attributes"][1], 14)
        self.assertEqual(response["entries"][0]["attributes"][2], "CSK")
        self.assertEqual(response["entries"][1]["attributes"][0], 1)
        self.assertEqual(response["entries"][1]["attributes"][1], 11)
        self.assertEqual(response["entries"][1]["attributes"][2], "vk")

        # deleting non-existing entry should be ignored silently
        request = (
            bios.isql()
            .delete()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0], [1], [2]])
            .build()
        )
        self.session.execute(request)

        select_request = (
            bios.isql()
            .select()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0], [1], [2]])
            .build()
        )
        reply = self.session.execute(select_request)
        self.assertEqual(len(reply.get_records()), 0)

        # double deletion is fine
        request = (
            bios.isql()
            .delete()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0], [1]])
            .build()
        )
        self.session.execute(request)

        select_request = (
            bios.isql()
            .select()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0], [1]])
            .build()
        )
        reply = self.session.execute(select_request)
        self.assertEqual(len(reply.get_records()), 0)

        # WARNING: fetching ContextRecords._response is not a right way to read
        # response and shoulnd't be used for testing. These assertions are kept
        # to make sure that there is no behavior change to existing applications
        response = reply._response
        self.assertEqual(response["contentRepresentation"], "UNTYPED")
        self.assertEqual(response["primaryKey"][0], "storeId")
        self.assertEqual(len(response["entries"]), 0)

    def test_bios_upsert_context_error(self):
        statement = (
            bios.isql()
            .upsert()
            .into(STORE_CONTEXT["contextName"])
            .csv_bulk(["0,10,rcb", "1,11"])
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.execute(statement)
        self.assertEqual(context.exception.error_code, ErrorCode.SCHEMA_MISMATCHED)
        self.assertEqual(
            context.exception.message,
            "Source event string has less values than pre-defined;"
            " tenant=biosPythonQA, context=storeContext",
        )

    def test_context_select_should_not_have_invalid_attributes(self):
        with self.assertRaises(ServiceError) as context:
            req = (
                bios.isql()
                .select("attribute")
                .from_context(STORE_CONTEXT["contextName"])
                .where(keys=[[1]])
                .build()
            )
            reply = self.session.execute(req)
            print(reply.get_records())
        self.assertEqual(context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_context_select_all_primary_keys(self):
        upsert_statement = (
            bios.isql()
            .upsert()
            .into(STORE_CONTEXT["contextName"])
            .csv_bulk(["8,10,rcb", "9,11,vk"])
            .build()
        )
        self.session.execute(upsert_statement)

        self.session.feature_refresh(STORE_CONTEXT["contextName"])

        time.sleep(40)

        select_request = (
            bios.isql().select("storeId").from_context(STORE_CONTEXT["contextName"]).build()
        )
        reply = self.session.execute(select_request)

        records = reply.get_records()
        self.assertEqual(len(records), 2)
        record0 = records[0]
        self.assertEqual(record0.get_primary_key(), [8])
        self.assertEqual(record0.get("storeId"), 8)
        self.assertIsNone(record0.get("zipCode"))
        self.assertIsNone(record0.get("address"))
        record1 = records[1]
        self.assertEqual(record1.get_primary_key(), [9])
        self.assertEqual(record1.get("storeId"), 9)
        self.assertIsNone(record1.get("zipCode"), None)
        self.assertIsNone(record1.get("address"), None)

        # WARNING: fetching ContextRecords._response is not a right way to read
        # response and shoulnd't be used for testing. These assertions are kept
        # to make sure that there is no behavior change to existing applications
        response = reply._response
        self.assertEqual(response["contentRepresentation"], "UNTYPED")
        self.assertEqual(response["primaryKey"][0], "storeId")
        self.assertEqual(len(response["entries"]), 2)
        self.assertEqual(len(response["entries"][0]["attributes"]), 1)
        self.assertEqual(len(response["entries"][1]["attributes"]), 1)
        self.assertEqual(response["entries"][0]["attributes"][0], 8)
        self.assertEqual(response["entries"][1]["attributes"][0], 9)

        select_request = (
            bios.isql().select("count()").from_context(STORE_CONTEXT["contextName"]).build()
        )
        for _ in range(30):
            reply = self.session.execute(select_request)
            records = reply.get_records()
            self.assertEqual(len(records), 1)
            record0 = records[0]
            if record0.get("count()") < 2:
                time.sleep(1)
                continue
            self.assertEqual(record0.get("count()"), 2)
            break

    def test_context_select_primary_keys_edge_cases(self):
        context_name = CONTEXT_PRIMARY_KEYS["contextName"]
        print("select empty primary keys")
        select_request = bios.isql().select("the_key").from_context(context_name).build()
        reply = self.session.execute(select_request)

        records = reply.get_records()
        assert len(records) == 0

        num_entries = 100000
        print(f"populating {num_entries} context entries")
        left = 0
        while left < num_entries:
            right = min(left + 2048, num_entries)
            entries = [f"{idx},{idx}" for idx in range(left, right)]
            statement = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
            self.session.execute(statement)
            print(f"  ... {right}")
            left = right
            if right % 32768 == 0:
                self._verify_listing_primary_keys(context_name, right)

        self._verify_listing_primary_keys(context_name, num_entries)

    def _verify_listing_primary_keys(self, context_name: str, num_entries: int):
        print(f"extracting primary keys, number of keys: {num_entries}")
        statement = bios.isql().select("the_key").from_context(context_name).build()
        keys = self.session.execute(statement).to_dict()
        print("verifying")
        assert len(keys) == num_entries
        for idx, key in enumerate(keys):
            assert key.get("the_key") == idx

    def select_entries(self, keys, context_name):
        select_request = bios.isql().select().from_context(context_name).where(keys=keys).build()
        reply = self.session.execute(select_request)
        self.assertIsInstance(reply, ContextRecords)
        return reply

    def upsert_entries(self, entries, context_name):
        upsert_statement = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
        self.session.execute(upsert_statement)

    def delete_entries(self, keys, context_name):
        delete_request = bios.isql().delete().from_context(context_name).where(keys=keys).build()
        self.session.execute(delete_request)

    def test_update_regular(self):
        context_name = STORE_CONTEXT["contextName"]

        self.upsert_entries(["0,10,abc-10", "1,11,def-11"], context_name)
        reply = self.select_entries([[0], [1]], context_name)
        records = reply.get_records()

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].get_primary_key(), [0])
        self.assertEqual(records[0].get("zipCode"), 10)
        self.assertEqual(records[0].get("address"), "abc-10")
        self.assertEqual(records[1].get_primary_key(), [1])
        self.assertEqual(records[1].get("zipCode"), 11)
        self.assertEqual(records[1].get("address"), "def-11")

        self.upsert_entries(["0,20,abc-20", "1,5,def-5"], context_name)
        reply = self.select_entries([[0], [1]], context_name)
        records = reply.get_records()

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].get_primary_key(), [0])
        self.assertEqual(records[0].get("zipCode"), 20)
        self.assertEqual(records[0].get("address"), "abc-20")
        self.assertEqual(records[1].get_primary_key(), [1])
        self.assertEqual(records[1].get("zipCode"), 5)
        self.assertEqual(records[1].get("address"), "def-5")

        self.delete_entries([[0], [1]], context_name)
        reply = self.select_entries([[0], [1], [2]], context_name)
        self.assertEqual(len(reply.get_records()), 0)

    def test_negative_upsert_empty_key(self):
        statement = (
            bios.isql()
            .upsert()
            .into(CONTEXT_WITH_STRING_KEY["contextName"])
            .csv_bulk(["hello,10,rcb", ",11,vk"])
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    def test_bios_multi_get_context_entries_errors(self):
        with self.assertRaises(ServiceError) as context:
            self.session.multi_execute()
        self.assertEqual(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)

        select_request1 = (
            bios.isql()
            .select()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[0]])
            .build()
        )
        select_request2 = (
            bios.isql().select().from_context("invalidStream").where(keys=[[1]]).build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.multi_execute(select_request1, select_request2)
        print(context.exception.message)
        self.assertEqual(context.exception.error_code, ErrorCode.NO_SUCH_STREAM)
        self.assertEqual(
            context.exception.message,
            "Error in 0-indexed query number 1: Stream not found: biosPythonQA.invalidStream;"
            " tenant=biosPythonQA",
        )

        select_request3 = (
            bios.isql()
            .select()
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[1])  # Error: keys should be a list of lists.
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.multi_execute(select_request1, select_request3)
        self.assertEqual(context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_bios_multi_get_context_entries(self):
        context_1 = STORE_CONTEXT["contextName"]
        self.upsert_entries(["0,10,rcb", "1,11,vk"], context_1)
        context_2 = CONTEXT_WITH_VERSION_ATTRIBUTE["contextName"]
        self.upsert_entries(["0,10,abc-10", "1,11,def-11", "2,-30,xyz--30"], context_2)

        select_request1 = bios.isql().select().from_context(context_1).where(keys=[[0]]).build()
        select_request2 = bios.isql().select().from_context(context_1).where(keys=[[1]]).build()
        select_request3 = (
            bios.isql().select().from_context(context_2).where(keys=[[0], [1], [2]]).build()
        )

        # 2 requests to same context.
        reply = self.session.multi_execute(select_request1, select_request2)
        self.assertIsInstance(reply, list)
        self.assertEqual(len(reply), 2)
        self.assertIsInstance(reply[0], ContextRecords)
        self.assertIsInstance(reply[1], ContextRecords)

        records0 = reply[0].get_records()
        self.assertEqual(len(records0), 1)
        record00 = records0[0]
        self.assertEqual(record00.get_primary_key(), [0])
        self.assertEqual(record00.get("storeId"), 0)
        self.assertEqual(record00.get("zipCode"), 10)
        self.assertEqual(record00.get("address"), "rcb")
        records1 = reply[1].get_records()
        self.assertEqual(len(records1), 1)
        record10 = records1[0]
        self.assertEqual(record10.get_primary_key(), [1])
        self.assertEqual(record10.get("storeId"), 1)
        self.assertEqual(record10.get("zipCode"), 11)
        self.assertEqual(record10.get("address"), "vk")

        # 30 requests to different contexts.
        reply = self.session.multi_execute(
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
            select_request1,
            select_request2,
            select_request3,
        )
        self.assertIsInstance(reply, list)
        self.assertEqual(len(reply), 30)
        self.assertIsInstance(reply[0], ContextRecords)
        self.assertIsInstance(reply[29], ContextRecords)

        records0 = reply[0].get_records()
        self.assertEqual(len(records0), 1)
        record00 = records0[0]
        self.assertEqual(record00.get_primary_key(), [0])
        self.assertEqual(record00.get("storeId"), 0)
        self.assertEqual(record00.get("zipCode"), 10)
        self.assertEqual(record00.get("address"), "rcb")
        records29 = reply[29].get_records()
        self.assertEqual(len(records29), 3)
        record292 = records29[2]
        self.assertEqual(record292.get_primary_key(), [2])
        self.assertEqual(record292.get("the_key"), 2)
        self.assertEqual(record292.get("the_version"), -30)
        self.assertEqual(record292.get("the_value"), "xyz--30")

        # Cleanup.
        self.delete_entries([[0], [1]], context_1)
        self.delete_entries([[0], [1], [2]], context_2)

    def test_large_scale_crud(self):
        print("")
        context_name = CONTEXT_INT_AND_STRING.get("contextName")
        num_context_entries = 10000
        entries = []
        key_values = {}
        for i in range(num_context_entries):
            key = i * 100
            value = str(i) + "a"
            entry = f"{key},{value}"
            entries.append(entry)
            key_values[key] = value
        upsert_statement = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
        print(f"Upserting {num_context_entries} entries")
        self.session.execute(upsert_statement)
        keys = [[key] for key in key_values]
        select_statement = bios.isql().select().from_context(context_name).where(keys=keys).build()
        print(f"Selecting {num_context_entries} entries")
        retrieved_entries = self.session.execute(select_statement).to_dict()
        print("verifying retrieved entries")
        assert len(retrieved_entries) == num_context_entries
        for entry in retrieved_entries:
            key = entry.get("the_key")
            value = entry.get("the_value")
            assert value == key_values.get(key)

    def test_negative_insert_context(self):
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(
                bios.isql()
                .insert()
                .into(STORE_CONTEXT.get("contextName"))
                .csv("1,2,three")
                .build()
            )
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert (
            "Invalid request: Signal data operation may not be done against a context;"
            in error.message
        )

    def test_negative_select_context_with_time_range(self):
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(
                bios.isql()
                .select()
                .from_signal(STORE_CONTEXT.get("contextName"))
                .time_range(bios.time.now(), bios.time.minutes(-10))
                .build()
            )
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert (
            "Invalid request: Signal data operation may not be done against a context;"
            in error.message
        )

    def test_list_primary_keys(self):
        """Regression BIOS-6118"""
        context_name = "contextEntryCrudTestListing"
        context = copy.deepcopy(STORE_CONTEXT)
        context["contextName"] = context_name
        setup_context(self.session, context)

        print("")
        num_context_entries = 50000
        entries = []
        entries_so_far = 0
        used_key = set()
        for _ in range(num_context_entries):
            while True:
                value = random.randrange(0, 10000000000)
                if value not in used_key:
                    break
            used_key.add(value)
            entry = f"{value+1},{value+1},area{value}"
            entries.append(entry)
            if len(entries) == 5000:
                upsert_statement = (
                    bios.isql().upsert().into(context_name).csv_bulk(entries).build()
                )
                entries_so_far += len(entries)
                self.session.execute(upsert_statement)
                print(f"Upserted {entries_so_far} / {num_context_entries} entries")
                entries.clear()

        # We verify twice in case we hit eventually consistency issue
        for _ in range(2):
            select_statement = bios.isql().select("storeId").from_context(context_name).build()
            retrieved_entries = self.session.execute(select_statement).to_dict()
            if len(retrieved_entries) == num_context_entries:
                break
            print("Eventually consistency issue suspected, will retry after 5 seconds")
            time.sleep(5)
        assert len(retrieved_entries) == num_context_entries

    def test_decimal_value_normal(self):
        context_name = CONTEXT_INT_AND_DECIMAL.get("contextName")
        key = random.randrange(0, 10000000)
        upsert_statement = bios.isql().upsert().into(context_name).csv(f"{key},4.3").build()
        self.session.execute(upsert_statement)
        response = self.session.execute(
            bios.isql().select().from_context(context_name).where(keys=[[key]]).build()
        ).to_dict()
        assert len(response) == 1
        assert response[0].get("the_key") == key
        assert response[0].get("the_value") == pytest.approx(4.3)

    def test_decimal_value_negative(self):
        context_name = CONTEXT_INT_AND_DECIMAL.get("contextName")
        key = random.randrange(0, 10000000)
        upsert_statement = bios.isql().upsert().into(context_name).csv(f"{key},NaN").build()
        with pytest.raises(ServiceError) as exc_info:
            self.session.execute(upsert_statement)
        error = exc_info.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert "Invalid value syntax: NaN value is not allowed;" in error.message

    def test_select_partial_attributes(self):
        """Regression BIOS-6620"""
        upsert_statement = (
            bios.isql()
            .upsert()
            .into(CONTEXT_WITH_VERSION_ATTRIBUTE["contextName"])
            .csv_bulk(["10000,303,tb", "20000,909,tr"])
            .build()
        )
        self.session.execute(upsert_statement)

        select_all_request = (
            bios.isql()
            .select()
            .from_context(CONTEXT_WITH_VERSION_ATTRIBUTE["contextName"])
            .where(keys=[[10000], [20000]])
            .build()
        )

        reply = self.session.execute(select_all_request)
        records = reply.get_records()
        self.assertEqual(len(records), 2)
        record0 = records[0]
        self.assertEqual(record0.get("the_key"), 10000)
        self.assertEqual(record0.get("the_version"), 303)
        self.assertEqual(record0.get("the_value"), "tb")
        record1 = records[1]
        self.assertEqual(record1.get("the_key"), 20000)
        self.assertEqual(record1.get("the_version"), 909)
        self.assertEqual(record1.get("the_value"), "tr")

        select_part_request = (
            bios.isql()
            .select("the_key", "the_version")
            .from_context(CONTEXT_WITH_VERSION_ATTRIBUTE["contextName"])
            .where("the_key in (10000, 20000)")
            .build()
        )
        reply = self.session.execute(select_part_request)
        # records = reply.get_records()
        records = reply.to_dict()
        self.assertEqual(len(records), 2)
        record0 = records[0]
        assert len(record0) == 2
        self.assertEqual(record0.get("the_key"), 10000)
        self.assertEqual(record0.get("the_version"), 303)
        record1 = records[1]
        assert len(record1) == 2
        self.assertEqual(record1.get("the_key"), 20000)
        self.assertEqual(record1.get("the_version"), 909)

        # select all attributes again
        reply = self.session.execute(select_all_request)
        records = reply.get_records()
        self.assertEqual(len(records), 2)
        record0 = records[0]
        self.assertEqual(record0.get("the_key"), 10000)
        self.assertEqual(record0.get("the_version"), 303)
        self.assertEqual(record0.get("the_value"), "tb")
        record1 = records[1]
        self.assertEqual(record1.get("the_key"), 20000)
        self.assertEqual(record1.get("the_version"), 909)
        self.assertEqual(record1.get("the_value"), "tr")


if __name__ == "__main__":
    pytest.main(sys.argv)
