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

import base64
import json
import logging
import os
import random
import string
import sys
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios.isql_request import ISqlRequest
from bios_tutils import get_all_types_signal, get_minimum_signal, load_signal_json
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosInsertTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIGNAL_MAXIMUM = {
    "signalName": "maximum",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "one", "type": "string"},
        {"attributeName": "three", "type": "string"},
        {"attributeName": "four", "type": "Decimal"},
        {"attributeName": "two", "type": "integer"},
    ],
}

SIGNAL_FOR_CREATE_TEST = {
    "signalName": "create_test_signal",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "foo", "type": "string"}],
}

SIGNAL_FOR_CREATE_TEST_JSON_FILE = "../resources/bios-signal-schema.json"

SIGNAL_ALL_TYPES_WITH_DEFAULT_FILE = "../resources/bios-signal-all-types-with-default.json"


class Bios2SelectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        cls.SIGNAL_MINIMUM = get_minimum_signal()

        cls.SIGNAL_ALL_TYPES = get_all_types_signal()

        cls.SIGNAL_ALL_TYPES_WITH_DEFAULT = load_signal_json(SIGNAL_ALL_TYPES_WITH_DEFAULT_FILE)

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_signal(cls.SIGNAL_MINIMUM)
            admin.create_signal(cls.SIGNAL_ALL_TYPES)
            admin.create_signal(cls.SIGNAL_ALL_TYPES_WITH_DEFAULT)
            admin.create_signal(SIGNAL_MAXIMUM)

        with open(SIGNAL_FOR_CREATE_TEST_JSON_FILE, "r") as json_file:
            cls.LOADED_SIGNAL = json.loads(json_file.read())
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
        cls.session.close()

    def test_bios_insert_select(self):
        request = (
            ISqlRequest().insert().into(self.SIGNAL_MINIMUM["signalName"]).csv("hello").build()
        )

        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        statement = (
            bios.isql()
            .select()
            .from_signal(self.SIGNAL_MINIMUM.get("signalName"))
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        self.assertEqual(len(response.data_windows), 1)
        self.assertEqual(len(response.data_windows[0].records), 1)
        record = response.data_windows[0].records[0]
        self.assertEqual(record.event_id, insert_record.event_id)
        self.assertEqual(record.timestamp, insert_record.timestamp)
        self.assertEqual(record.get("one"), "hello")
        self.assertEqual(record.get_by_index(0), "hello")

    def test_bios_insert_long_string(self):
        text = "".join(random.choice(string.ascii_letters) for _ in range(500000))
        request = (
            ISqlRequest()
            .insert()
            .into(SIGNAL_MAXIMUM["signalName"])
            .csv(f"{text},more,12.4,10")
            .build()
        )
        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        statement = (
            bios.isql()
            .select()
            .from_signal(SIGNAL_MAXIMUM["signalName"])
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        self.assertEqual(len(response.data_windows), 1)
        self.assertEqual(len(response.data_windows[0].records), 1)
        record = response.data_windows[0].records[0]
        self.assertEqual(record.event_id, insert_record.event_id)
        self.assertEqual(record.timestamp, insert_record.timestamp)
        self.assertEqual(record.get("one"), text)

    def test_bios_insert_long_string(self):
        text = "".join(random.choice(string.ascii_letters) for _ in range(1000000))
        request = (
            ISqlRequest()
            .insert()
            .into(SIGNAL_MAXIMUM["signalName"])
            .csv(f"{text},more,12.4,10")
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.REQUEST_TOO_LARGE)
        self.assertRegex(
            error_context.exception.message, "Request body size.*that exceeds maximum 1000000"
        )

    def test_bios_insert_more_than_one_attribute_csv(self):
        request = (
            ISqlRequest()
            .insert()
            .into(SIGNAL_MAXIMUM["signalName"])
            .csv("string,more,12.4,10")
            .build()
        )
        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        statement = (
            bios.isql()
            .select()
            .from_signal(SIGNAL_MAXIMUM.get("signalName"))
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        self.assertEqual(len(response.data_windows), 1)
        self.assertEqual(len(response.data_windows[0].records), 1)
        record = response.data_windows[0].records[0]
        self.assertEqual(record.event_id, insert_record.event_id)
        self.assertEqual(record.timestamp, insert_record.timestamp)
        self.assertEqual(record.get("one"), "string")
        self.assertEqual(record.get_by_index(0), "string")
        self.assertEqual(record.get("three"), "more")
        self.assertEqual(record.get_by_index(1), "more")
        self.assertEqual(record.get("four"), 12.4)
        self.assertEqual(record.get_by_index(2), 12.4)
        self.assertEqual(record.get("two"), 10)
        self.assertEqual(record.get_by_index(3), 10)

    def test_bios_insert_all_types(self):
        bytes_source = os.urandom(32)
        encoded = base64.b64encode(bytes_source).decode()
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_ALL_TYPES["signalName"])
            .csv(f"Hello World,94061,12.4,true,{encoded}")
            .build()
        )
        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        statement = (
            bios.isql()
            .select()
            .from_signal(self.SIGNAL_ALL_TYPES.get("signalName"))
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        self.assertEqual(len(response.data_windows), 1)
        self.assertEqual(len(response.data_windows[0].records), 1)
        record = response.data_windows[0].records[0]
        self.assertEqual(record.event_id, insert_record.event_id)
        self.assertEqual(record.timestamp, insert_record.timestamp)
        self.assertEqual(record.get("stringData"), "Hello World")
        self.assertEqual(record.get("integerData"), 94061)
        self.assertEqual(record.get("decimalData"), 12.4)
        self.assertEqual(record.get("booleanData"), True)
        self.assertEqual(record.get("blobData"), bytes_source)

    def test_bios_insert_more_than_one_attribute_values(self):
        values_list = ['"enclosed in double quotes"', "more", 12.4, 10]
        request = (
            ISqlRequest().insert().into(SIGNAL_MAXIMUM["signalName"]).values(values_list).build()
        )
        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        statement = (
            bios.isql()
            .select()
            .from_signal(SIGNAL_MAXIMUM["signalName"])
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        self.assertEqual(len(response.data_windows), 1)
        self.assertEqual(len(response.data_windows[0].records), 1)
        record = response.data_windows[0].records[0]
        self.assertEqual(record.event_id, insert_record.event_id)
        self.assertEqual(record.timestamp, insert_record.timestamp)
        self.assertEqual(record.get("one"), values_list[0])
        self.assertEqual(record.get("three"), values_list[1])
        self.assertEqual(record.get("four"), values_list[2])
        self.assertEqual(record.get("two"), values_list[3])

    def test_bios_insert_values_bulk(self):
        values_list1 = ["with a \n newline", 'double "quotes and \n newline', 12.4, 10]
        values_list2 = ['with a single "double quote', 'more " double " quotes "', 12.4, 10]
        values_list3 = ["with, a comma", "many,,, , commas, ", 12.4, 10]
        request = (
            ISqlRequest()
            .insert()
            .into(SIGNAL_MAXIMUM["signalName"])
            .values_bulk([values_list1, values_list2, values_list3])
            .build()
        )
        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        statement = (
            bios.isql()
            .select()
            .from_signal(SIGNAL_MAXIMUM["signalName"])
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        self.assertEqual(len(response.data_windows), 1)
        self.assertEqual(len(response.data_windows[0].records), 3)
        record = response.data_windows[0].records[0]
        self.assertEqual(record.event_id, ret.records[0].event_id)
        self.assertEqual(record.timestamp, ret.records[0].timestamp)
        self.assertEqual(record.get("one"), values_list1[0])
        self.assertEqual(record.get("three"), values_list1[1])
        self.assertEqual(record.get("four"), values_list1[2])
        self.assertEqual(record.get("two"), values_list1[3])

        record = response.data_windows[0].records[1]
        self.assertEqual(record.event_id, ret.records[1].event_id)
        self.assertEqual(record.timestamp, ret.records[1].timestamp)
        self.assertEqual(record.get("one"), values_list2[0])
        self.assertEqual(record.get("three"), values_list2[1])
        self.assertEqual(record.get("four"), values_list2[2])
        self.assertEqual(record.get("two"), values_list2[3])

        record = response.data_windows[0].records[2]
        self.assertEqual(record.event_id, ret.records[2].event_id)
        self.assertEqual(record.timestamp, ret.records[2].timestamp)
        self.assertEqual(record.get("one"), values_list3[0])
        self.assertEqual(record.get("three"), values_list3[1])
        self.assertEqual(record.get("four"), values_list3[2])
        self.assertEqual(record.get("two"), values_list3[3])

    def test_insert_csv_rfc4180(self):
        request = (
            ISqlRequest()
            .insert()
            .into(SIGNAL_MAXIMUM["signalName"])
            .csv_bulk(
                [
                    '"CA, USA",Julian "Cannonball" Adderley,12.4,10',
                    '"Tokyo,\r\n Japan","Charlie ""Bird"" Parker",98.7,20',
                ]
            )
            .build()
        )
        ret = self.session.execute(request)
        insert_record = ret.records[0]
        self.assertGreater(insert_record.timestamp, 0)

        # check inserted records
        statement = (
            bios.isql()
            .select()
            .from_signal(SIGNAL_MAXIMUM.get("signalName"))
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)
        data_window = response.data_windows[0]
        self.assertEqual(len(data_window.records), 2)
        self.assertEqual(data_window.records[0].get("one"), "CA, USA")
        self.assertEqual(data_window.records[0].get("three"), 'Julian "Cannonball" Adderley')
        self.assertEqual(data_window.records[1].get("one"), "Tokyo,\r\n Japan")
        self.assertEqual(data_window.records[1].get("three"), 'Charlie "Bird" Parker')

    def test_insert_missing_attribute(self):
        request = (
            ISqlRequest()
            .insert()
            .into(SIGNAL_MAXIMUM["signalName"])
            .values(["string", 12.4, 10])
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.execute(request)
        self.assertTrue(context.exception.error_code == ErrorCode.SCHEMA_MISMATCHED)
        self.assertEqual(
            context.exception.message,
            "Source event string has less values than pre-defined;"
            " tenant=biosInsertTest, signal=maximum",
        )

    def test_insert_none_value(self):
        with self.assertRaises(ServiceError) as context:
            ISqlRequest().insert().into(SIGNAL_MAXIMUM["signalName"]).values(None).build()
            self.assertTrue(context.exception.error_code == ErrorCode.INVALID_ARGUMENT)
            self.assertRegex(
                context.exception.message,
                "Parameter 'values' must be of type <class 'list'>, but.*was specified",
            )

    def test_insert_list_none_value(self):
        with self.assertRaises(ServiceError) as context:
            ISqlRequest().insert().into(SIGNAL_MAXIMUM["signalName"]).values([None]).build()
            self.assertTrue(context.exception.error_code == ErrorCode.INVALID_ARGUMENT)
            self.assertRegex(
                context.exception.message,
                "Parameter 'values' must be of type <class 'list'>, but.*was specified",
            )

    def test_insert_invalid_value_type(self):
        bytes_source = os.urandom(32)
        encoded = base64.b64encode(bytes_source).decode()
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_ALL_TYPES["signalName"])
            .values(["Hello", "someInt", 12.4, True, encoded])
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.execute(request)
            self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
            self.assertRegex(
                context.exception.message,
                "Input must be a numeric string*",
            )

    def test_insert_empty_int(self):
        bytes_source = os.urandom(32)
        encoded = base64.b64encode(bytes_source).decode()
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_ALL_TYPES["signalName"])
            .values(["Hello", "", 12.4, True, encoded])
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.execute(request)
            self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
            self.assertRegex(
                context.exception.message,
                "Input must be a numeric string*",
            )

    def test_insert_empty_number(self):
        bytes_source = os.urandom(32)
        encoded = base64.b64encode(bytes_source).decode()
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_ALL_TYPES["signalName"])
            .values(["Hello", 0, "", True, encoded])
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.execute(request)
            self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
            self.assertRegex(
                context.exception.message,
                "Input must be a numeric string*",
            )

    def test_bios_insert_with_default_values(self):
        signature = os.urandom(32)
        signature_encoded = base64.b64encode(signature).decode()
        picture = os.urandom(32)
        picture_encoded = base64.b64encode(picture).decode()
        attributes = [
            "-94061",
            "40",
            "45.4",
            "-123.4",
            "Bill Evans",
            "MALE",
            "Japan",
            "Miyagi",
            "false",
            signature_encoded,
            picture_encoded,
        ]
        expected = {
            "customerId": -94061,
            "stayLength": 40,
            "latitude": 45.4,
            "longitude": -123.4,
            "customerName": "Bill Evans",
            "gender": "MALE",
            "country": "Japan",
            "state": "Miyagi",
            "active": False,
            "signature": signature,
            "picture": picture,
        }
        all_filled = ",".join(attributes)
        self._run_test_bios_insert_with_empty_values(all_filled, [expected])

        # signature2 = int(7226550).to_bytes(4, "little")
        expected2 = {
            "customerId": -1,
            "stayLength": 999,
            "latitude": -1.2,
            "longitude": 3.4,
            "customerName": "",
            "gender": "UNKNOWN",
            "country": "",
            "state": "",
            "active": True,
            "signature": b"",
            "picture": b"",
        }
        all_empty = ",,,,,,,,,,"
        self._run_test_bios_insert_with_empty_values(all_empty, [expected2])

        self._run_test_bios_insert_with_empty_values(
            [all_filled, all_empty], [expected, expected2]
        )

    def _run_test_bios_insert_with_empty_values(self, csv_or_bulk, expected_records):
        statement = bios.isql().insert().into(self.SIGNAL_ALL_TYPES_WITH_DEFAULT["signalName"])
        if isinstance(csv_or_bulk, str):
            statement = statement.csv(csv_or_bulk).build()
        else:
            statement = statement.csv_bulk(csv_or_bulk).build()

        ret = self.session.execute(statement)
        insert_record = ret.records[0]
        assert insert_record.timestamp > 0

        statement = (
            bios.isql()
            .select()
            .from_signal(self.SIGNAL_ALL_TYPES_WITH_DEFAULT.get("signalName"))
            .time_range(insert_record.timestamp, bios.time.seconds(10))
            .build()
        )
        response = self.session.execute(statement)

        assert len(response.data_windows) == 1
        assert len(response.data_windows[0].records) == len(expected_records)
        for i, expected in enumerate(expected_records):
            record = response.data_windows[0].records[i]
            assert record.event_id == ret.records[i].event_id
            assert record.timestamp == ret.records[i].timestamp
            for key, value in expected.items():
                assert record.get(key) == value, key

    def test_negative_upsert_into_signal(self):
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(
                bios.isql()
                .upsert()
                .into(SIGNAL_MAXIMUM.get("signalName"))
                .csv("one,two,3.0,4")
                .build()
            )
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert (
            "Invalid request: Context data operation may not be done against a signal;"
            in error.message
        )

    def test_negative_select_signal_by_keys(self):
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(
                bios.isql()
                .select()
                .from_context(SIGNAL_MAXIMUM.get("signalName"))
                .where(keys=[["whatever"]])
                .build()
            )
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert (
            "Invalid request: Context data operation may not be done against a signal;"
            in error.message
        )

    def test_negative_insert_nan(self):
        self._invalid_double_value_test("NaN")

    def test_negative_insert_neg_nan(self):
        self._invalid_double_value_test("-NaN")

    def test_negative_insert_pos_nan(self):
        self._invalid_double_value_test("+NaN")

    def test_negative_insert_inf(self):
        self._invalid_double_value_test("Infinity")

    def test_negative_insert_neg_inf(self):
        self._invalid_double_value_test("-Infinity")

    def test_negative_insert_pos_inf(self):
        self._invalid_double_value_test("+Infinity")

    def _invalid_double_value_test(self, src):
        single_insertion = (
            bios.isql()
            .insert()
            .into(SIGNAL_MAXIMUM.get("signalName"))
            .csv(f"hello,world,{src},-1")
            .build()
        )
        bulk_insertion = (
            bios.isql()
            .insert()
            .into(SIGNAL_MAXIMUM.get("signalName"))
            .csv_bulk([f"hello,world,{src},-1", "abc,def,2.4,20"])
            .build()
        )
        with pytest.raises(ServiceError) as exc_info:
            self.session.execute(single_insertion)
        error = exc_info.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert (
            f"Invalid value syntax: {src} value is not allowed; attribute=four, type=DECIMAL;"
            in error.message
        )
        with pytest.raises(ServiceError) as exc_info:
            self.session.execute(bulk_insertion)
        error = exc_info.value
        assert error.error_code == ErrorCode.BAD_INPUT
        assert (
            f"Invalid value syntax: {src} value is not allowed; attribute=four, type=DECIMAL;"
            in error.message
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
