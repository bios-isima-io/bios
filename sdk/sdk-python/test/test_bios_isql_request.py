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
import datetime
import json
import os
import sys
import unittest
from collections import OrderedDict
from datetime import datetime
from unittest import mock

import pytest

from bios import ErrorCode, ServiceError, rollback_on_failure
from bios._proto._bios import data_pb2
from bios.client import Client
from bios.isql_insert_request import ISqlInsertBulkCompleteRequest
from bios.isql_request import ISqlRequest, ISqlRequestType
from bios.isql_select_request import ISqlSelectRequestMessage

CLICK_JSON = "resources/click.json"
CLICK_SINGLE_JSON = "resources/click_single.json"
DOUBLE_NESTED_JSON = "resources/double-nested.json"
COMMON_KEY_JSON = "resources/common-key-nested.json"
NESTED_WITH_ROOT_ATTRIBS_JSON = "resources/nested-with-root-attribs.json"


def transform_function(x, y, date_str):
    return f'{x}_{y}_{datetime.strptime(str(date_str), "%Y-%m-%d")}'


class BiosIsqlRequestTest(unittest.TestCase):
    @classmethod
    @mock.patch("bios.client.BiosCSdkSession")
    def setUpClass(cls, mock_CSdkSession):
        cls.session = Client("mock_host", 443, True, ".")
        cls.mock_csdk = mock_CSdkSession

    def test_isql_statement_of_type_str_is_not_supported(self):
        with self.assertRaises(ServiceError) as context:
            self.session.execute("something")
        self.assertEqual(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        self.assertEqual(
            context.exception.message,
            "Statement must be an instance of ISqlRequestMessage",
        )

    def test_isql_statement_which_is_not_built_fails(self):
        statement = ISqlRequest().insert().into("signal").csv("val")
        with self.assertRaises(ServiceError) as context:
            self.session.execute(statement)
        self.assertEqual(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        self.assertEqual(
            context.exception.message,
            "Statement must be an instance of ISqlRequestMessage",
        )

    def test_isql_statement_of_type_int_is_supported(self):
        statement = ISqlRequest().insert().into("somesignal").values([123]).build()
        self.assertEqual(
            statement.request.content_rep,
            data_pb2.ContentRepresentation.CSV,
        )
        self.session.execute(statement)
        self.mock_csdk.assert_called()

    def test_isql_insert_statement(self):
        statement = ISqlRequest().insert().into("somesignal").csv("123,456").build()
        self.assertEqual(
            statement.request.content_rep,
            data_pb2.ContentRepresentation.CSV,
        )
        self.session.execute(statement)
        self.mock_csdk.assert_called()

    def test_isql_insert_bulk_statement(self):
        statement = (
            ISqlRequest().insert().into("somesignal").csv_bulk(["123,456", "789,012"]).build()
        )
        self.assertEqual(
            statement.content_representation,
            data_pb2.ContentRepresentation.CSV,
        )
        self.session.execute(statement)
        self.mock_csdk.assert_called()

    def test_isql_statement_with_list_of_arguments(self):
        statement = ISqlRequest().insert().into("somesignal").values(["abc", "123", 12.4]).build()
        self.assertEqual(
            statement.request.content_rep,
            data_pb2.ContentRepresentation.CSV,
        )
        self.session.execute(statement)
        self.mock_csdk.assert_called()

    def test_isql_insert_bulk_with_values(self):
        statement = (
            ISqlRequest()
            .insert()
            .into("somesignal")
            .values_bulk([["aaa", "111", 12.4], ["bbb", "222", 12.4], ["ccc", "333", 12.4]])
            .build()
        )
        self.assertEqual(
            statement.content_representation,
            data_pb2.ContentRepresentation.CSV,
        )
        self.session.execute(statement)
        self.mock_csdk.assert_called()

    def test_isql_insert_json_bulk(self):
        click_mapping = {
            "commonKeys": ["profiles", "*", "event_properties"],
            "data": OrderedDict(
                [
                    ("impressionTrigger", "trigger"),
                    ("destination", "destination"),
                    ("destinationId", "destination_id"),
                ]
            ),
        }
        with open(CLICK_JSON, "r") as file:
            click_json = json.load(file)
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertIsInstance(insert_request, ISqlInsertBulkCompleteRequest)
        self.assertIsInstance(insert_request.values, list)
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )
        self.assertEqual(
            insert_request.content_representation,
            data_pb2.ContentRepresentation.CSV,
        )

    def test_isql_insert_json(self):
        click_mapping = {
            "commonKeys": ["profiles", "event_properties"],
            "data": OrderedDict(
                [
                    ("impressionTrigger", "trigger"),
                    ("destination", "destination"),
                    ("destinationId", "destination_id"),
                ]
            ),
        }
        with open(CLICK_SINGLE_JSON, "r") as file:
            click_json = json.load(file)
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertIsInstance(insert_request.values, str)
        self.assertEqual(
            insert_request.values,
            '"on_scroll","p_product_details",""',
        )

    def test_isql_insert_json_no_common(self):
        click_mapping = {
            "data": OrderedDict(
                [
                    (
                        "impressionTrigger",
                        ["profiles", "event_properties", "trigger"],
                    ),
                    (
                        "destination",
                        [
                            "profiles",
                            "event_properties",
                            "destination",
                        ],
                    ),
                    (
                        "destinationId",
                        [
                            "profiles",
                            "event_properties",
                            "destination_id",
                        ],
                    ),
                ]
            ),
        }
        with open(CLICK_SINGLE_JSON, "r") as file:
            click_json = json.load(file)
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertIsInstance(insert_request.values, str)
        self.assertEqual(
            insert_request.values,
            '"on_scroll","p_product_details",""',
        )

    def test_isql_insert_json_split_common(self):
        click_mapping = {
            "commonKeys": ["profiles", "*"],
            "data": OrderedDict(
                [
                    (
                        "impressionTrigger",
                        ["event_properties", "trigger"],
                    ),
                    (
                        "destination",
                        ["event_properties", "destination"],
                    ),
                    (
                        "destinationId",
                        ["event_properties", "destination_id"],
                    ),
                ]
            ),
        }
        with open(CLICK_JSON, "r") as file:
            click_json = json.load(file)
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertIsInstance(insert_request.values, list)
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )

    def test_isql_insert_json_split_common_single(self):
        click_mapping = {
            "commonKeys": ["profiles"],
            "data": OrderedDict(
                [
                    (
                        "impressionTrigger",
                        ["event_properties", "trigger"],
                    ),
                    (
                        "destination",
                        ["event_properties", "destination"],
                    ),
                    (
                        "destinationId",
                        ["event_properties", "destination_id"],
                    ),
                ]
            ),
        }
        with open(CLICK_SINGLE_JSON, "r") as file:
            click_json = json.load(file)
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertIsInstance(insert_request.values, str)
        self.assertEqual(
            insert_request.values,
            '"on_scroll","p_product_details",""',
        )

    def test_isql_insert_json_split_common_leaf_list(self):
        click_mapping = {
            "commonKeys": ["profiles"],
            "data": OrderedDict(
                [
                    (
                        "impressionTrigger",
                        ["*", "event_properties", "trigger"],
                    ),
                    (
                        "destination",
                        ["event_properties", "destination"],
                    ),
                    (
                        "destinationId",
                        ["event_properties", "destination_id"],
                    ),
                ]
            ),
        }
        with open(CLICK_JSON, "r") as file:
            click_json = json.load(file)
        with self.assertRaises(ServiceError) as e:
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertEqual(e.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_isql_insert_json_split_common_single_leaf_list(self):
        click_mapping = {
            "commonKeys": ["profiles"],
            "data": OrderedDict(
                [
                    (
                        "impressionTrigger",
                        ["*", "event_properties", "trigger"],
                    ),
                    (
                        "destination",
                        ["event_properties", "destination"],
                    ),
                    (
                        "destinationId",
                        ["event_properties", "destination_id"],
                    ),
                ]
            ),
        }
        with open(CLICK_SINGLE_JSON, "r") as file:
            click_json = json.load(file)
        with self.assertRaises(ServiceError) as e:
            insert_request = (
                ISqlRequest().insert().into("testSignal").json(click_json, click_mapping).build()
            )
        self.assertEqual(e.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_isql_insert_json_double_nested_array(self):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    ("itemName", "item"),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            self.assertEqual(
                insert_request.values,
                [
                    '"Manish","Crocin","6"',
                    '"Manish","Dolo650","10"',
                    '"Manish","Dolo650","10"',
                    '"Manish","Betadine","1"',
                    '"Aditya","Crocin","5"',
                    '"Aditya","Dolo650","12"',
                    '"Aditya","Betadine","3"',
                ],
            )

    def test_nested_with_root_attribs(self):
        nested_with_root_attribs_mapping = {
            "commonKeys": ["childOrders", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("itemId", "itemId"),
                    ("itemType", "itemType"),
                    ("name", "name"),
                    ("quantity", "quantity"),
                    ("couponCode", "/couponCode"),
                ]
            ),
        }
        with open(NESTED_WITH_ROOT_ATTRIBS_JSON, "r") as file:
            nested_with_root_attribs = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(nested_with_root_attribs, nested_with_root_attribs_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            self.assertEqual(
                insert_request.values,
                ['"I44637","COSMETICS","soap","3","ABC123"'],
            )

    def test_isql_insert_json_with_simple_transforms(self):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    (
                        "itemName",
                        {
                            "keys": ["item", "user"],
                            "transform": """ lambda x, y: f"{x}_{y}" """,
                            "default": "UNKNOWN",
                        },
                    ),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                '"Manish","Crocin_Manish","6"',
                '"Manish","Dolo650_Manish","10"',
                '"Manish","Dolo650_Manish","10"',
                '"Manish","Betadine_Manish","1"',
                '"Aditya","Crocin_Aditya","5"',
                '"Aditya","Dolo650_Aditya","12"',
                '"Aditya","Betadine_Aditya","3"',
            ]
            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_isql_insert_json_with_simple_transforms_callable(self):
        year = datetime.strptime("2020-12-18", "%Y-%m-%d")
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    (
                        "itemName",
                        {
                            "keys": ["item", "user", "date_str"],
                            "transform": transform_function,
                            "default": "UNKNOWN",
                        },
                    ),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                f'"Manish","Crocin_Manish_{year}","6"',
                f'"Manish","Dolo650_Manish_{year}","10"',
                f'"Manish","Dolo650_Manish_{year}","10"',
                f'"Manish","Betadine_Manish_{year}","1"',
                f'"Aditya","Crocin_Aditya_{year}","5"',
                f'"Aditya","Dolo650_Aditya_{year}","12"',
                f'"Aditya","Betadine_Aditya_{year}","3"',
            ]

            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_isql_insert_json_with_simple_transforms_default_if_not_key_in_items(
        self,
    ):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    (
                        "itemName",
                        {
                            "keys": ["item", "user2"],
                            "transform": """ lambda x, y: f"{x}_{y}" """,
                            "default": "UNKNOWN",
                        },
                    ),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                '"Manish","Crocin_UNKNOWN","6"',
                '"Manish","Dolo650_UNKNOWN","10"',
                '"Manish","Dolo650_UNKNOWN","10"',
                '"Manish","Betadine_UNKNOWN","1"',
                '"Aditya","Crocin_UNKNOWN","5"',
                '"Aditya","Dolo650_UNKNOWN","12"',
                '"Aditya","Betadine_UNKNOWN","3"',
            ]
            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_isql_insert_json_with_simple_transforms_default_if_not_key(
        self,
    ):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    (
                        "itemName",
                        {
                            "keys": ["item2"],
                            "transform": """ lambda x : x """,
                            "default": "UNKNOWN",
                        },
                    ),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                '"Manish","UNKNOWN","6"',
                '"Manish","UNKNOWN","10"',
                '"Manish","UNKNOWN","10"',
                '"Manish","UNKNOWN","1"',
                '"Aditya","UNKNOWN","5"',
                '"Aditya","UNKNOWN","12"',
                '"Aditya","UNKNOWN","3"',
            ]
            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_isql_insert_json_with_multiple_transforms(self):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    (
                        "itemName",
                        {
                            "keys": ["item2"],
                            "transform": """ lambda x : x """,
                            "default": "UNKNOWN",
                        },
                    ),
                    (
                        "item",
                        {
                            "keys": ["item", "user", "quantity"],
                            "transform": """lambda i, u, q: f"user {u} has bought {q} {i}" """,
                        },
                    ),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                '"Manish","UNKNOWN","user Manish has bought 6 Crocin","6"',
                '"Manish","UNKNOWN","user Manish has bought 10 Dolo650","10"',
                '"Manish","UNKNOWN","user Manish has bought 1 Betadine","1"',
                '"Aditya","UNKNOWN","user Aditya has bought 5 Crocin","5"',
                '"Aditya","UNKNOWN","user Aditya has bought 12 Dolo650","12"',
                '"Aditya","UNKNOWN","user Aditya has bought 3 Betadine","3"',
            ]
            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_isql_insert_json_with_simple_transforms_are_prevented_with_environment(
        self,
    ):
        os.environ["BIOS_LAMBDA_DISABLED"] = "true"
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("userName", "user"),
                    (
                        "itemName",
                        {
                            "keys": ["item", "user"],
                            "transform": """ lambda x, y: f"{x}_{y}" """,
                            "default": "UNKNOWN",
                        },
                    ),
                    ("itemQuantity", "quantity"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
        with self.assertRaises(ServiceError) as e:
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )
        self.assertEqual(e.exception.error_code, ErrorCode.INVALID_REQUEST)
        del os.environ["BIOS_LAMBDA_DISABLED"]

    def test_isql_insert_json_index_of_arrays(self):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("cartIndex", "index(carts)"),
                    ("itemIndex", "index(items)"),
                    ("userName", "user"),
                    ("itemName", "item"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                '"0","0","Manish","Crocin"',
                '"0","1","Manish","Dolo650"',
                '"0","2","Manish","Dolo650"',
                '"0","3","Manish","Betadine"',
                '"1","0","Aditya","Crocin"',
                '"1","1","Aditya","Dolo650"',
                '"1","2","Aditya","Betadine"',
            ]
            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_isql_insert_json_index_of_arrays_with_nameless_nested_array(
        self,
    ):
        double_nested_mapping = {
            "commonKeys": ["carts", "*", "*"],
            "data": OrderedDict(
                [
                    ("cartIndex", "index(carts)"),
                    ("userName", "user"),
                    ("itemName", "item"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            with self.assertRaises(ServiceError) as e:
                ISqlRequest().insert().into("testSignal").json(
                    double_nested, double_nested_mapping
                ).build()
            self.assertEqual(e.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_isql_insert_json_index_of_arrays_with_nameless_nested_array_at_the_begining(
        self,
    ):
        double_nested_mapping = {
            "commonKeys": ["*", "carts", "*"],
            "data": OrderedDict(
                [
                    ("cartIndex", "index(carts)"),
                    ("userName", "user"),
                    ("itemName", "item"),
                ]
            ),
        }
        with open(DOUBLE_NESTED_JSON, "r") as file:
            double_nested = json.load(file)
            with self.assertRaises(ServiceError) as e:
                ISqlRequest().insert().into("testSignal").json(
                    double_nested, double_nested_mapping
                ).build()
            self.assertEqual(e.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_isql_insert_json_index_of_arrays_with_common_key(self):
        double_nested_mapping = {
            "commonKeys": ["more", "carts", "*", "items", "*"],
            "data": OrderedDict(
                [
                    ("cartIndex", "index(carts)"),
                    ("itemIndex", "index(items)"),
                    ("userName", "user"),
                    ("itemName", "item"),
                ]
            ),
        }
        with open(COMMON_KEY_JSON, "r") as file:
            double_nested = json.load(file)
            insert_request = (
                ISqlRequest()
                .insert()
                .into("testSignal")
                .json(double_nested, double_nested_mapping)
                .build()
            )

            self.assertIsInstance(insert_request.values, list)
            expected_values = [
                '"0","0","Manish","Crocin"',
                '"0","1","Manish","Dolo650"',
                '"0","2","Manish","Dolo650"',
                '"0","3","Manish","Betadine"',
                '"1","0","Aditya","Crocin"',
                '"1","1","Aditya","Dolo650"',
                '"1","2","Aditya","Betadine"',
            ]
            for e in expected_values:
                self.assertIn(e, insert_request.values)

    def test_snapped_time_range_calc(self):
        origin = 1685576262000
        interval = 60000
        delta = interval * 6

        (start, end) = ISqlSelectRequestMessage._calculate_snapped_time_range(
            origin, delta, interval, True
        )
        assert start == 1685576220000
        assert end == 1685576622000

        (start, end) = ISqlSelectRequestMessage._calculate_snapped_time_range(
            origin + delta, -delta, interval, True
        )
        assert start == 1685576220000
        assert end == 1685576622000

    def test_rollback_on_failure_statement(self):
        child_statement = ISqlRequest().insert().into("somesignal").csv("123,456").build()
        statement = rollback_on_failure(child_statement)
        assert statement.get_type() == ISqlRequestType.ATOMIC_MUTATION
        assert statement.statements[0] == child_statement

    def test_rollback_on_failure_nested(self):
        child_statement = ISqlRequest().insert().into("somesignal").csv("123,456").build()
        statement = rollback_on_failure(child_statement)
        with pytest.raises(ServiceError) as exc_info:
            rollback_on_failure(statement)
        assert exc_info.value.error_code == ErrorCode.INVALID_ARGUMENT

    def test_wrong_type_to_rollback_on_failure(self):
        invalid_statement = 123
        with pytest.raises(ServiceError) as exc_info:
            rollback_on_failure(invalid_statement)
        assert exc_info.value.error_code == ErrorCode.INVALID_ARGUMENT


if __name__ == "__main__":
    pytest.main(sys.argv)
