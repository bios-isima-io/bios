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
import json
import unittest
import unittest.mock as mock
from collections import OrderedDict

from bios import ErrorCode, ServiceError, isql
from bios.client import Client

CLICK_JSON = "./resources/click.json"
CLICK_SINGLE_JSON = "./resources/click_single.json"
DOUBLE_NESTED_JSON = "./resources/double-nested.json"


class BiosIsqlRequestTest(unittest.TestCase):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertIsInstance(insert_request.values, list)
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )

    def test_isql_upsert_json(self):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )

    def test_isql_upsert_json_no_common(self):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )

    def test_isql_upsert_json_split_common(self):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )

    def test_isql_upsert_json_split_common_single(self):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertEqual(
            insert_request.values,
            ['"on_scroll","p_product_details",""'],
        )

    def test_isql_upsert_json_split_common_leaf_list(self):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertEqual(e.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_isql_upsert_json_split_common_single_leaf_list(self):
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
                isql().upsert().into("testContext").json(click_json, click_mapping).build()
            )
        self.assertEqual(e.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_isql_upsert_json_double_nested_array(self):
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
                isql()
                .upsert()
                .into("testContext")
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


if __name__ == "__main__":
    unittest.main()
