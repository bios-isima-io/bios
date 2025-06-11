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

from collections import OrderedDict
import json
import sys
import unittest

import pytest

from bios import ServiceError
from bios import login
import bios
from bios.models.isql_response_message import ISqlResponseType
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url


ADMIN_USER = admin_user + "@" + "_system"


class BiosIsqlRequestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_isql_insert_json(self):
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
        try:
            self.session.delete_signal("testSignal")
        except ServiceError:
            pass

        with open("../resources/test-insert-json-signal.json", "r", encoding="utf-8") as file:
            signal = file.read()
            self.session.create_signal(signal)

        with open("../resources/double-nested.json", "r", encoding="utf-8") as file:
            double_nested = json.load(file)
            insert_request = (
                bios.isql()
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
            response = self.session.execute(insert_request)
            self.assertEqual(response.response_type, ISqlResponseType.INSERT)
            print(response)


if __name__ == "__main__":
    pytest.main(sys.argv)
