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
import time
import unittest

import bios
from tsetup import (
    admin_user,
    admin_pass,
)
from tsetup import get_endpoint_url as ep_url


class TestDiscoveryMySql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "simpleSignal"
        cls.IMPORT_SOURCE_ID = "209"

    def test_discovery(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            discovered_schema = client.discover_import_subjects(self.IMPORT_SOURCE_ID)

            #
            # Check the schema metadata
            #
            self.assertEqual(discovered_schema.get("importSourceName"), "MySQL Inventory")
            self.assertEqual(discovered_schema.get("importSourceId"), "209")

            #
            # Check whether the finder picks up all tables in the database
            #
            expected_tables = {
                "addresses",
                "customers",
                "geom",
                "orders",
                "products",
                "products_on_hand",
            }
            discovered_subjects = {}
            for subject in discovered_schema.get("subjects"):
                discovered_subjects[subject.get("subjectName")] = subject
            self.assertEqual(discovered_subjects.keys(), expected_tables)

            #
            # Check details of table 'products'
            #
            products = discovered_subjects["products"]
            self.assertEqual(len(products.get("sourceAttributes")), 6)
            self.assertEqual(products.get("payloadType"), "Json")

            # +-------------+--------------+------+-----+---------+----------------+
            # | Field       | Type         | Null | Key | Default | Extra          |
            # +-------------+--------------+------+-----+---------+----------------+
            # | id          | int          | NO   | PRI | NULL    | auto_increment |
            # | name        | varchar(255) | NO   |     | NULL    |                |
            # | description | varchar(512) | YES  |     | NULL    |                |
            # | weight      | float        | YES  |     | NULL    |                |
            # | active      | tinyint(1)   | YES  |     | NULL    |                |
            # | flag        | tinyint      | YES  |     | NULL    |                |
            # +-------------+--------------+------+-----+---------+----------------+
            attribute = products.get("sourceAttributes")[0]
            self.assertEqual(attribute.get("sourceAttributeName"), "id")
            self.assertEqual(attribute.get("suggestedType"), "Integer")
            self.assertEqual(attribute.get("originalType"), "INT")
            self.assertEqual(attribute.get("isNullable"), False)

            attribute = products.get("sourceAttributes")[1]
            self.assertEqual(attribute.get("sourceAttributeName"), "name")
            self.assertEqual(attribute.get("suggestedType"), "String")
            self.assertEqual(attribute.get("originalType"), "VARCHAR")
            self.assertEqual(attribute.get("isNullable"), False)

            attribute = products.get("sourceAttributes")[2]
            self.assertEqual(attribute.get("sourceAttributeName"), "description")
            self.assertEqual(attribute.get("suggestedType"), "String")
            self.assertEqual(attribute.get("originalType"), "VARCHAR")
            self.assertEqual(attribute.get("isNullable"), True)

            attribute = products.get("sourceAttributes")[3]
            self.assertEqual(attribute.get("sourceAttributeName"), "weight")
            self.assertEqual(attribute.get("suggestedType"), "Decimal")
            self.assertEqual(attribute.get("originalType"), "FLOAT")
            self.assertEqual(attribute.get("isNullable"), True)

            attribute = products.get("sourceAttributes")[4]
            self.assertEqual(attribute.get("sourceAttributeName"), "active")
            self.assertEqual(attribute.get("suggestedType"), "Integer")
            self.assertEqual(attribute.get("originalType"), "BIT")
            self.assertEqual(attribute.get("isNullable"), True)

            attribute = products.get("sourceAttributes")[5]
            self.assertEqual(attribute.get("sourceAttributeName"), "flag")
            self.assertEqual(attribute.get("suggestedType"), "Integer")
            self.assertEqual(attribute.get("originalType"), "TINYINT")
            self.assertEqual(attribute.get("isNullable"), True)


if __name__ == "__main__":
    unittest.main()
