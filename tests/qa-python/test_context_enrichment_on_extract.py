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
import sys
import unittest
from typing import Tuple

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config


class TestContextEnrichmentOnExtraction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.INTERVAL = 60000
        cls.CONTEXT_PRODUCT_CATEGORY_NAME = "productCategory"
        cls.CONTEXT_PRODUCT_CATEGORY = {
            "contextName": cls.CONTEXT_PRODUCT_CATEGORY_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "categoryKey", "type": "String"},
                {"attributeName": "categoryId", "type": "Integer"},
                {"attributeName": "groupTitle", "type": "String"},
                {"attributeName": "categoryTitle", "type": "String"},
                {"attributeName": "subCategoryTitle", "type": "String"},
            ],
            "primaryKey": ["categoryKey"],
            "auditEnabled": False,
        }

        cls.CONTEXT_PRODUCT_TO_CATEGORY_NAME = "productToCategory"
        cls.CONTEXT_PRODUCT_TO_CATEGORY = {
            "contextName": cls.CONTEXT_PRODUCT_TO_CATEGORY_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "productKey", "type": "String"},
                {"attributeName": "categoryKey", "type": "String"},
            ],
            "primaryKey": ["productKey"],
            "auditEnabled": False,
        }

        cls.CONTEXT_PRODUCT_NAME = "product"
        cls.CONTEXT_PRODUCT = {
            "contextName": cls.CONTEXT_PRODUCT_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "productKey", "type": "String"},
                {"attributeName": "productId", "type": "Integer"},
                {"attributeName": "productName", "type": "String"},
            ],
            "primaryKey": ["productKey"],
            "enrichments": [
                {
                    "enrichmentName": "categoryKeyLookup",
                    "foreignKey": ["productKey"],
                    "missingLookupPolicy": "StoreFillInValue",
                    "enrichedAttributes": [
                        {
                            "value": "productToCategory.categoryKey",
                            "as": "categoryKey",
                            "fillIn": "",
                        }
                    ],
                },
                {
                    "enrichmentName": "categoryLookup",
                    "foreignKey": ["categoryKey"],
                    "missingLookupPolicy": "StoreFillInValue",
                    "enrichedAttributes": [
                        {
                            "value": "productCategory.categoryId",
                            "as": "categoryId",
                            "fillIn": 0,
                        },
                        {
                            "value": "productCategory.categoryTitle",
                            "as": "categoryTitle",
                            "fillIn": "",
                        },
                    ],
                },
            ],
            "auditEnabled": False,
        }

        cls.ADMIN_USER = admin_user + "@" + TENANT_NAME

        setup_tenant_config()

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            try:
                admin.delete_context(cls.CONTEXT_PRODUCT_NAME)
            except ServiceError:
                pass

            try:
                admin.delete_context(cls.CONTEXT_PRODUCT_CATEGORY_NAME)
            except ServiceError:
                pass
            admin.create_context(cls.CONTEXT_PRODUCT_CATEGORY)

            try:
                admin.delete_context(cls.CONTEXT_PRODUCT_TO_CATEGORY_NAME)
            except ServiceError:
                pass
            admin.create_context(cls.CONTEXT_PRODUCT_TO_CATEGORY)

            category_entries = [
                "ca.us_12345,12345,group01,category01,subcategory01",  # 0
                "ca.us_23456,23456,group02,category02,subcategory02",  # 1
                "tk.jp_12345,12345,group03,category03,subcategory03",  # 2
            ]
            admin.execute(
                bios.isql()
                .upsert()
                .into(cls.CONTEXT_PRODUCT_CATEGORY_NAME)
                .csv_bulk(category_entries)
                .build()
            )

            to_category_entries = [
                "ca.us_98765,ca.us_12345",
                "ca.us_98764,ca.us_12345",
                "ca.us_98653,ca.us_22345",
                "tk.jp_98765,tk.jp_12345",
            ]
            admin.execute(
                bios.isql()
                .upsert()
                .into(cls.CONTEXT_PRODUCT_TO_CATEGORY_NAME)
                .csv_bulk(to_category_entries)
                .build()
            )

    def setUp(self):
        self.client = bios.login(ep_url(), self.ADMIN_USER, admin_pass)

    def tearDown(self):
        self.client.close()

    def test_fundamental(self):
        self.client.create_context(self.CONTEXT_PRODUCT)
        retrieved_context = self.client.get_context(self.CONTEXT_PRODUCT_NAME)
        assert retrieved_context.get("auditEnabled") is True

        products = [
            "ca.us_98765,98765,product01",
            "ca.us_98764,98764,product02",
            "ca.us_98653,98653,product03",
            "tk.jp_98765,98765,product04",
            "tk.jp_99999,99999,product05",
        ]
        self.client.execute(
            bios.isql().upsert().into(self.CONTEXT_PRODUCT_NAME).csv_bulk(products).build()
        )

        keys = [
            ["ca.us_98765"],
            ["ca.us_98764"],
            ["ca.us_98653"],
            ["tk.jp_98765"],
            ["tk.jp_99999"],
        ]
        entries = self.client.execute(
            bios.isql().select().from_context(self.CONTEXT_PRODUCT_NAME).where(keys=keys).build()
        ).to_dict()

        expected_entries = [
            ("ca.us_98765", 98765, "product01", "ca.us_12345", 12345, "category01"),
            ("ca.us_98764", 98764, "product02", "ca.us_12345", 12345, "category01"),
            ("ca.us_98653", 98653, "product03", "ca.us_22345", 0, ""),
            ("tk.jp_98765", 98765, "product04", "tk.jp_12345", 12345, "category03"),
            ("tk.jp_99999", 99999, "product05", "", 0, ""),
        ]

        assert len(entries) == 5
        for i in range(5):
            assert self._get_product_attributes(entries[i]) == expected_entries[i], f"index={i}"

        #
        # Select by string where clause against the primary key
        #
        def make_verify(i: int):
            def verify_core():
                result = self.client.execute(
                    bios.isql()
                    .select()
                    .from_context(self.CONTEXT_PRODUCT_NAME)
                    .where(f"productKey = '{keys[i][0]}'")
                    .build()
                )
                assert (
                    self._get_product_attributes(result.get_records()[0]) == expected_entries[i]
                ), f"index=[{i}]"

            return verify_core

        make_verify(0)()
        make_verify(1)()
        make_verify(2)()
        make_verify(3)()
        make_verify(4)()

        #
        # Select by string where clause against a non-primary key
        #
        result = self.client.execute(
            bios.isql()
            .select()
            .from_context(self.CONTEXT_PRODUCT_NAME)
            .where(f"productId = 98765")
            .build()
        )

        records = result.get_records()
        assert len(records) == 2
        result_map = {record.get("productKey"): record for record in records}

        assert self._get_product_attributes(result_map[keys[0][0]]) == expected_entries[0]
        assert self._get_product_attributes(result_map[keys[3][0]]) == expected_entries[3]

    @classmethod
    def _get_product_attributes(cls, entry: dict) -> Tuple[str, int, str, str, int, str]:
        return (
            entry.get("productKey"),
            entry.get("productId"),
            entry.get("productName"),
            entry.get("categoryKey"),
            entry.get("categoryId"),
            entry.get("categoryTitle"),
        )

    def test_admin_negative_no_fill_in(self):
        context = copy.deepcopy(self.CONTEXT_PRODUCT)
        del context.get("enrichments")[1].get("enrichedAttributes")[1]["fillIn"]
        with self.assertRaises(ServiceError) as ec:
            self.client.create_context(context)
        self.assertEqual(ec.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertTrue("'fillIn' must be set" in ec.exception.message)


if __name__ == "__main__":
    pytest.main(sys.argv)
