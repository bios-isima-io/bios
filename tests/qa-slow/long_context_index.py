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
import time
import unittest
from pprint import pprint

import bios
import pytest
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config, try_delete_context
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
CONTEXT = {
    "contextName": "productCatalog",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "productId", "type": "String"},
        {
            "attributeName": "title",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "",
        },
        {
            "attributeName": "description",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "",
        },
        {
            "attributeName": "group",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "",
        },
        {
            "attributeName": "category",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "",
        },
        {
            "attributeName": "subCategory",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "",
        },
        {"attributeName": "salePrice", "type": "String"},
        {"attributeName": "inStockCount", "type": "Integer"},
        {
            "attributeName": "isAdult",
            "type": "Boolean",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": False,
        },
        {
            "attributeName": "status",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
    ],
    "primaryKey": ["productId"],
    "auditEnabled": True,
    "features": [
        {
            "featureName": "byStockCount",
            "dimensions": ["inStockCount"],
            "attributes": ["productId", "title"],
            "featureInterval": 30000,
            "indexed": True,
            "indexType": "RangeQuery",
        },
        {
            "featureName": "bycategory",
            "dimensions": ["category", "subCategory"],
            "attributes": ["title"],
            "featureInterval": 30000,
            "indexed": True,
            "indexType": "ExactMatch",
        },
        {
            "featureName": "byCategoryStockCount",
            "dimensions": ["category", "subCategory", "inStockCount"],
            "attributes": ["productId"],
            "featureInterval": 30000,
            "indexed": True,
            "indexType": "RangeQuery",
        },
    ],
}


class LongContextIndexTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_context(cls.session, CONTEXT)

    @classmethod
    def tearDownClass(cls):
        try_delete_context(cls.session, CONTEXT["contextName"])
        cls.session.close()

    @classmethod
    def upsert_entries(cls, context_name, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().upsert().into(context_name).csv_bulk(records).build()
        cls.session.execute(request)

    def test_index_queries(self):
        context_name = CONTEXT["contextName"]
        records = [
            "id1,product1,description1,group1,cat1,sub1,10,11,False,",
            "id2,product2,description2,group2,cat1,sub2,20,12,False,",
            "id3,product3,description3,group3,cat1,sub3,30,13,True,",
            "id4,product4,description4,group4,cat2,sub1,40,14,False,",
            "id5,product5,description5,group5,cat2,sub2,50,15,False,",
            "id6,product6,description6,group6,cat2,sub3,50,16,False,",
            "id7,product7,description7,group7,cat3,sub1,40,17,True,",
            "id8,product8,description8,group8,cat3,sub2,30,18,False,",
            "id9,product9,description9,group9,cat3,sub3,20,19,False,",
            "id10,product10,description10,group10,cat4,sub1,10,20,False,",
        ]
        self.upsert_entries(context_name, records)

        time.sleep(50)

        select_req = (
            bios.isql()
            .select()
            .from_context(context_name)
            .where("productId IN ('id1', 'id3', 'id8')")
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        self.assertEqual(len(select_resp.get_records()), 3)

        select_req = (
            bios.isql()
            .select("productId", "category", "title", "subCategory")
            .from_context(context_name)
            .where("category IN ('cat1', 'cat3') AND subCategory = 'sub2'")
            .build()
        )
        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records(), width=100)
        expected_records = [
            {"productId": "id2", "title": "product2", "category": "cat1", "subCategory": "sub2"},
            {"productId": "id8", "title": "product8", "category": "cat3", "subCategory": "sub2"},
        ]
        self.assertEqual(len(select_resp.get_records()), 2)
        for record in select_resp.get_records():
            assert record in expected_records

        select_req = (
            bios.isql()
            .select("productId", "category", "subCategory", "inStockCount")
            .from_context(context_name)
            .where(
                "category IN ('cat2', 'cat3', 'cat4') AND "
                "subCategory IN ('sub1', 'sub2', 'sub3') AND "
                "inStockCount >= 18"
            )
            .build()
        )
        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records(), width=100)

        self.assertEqual(len(select_resp.get_records()), 3)
        expected_records = [
            {"productId": "id8", "category": "cat3", "subCategory": "sub2", "inStockCount": 18},
            {"productId": "id9", "category": "cat3", "subCategory": "sub3", "inStockCount": 19},
            {"productId": "id10", "category": "cat4", "subCategory": "sub1", "inStockCount": 20},
        ]
        for record in select_resp.get_records():
            assert record in expected_records


if __name__ == "__main__":
    pytest.main(sys.argv)
