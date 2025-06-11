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
import sys
import time
import unittest
from pprint import pprint

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import setup_context, setup_tenant_config

TENANT_NAME = "contextSynopsisTest"

ADMIN_USER = admin_user + "@" + TENANT_NAME
CONTEXT_WITH_FEATURE = {
    "contextName": "product",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "productId", "type": "Integer"},
        {"attributeName": "productName", "type": "String"},
        {"attributeName": "category", "type": "String"},
        {"attributeName": "price", "type": "Decimal"},
    ],
    "primaryKey": ["productId"],
    "auditEnabled": True,
    "features": [
        {
            "featureName": "byCategory",
            "dimensions": ["category"],
            "attributes": ["price"],
            "featureInterval": 15000,
            "aggregated": True,
            "indexed": False,
        }
    ],
}

PRODUCT_CATEGORY = {
    "contextName": "productCategory",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "categoryId", "type": "String"},
        {"attributeName": "groupTitle", "type": "String"},
        {"attributeName": "categoryTitle", "type": "String"},
    ],
    "primaryKey": ["categoryId"],
    "auditEnabled": True,
}

CONTEXT_WITH_ENRICHMENT = {
    "contextName": "productCatalog",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "productId", "type": "Integer"},
        {"attributeName": "productName", "type": "String"},
        {"attributeName": "categoryId", "type": "String"},
        {"attributeName": "price", "type": "Decimal"},
    ],
    "primaryKey": ["productId"],
    "auditEnabled": True,
    "enrichments": [
        {
            "enrichmentName": "categoryLookup",
            "foreignKey": ["categoryId"],
            "missingLookupPolicy": "StoreFillInValue",
            "enrichedAttributes": [
                {
                    "value": "productCategory.groupTitle",
                    "as": "groupTitle",
                    "fillIn": "",
                },
                {
                    "value": "productCategory.categoryTitle",
                    "as": "categoryTitle",
                    "fillIn": "",
                },
            ],
        },
    ],
}


class BiosContextFeatureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config(tenant_name=TENANT_NAME, clear_tenant=True)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    @classmethod
    def upsert_entries(cls, context_name, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().upsert().into(context_name).csv_bulk(records).build()
        cls.session.execute(request)

    @classmethod
    def insert_entries(cls, signal_name, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(signal_name).csv_bulk(records).build()
        cls.session.execute(request)

    @classmethod
    def update_entries(cls, context_name, key, values):
        update_request = bios.isql().update(context_name).set(values).where(key=key).build()
        cls.session.execute(update_request)

    @classmethod
    def delete_entries(cls, context_name, keys):
        delete_request = bios.isql().delete().from_context(context_name).where(keys=keys).build()
        cls.session.execute(delete_request)

    def test_simple_context_synopses(self):
        context_config = copy.deepcopy(CONTEXT_WITH_FEATURE)
        context_name = context_config.get("contextName")
        print(f"\n ... creating context context {context_name}")
        setup_context(self.session, context_config)

        print(" ... upserting entries")
        self.upsert_entries(
            context_name,
            [
                "10001,Detergent,Cleaning,5.00",
                "10002,Light Bulbs,Hardwares,15.00",
                "10003,Notebook,Stationaries,2.68",
                "10004,Ballpoint Pens,Stationaries,10.23",
                "10005,Bloom,Cleaning,18.13",
                "10006,Sparkling Water - Orange Flavor,Beverages,21.13",
                "10007,Sponges,Cleaning,3.45",
            ],
        )

        time0 = bios.time.now()
        self.session.feature_refresh(context_name)

        print(" ... sleeping for 20 seconds to ensure at least one maintenance cycle is done")
        time.sleep(20)
        time1 = bios.time.now()
        context_synopses = self.session.get_all_context_synopses()
        pprint(context_synopses)
        contexts = self.session.get_contexts(include_internal=True)
        assert len(context_synopses.get("contexts")) == len(contexts)
        found = False
        for synopsis in context_synopses.get("contexts"):
            if synopsis.get("contextName") == context_name:
                found = True
                assert synopsis.get("count") == 7
                assert synopsis.get("lastUpdated") > time0
                assert synopsis.get("lastUpdated") < time1
                break
        assert found

        context_synopsis = self.session.get_context_synopsis(context_name)
        pprint(context_synopsis)
        attributes = context_synopsis.get("attributes")
        assert len(attributes) == 4
        assert attributes[0].get("attributeName") == "productId"
        assert attributes[0].get("attributeType") == "Integer"
        assert attributes[0].get("attributeOrigin") == "ORIGINAL"
        assert attributes[0].get("count") == [7]
        assert attributes[0].get("distinctCount")[0] == pytest.approx(7, 1.0e-3)
        assert attributes[0].get("firstSummary") == "AVG"
        assert attributes[0].get("firstSummaryCurrent") == pytest.approx(10004, 1.0e-3)

        assert attributes[1].get("attributeName") == "productName"
        assert attributes[1].get("attributeType") == "String"
        assert attributes[1].get("attributeOrigin") == "ORIGINAL"
        assert attributes[1].get("count") == [7]
        assert attributes[1].get("distinctCount")[0] == pytest.approx(7, 1.0e-3)
        assert attributes[1].get("firstSummary") == "WORD_CLOUD"
        assert attributes[1].get("samples") == [
            "Ballpoint Pens",
            "Bloom",
            "Detergent",
            "Light Bulbs",
            "Notebook",
            "Sparkling Water - Orange Flavor",
            "Sponges",
        ]
        assert attributes[1].get("sampleCounts") == [1, 1, 1, 1, 1, 1, 1]
        assert attributes[1].get("sampleLengths") == [14, 5, 9, 11, 8, 31, 7]

        assert attributes[2].get("attributeName") == "category"
        assert attributes[2].get("attributeType") == "String"
        assert attributes[2].get("attributeOrigin") == "ORIGINAL"
        assert attributes[2].get("count") == [7]
        assert attributes[2].get("distinctCount")[0] == pytest.approx(4, 1.0e-3)
        assert attributes[2].get("firstSummary") == "WORD_CLOUD"
        assert attributes[2].get("samples") == [
            "Cleaning",
            "Stationaries",
            "Beverages",
            "Hardwares",
        ]
        assert attributes[2].get("sampleCounts") == [3, 2, 1, 1]
        assert attributes[2].get("sampleLengths") == [8, 12, 9, 9]

        assert attributes[3].get("attributeName") == "price"
        assert attributes[3].get("attributeType") == "Decimal"
        assert attributes[3].get("attributeOrigin") == "ORIGINAL"
        assert attributes[3].get("count") == [7]
        assert attributes[3].get("distinctCount")[0] == pytest.approx(7, 1.0e-3)
        assert attributes[3].get("firstSummary") == "AVG"
        assert attributes[3].get("firstSummaryCurrent") == pytest.approx(10.8, 1.0e-3)

    def test_enriched_context_synopses(self):
        setup_context(self.session, PRODUCT_CATEGORY)
        context_config = copy.deepcopy(CONTEXT_WITH_ENRICHMENT)
        context_name = context_config.get("contextName")
        print(f"\n ... creating context context {context_name}")
        setup_context(self.session, context_config)

        print(" ... upserting entries")
        self.upsert_entries(
            PRODUCT_CATEGORY.get("contextName"),
            [
                "prod001,Home,Cleaning",
                "prod002,Home,Hardware",
                "prod003,Stationaries,Pens",
                "prod004,Stationaries,Notebooks",
                "prod005,Food,Beverages",
            ],
        )

        self.upsert_entries(
            context_name,
            [
                "10001,Detergent,prod001,5.00",
                "10002,Light Bulbs,prod002,15.00",
                "10003,Notebook,prod003,2.68",
                "10004,Ballpoint Pens,prod004,10.23",
                "10005,Bloom,prod001,18.13",
                "10006,Sparkling Water - Orange Flavor,prod005,21.13",
                "10007,Sponges,prod001,3.45",
                "10008,Pencil,prod004,2.43",
            ],
        )

        time0 = bios.time.now()

        self.session.feature_refresh(context_name)
        self.session.feature_refresh("productCategory")

        print(" ... sleeping for 20 seconds to ensure at least one maintenance cycle is done")
        time.sleep(20)
        time1 = bios.time.now()

        context_synopses = self.session.get_all_context_synopses()
        pprint(context_synopses)
        contexts = self.session.get_contexts(include_internal=True)
        assert len(context_synopses.get("contexts")) == len(contexts)
        found = False
        for synopsis in context_synopses.get("contexts"):
            if synopsis.get("contextName") == context_name:
                found = True
                assert synopsis.get("count") == 8
                break
        assert found

        context_synopsis = self.session.get_context_synopsis(context_name)
        pprint(context_synopsis)
        attributes = context_synopsis.get("attributes")
        assert len(attributes) == 6
        assert attributes[0].get("attributeName") == "productId"
        assert attributes[0].get("attributeType") == "Integer"
        assert attributes[0].get("attributeOrigin") == "ORIGINAL"
        assert attributes[0].get("count") == [8]
        assert attributes[0].get("distinctCount")[0] == pytest.approx(8, 1.0e-3)
        assert attributes[0].get("firstSummary") == "AVG"
        assert attributes[0].get("firstSummaryCurrent") == pytest.approx(10004.5, 1.0e-3)
        assert attributes[0].get("lastUpdated") > time0
        assert attributes[0].get("lastUpdated") < time1

        assert attributes[1].get("attributeName") == "productName"
        assert attributes[1].get("attributeType") == "String"
        assert attributes[1].get("attributeOrigin") == "ORIGINAL"
        assert attributes[1].get("count") == [8]
        assert attributes[1].get("distinctCount")[0] == pytest.approx(8, 1.0e-3)
        assert attributes[1].get("firstSummary") == "WORD_CLOUD"
        assert attributes[1].get("samples") == [
            "Ballpoint Pens",
            "Bloom",
            "Detergent",
            "Light Bulbs",
            "Notebook",
            "Pencil",
            "Sparkling Water - Orange Flavor",
            "Sponges",
        ]
        assert attributes[1].get("sampleCounts") == [1, 1, 1, 1, 1, 1, 1, 1]
        assert attributes[1].get("sampleLengths") == [14, 5, 9, 11, 8, 6, 31, 7]
        assert attributes[1].get("lastUpdated") > time0
        assert attributes[1].get("lastUpdated") < time1

        assert attributes[2].get("attributeName") == "categoryId"
        assert attributes[2].get("attributeType") == "String"
        assert attributes[2].get("attributeOrigin") == "ORIGINAL"
        assert attributes[2].get("count") == [8]
        assert attributes[2].get("distinctCount")[0] == pytest.approx(5, 1.0e-3)
        assert attributes[2].get("firstSummary") == "WORD_CLOUD"
        assert attributes[2].get("samples") == [
            "prod001",
            "prod004",
            "prod002",
            "prod003",
            "prod005",
        ]
        assert attributes[2].get("sampleCounts") == [3, 2, 1, 1, 1]
        assert attributes[2].get("sampleLengths") == [7, 7, 7, 7, 7]
        assert attributes[2].get("lastUpdated") > time0
        assert attributes[2].get("lastUpdated") < time1

        assert attributes[3].get("attributeName") == "price"
        assert attributes[3].get("attributeType") == "Decimal"
        assert attributes[3].get("attributeOrigin") == "ORIGINAL"
        assert attributes[3].get("count") == [8]
        assert attributes[3].get("distinctCount")[0] == pytest.approx(8, 1.0e-3)
        assert attributes[3].get("firstSummary") == "AVG"
        assert attributes[3].get("firstSummaryCurrent") == pytest.approx(9.75625)
        assert attributes[3].get("lastUpdated") > time0
        assert attributes[3].get("lastUpdated") < time1

        assert attributes[4].get("attributeName") == "groupTitle"
        assert attributes[4].get("attributeType") == "String"
        assert attributes[4].get("attributeOrigin") == "ENRICHED"
        assert attributes[4].get("count") == [5]
        assert attributes[4].get("distinctCount")[0] == pytest.approx(3, 1.0e-3)
        assert attributes[4].get("firstSummary") == "WORD_CLOUD"
        assert attributes[4].get("samples") == ["Home", "Stationaries", "Food"]
        assert attributes[4].get("sampleCounts") == [2, 2, 1]
        assert attributes[4].get("sampleLengths") == [4, 12, 4]
        assert attributes[4].get("lastUpdated") > time0
        assert attributes[4].get("lastUpdated") < time1

        assert attributes[5].get("attributeName") == "categoryTitle"
        assert attributes[5].get("attributeType") == "String"
        assert attributes[5].get("attributeOrigin") == "ENRICHED"
        assert attributes[5].get("count") == [5]
        assert attributes[5].get("distinctCount")[0] == pytest.approx(5, 1.0e-3)
        assert attributes[5].get("firstSummary") == "WORD_CLOUD"
        assert attributes[5].get("samples") == [
            "Beverages",
            "Cleaning",
            "Hardware",
            "Notebooks",
            "Pens",
        ]
        assert attributes[5].get("sampleCounts") == [1, 1, 1, 1, 1]
        assert attributes[5].get("sampleLengths") == [9, 8, 8, 9, 4]
        assert attributes[5].get("lastUpdated") > time0
        assert attributes[5].get("lastUpdated") < time1


if __name__ == "__main__":
    pytest.main(sys.argv)
