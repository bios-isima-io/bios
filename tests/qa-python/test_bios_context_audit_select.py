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
import pprint
import random
import sys
import time
import unittest
from collections import OrderedDict

import pytest

import bios
import logging
from bios import ServiceError, ErrorCode
from tsetup import sadmin_user, sadmin_pass, admin_user, admin_pass
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as endpoint
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME, setup_tenant_config


ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

PRODUCT_CONTEXT_NAME = "productContext"
PRODUCT_AUDIT_SIGNAL_NAME = "auditProductContext"
PRODUCT_CONTEXT = {
    "contextName": PRODUCT_CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "product_id", "type": "integer"},
        {"attributeName": "product_name", "type": "string"},
        {"attributeName": "product_price", "type": "string"},
        {"attributeName": "source", "type": "string", "allowedValues": ["feed", "api"]},
    ],
    "auditEnabled": True,
    "primaryKey": ["product_id"],
}


def generate_test_data(count):
    products = []
    id_max = 10000000
    for product_id in range(count):
        name = "p" + str(product_id)
        price = random.randint(1, id_max)
        source = "api" if random.randint(0, 1) else "feed"
        products.append(
            OrderedDict(
                [
                    ("product_id", product_id),
                    ("product_name", name),
                    ("product_price", price),
                    ("source", source),
                ]
            )
        )
    return products


def to_csv(products):
    recs = []
    for item in products:
        record = ",".join([str(x) for x in list(item.values())])
        recs.append(record)
    return recs


def update_test_data(products, count):
    updated_products = copy.deepcopy(products)
    id_max = 100
    for index in range(count):
        entry = updated_products[index]
        entry["product_price"] = entry["product_price"] + random.randint(1, id_max)
    return updated_products


class ContextAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            try:
                admin.delete_context(PRODUCT_CONTEXT_NAME)
            except ServiceError:
                pass
            admin.create_context(PRODUCT_CONTEXT)
            cls.record_count = 10
            cls.products = generate_test_data(cls.record_count)
            records = to_csv(cls.products)
            ctx_upsert_request = (
                bios.isql().upsert().into(PRODUCT_CONTEXT_NAME).csv_bulk(records).build()
            )
            admin.execute(ctx_upsert_request)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_select_context_entries(self):
        request = (
            bios.isql()
            .select()
            .from_context(PRODUCT_CONTEXT_NAME)
            .where("product_name = 'p1'")
            .build()
        )
        reply = self.session.execute(request)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["product_name"], "p1")

    def test_select_audit_entries(self):
        # verify quickly that the audit signal attributes for a context attribute with allowed
        # values would have allowed values, too
        signal = self.session.get_signal(PRODUCT_AUDIT_SIGNAL_NAME)
        assert len(signal.get("attributes")) == 9
        assert signal.get("attributes")[7].get("allowedValues") == ["feed", "api"]
        assert signal.get("attributes")[8].get("allowedValues") == ["feed", "api"]

        start = bios.time.now()
        delta = -(bios.time.minutes(5))
        req = (
            bios.isql()
            .select()
            .from_signal(PRODUCT_AUDIT_SIGNAL_NAME)
            .time_range(start, delta)
            .build()
        )
        reply = self.session.execute(req)
        records = reply.data_windows[0].records
        self.assertEqual(len(records), self.record_count)
        # check insert count
        insertions = [r for r in records if r.get("_operation") == "Insert"]
        self.assertEqual(len(insertions), self.record_count)

        # update the records
        update_count = int(self.record_count / 2)
        updated_products = update_test_data(self.products, update_count)
        records = to_csv(updated_products)
        start = bios.time.now()
        ctx_upsert_request = (
            bios.isql().upsert().into(PRODUCT_CONTEXT_NAME).csv_bulk(records).build()
        )
        reply = self.session.execute(ctx_upsert_request)
        # check update count
        time.sleep(3)
        delta = bios.time.minutes(1)
        req = (
            bios.isql()
            .select()
            .from_signal(PRODUCT_AUDIT_SIGNAL_NAME)
            .time_range(start, delta)
            .build()
        )
        reply = self.session.execute(req)
        records = reply.data_windows[0].records
        self.assertEqual(len(records), update_count)
        updates = [r for r in records if r.get("_operation") == "Update"]
        self.assertEqual(len(updates), update_count)

        # delete the records
        keys = [[product["product_id"]] for product in self.products]
        ctx_delete_request = (
            bios.isql().delete().from_context(PRODUCT_CONTEXT_NAME).where(keys=keys).build()
        )
        start = bios.time.now()
        reply = self.session.execute(ctx_delete_request)
        # check delete count
        time.sleep(3)
        delta = bios.time.minutes(1)
        req = (
            bios.isql()
            .select()
            .from_signal(PRODUCT_AUDIT_SIGNAL_NAME)
            .time_range(start, delta)
            .build()
        )
        reply = self.session.execute(req)
        records = reply.data_windows[0].records
        updates = [r for r in records if r.get("_operation") == "Delete"]
        self.assertEqual(len(updates), self.record_count)

        # insert the same records again
        records = to_csv(self.products)
        ctx_upsert_request = (
            bios.isql().upsert().into(PRODUCT_CONTEXT_NAME).csv_bulk(records).build()
        )
        start = bios.time.now()
        self.session.execute(ctx_upsert_request)
        time.sleep(3)
        delta = bios.time.minutes(2)
        req = (
            bios.isql()
            .select()
            .from_signal(PRODUCT_AUDIT_SIGNAL_NAME)
            .time_range(start, delta)
            .build()
        )
        reply = self.session.execute(req)
        records = reply.data_windows[0].records
        self.assertEqual(len(records), self.record_count)

        # check all the operation counts
        insertions = [r for r in records if r.get("_operation") == "Insert"]
        self.assertEqual(len(insertions), self.record_count)
        deletions = [r for r in records if r.get("_operation") == "Delete"]
        self.assertEqual(len(deletions), 0)
        updates = [r for r in records if r.get("_operation") == "Update"]
        self.assertEqual(len(updates), 0)

    def test_modify_audit(self):
        context_product_catalog_name = "productCatalog"
        context_product_catalog = {
            "contextName": context_product_catalog_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "productId", "type": "String"},
                {"attributeName": "title", "type": "String"},
                {"attributeName": "categoryId", "type": "Integer"},
                {"attributeName": "salePrice", "type": "String"},
            ],
            "primaryKey": ["productId"],
            "auditEnabled": True,
        }

        context_category_name = "category"
        context_category = {
            "contextName": context_category_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "categoryId", "type": "Integer"},
                {"attributeName": "categoryName", "type": "String"},
            ],
            "primaryKey": ["categoryId"],
        }

        try:
            self.session.delete_context(context_product_catalog_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise
        try:
            self.session.delete_context(context_category_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise

        self.session.create_context(context_category)
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_category_name)
            .csv_bulk(["100,Tires", "200,Tubes", "300,Brake Pads"])
            .build()
        )

        self.session.create_context(context_product_catalog)

        signal_audit_name = "auditProductCatalog"
        signal_audit = self.session.get_signal(signal_audit_name)

        audit_signal_name = self._context_name_to_audit(context_product_catalog_name)
        audit_with_enrichment = {
            "signalName": audit_signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "_operation", "type": "String"},
                {"attributeName": "productId", "type": "String"},
                {"attributeName": "prevProductId", "type": "String"},
                {"attributeName": "title", "type": "String"},
                {"attributeName": "prevTitle", "type": "String"},
                {"attributeName": "categoryId", "type": "Integer"},
                {"attributeName": "prevCategoryId", "type": "Integer"},
                {"attributeName": "salePrice", "type": "String"},
                {"attributeName": "prevSalePrice", "type": "String"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "category",
                        "foreignKey": ["categoryId"],
                        "missingLookupPolicy": "StoreFillInvalue",
                        "contextName": "category",
                        "contextAttributes": [
                            {"attributeName": "categoryName", "fillIn": "MISSING"}
                        ],
                    }
                ]
            },
        }
        self.session.update_signal(audit_signal_name, audit_with_enrichment)

        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_product_catalog_name)
            .csv("tire-001,Continental Gator Skin 700x28,100,50.00")
            .build()
        )

        statement = (
            bios.isql()
            .select()
            .from_signal(signal_audit_name)
            .time_range(bios.time.now(), bios.time.minutes(-10))
            .build()
        )
        audit_records = self.session.execute(statement).to_dict()
        assert len(audit_records) == 1

        assert audit_records[0].get("title") == "Continental Gator Skin 700x28"
        assert audit_records[0].get("categoryId") == 100
        assert audit_records[0].get("salePrice") == "50.00"
        assert audit_records[0].get("categoryName") == "Tires"

    def _context_name_to_audit(self, context_name):
        return "audit" + context_name[0:1].upper() + context_name[1:]


if __name__ == "__main__":
    pytest.main(sys.argv)
