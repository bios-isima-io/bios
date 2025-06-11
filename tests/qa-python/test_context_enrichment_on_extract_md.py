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
from typing import Tuple

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config

CONTEXT_PRODUCT_CATEGORY_NAME = "productCategoryMd"
CONTEXT_PRODUCT_CATEGORY = {
    "contextName": CONTEXT_PRODUCT_CATEGORY_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "group", "type": "String"},
        {"attributeName": "categoryId", "type": "Integer"},
        {"attributeName": "groupTitle", "type": "String"},
        {"attributeName": "categoryTitle", "type": "String"},
        {"attributeName": "subCategoryTitle", "type": "String"},
    ],
    "primaryKey": ["group", "categoryId"],
}

CONTEXT_PRODUCT_TO_CATEGORY_NAME = "productToCategoryMd"
CONTEXT_PRODUCT_TO_CATEGORY = {
    "contextName": CONTEXT_PRODUCT_TO_CATEGORY_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "site", "type": "String", "allowedValues": ["ca.us", "or.us", "tk.jp"]},
        {"attributeName": "productId", "type": "Integer"},
        {"attributeName": "group", "type": "String"},
        {"attributeName": "categoryId", "type": "Integer"},
    ],
    "primaryKey": ["site", "productId"],
}

CONTEXT_PRODUCT_NAME = "productMd"
CONTEXT_PRODUCT = {
    "contextName": CONTEXT_PRODUCT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "site", "type": "String", "allowedValues": ["ca.us", "or.us", "tk.jp"]},
        {"attributeName": "productId", "type": "Integer"},
        {"attributeName": "productName", "type": "String"},
    ],
    "primaryKey": ["site", "productId"],
    "enrichments": [
        {
            "enrichmentName": "categoryKeyLookup",
            "foreignKey": ["site", "productId"],
            "missingLookupPolicy": "StoreFillInValue",
            "enrichedAttributes": [
                {
                    "value": f"{CONTEXT_PRODUCT_TO_CATEGORY_NAME}.group",
                    "as": "group",
                    "fillIn": "NO_GROUP",
                },
                {
                    "value": f"{CONTEXT_PRODUCT_TO_CATEGORY_NAME}.categoryId",
                    "as": "categoryId",
                    "fillIn": 0,
                },
            ],
        },
        {
            "enrichmentName": "categoryLookup",
            "foreignKey": ["group", "categoryId"],
            "missingLookupPolicy": "StoreFillInValue",
            "enrichedAttributes": [
                {
                    "value": f"{CONTEXT_PRODUCT_CATEGORY_NAME}.groupTitle",
                    "as": "groupTitle",
                    "fillIn": "NO_GROUP_TITLE",
                },
                {
                    "value": f"{CONTEXT_PRODUCT_CATEGORY_NAME}.categoryTitle",
                    "as": "categoryTitle",
                    "fillIn": "NO_CATEGORY_TITLE",
                },
            ],
        },
    ],
}

ADMIN_USER = admin_user + "@" + TENANT_NAME


@pytest.fixture(scope="module")
def set_up_class():
    setup_tenant_config()

    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        try:
            admin.delete_context(CONTEXT_PRODUCT_NAME)
        except ServiceError:
            pass

        try:
            admin.delete_context(CONTEXT_PRODUCT_CATEGORY_NAME)
        except ServiceError:
            pass
        admin.create_context(CONTEXT_PRODUCT_CATEGORY)

        try:
            admin.delete_context(CONTEXT_PRODUCT_TO_CATEGORY_NAME)
        except ServiceError:
            pass
        admin.create_context(CONTEXT_PRODUCT_TO_CATEGORY)

        category_entries = [
            "ca.us,12345,group01,category01,subcategory01",  # 0
            "ca.us,23456,group02,category02,subcategory02",  # 1
            "tk.jp,12345,group03,category03,subcategory03",  # 2
        ]
        admin.execute(
            bios.isql()
            .upsert()
            .into(CONTEXT_PRODUCT_CATEGORY_NAME)
            .csv_bulk(category_entries)
            .build()
        )

        to_category_entries = [
            "ca.us,98765,ca.us,12345",
            "ca.us,98764,ca.us,12345",
            "ca.us,98653,ca.us,22345",
            "tk.jp,98765,tk.jp,12345",
        ]
        admin.execute(
            bios.isql()
            .upsert()
            .into(CONTEXT_PRODUCT_TO_CATEGORY_NAME)
            .csv_bulk(to_category_entries)
            .build()
        )


@pytest.fixture(autouse=True)
def session(set_up_class):
    client = bios.login(ep_url(), ADMIN_USER, admin_pass)

    yield client

    client.close()


def test_fundamental(session):
    session.create_context(CONTEXT_PRODUCT)
    retrieved_context = session.get_context(CONTEXT_PRODUCT_NAME)

    products = [
        "ca.us,98765,product01",
        "ca.us,98764,product02",
        "ca.us,98653,product03",
        "tk.jp,98765,product04",
        "tk.jp,99999,product05",
    ]
    session.execute(bios.isql().upsert().into(CONTEXT_PRODUCT_NAME).csv_bulk(products).build())

    keys = [
        ["ca.us", 98765],
        ["ca.us", 98764],
        ["ca.us", 98653],
        ["tk.jp", 98765],
        ["tk.jp", 99999],
    ]
    entries = session.execute(
        bios.isql().select().from_context(CONTEXT_PRODUCT_NAME).where(keys=keys).build()
    ).to_dict()

    expected_entries = [
        ("ca.us", 98765, "product01", "ca.us", 12345, "group01", "category01"),
        ("ca.us", 98764, "product02", "ca.us", 12345, "group01", "category01"),
        ("ca.us", 98653, "product03", "ca.us", 22345, "NO_GROUP_TITLE", "NO_CATEGORY_TITLE"),
        ("tk.jp", 98765, "product04", "tk.jp", 12345, "group03", "category03"),
        ("tk.jp", 99999, "product05", "NO_GROUP", 0, "NO_GROUP_TITLE", "NO_CATEGORY_TITLE"),
    ]

    assert len(entries) == 5
    for i in range(5):
        assert _get_product_attributes(entries[i]) == expected_entries[i], f"index={i}"

    #
    # Select by string where clause against the primary key
    #
    def make_verify(i: int):
        def verify_core():
            result = session.execute(
                bios.isql()
                .select()
                .from_context(CONTEXT_PRODUCT_NAME)
                .where(f"site = '{keys[i][0]}' AND productId = {keys[i][1]}")
                .build()
            )
            assert (
                _get_product_attributes(result.get_records()[0]) == expected_entries[i]
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
    result = session.execute(
        bios.isql().select().from_context(CONTEXT_PRODUCT_NAME).where("productId = 98765").build()
    )

    records = result.get_records()
    assert len(records) == 2
    result_map = {(record.get("site"), record.get("productId")): record for record in records}

    assert _get_product_attributes(result_map[(keys[0][0], keys[0][1])]) == expected_entries[0]
    assert _get_product_attributes(result_map[(keys[3][0], keys[3][1])]) == expected_entries[3]


def _get_product_attributes(entry: dict) -> Tuple[str, int, str, str, int, str]:
    return (
        entry.get("site"),
        entry.get("productId"),
        entry.get("productName"),
        entry.get("group"),
        entry.get("categoryId"),
        entry.get("groupTitle"),
        entry.get("categoryTitle"),
    )


def _test_admin_negative_no_fill_in(session):
    context = copy.deepcopy(CONTEXT_PRODUCT)
    context["contextName"] = "productNegative"
    del context.get("enrichments")[1].get("enrichedAttributes")[1]["fillIn"]
    with pytest.raises(ServiceError) as ec:
        session.create_context(context)
    assert ec.value.error_code == ErrorCode.BAD_INPUT
    assert "'fillIn' must be set" in ec.value.message


if __name__ == "__main__":
    pytest.main(sys.argv)
