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
import logging
import os
import sys
import time
import unittest
import pprint

import pytest

from bios import ServiceError, ErrorCode
import bios
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME, setup_tenant_config
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

TEST_NAME = "SelectMetrics"

CONTEXT_NAME = "countryContext" + TEST_NAME
COUNTRY_CONTEXT = {
    "contextName": CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "countryName", "type": "String"},
        {"attributeName": "countryCode", "type": "Integer"},
        {
            "attributeName": "continent",
            "type": "String",
            "allowedValues": [
                "Asia",
                "Africa",
                "Europe",
                "Eurasia",
                "Australia",
                "North America",
                "South America",
            ],
        },
    ],
    "primaryKey": ["countryName"],
}

INITIAL_COUNTRIES = [
    "United States,1,North America",
    "Canada,2,North America",
    "China,86,Asia",
    "Albania,355,Europe",
    "Algeria,213,Africa",
    "India,91,Asia",
    "Andorra,376,Europe",
    "日本,81,Asia",
    "United Kingdom,44,Europe",
    "Germany,49,Europe",
    "Brazil,55,South America",
    "Australia,61,Australia",
    "New Zealand,64,Australia",
]

SIGNAL_NAME = "orders" + TEST_NAME
TEST_SIGNAL = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "productName", "type": "String"},
        {"attributeName": "country", "type": "String"},
        {"attributeName": "quantity", "type": "Integer"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "countryInfo",
                "foreignKey": ["country"],
                "missingLookupPolicy": "Reject",
                "contextName": CONTEXT_NAME,
                "contextAttributes": [
                    {"attributeName": "countryCode"},
                    {"attributeName": "continent"},
                ],
            }
        ],
    },
}


SIGNAL_DATA = [
    "earphones,India,1",
    "socks,Australia,5",
    "earphones,United Kingdom,2",
    "broom,United Kingdom,1",
    "earphones,New Zealand,1",
    "earphones,日本,1",
    "detergent,Canada,3",
    "detergent,United Kingdom,3",
    "socks,Canada,9",
    "socks,Andorra,3",
    "broom,Algeria,2",
    "detergent,China,1",
    "sponge,Andorra,5",
    "earphones,日本,1",
    "pants,Albania,2",
    "nails,Andorra,15",
    "smartphone,United States,1",
    "broom,United States,3",
    "socks,United States,5",
    "nails,India,12",
    "detergent,United Kingdom,2",
    "smartphone,Brazil,1",
]

# test expected results
countries = 12
products = 8
products_per_country = {
    "Albania": 1,
    "Algeria": 1,
    "Andorra": 3,
    "Australia": 1,
    "Brazil": 1,
    "Canada": 2,
    "China": 1,
    "India": 2,
    "New Zealand": 1,
    "United Kingdom": 3,
    "United States": 3,
    "日本": 1,
}

countries_per_product = {
    "broom": 3,
    "detergent": 3,
    "earphones": 4,
    "nails": 2,
    "pants": 1,
    "smartphone": 2,
    "socks": 4,
    "sponge": 1,
}

continents_per_product = {
    "broom": 3,
    "detergent": 3,
    "earphones": 3,
    "nails": 2,
    "pants": 1,
    "smartphone": 2,
    "socks": 3,
    "sponge": 1,
}

# count(), sum(quantity)
aggregates_per_product = {
    "broom": (3, 6),
    "detergent": (4, 9),
    "earphones": (5, 6),
    "nails": (2, 27),
    "pants": (1, 2),
    "smartphone": (2, 2),
    "socks": (4, 22),
    "sponge": (1, 5),
}


@pytest.fixture(scope="module")
def set_up_class():
    setup_tenant_config()
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        try:
            admin.delete_signal(SIGNAL_NAME)
        except ServiceError:
            pass
        try:
            admin.delete_context(CONTEXT_NAME)
        except ServiceError:
            pass
        admin.create_context(COUNTRY_CONTEXT)
        ctx_upsert_request = (
            bios.isql().upsert().into(CONTEXT_NAME).csv_bulk(INITIAL_COUNTRIES).build()
        )
        admin.execute(ctx_upsert_request)

        admin.create_signal(TEST_SIGNAL)
        start = bios.time.now()
        insert_request = bios.isql().insert().into(SIGNAL_NAME).csv_bulk(SIGNAL_DATA).build()
        admin.execute(insert_request)
        end = bios.time.now()
        yield admin, start, end


def test_distinct_products_global(set_up_class):
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("distinctcount(productname)")
        .from_signal(SIGNAL_NAME)
        .time_range(start, end - start)
        .build()
    )
    results = session.execute(statement).to_dict()
    assert len(results) == 1
    assert results[0].get("distinctcount(productName)") == products


def test_distinct_countries_global(set_up_class):
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("distinctcount(country)")
        .from_signal(SIGNAL_NAME)
        .time_range(start, end - start)
        .build()
    )
    results = session.execute(statement).to_dict()
    assert len(results) == 1
    assert results[0].get("distinctcount(country)") == countries


def test_distinct_products_group_by_country(set_up_class):
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("distinctcount(productName)")
        .from_signal(SIGNAL_NAME)
        .group_by("country")
        .time_range(start, end - start)
        .build()
    )
    records = session.execute(statement).to_dict()
    assert len(records) == countries
    for record in records:
        country = record.get("country")
        count = record.get("distinctcount(productName)")
        assert country in products_per_country
        assert count == products_per_country[country]


def test_distinct_country_group_by_product(set_up_class):
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("distinctcount(country)")
        .from_signal(SIGNAL_NAME)
        .group_by("productName")
        .time_range(start, end - start)
        .build()
    )
    records = session.execute(statement).to_dict()
    assert len(records) == products
    for record in records:
        product_name = record.get("productName")
        count = record.get("distinctcount(country)")
        assert product_name in countries_per_product
        assert count == countries_per_product[product_name]


def test_distinct_country_code_group_by_product(set_up_class):
    """test distinct-counting integers"""
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("distinctcount(countryCode)")
        .from_signal(SIGNAL_NAME)
        .group_by("productName")
        .time_range(start, end - start)
        .build()
    )
    records = session.execute(statement).to_dict()
    assert len(records) == products
    for record in records:
        product_name = record.get("productName")
        count = record.get("distinctcount(countryCode)")
        assert product_name in countries_per_product
        assert count == countries_per_product[product_name]


def test_distinct_continent_group_by_product(set_up_class):
    """test distinct-counting integers"""
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("distinctcount(continent)")
        .from_signal(SIGNAL_NAME)
        .group_by("productName")
        .time_range(start, end - start)
        .build()
    )
    records = session.execute(statement).to_dict()
    assert len(records) == products
    for record in records:
        product_name = record.get("productName")
        count = record.get("distinctcount(continent)")
        assert product_name in continents_per_product
        assert count == continents_per_product[product_name]


def test_aggregates_group_by_product(set_up_class):
    """test distinct-counting integers"""
    session, start, end = set_up_class
    statement = (
        bios.isql()
        .select("count()", "sum(quantity)", "avg(quantity)")
        .from_signal(SIGNAL_NAME)
        .group_by("productName")
        .time_range(start, end - start)
        .build()
    )
    records = session.execute(statement).to_dict()
    assert len(records) == products
    for record in records:
        product_name = record.get("productName")
        count = record.get("count()")
        total_quantity = record.get("sum(quantity)")
        avg = record.get("avg(quantity)")
        assert product_name in aggregates_per_product, product_name
        assert (count, total_quantity) == aggregates_per_product[product_name], product_name
        assert pytest.approx(avg, 1.0e-5) == total_quantity / count, product_name


if __name__ == "__main__":
    pytest.main(sys.argv)
