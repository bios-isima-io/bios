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
import random
import sys
import time
import unittest
from typing import List, Tuple

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

TEST_CONTEXT = {
    "contextName": "testSelectContextEntriesMd",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "area", "type": "string"},
        {
            "attributeName": "site",
            "type": "string",
            "allowedValues": ["s9", "s8", "s7", "s6", "s5", "s4", "s3", "s2", "s1", "s0"],
        },
        # {"attributeName": "site", "type": "string"},
        {"attributeName": "product_id", "type": "integer"},
        {"attributeName": "product_name", "type": "string"},
        {"attributeName": "product_type", "type": "integer"},
    ],
    "primaryKey": ["area", "site", "product_id"],
}

SIZE = 10000
NUM_AREAS = 5
AREA_SIZE = int(SIZE / NUM_AREAS)
NUM_SITES = 10
SITE_SIZE = int(AREA_SIZE / NUM_SITES)


class ContextSelectTestMultiDimension(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.products = cls.generate_test_data()

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            cls.orig_cache_expiry = sadmin.get_property("prop.sharedPropertiesCacheExpiryMillis")
            sadmin.set_property("prop.sharedPropertiesCacheExpiryMillis", "1000")
            sadmin.set_property("prop.select.context.db.limit", "")

        setup_tenant_config()
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            setup_context(admin, TEST_CONTEXT)

            #
            # prepare data
            #
            ctx_upsert_statement = (
                bios.isql()
                .upsert()
                .into(TEST_CONTEXT["contextName"])
                .csv_bulk(cls.products)
                .build()
            )
            admin.execute(ctx_upsert_statement)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("prop.sharedPropertiesCacheExpiryMillis", cls.orig_cache_expiry)
        cls.session.close()

    def tearDown(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("prop.select.context.db.limit", "")

    @classmethod
    def generate_test_data(cls):
        products = []
        for index in range(SIZE):
            i_area = int(index / AREA_SIZE)
            i_site = int((index % AREA_SIZE) / SITE_SIZE)
            product_id = index % SITE_SIZE
            product_num = (product_id * NUM_SITES + i_site) * NUM_AREAS + i_area
            area = f"a{i_area}"
            site = f"s{i_site}"
            product_type = random.randint(1, SIZE)
            product_name = f"p{product_num:04d}"
            _data = ",".join([area, site, str(product_id), product_name, str(product_type)])
            products.append(_data)
        return products

    @classmethod
    def get_primary_key_from_product_num(cls, product_num: int) -> Tuple[str, str, int]:
        area_num = product_num % NUM_AREAS
        site_num = int(product_num / NUM_AREAS) % NUM_SITES
        product_id = int(product_num / NUM_AREAS / NUM_SITES)
        return f"a{area_num}", f"s{site_num}", product_id

    @classmethod
    def make_product_numbers(
        cls, areas: List[str], sites: List[str], product_ids: List[int]
    ) -> List[int]:
        area_numbers = [int(area[1:]) for area in areas]
        site_numbers = [int(site[1:]) for site in sites]
        product_numbers = []
        for area_num in area_numbers:
            for site_num in site_numbers:
                for product_id in product_ids:
                    product_num = (product_id * NUM_SITES + site_num) * NUM_AREAS + area_num
                    product_numbers.append(product_num)
        return product_numbers

    @classmethod
    def make_primary_key_filter(cls, areas, sites, product_ids):
        area_values = [f"'{area}'" for area in areas]
        site_values = [f"'{site}'" for site in sites]
        product_id_values = [str(product_id) for product_id in product_ids]
        filter_terms = []
        filter_terms.append(f"site IN ({','.join(site_values)})")
        filter_terms.append(f"product_id IN ({','.join(product_id_values)})")
        filter_terms.append(f"area IN ({','.join(area_values)})")
        return " AND ".join(filter_terms)

    def test_equal_to_value(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name = 'p5913'")
            .build()
        )
        self._verify_equal(statement)

    def test_equal_to_primary_key(self):
        area, site, product_id = self.get_primary_key_from_product_num(5913)
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id = {product_id} AND site = '{site}' AND area = '{area}'")
            .build()
        )
        self._verify_equal(statement)

    def _verify_equal(self, statement):
        reply = self.session.execute(statement)
        records = reply.get_records()
        assert len(records) == 1
        record = records[0]
        assert len(record) == 5
        product_num = 5913
        area, site, product_id = self.get_primary_key_from_product_num(product_num)
        assert records[0]["area"] == area
        assert records[0]["site"] == site
        assert records[0]["product_id"] == product_id
        assert records[0]["product_name"] == f"p{product_num}"
        assert 1 <= records[0]["product_type"] <= SIZE

    def test_in_value(self):
        ids_to_pickup = [3377, 4165, 9207, 157, 8378, 2, 5922]
        filter_string = ", ".join([f"'p{id_num:04d}'" for id_num in ids_to_pickup])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_name in ({filter_string})")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_in_primary_key(self):
        areas = ["a3", "a1", "a4"]
        sites = ["s0", "s2", "s1", "s3"]
        product_ids = [10, 0, 51, 31, 2, 177, 153, 12, 27, 59]

        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        filter_string = self.make_primary_key_filter(areas, sites, product_ids)
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(filter_string)
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def _verify_in(self, statement, ids):
        reply = self.session.execute(statement)
        records = reply.get_records()
        assert len(records) == len(ids)
        for record in records:
            assert len(record) == 5
            assert int(record["product_name"][1:]) in ids
            assert 1 <= record["product_type"] <= SIZE

    def test_in_primary_key_and_value_range(self):
        areas = ["a1", "a0", "a3"]
        sites = ["s7", "s1", "s3", "s9", "s8"]
        product_ids = [10, 0, 151, 31, 2, 177, 53, 12, 127, 59]

        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        filter_string = self.make_primary_key_filter(areas, sites, product_ids)
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"{filter_string} AND product_type < 5000")
            .build()
        )
        self._verify_in_value_and_value_zone(statement, ids_to_pickup, 1, 4999)

    def test_in_primary_key_and_value_zone(self):
        areas = ["a1", "a0", "a3"]
        sites = ["s7", "s1", "s3", "s9", "s4"]
        product_ids = [10, 0, 451, 31, 2, 177, 253, 12, 327, 59]

        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        filter_string = self.make_primary_key_filter(areas, sites, product_ids)
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"{filter_string} AND product_type <= 5000 AND product_type >= 2500")
            .build()
        )
        self._verify_in_value_and_value_zone(statement, ids_to_pickup, 2500, 5000)

    def _verify_in_value_and_value_zone(self, statement, ids, lower_inclusive, upper_inclusive):
        reply = self.session.execute(statement)
        records = reply.get_records()
        assert 0 < len(records) < len(ids)
        for record in records:
            assert len(record) == 5
            assert int(record["product_name"][1:]) in ids
            assert lower_inclusive <= record["product_type"] <= upper_inclusive

    def test_equal_equal_range(self):
        areas = ["a4"]
        sites = ["s9"]
        product_ids = list(range(0, 40))
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area = 'a4' AND site = 's9' AND product_id < 40")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_equal_equal_zone(self):
        areas = ["a4"]
        sites = ["s9"]
        product_ids = list(range(21, 41))
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area = 'a4' AND site = 's9' AND product_id <= 40 AND product_id > 20")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    @pytest.mark.skip("BIOS-6052")
    def test_equal_range_equal(self):
        areas = ["a2"]
        sites = ["s0", "s1", "s2"]
        product_ids = [110]
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area = 'a2' AND site >= 's2' AND product_id = 110")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_range_equal_equal(self):
        areas = ["a0", "a1"]
        sites = ["s9"]
        product_ids = [113]
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area < 'a2' AND site = 's9' AND product_id = 113")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_only_first_primary_key(self):
        areas = ["a3"]
        sites = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"]
        product_ids = list(range(SITE_SIZE))
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area = 'a3'")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_first_and_second_primary_key(self):
        areas = ["a3"]
        sites = ["s6"]
        product_ids = list(range(SITE_SIZE))
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area = 'a3' AND site = 's6'")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_only_second_primary_key(self):
        areas = ["a0", "a1", "a2", "a3", "a4"]
        sites = ["s6"]
        product_ids = list(range(SITE_SIZE))
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("site = 's6'")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_first_and_third(self):
        areas = ["a0"]
        sites = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"]
        product_ids = [91]
        ids_to_pickup = self.make_product_numbers(areas, sites, product_ids)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("area = 'a0' AND product_id = 91")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def test_db_fetch_size_limitation(self):
        sleep_time = 5
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("prop.select.context.db.limit", "6000")
            print(
                f"Sleeping for {sleep_time} seconds to wait for the shared property"
                " 'prop.select.context.db.limit' being effective"
            )
            time.sleep(sleep_time)

        try:
            statement = (
                bios.isql()
                .select()
                .from_context(TEST_CONTEXT["contextName"])
                .where("site = 's6'")
                .build()
            )
            with pytest.raises(ServiceError) as exc_info:
                self.session.execute(statement)
            assert exc_info.value.error_code == ErrorCode.BAD_INPUT
            assert (
                "Scale of the query was too large to process."
                " Please downsize the query or consider creating an index"
                in exc_info.value.message
            )
        finally:
            with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
                sadmin.set_property("prop.select.context.db.limit", "")
                print(
                    f"Sleeping for {sleep_time} seconds to wait for the shared property"
                    " 'prop.select.context.db.limit' being cleared"
                )
                time.sleep(sleep_time)


if __name__ == "__main__":
    pytest.main(sys.argv)
