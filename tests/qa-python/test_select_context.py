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
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config, try_delete_signal

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

TEST_CONTEXT = {
    "contextName": "testSelectContextEntries",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "product_id", "type": "integer"},
        {"attributeName": "product_name", "type": "string"},
        {"attributeName": "product_type", "type": "integer"},
    ],
    "primaryKey": ["product_id"],
}

TEST_CONTEXT1 = {
    "contextName": "testSelectContextEntries1",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "testKey", "type": "string"},
        {"attributeName": "testAttr", "type": "string"},
    ],
    "primaryKey": ["testKey"],
}

SIZE = 10000


def generate_test_data():
    products = []
    test_key = 5000
    eq, lt, lte, gt, gte = 0, 0, 0, 0, 0
    for _id in range(SIZE):
        _type = random.randint(1, SIZE)
        _name = f"p{_id:04d}"
        _data = str(_id) + "," + _name + "," + str(_type)
        products.append(_data)
        if _type == test_key:
            eq += 1
        if _type < test_key:
            lt += 1
        if _type <= test_key:
            lte += 1
        if _type > test_key:
            gt += 1
        if _type >= test_key:
            gte += 1
    return products, eq, lt, lte, gt, gte


class BiosSelectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        cls.products, cls.eq, cls.lt, cls.lte, cls.gt, cls.gte = generate_test_data()
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            setup_context(admin, TEST_CONTEXT)
            setup_context(admin, TEST_CONTEXT1)

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
        cls.session.close()

    def test_equal_to_value(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name = 'p0001'")
            .build()
        )
        self._verify_equal(statement)

    def test_equal_to_primary_key(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id = 1")
            .build()
        )
        self._verify_equal(statement)

    def _verify_equal(self, statement):
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        record = records[0]
        assert len(record) == 3
        assert records[0]["product_id"] == 1
        assert records[0]["product_name"] == "p0001"
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
        ids_to_pickup = [8378, 2, 5922, 2424, 2725, 5406, 3817, 111]
        filter_string = ", ".join([str(id_num) for id_num in ids_to_pickup])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id in ({filter_string})")
            .build()
        )
        self._verify_in(statement, ids_to_pickup)

    def _verify_in(self, statement, ids):
        reply = self.session.execute(statement)
        records = reply.get_records()
        assert len(records) == len(ids)
        for record in records:
            assert len(record) == 3
            assert record["product_id"] in ids
            assert 1 <= record["product_type"] <= SIZE

    def test_in_value_and_value_range(self):
        ids_to_pickup = list(range(1, 30001))
        # ids_to_pickup = [i + 1 for i in range(3000)]
        filter_string = ", ".join([f"'p{id_num}'" for id_num in ids_to_pickup])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_name IN ({filter_string}) AND product_type < 5000")
            .build()
        )
        self._verify_in_value_and_value_zone(statement, ids_to_pickup, 1, 4999)

    def test_in_primary_key_and_value_range(self):
        ids_to_pickup = [i + 1 for i in range(3000)]
        filter_string = ", ".join([str(id_num) for id_num in ids_to_pickup])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id IN ({filter_string}) AND product_type < 5000")
            .build()
        )
        self._verify_in_value_and_value_zone(statement, ids_to_pickup, 1, 4999)

    def test_in_value_and_value_zone(self):
        ids_to_pickup = [i + 1 for i in range(3000)]
        filter_string = ", ".join([f"'p{id_num}'" for id_num in ids_to_pickup])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(
                f"product_name IN ({filter_string})"
                " AND product_type <= 5000 AND product_type >= 2500"
            )
            .build()
        )
        self._verify_in_value_and_value_zone(statement, ids_to_pickup, 2500, 5000)

    def test_in_primary_key_and_value_zone(self):
        ids_to_pickup = [i + 1 for i in range(3000)]
        filter_string = ", ".join([str(id_num) for id_num in ids_to_pickup])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(
                f"product_id IN ({filter_string})"
                " AND product_type < 5000 AND product_type > 2500"
            )
            .build()
        )
        self._verify_in_value_and_value_zone(statement, ids_to_pickup, 2501, 4999)

    def _verify_in_value_and_value_zone(self, statement, ids, lower_inclusive, upper_inclusive):
        reply = self.session.execute(statement)
        records = reply.get_records()
        assert 0 < len(records) < len(ids)
        for record in records:
            assert len(record) == 3
            assert record["product_id"] in ids
            assert lower_inclusive <= record["product_type"] <= upper_inclusive

    def test_string_value_range(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name < 'p0050'")
            .build()
        )
        self._verify_ranges(statement, 0, 49, num_results=50)

    def test_string_value_zone(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name >= 'p0051' AND product_name <= 'p0100'")
            .build()
        )
        self._verify_ranges(statement, 51, 100, num_results=50)

    def test_primary_key_range(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id >= 6000")
            .build()
        )
        self._verify_ranges(statement, 4000, 9999, num_results=4000)

    def test_primary_key_zone(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id > 8000 AND product_id <= 9000")
            .build()
        )
        self._verify_ranges(statement, 8001, 9000, num_results=1000)

    def test_string_and_int_value_ranges(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name > 'p5000' AND product_type >= 4000")
            .build()
        )
        self._verify_ranges(
            statement, 5001, 9999, lower_int_value_inclusive=4000, upper_int_value_inclusive=10000
        )

    def test_primary_key_and_int_value_ranges(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id < 5000 AND product_type >= 4000")
            .build()
        )
        self._verify_ranges(
            statement, 0, 4999, lower_int_value_inclusive=4000, upper_int_value_inclusive=10000
        )

    def test_order_by_int_value(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id >= 2500")
            .order_by("product_type")
            .build()
        )
        self._verify_ranges(statement, 2500, 9999, num_results=7500, order_by="product_type")

    def test_order_by_int_value_reverse(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id >= 2500")
            .order_by("product_type", reverse=True)
            .build()
        )
        self._verify_ranges(
            statement, 2500, 9999, num_results=7500, order_by="product_type", reverse=True
        )

    def test_order_by_str_value(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id < 3000")
            .order_by("product_name")
            .build()
        )
        self._verify_ranges(statement, 0, 2999, num_results=3000, order_by="product_name")

    def test_order_by_str_value_reverse(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id < 3000")
            .order_by("product_name", reverse=True)
            .build()
        )
        self._verify_ranges(
            statement, 0, 2999, num_results=3000, order_by="product_name", reverse=True
        )

    def test_order_by_primary_key(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name < 'p6300'")
            .order_by("product_id")
            .build()
        )
        self._verify_ranges(statement, 0, 6299, num_results=6300, order_by="product_id")

    def test_order_by_primary_key_reverse(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name < 'p6300'")
            .order_by("product_id", reverse=True)
            .build()
        )
        self._verify_ranges(
            statement, 0, 6299, num_results=6300, order_by="product_id", reverse=True
        )

    def test_limit_after_primary_key_range(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id >= 2500")
            .limit(2000)
            .build()
        )
        self._verify_ranges(statement, 2500, 9999, num_results=2000)

    def test_limit_after_primary_key_exact_matches(self):
        primary_keys = list(range(1000, 2000))
        filter_string = ", ".join([str(key) for key in primary_keys])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id in ({filter_string})")
            .limit(350)
            .build()
        )
        self._verify_ranges(statement, 1000, 2000, num_results=350)

    def test_limit_after_value_range(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name < 'p1000'")
            .limit(228)
            .build()
        )
        self._verify_ranges(statement, 0, 999, num_results=228)

    def test_limit_after_primary_key_exact_matches_and_value_range(self):
        primary_keys = list(range(4000, 5000))
        filter_string = ", ".join([str(key) for key in primary_keys])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id in ({filter_string}) and product_name < 'p4500'")
            .limit(350)
            .build()
        )
        self._verify_ranges(statement, 4000, 4499, num_results=350)

    def test_limit_after_primary_key_exact_matches_and_sort_by_primary_key(self):
        primary_keys = list(range(40, 50))
        filter_string = ", ".join([str(key) for key in primary_keys])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id in ({filter_string})")
            .order_by("product_id")
            .limit(5)
            .build()
        )
        self._verify_ranges(statement, 40, 44, num_results=5)

    def test_limit_after_primary_key_exact_matches_and_sort_by_value(self):
        primary_keys = list(range(4000, 5000))
        filter_string = ", ".join([str(key) for key in primary_keys])
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where(f"product_id in ({filter_string})")
            .order_by("product_name", reverse=True)
            .limit(350)
            .build()
        )
        self._verify_ranges(statement, 4650, 4999, num_results=350)

    def test_limit_after_value_range_query_and_sort_by_primary_key(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name < 'p3800'")
            .order_by("product_id", reverse=True)
            .limit(200)
            .build()
        )
        self._verify_ranges(statement, 3600, 3799, num_results=200)

    def test_limit_after_value_range_query_and_sort_by_value(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_name > 'p3800'")
            .order_by("product_name")
            .limit(200)
            .build()
        )
        self._verify_ranges(statement, 3801, 4000, num_results=200)

    def _verify_ranges(
        self,
        statement,
        lower_pkey_inclusive,
        upper_pkey_inclusive,
        lower_int_value_inclusive=None,
        upper_int_value_inclusive=None,
        num_results=None,
        order_by=None,
        reverse=False,
    ):
        reply = self.session.execute(statement)
        records = reply.get_records()

        assert len(records) > 0

        if num_results is not None:
            assert len(records) == num_results

        prev = {}
        for i, record in enumerate(records):
            assert len(record) == 3
            print(record)
            assert lower_pkey_inclusive <= record["product_id"] <= upper_pkey_inclusive
            if lower_int_value_inclusive is not None:
                assert record["product_type"] >= lower_int_value_inclusive
            if upper_int_value_inclusive is not None:
                assert record["product_type"] <= upper_int_value_inclusive
            if order_by and prev:
                if reverse:
                    assert record[order_by] <= prev[order_by]
                else:
                    assert record[order_by] >= prev[order_by]
            prev = record

    def test_int_equality(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_type = 5000")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), self.eq)

    def test_int_lt(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_type < 5000")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), self.lt)

    def test_int_le(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_type <= 5000")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), self.lte)

    def test_int_gt(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_type > 5000")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), self.gt)

    def test_int_ge(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_type >= 5000")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), self.gte)

    def test_int_in(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id IN (2, 3, 4)")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 3)

    def test_specify_attributes(self):
        statement = (
            bios.isql()
            .select("product_id")
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id IN (2, 3, 4)")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 3)
        for record in records:
            assert len(record) == 1
            assert record.get("product_id") in {2, 3, 4}

    def test_bios_context_select_multiple_version(self):
        #
        # prepare data
        #
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            entries = ["str1,attr1"]
            ctx_upsert_statement = (
                bios.isql().upsert().into(TEST_CONTEXT1["contextName"]).csv_bulk(entries).build()
            )
            admin.execute(ctx_upsert_statement)
            entries = ["str1,attr2"]
            ctx_upsert_statement = (
                bios.isql().upsert().into(TEST_CONTEXT1["contextName"]).csv_bulk(entries).build()
            )
            admin.execute(ctx_upsert_statement)

        #
        # verify
        #
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey = 'str1'")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["testKey"], "str1")
        self.assertEqual(records[0]["testAttr"], "attr2")

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey IN ('str1')")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["testKey"], "str1")
        self.assertEqual(records[0]["testAttr"], "attr2")

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey >= 'str1'")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["testKey"], "str1")
        self.assertEqual(records[0]["testAttr"], "attr2")

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey <= 'str1'")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["testKey"], "str1")
        self.assertEqual(records[0]["testAttr"], "attr2")

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey <= 'str1'")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["testKey"], "str1")
        self.assertEqual(records[0]["testAttr"], "attr2")

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey < 'str1'")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 0)

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey > 'str1'")
            .build()
        )
        reply = self.session.execute(statement)
        records = reply.get_records()
        self.assertEqual(len(records), 0)

    def test_negative_invalid_where_clauses(self):
        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_id = 'str1'")
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("product_type = '3'")
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT["contextName"])
            .where("no_such_attribute = '3'")
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testKey = ['str1']")
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT

        statement = (
            bios.isql()
            .select()
            .from_context(TEST_CONTEXT1["contextName"])
            .where("testAttr = 123")
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT


if __name__ == "__main__":
    pytest.main(sys.argv)
