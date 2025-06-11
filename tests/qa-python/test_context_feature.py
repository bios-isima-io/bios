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
from typing import Any, List

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


def make_context_with_feature(context_name, primary_key: List[dict]):
    attributes = copy.deepcopy(primary_key)
    attributes.extend(
        [
            {"attributeName": "zipCode", "type": "Integer"},
            {"attributeName": "address", "type": "String"},
            {"attributeName": "numProducts", "type": "Integer"},
        ]
    )
    return {
        "contextName": context_name,
        "missingAttributePolicy": "Reject",
        "attributes": attributes,
        "primaryKey": [element["attributeName"] for element in primary_key],
        "auditEnabled": True,
        "features": [
            {
                "featureName": "byZipCode",
                "dimensions": ["zipCode"],
                "attributes": ["numProducts"],
                "featureInterval": 15000,
                "aggregated": True,
                "indexed": True,
                "indexType": "RangeQuery",
            }
        ],
    }


CONTEXT_WITH_FEATURE = make_context_with_feature(
    "contextWithFeature", [{"attributeName": "storeId", "type": "Integer"}]
)

CONTEXT_WITH_FEATURE_MD = make_context_with_feature(
    "contextWithFeatureMd",
    [
        {"attributeName": "storeId", "type": "Integer"},
        {"attributeName": "afterRevision", "type": "Boolean"},
    ],
)


def make_context_with_multi_dimensional_feature(context_name, primary_key: List[dict]):
    attributes = copy.deepcopy(primary_key)
    attributes.extend(
        [
            {"attributeName": "city", "type": "String"},
            {"attributeName": "storeType", "type": "Integer"},
            {"attributeName": "numProducts", "type": "Integer"},
            {"attributeName": "quantity", "type": "Integer"},
        ]
    )
    return {
        "contextName": context_name,
        "missingAttributePolicy": "Reject",
        "attributes": attributes,
        "primaryKey": [element["attributeName"] for element in primary_key],
        "auditEnabled": True,
        "features": [
            {
                "featureName": "byCityStoreType",
                "dimensions": ["city", "storeType"],
                "attributes": ["numProducts", "quantity"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "ExactMatch",
            }
        ],
    }


CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE = make_context_with_multi_dimensional_feature(
    "contextWithMultiDimensionalFeature", [{"attributeName": "storeId", "type": "Integer"}]
)


class BiosContextFeatureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

        # Set up a single-dimensional feature context
        print(f"\n ... creating context context {CONTEXT_WITH_FEATURE.get('contextName')}")
        setup_context(cls.session, CONTEXT_WITH_FEATURE)

        # Set up a multi-dimensional feature context
        initial_context = copy.deepcopy(CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE)
        context_name = initial_context.get("contextName")
        modified_context = copy.deepcopy(initial_context)
        del initial_context["features"]
        print(f"\n ... creating context context {context_name}")
        setup_context(cls.session, initial_context)

        # Upsert a few entries, but it does not update the feature
        print(" ... upserting entries")
        cls.upsert_entries(
            context_name,
            [
                "10001,Menlo Park,100,8,1946",
                "10002,Menlo Park,101,10,2230",
                "10003,Palo Alto,100,5,819",
                "10004,Menlo Park,102,19,3100",
                "10005,Santa Clara,101,8,1711",
                "10006,Menlo Park,101,9,1100",
                "10007,Santa Clara,102,12,9876",
            ],
        )

        print(" ... adding feature")
        cls.session.update_context(context_name, modified_context)

        print(" ... sleeping for 20 seconds to wait for the initial feature calculation is done")
        time.sleep(20)

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

    def test_context_feature_status_refresh(self):
        context_name = "featureStatusRefreshTest"
        context_config = CONTEXT_WITH_FEATURE
        primary_keys = [[10001], [10002], [10003], [10004], [10005], [10006], [10007], [10008]]
        self._run_fundamental_test(context_name, context_config, primary_keys)

    def test_context_feature_status_refresh_md(self):
        context_name = "featureStatusRefreshTestMd"
        context_config = CONTEXT_WITH_FEATURE_MD
        primary_keys = [
            [10001, True],
            [10002, True],
            [10003, False],
            [10002, False],
            [10005, False],
            [10006, True],
            [10007, False],
            [10008, True],
        ]
        self._run_fundamental_test(context_name, context_config, primary_keys)

    def _run_fundamental_test(
        self, context_name: str, context_template: dict, primary_keys: List[List[Any]]
    ):
        stream_config = copy.deepcopy(context_template)
        stream_config["features"].append(
            {
                "featureName": "byAddress",
                "dimensions": ["address"],
                "attributes": ["numProducts"],
                "featureInterval": 15000,
                "aggregated": True,
                "indexed": False,
            }
        )
        stream_config["contextName"] = context_name
        print(f"\n ... creating context context {context_name}")
        setup_context(self.session, stream_config)

        print(" ... sleeping for 20 seconds to wait for the initial feature calculation is done")
        time.sleep(20)

        # Check status of feature before and after requesting a refresh.
        print(" ... checking the feature status")
        status = self.session.feature_status(context_name, feature="byZipCode")
        pprint(status)
        assert status["refreshRequested"] is False
        initial_done_time = status["doneUntil"]

        # Upsert a few entries, but it does not update the feature
        print(" ... upserting entries")
        self.upsert_entries(
            context_name,
            [
                f"{self.csv(primary_keys[0])},94061,Roosevelt,20",
                f"{self.csv(primary_keys[1])},94061,Woodside,31",
                f"{self.csv(primary_keys[2])},94064,The Willows,17",
                f"{self.csv(primary_keys[3])},94066,Menlo Park,19",
                f"{self.csv(primary_keys[4])},94064,Santa Clara,8",
                f"{self.csv(primary_keys[5])},94067,Palo Alto,9",
                f"{self.csv(primary_keys[6])},94061,Roosevelt,12",
            ],
        )

        print(" ... sleeping for 20 seconds to ensure at least one maintenance cycle is done")
        time.sleep(20)

        print(" ... try extracting feature -- should be empty")
        statement = (
            bios.isql()
            .select("sum(numProducts)")
            .from_context(context_name)
            .group_by("zipCode")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 0

        # Refresh the feature
        print(" ... request to refresh the feature")
        self.session.feature_refresh(context_name, feature="byZipCode")
        status = self.session.feature_status(context_name, feature="byZipCode")
        pprint(status)
        assert status["refreshRequested"] is True
        assert status["doneUntil"] == initial_done_time

        print(" ... wait 20 seconds for the feature being processed")
        time.sleep(20)

        print(" ... checking the status again")
        status = self.session.feature_status(context_name, feature="byZipCode")
        pprint(status)
        assert status["refreshRequested"] is False
        assert status["doneUntil"] > initial_done_time

        print(" ... fetching a feature")
        statement = (
            bios.isql()
            .select(
                "zipCode",
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
            )
            .from_context(context_name)
            .group_by("zipCode")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 4
        groups = {entry.get("zipCode"): entry for entry in result.get_records()}
        assert groups.keys() == {94061, 94064, 94066, 94067}
        assert groups[94061].get("count()") == 3
        assert groups[94061].get("sum(numProducts)") == 63
        assert groups[94061].get("min(numProducts)") == 12
        assert groups[94061].get("max(numProducts)") == 31
        assert groups[94061].get("avg(numProducts)") == pytest.approx(21)

        assert groups[94064].get("count()") == 2
        assert groups[94064].get("sum(numProducts)") == 25
        assert groups[94064].get("min(numProducts)") == 8
        assert groups[94064].get("max(numProducts)") == 17
        assert groups[94064].get("avg(numProducts)") == pytest.approx(12.5)

        assert groups[94066].get("count()") == 1
        assert groups[94066].get("sum(numProducts)") == 19
        assert groups[94066].get("min(numProducts)") == 19
        assert groups[94066].get("max(numProducts)") == 19
        assert groups[94066].get("avg(numProducts)") == pytest.approx(19)

        assert groups[94067].get("count()") == 1
        assert groups[94067].get("sum(numProducts)") == 9
        assert groups[94067].get("min(numProducts)") == 9
        assert groups[94067].get("max(numProducts)") == 9
        assert groups[94067].get("avg(numProducts)") == pytest.approx(9)

        print(" ... fetching another feature")
        statement = (
            bios.isql()
            .select(
                "address",
                "count()",
                "sum(numProducts)",
            )
            .from_context(context_name)
            .group_by("address")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 6
        record = result.get_records()[0]
        assert record.get("address") == "Menlo Park"
        assert record.get("count()") == 1
        assert record.get("sum(numProducts)") == 19
        record = result.get_records()[1]
        assert record.get("address") == "Palo Alto"
        assert record.get("count()") == 1
        assert record.get("sum(numProducts)") == 9
        record = result.get_records()[2]
        assert record.get("address") == "Roosevelt"
        assert record.get("count()") == 2
        assert record.get("sum(numProducts)") == 32
        record = result.get_records()[3]
        assert record.get("address") == "Santa Clara"
        assert record.get("count()") == 1
        assert record.get("sum(numProducts)") == 8
        record = result.get_records()[4]
        assert record.get("address") == "The Willows"
        assert record.get("count()") == 1
        assert record.get("sum(numProducts)") == 17
        record = result.get_records()[5]
        assert record.get("address") == "Woodside"
        assert record.get("count()") == 1
        assert record.get("sum(numProducts)") == 31

        print(" ... fetching distinct count")
        statement = bios.isql().select("distinctcount(zipCode)").from_context(context_name).build()
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 1
        assert result.get_records()[0].get("distinctcount(zipCode)") == pytest.approx(4, 1.0e-3)

        print("CRD entries")
        self.upsert_entries(context_name, [f"{self.csv(primary_keys[7])},94064,Lawrence,10"])
        self.update_entries(
            context_name,
            primary_keys[3],
            {"zipCode": 94065, "numProducts": 21},
        )
        self.delete_entries(context_name, [primary_keys[0], primary_keys[5]])
        print(" ... request to refresh the feature")
        self.session.feature_refresh(context_name, feature="byZipCode")
        print(" ... wait 20 seconds for the feature being processed")
        time.sleep(20)

        print(" ... fetching the feature")
        statement = (
            bios.isql()
            .select(
                "zipCode",
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
            )
            .from_context(context_name)
            .group_by("zipCode")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        entries = result.get_records()
        assert entries[0].get("zipCode") == 94061
        assert entries[0].get("count()") == 2
        assert entries[0].get("sum(numProducts)") == 31 + 12
        assert entries[0].get("min(numProducts)") == 12
        assert entries[0].get("max(numProducts)") == 31
        assert entries[0].get("avg(numProducts)") == pytest.approx((31 + 12) / 2)

        assert entries[1].get("zipCode") == 94064
        assert entries[1].get("count()") == 3
        assert entries[1].get("sum(numProducts)") == 25 + 10
        assert entries[1].get("min(numProducts)") == 8
        assert entries[1].get("max(numProducts)") == 17
        assert entries[1].get("avg(numProducts)") == pytest.approx((25 + 10) / 3)

        assert entries[2].get("zipCode") == 94065
        assert entries[2].get("count()") == 1
        assert entries[2].get("sum(numProducts)") == 21
        assert entries[2].get("min(numProducts)") == 21
        assert entries[2].get("max(numProducts)") == 21
        assert entries[2].get("avg(numProducts)") == pytest.approx(21)

    @classmethod
    def csv(cls, src: List[Any]) -> str:
        return ",".join([str(element) for element in src])

    def test_multi_dimensional_feature(self):
        context_name = CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE.get("contextName")

        print(" ... fetching feature using the first dimension")
        statement = (
            bios.isql()
            .select(
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
                "sum(quantity)",
                "min(quantity)",
                "max(quantity)",
                "avg(quantity)",
            )
            .from_context(context_name)
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 1
        record = result.get_records()[0]
        sum_num_products = 8 + 10 + 19 + 9 + 5 + 8 + 12
        sum_quantity = 1946 + 2230 + 3100 + 1100 + 819 + 1711 + 9876
        assert record.get("count()") == 7
        assert record.get("sum(numProducts)") == sum_num_products
        assert record.get("min(numProducts)") == 5
        assert record.get("max(numProducts)") == 19
        assert record.get("avg(numProducts)") == pytest.approx(sum_num_products / 7)
        assert record.get("sum(quantity)") == sum_quantity
        assert record.get("min(quantity)") == 819
        assert record.get("max(quantity)") == 9876
        assert record.get("avg(quantity)") == pytest.approx(sum_quantity / 7)

        print(" ... fetching feature using the first dimension")
        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
                "sum(quantity)",
                "min(quantity)",
                "max(quantity)",
                "avg(quantity)",
            )
            .from_context(context_name)
            .group_by("City")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        groups = {entry.get("city"): entry for entry in result.get_records()}
        assert groups.keys() == {"Menlo Park", "Palo Alto", "Santa Clara"}

        rwc = groups.get("Menlo Park")
        assert rwc.get("count()") == 4
        assert rwc.get("sum(numProducts)") == 8 + 10 + 19 + 9
        assert rwc.get("min(numProducts)") == 8
        assert rwc.get("max(numProducts)") == 19
        assert rwc.get("avg(numProducts)") == pytest.approx((8 + 10 + 19 + 9) / 4)
        assert rwc.get("sum(quantity)") == 1946 + 2230 + 3100 + 1100
        assert rwc.get("min(quantity)") == 1100
        assert rwc.get("max(quantity)") == 3100
        assert rwc.get("avg(quantity)") == pytest.approx((1946 + 2230 + 3100 + 1100) / 4)

        sanc = groups.get("Palo Alto")
        assert sanc.get("count()") == 1
        assert sanc.get("sum(numProducts)") == 5
        assert sanc.get("min(numProducts)") == 5
        assert sanc.get("max(numProducts)") == 5
        assert sanc.get("avg(numProducts)") == pytest.approx(5)
        assert sanc.get("sum(quantity)") == 819
        assert sanc.get("min(quantity)") == 819
        assert sanc.get("max(quantity)") == 819
        assert sanc.get("avg(quantity)") == pytest.approx(819)

        paloa = groups.get("Santa Clara")
        assert paloa.get("count()") == 2
        assert paloa.get("sum(numProducts)") == 8 + 12
        assert paloa.get("min(numProducts)") == 8
        assert paloa.get("max(numProducts)") == 12
        assert paloa.get("avg(numProducts)") == pytest.approx((8 + 12) / 2)
        assert paloa.get("sum(quantity)") == 1711 + 9876
        assert paloa.get("min(quantity)") == 1711
        assert paloa.get("max(quantity)") == 9876
        assert paloa.get("avg(quantity)") == pytest.approx((1711 + 9876) / 2)

        print(" ... fetching feature using the second dimension")
        statement = (
            bios.isql()
            .select(
                "storeType",
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
            )
            .from_context(context_name)
            .group_by("storeType")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        groups = {entry.get("storeType"): entry for entry in result.get_records()}
        assert groups.keys() == {100, 101, 102}

        type_100 = groups.get(100)
        assert type_100.get("count()") == 2
        assert type_100.get("sum(numProducts)") == 8 + 5
        assert type_100.get("min(numProducts)") == 5
        assert type_100.get("max(numProducts)") == 8
        assert type_100.get("avg(numProducts)") == pytest.approx((8 + 5) / 2)

        type_101 = groups.get(101)
        assert type_101.get("count()") == 3
        assert type_101.get("sum(numProducts)") == 10 + 8 + 9
        assert type_101.get("min(numProducts)") == 8
        assert type_101.get("max(numProducts)") == 10
        assert type_101.get("avg(numProducts)") == pytest.approx((10 + 8 + 9) / 3)

        type_102 = groups.get(102)
        assert type_102.get("count()") == 2
        assert type_102.get("sum(numProducts)") == 19 + 12
        assert type_102.get("min(numProducts)") == 12
        assert type_102.get("max(numProducts)") == 19
        assert type_102.get("avg(numProducts)") == pytest.approx((12 + 19) / 2)

        print(" ... fetching feature using the both dimensions")
        statement = (
            bios.isql()
            .select("city", "storeType", "count()", "sum(quantity)")
            .from_context(context_name)
            .group_by("city", "storetype")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 6
        groups = {
            ",".join([entry.get("city"), str(entry.get("storeType"))]): entry
            for entry in result.get_records()
        }
        expected_keys = [
            "Menlo Park,100",
            "Menlo Park,101",
            "Menlo Park,102",
            "Palo Alto,100",
            "Santa Clara,101",
            "Santa Clara,102",
        ]
        assert sorted(groups.keys()) == sorted(expected_keys)

        assert groups.get(expected_keys[0]).get("count()") == 1
        assert groups.get(expected_keys[0]).get("sum(quantity)") == 1946

        assert groups.get(expected_keys[1]).get("count()") == 2
        assert groups.get(expected_keys[1]).get("sum(quantity)") == 2230 + 1100

        assert groups.get(expected_keys[2]).get("count()") == 1
        assert groups.get(expected_keys[2]).get("sum(quantity)") == 3100

        assert groups.get(expected_keys[3]).get("count()") == 1
        assert groups.get(expected_keys[3]).get("sum(quantity)") == 819

        assert groups.get(expected_keys[4]).get("count()") == 1
        assert groups.get(expected_keys[4]).get("sum(quantity)") == 1711

        assert groups.get(expected_keys[5]).get("count()") == 1
        assert groups.get(expected_keys[5]).get("sum(quantity)") == 9876

        print(" ... verifying that attributes not in select list are not returned.")
        statement = (
            bios.isql()
            .select("count()", "sum(quantity)")
            .from_context(context_name)
            .group_by("city", "storetype")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 6
        record = result.get_records()[0]
        assert record.get("city") is None
        assert record.get("storetype") is None

        print(" ... total distinct count")
        statement = (
            bios.isql()
            .select(
                "distinctcount(city)",
                "distinctcount(storeType)",
            )
            .from_context(context_name)
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 1
        assert result.get_records()[0].get("distinctcount(city)") == pytest.approx(3, 1.0e-3)
        assert result.get_records()[0].get("distinctcount(storeType)") == pytest.approx(3, 1.0e-3)

        print(" ... distinct count group by city")
        statement = (
            bios.isql()
            .select(
                "city",
                "distinctcount(storeType)",
            )
            .from_context(context_name)
            .group_by("city")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        expected = {
            "Menlo Park": 3,
            "Palo Alto": 1,
            "Santa Clara": 2,
        }
        for record in result.get_records():
            assert record.get("distinctcount(storeType)") == expected.get(record.get("city"))

        print(" ... distinct count group by storeType")
        statement = (
            bios.isql()
            .select(
                "storeType",
                "distinctcount(city)",
            )
            .from_context(context_name)
            .group_by("storeType")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        expected = {
            100: 2,
            101: 2,
            102: 2,
        }
        for record in result.get_records():
            assert record.get("distinctcount(city)") == expected.get(record.get("storeType"))

    def test_query_validation(self):
        stream_config = copy.deepcopy(CONTEXT_WITH_FEATURE)
        context_name = stream_config.get("contextName")

        # invlid metric function domain
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql()
                .select("zipCode", "sum(numProductsx)")
                .from_context(context_name)
                .group_by("zipCode")
                .build()
            )
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        # invlid invalid attribute name
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql()
                .select("zipCodex", "sum(numProducts)")
                .from_context(context_name)
                .group_by("zipCode")
                .build()
            )
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        # attribute name not in group by key
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql()
                .select("numProducts", "sum(numProducts)")
                .from_context(context_name)
                .group_by("zipCode")
                .build()
            )
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        # attribute name not in group by key #2
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql()
                .select("zipCode", "sum(numProducts)")
                .from_context(context_name)
                .build()
            )
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        # sum metric function not taking an argument
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql()
                .select("zipCode", "sum()")
                .from_context(context_name)
                .group_by("zipCode")
                .build()
            )
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.INVALID_ARGUMENT

    def test_filter(self):
        context_name = CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE.get("contextName")

        # Single value filter without groups
        statement = (
            bios.isql()
            .select(
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
                "sum(quantity)",
                "min(quantity)",
                "max(quantity)",
                "avg(quantity)",
            )
            .from_context(context_name)
            .where("city = 'Menlo Park'")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 1
        record = result.get_records()[0]
        assert record.get("count()") == 4
        assert record.get("sum(numProducts)") == 8 + 10 + 19 + 9
        assert record.get("min(numProducts)") == 8
        assert record.get("max(numProducts)") == 19
        assert record.get("avg(numProducts)") == pytest.approx((8 + 10 + 19 + 9) / 4)
        assert record.get("sum(quantity)") == 1946 + 2230 + 3100 + 1100
        assert record.get("min(quantity)") == 1100
        assert record.get("max(quantity)") == 3100
        assert record.get("avg(quantity)") == pytest.approx((1946 + 2230 + 3100 + 1100) / 4)
        assert record.get("city") is None

        # Multi-value filter without groups
        statement = (
            bios.isql()
            .select(
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
                "sum(quantity)",
                "min(quantity)",
                "max(quantity)",
                "avg(quantity)",
            )
            .from_context(context_name)
            .where("city IN ('Menlo Park', 'Santa Clara')")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 1
        record = result.get_records()[0]
        assert record.get("count()") == 6
        assert record.get("sum(numProducts)") == 8 + 10 + 19 + 9 + 8 + 12
        assert record.get("min(numProducts)") == 8
        assert record.get("max(numProducts)") == 19
        assert record.get("avg(numProducts)") == pytest.approx((8 + 10 + 19 + 9 + 8 + 12) / 6)
        assert record.get("sum(quantity)") == 1946 + 2230 + 3100 + 1100 + 1711 + 9876
        assert record.get("min(quantity)") == 1100
        assert record.get("max(quantity)") == 9876
        assert record.get("avg(quantity)") == pytest.approx(
            (1946 + 2230 + 3100 + 1100 + 1711 + 9876) / 6
        )
        assert record.get("city") is None

        # Multi-value filter with parallel group
        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
                "sum(quantity)",
                "min(quantity)",
                "max(quantity)",
                "avg(quantity)",
            )
            .from_context(context_name)
            .where("city IN ('Menlo Park', 'Santa Clara')")
            .group_by("city")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 2
        groups = {entry.get("city"): entry for entry in result.get_records()}
        assert groups.keys() == {"Menlo Park", "Santa Clara"}

        rwc = groups.get("Menlo Park")
        assert rwc.get("count()") == 4
        assert rwc.get("sum(numProducts)") == 8 + 10 + 19 + 9
        assert rwc.get("min(numProducts)") == 8
        assert rwc.get("max(numProducts)") == 19
        assert rwc.get("avg(numProducts)") == pytest.approx((8 + 10 + 19 + 9) / 4)
        assert rwc.get("sum(quantity)") == 1946 + 2230 + 3100 + 1100
        assert rwc.get("min(quantity)") == 1100
        assert rwc.get("max(quantity)") == 3100
        assert rwc.get("avg(quantity)") == pytest.approx((1946 + 2230 + 3100 + 1100) / 4)

        paloa = groups.get("Santa Clara")
        assert paloa.get("count()") == 2
        assert paloa.get("sum(numProducts)") == 8 + 12
        assert paloa.get("min(numProducts)") == 8
        assert paloa.get("max(numProducts)") == 12
        assert paloa.get("avg(numProducts)") == pytest.approx((8 + 12) / 2)
        assert paloa.get("sum(quantity)") == 1711 + 9876
        assert paloa.get("min(quantity)") == 1711
        assert paloa.get("max(quantity)") == 9876
        assert paloa.get("avg(quantity)") == pytest.approx((1711 + 9876) / 2)

        # Multi-value filter with orthogonal group
        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "min(numProducts)",
                "max(numProducts)",
                "avg(numProducts)",
                "sum(quantity)",
                "min(quantity)",
                "max(quantity)",
                "avg(quantity)",
            )
            .from_context(context_name)
            .where("City IN ('Menlo Park', 'Santa Clara') AND storetype = 101")
            .group_by("city")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 2
        groups = {entry.get("city"): entry for entry in result.get_records()}
        assert groups.keys() == {"Menlo Park", "Santa Clara"}

        rwc = groups.get("Menlo Park")
        assert rwc.get("count()") == 2
        assert rwc.get("sum(numProducts)") == 10 + 9
        assert rwc.get("min(numProducts)") == 9
        assert rwc.get("max(numProducts)") == 10
        assert rwc.get("avg(numProducts)") == pytest.approx((10 + 9) / 2)
        assert rwc.get("sum(quantity)") == 2230 + 1100
        assert rwc.get("min(quantity)") == 1100
        assert rwc.get("max(quantity)") == 2230
        assert rwc.get("avg(quantity)") == pytest.approx((2230 + 1100) / 2)

        paloa = groups.get("Santa Clara")
        assert paloa.get("count()") == 1
        assert paloa.get("sum(numProducts)") == 8
        assert paloa.get("min(numProducts)") == 8
        assert paloa.get("max(numProducts)") == 8
        assert paloa.get("avg(numProducts)") == pytest.approx(8 / 1)
        assert paloa.get("sum(quantity)") == 1711
        assert paloa.get("min(quantity)") == 1711
        assert paloa.get("max(quantity)") == 1711
        assert paloa.get("avg(quantity)") == pytest.approx(1711 / 1)

    def test_sorting(self):
        context_name = CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE.get("contextName")

        record_mp = {
            "city": "Menlo Park",
            "count()": 4,
            "sum(numProducts)": 8 + 10 + 19 + 9,
            "sum(quantity)": 1946 + 2230 + 3100 + 1100,
        }
        record_pa = {
            "city": "Palo Alto",
            "count()": 1,
            "sum(numProducts)": 5,
            "sum(quantity)": 819,
        }
        record_sc = {
            "city": "Santa Clara",
            "count()": 2,
            "sum(numProducts)": 8 + 12,
            "sum(quantity)": 1711 + 9876,
        }

        # order by dimension
        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "sum(quantity)",
            )
            .from_context(context_name)
            .group_by("city")
            .order_by("city")
            .build()
        )
        result = self.session.execute(statement)
        assert result.get_records() == [record_mp, record_pa, record_sc]

        # reverse
        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "sum(quantity)",
            )
            .from_context(context_name)
            .group_by("city")
            .order_by("City", reverse=True)
            .build()
        )
        result = self.session.execute(statement)
        assert result.get_records() == [record_sc, record_pa, record_mp]

        # sort by count
        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "sum(quantity)",
            )
            .from_context(context_name)
            .group_by("city")
            .order_by("count()")
            .build()
        )
        result = self.session.execute(statement)
        assert result.get_records() == [record_pa, record_sc, record_mp]

        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numproducts)",
                "sum(quantity)",
            )
            .from_context(context_name)
            .group_by("city")
            .order_by("count()", reverse=True)
            .build()
        )
        result = self.session.execute(statement)
        assert result.get_records() == [record_mp, record_sc, record_pa]

        # sort by sum
        statement = (
            bios.isql()
            .select(
                "CITY",
                "COUNT()",
                "SUM(NUMPRODUCTS)",
                "SUM(QUANTITY)",
            )
            .from_context(context_name)
            .group_by("city")
            .order_by("sum(quantity)")
            .build()
        )
        result = self.session.execute(statement)
        assert result.get_records() == [record_pa, record_mp, record_sc]

        statement = (
            bios.isql()
            .select(
                "city",
                "count()",
                "sum(numProducts)",
                "sum(quantity)",
            )
            .from_context(context_name)
            .group_by("city")
            .order_by("sum(Quantity)", reverse=True)
            .build()
        )
        result = self.session.execute(statement)
        assert result.get_records() == [record_sc, record_mp, record_pa]

    def test_context_feature_modifications(self):
        print("\n#### test_context_feature_modifications")
        stream_config = copy.deepcopy(CONTEXT_WITH_FEATURE)
        context_name = "test_context_feature_update"
        stream_config["contextName"] = context_name
        print(f" ... adding the initial context {context_name}")
        setup_context(self.session, stream_config)

        self.upsert_entries(
            context_name,
            [
                "10001,94061,Roosevelt,20",
                "10002,94061,Woodside,10",
                "10003,94064,The Willows,30",
                "10004,94066,Menlo Park,10",
                "10005,94064,Santa Clara,40",
                "10006,94067,Palo Alto,10",
                "10007,94061,Palm Park,30",
            ],
        )

        # Add a new feature.
        stream_config["features"].append(
            {
                "featureName": "byNumProducts",
                "dimensions": ["numProducts"],
                "attributes": ["storeId", "address"],
                "featureInterval": 15000,
                "indexed": False,
                "aggregated": True,
            }
        )
        print(f" ... modifying context {context_name} to add a feature")
        self.session.update_context(context_name, stream_config)

        time.sleep(25)
        statement = (
            bios.isql()
            .select("numProducts", "count()", "min(storeId)")
            .from_context(context_name)
            .group_by("numProducts")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 4
        assert result.get_records()[0].get("numProducts") == 10
        assert result.get_records()[0].get("count()") == 3
        assert result.get_records()[0].get("min(storeId)") == 10002
        assert result.get_records()[1].get("numProducts") == 20
        assert result.get_records()[1].get("count()") == 1
        assert result.get_records()[1].get("min(storeId)") == 10001
        assert result.get_records()[2].get("numProducts") == 30
        assert result.get_records()[2].get("count()") == 2
        assert result.get_records()[2].get("min(storeId)") == 10003
        assert result.get_records()[3].get("numProducts") == 40
        assert result.get_records()[3].get("count()") == 1
        assert result.get_records()[3].get("min(storeId)") == 10005

        # Edit the newly added feature.
        stream_config["features"][1]["attributes"] = ["zipCode"]
        print(f" ... modifying a feature in context {context_name}")
        new_context = self.session.update_context(context_name, stream_config)
        assert new_context["features"][1]["attributes"] == ["zipCode"]

        time.sleep(20)
        statement = (
            bios.isql()
            .select("numProducts", "count()", "max(zipCode)")
            .from_context(context_name)
            .group_by("numProducts")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 4
        assert result.get_records()[0].get("numProducts") == 10
        assert result.get_records()[0].get("count()") == 3
        assert result.get_records()[0].get("max(zipCode)") == 94067
        assert result.get_records()[1].get("numProducts") == 20
        assert result.get_records()[1].get("count()") == 1
        assert result.get_records()[1].get("max(zipCode)") == 94061
        assert result.get_records()[2].get("numProducts") == 30
        assert result.get_records()[2].get("count()") == 2
        assert result.get_records()[2].get("max(zipCode)") == 94064
        assert result.get_records()[3].get("numProducts") == 40
        assert result.get_records()[3].get("count()") == 1
        assert result.get_records()[3].get("max(zipCode)") == 94064

        # Remove the newly added feature.
        del stream_config["features"][1]
        print(f" ... removing the newly added feature in context {context_name}")
        new_context = self.session.update_context(context_name, stream_config)
        assert len(new_context["features"]) == 1

        with pytest.raises(ServiceError) as excinfo:
            self.session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        statement = (
            bios.isql()
            .select("zipCode", "count()")
            .from_context(context_name)
            .group_by("zipCode")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 4
        assert result.get_records()[0].get("zipCode") == 94061
        assert result.get_records()[0].get("count()") == 3
        assert result.get_records()[1].get("zipCode") == 94064
        assert result.get_records()[1].get("count()") == 2
        assert result.get_records()[2].get("zipCode") == 94066
        assert result.get_records()[2].get("count()") == 1
        assert result.get_records()[3].get("zipCode") == 94067
        assert result.get_records()[3].get("count()") == 1

        # Add an attribute to the context.
        stream_config["attributes"].append(
            {"attributeName": "extra", "type": "Integer", "default": 0}
        )
        print(f" ... adding an attribute to context {context_name}")
        new_context = self.session.update_context(context_name, stream_config)
        assert new_context["attributes"][-1]["attributeName"] == "extra"

        # TODO(BIOS-5611): Enable this verification
        result = self.session.execute(statement)
        assert len(result.get_records()) == 4
        assert result.get_records()[0].get("zipCode") == 94061
        assert result.get_records()[0].get("count()") == 3
        assert result.get_records()[1].get("zipCode") == 94064
        assert result.get_records()[1].get("count()") == 2
        assert result.get_records()[2].get("zipCode") == 94066
        assert result.get_records()[2].get("count()") == 1
        assert result.get_records()[3].get("zipCode") == 94067
        assert result.get_records()[3].get("count()") == 1

        # Delete the newly added attribute.
        stream_config["attributes"].pop()
        print(f" ... removing the newly added attribute from context {context_name}")
        new_context = self.session.update_context(context_name, stream_config)
        assert new_context["attributes"][-1]["attributeName"] != "extra"

        # TODO(BIOS-5611): Enable this verification
        result = self.session.execute(statement)
        assert len(result.get_records()) == 4
        assert result.get_records()[0].get("zipCode") == 94061
        assert result.get_records()[0].get("count()") == 3
        assert result.get_records()[1].get("zipCode") == 94064
        assert result.get_records()[1].get("count()") == 2
        assert result.get_records()[2].get("zipCode") == 94066
        assert result.get_records()[2].get("count()") == 1
        assert result.get_records()[3].get("zipCode") == 94067
        assert result.get_records()[3].get("count()") == 1

    def test_context_update(self):
        print("\n#### test_context_update")
        stream_config = copy.deepcopy(CONTEXT_WITH_FEATURE)
        context_name = "test_feature_with_context_update"
        stream_config["contextName"] = context_name
        print(f" ... adding the initial context {context_name}")
        setup_context(self.session, stream_config)

        self.upsert_entries(
            context_name,
            [
                "10001,94061,Roosevelt,20",
                "10002,94061,Woodside,10",
                "10003,94064,The Willows,30",
                "10004,94066,Menlo Park,10",
                "10005,94064,Santa Clara,40",
                "10006,94067,Palo Alto,10",
                "10007,94061,Palm Park,30",
            ],
        )

        # Modify context
        stream_config["attributes"].append(
            {"attributeName": "areaCode", "type": "Integer", "default": -1}
        )
        stream_config["features"].append(
            {
                "featureName": "byNumProducts",
                "dimensions": ["numProducts"],
                "attributes": ["storeId", "address"],
                "featureInterval": 15000,
                "indexed": False,
                "aggregated": True,
            }
        )
        stream_config["features"].append(
            {
                "featureName": "byProductCategory",
                "dimensions": ["areaCode"],
                "attributes": ["storeId", "numProducts", "address"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "RangeQuery",
                "aggregated": True,
            }
        )
        print(f" ... modifying context {context_name} to add a feature")
        self.session.update_context(context_name, stream_config)

        self.upsert_entries(
            context_name,
            [
                "10004,94066,Menlo Park,5,21",
                "10011,95023,Hollister,60,31",
                "10010,95021,Gilroy,5,31",
            ],
        )

        time.sleep(25)
        statement = (
            bios.isql()
            .select("numProducts", "count()", "min(storeId)")
            .from_context(context_name)
            .group_by("numProducts")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 6
        assert result.get_records()[0].get("numProducts") == 5
        assert result.get_records()[0].get("count()") == 2
        assert result.get_records()[0].get("min(storeId)") == 10004
        assert result.get_records()[1].get("numProducts") == 10
        assert result.get_records()[1].get("count()") == 2
        assert result.get_records()[1].get("min(storeId)") == 10002
        assert result.get_records()[2].get("numProducts") == 20
        assert result.get_records()[2].get("count()") == 1
        assert result.get_records()[2].get("min(storeId)") == 10001
        assert result.get_records()[3].get("numProducts") == 30
        assert result.get_records()[3].get("count()") == 2
        assert result.get_records()[3].get("min(storeId)") == 10003
        assert result.get_records()[4].get("numProducts") == 40
        assert result.get_records()[4].get("count()") == 1
        assert result.get_records()[4].get("min(storeId)") == 10005
        assert result.get_records()[5].get("numProducts") == 60
        assert result.get_records()[5].get("count()") == 1
        assert result.get_records()[5].get("min(storeId)") == 10011

        statement = (
            bios.isql()
            .select("storeId", "areaCode", "address")
            .from_context(context_name)
            .where("areaCode < 0")
            .build()
        )
        result = self.session.execute(statement)
        assert len(result.get_records()) == 6
        assert result.get_records()[0].get("areaCode") == -1
        assert result.get_records()[0].get("storeId") == 10001
        assert result.get_records()[0].get("address") == "Roosevelt"
        assert result.get_records()[1].get("areaCode") == -1
        assert result.get_records()[1].get("storeId") == 10002
        assert result.get_records()[1].get("address") == "Woodside"
        assert result.get_records()[2].get("areaCode") == -1
        assert result.get_records()[2].get("storeId") == 10003
        assert result.get_records()[2].get("address") == "The Willows"
        assert result.get_records()[3].get("areaCode") == -1
        assert result.get_records()[3].get("storeId") == 10005
        assert result.get_records()[3].get("address") == "Santa Clara"
        assert result.get_records()[4].get("areaCode") == -1
        assert result.get_records()[4].get("storeId") == 10006
        assert result.get_records()[4].get("address") == "Palo Alto"
        assert result.get_records()[5].get("areaCode") == -1
        assert result.get_records()[5].get("storeId") == 10007
        assert result.get_records()[5].get("address") == "Palm Park"

        statement = (
            bios.isql()
            .select("storeId", "areaCode", "address")
            .from_context(context_name)
            .where("areaCode > 0")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        assert result.get_records()[0].get("areaCode") == 21
        assert result.get_records()[0].get("storeId") == 10004
        assert result.get_records()[0].get("address") == "Menlo Park"
        assert result.get_records()[1].get("areaCode") == 31
        assert result.get_records()[1].get("storeId") == 10010
        assert result.get_records()[1].get("address") == "Gilroy"
        assert result.get_records()[2].get("areaCode") == 31
        assert result.get_records()[2].get("storeId") == 10011
        assert result.get_records()[2].get("address") == "Hollister"

        statement = (
            bios.isql()
            .select("areaCode", "count()", "sum(numProducts)")
            .from_context(context_name)
            .group_by("areaCode")
            .build()
        )
        result = self.session.execute(statement)
        pprint(result.to_dict())
        assert len(result.get_records()) == 3
        assert result.get_records()[0].get("areaCode") == -1
        assert result.get_records()[0].get("count()") == 6
        assert result.get_records()[0].get("sum(numProducts)") == 20 + 10 + 30 + 40 + 10 + 30
        assert result.get_records()[1].get("areaCode") == 21
        assert result.get_records()[1].get("count()") == 1
        assert result.get_records()[1].get("sum(numProducts)") == 5
        assert result.get_records()[2].get("areaCode") == 31
        assert result.get_records()[2].get("count()") == 2
        assert result.get_records()[2].get("sum(numProducts)") == 65


if __name__ == "__main__":
    pytest.main(sys.argv)
