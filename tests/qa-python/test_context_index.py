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
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
CONTEXT = {
    "contextName": "storeContextUpdateIndex",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "storeId", "type": "Integer"},
        {"attributeName": "zipCode", "type": "Integer"},
        {"attributeName": "address", "type": "String"},
        {"attributeName": "numProducts", "type": "Integer"},
    ],
    "primaryKey": ["storeId"],
    "auditEnabled": False,
}

CONTEXT_WITH_INDEX = {
    "contextName": "storeContextWithIndex",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "storeId", "type": "Integer"},
        {"attributeName": "zipCode", "type": "Integer"},
        {"attributeName": "address", "type": "String"},
        {"attributeName": "numProducts", "type": "Integer"},
    ],
    "primaryKey": ["storeId"],
    "auditEnabled": True,
    "features": [
        {
            "featureName": "byZipCode",
            "dimensions": ["zipCode"],
            "attributes": ["numProducts"],
            "featureInterval": 15000,
            "indexed": True,
            "indexType": "RangeQuery",
        }
    ],
}

CONTEXT_WITH_MULTIPLE_INDEX = {
    "contextName": "storeContextWithMultiIndex",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "storeId", "type": "Integer"},
        {"attributeName": "zipCode", "type": "Integer"},
        {"attributeName": "address", "type": "String"},
        {"attributeName": "numProducts", "type": "Integer"},
    ],
    "primaryKey": ["storeId"],
    "auditEnabled": True,
    "features": [
        {
            "featureName": "byZipCodeRange",
            "dimensions": ["zipCode"],
            "attributes": ["numProducts"],
            "featureInterval": 15000,
            "indexed": True,
            "indexType": "RangeQuery",
        },
        {
            "featureName": "byZipCodeExact",
            "dimensions": ["zipCode"],
            "attributes": ["numProducts"],
            "featureInterval": 15000,
            "indexed": True,
            "indexType": "ExactMatch",
        },
    ],
}


class BiosContextIndexTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

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
    def update_entries(cls, context_name, key, new_values):
        update_request = bios.isql().update(context_name).set(new_values).where(key=key).build()
        cls.session.execute(update_request)

    @classmethod
    def delete_entries(cls, context_name, keys):
        delete_request = bios.isql().delete().from_context(context_name).where(keys=keys).build()
        cls.session.execute(delete_request)

    @classmethod
    def make_audit_signal_name(cls, context_name: str):
        return "audit" + context_name[0:1].upper() + context_name[1:]

    def test_index_rangequery(self):
        context_name = "storeContextWithIndexRangeQuery"
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["attributes"].append({"attributeName": "storeName", "type": "String"})
        context["features"][0]["attributes"] = ["address", "numProducts"]
        context["contextName"] = context_name

        print(f"\n ... creating context {context_name}")
        setup_context(self.session, context)

        print(" ... upserting entries")
        records = [
            "30001,94061,Redwood City,10,Roosevelt",
            "30002,94064,The Willows,17,Willows",
            "30003,94061,Woodside,20,Canada Road",
            "30004,94050,Santa Clara,9,Lawrence",
        ]
        self.upsert_entries(context_name, records)

        print(" ... sleeping to wait for indexing done")
        time.sleep(35)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address", "numProducts")
            .from_context(context_name)
            .where("zipCode <= 94061")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 3
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30004
        assert record0["zipCode"] == 94050
        assert record0["address"] == "Santa Clara"
        assert record0["numProducts"] == 9
        assert "storeName" not in record0

        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30001
        assert record1["zipCode"] == 94061
        assert record1["address"] == "Redwood City"
        assert record1["numProducts"] == 10

        record2 = select_resp.get_records()[2]
        assert record2["storeId"] == 30003
        assert record2["zipCode"] == 94061
        assert record2["address"] == "Woodside"
        assert record2["numProducts"] == 20

        print(" ... testing exact match")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context_name)
            .where("zipCode in (94064, 94050)")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30004
        assert record0["zipCode"] == 94050
        assert record0["numProducts"] == 9
        assert "address" not in record0

        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30002
        assert record1["zipCode"] == 94064
        assert record1["numProducts"] == 17
        assert "address" not in record1

        print(" ... query not covered by index should be rejected")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address", "numProducts", "storeName")
            .from_context(context_name)
            .where("zipCode = 94061")
            .build()
        )
        with pytest.raises(ServiceError) as exc_info:
            result = self.session.execute(select_req)
            pprint(result.get_records(), width=200)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT
        assert "No suitable features found for the specified query" in exc_info.value.message

    def test_index_exactmatch(self):
        context_name = "storeContextWithIndexExactMatch"
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["attributes"].append({"attributeName": "storeName", "type": "String"})
        context["features"][0]["indexType"] = "ExactMatch"
        context["features"][0]["attributes"] = ["address", "numProducts"]
        context["contextName"] = context_name
        print(f"\n ... creating context {context_name}")
        setup_context(self.session, context)

        print(" ... upserting entries")
        records = [
            "30001,94061,Redwood City,10,Roosevelt",
            "30002,94064,The Willows,17,Willows",
            "30003,94061,Woodside,20,Canada Road",
        ]
        self.upsert_entries(context_name, records)

        print(" ... sleeping to wait for indexing done")
        time.sleep(35)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address", "numProducts")
            .from_context(context_name)
            .where("zipCode = 94061")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30001
        assert record0["zipCode"] == 94061
        assert record0["address"] == "Redwood City"
        assert record0["numProducts"] == 10

        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30003
        assert record1["zipCode"] == 94061
        assert record1["address"] == "Woodside"
        assert record1["numProducts"] == 20

        print(" ... query not covered by index should be rejected")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address", "numProducts", "storeName")
            .from_context(context_name)
            .where("zipCode = 94061")
            .build()
        )
        pprint(select_req.query)
        with pytest.raises(ServiceError) as exc_info:
            result = self.session.execute(select_req)
            pprint(result.get_records(), width=200)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT
        assert "No suitable features found for the specified query" in exc_info.value.message

        print(" ... range query should be rejected")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address", "numProducts")
            .from_context(context_name)
            .where("zipCode > 94061")
            .build()
        )
        with pytest.raises(ServiceError) as exc_info:
            result = self.session.execute(select_req)
            pprint(result.get_records(), width=200)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT
        assert "No suitable features found for the specified query" in exc_info.value.message

    def test_index_rangequery_to_primary_key(self):
        context_name = "storeContextWithPrimaryKeyIndex"
        context = copy.deepcopy(CONTEXT)
        context["contextName"] = context_name
        context["features"] = [
            {
                "featureName": "byStoreId",
                "dimensions": ["storeId"],
                "attributes": ["zipCode", "address"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "RangeQuery",
            }
        ]

        print(f"\n ... creating context {context_name}")
        setup_context(self.session, context)

        print(" ... upserting entries")
        records = [
            "30001,94061,Redwood City,10",
            "30002,94064,The Willows,17",
            "30003,94061,Woodside,20",
            "30004,94050,Santa Clara,9",
        ]
        self.upsert_entries(context_name, records)

        print(" ... sleeping to wait for indexing done")
        time.sleep(35)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address")
            .from_context(context_name)
            .where("storeId <= 30002")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30001
        assert record0["zipCode"] == 94061
        assert record0["address"] == "Redwood City"

        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30002
        assert record1["zipCode"] == 94064
        assert record1["address"] == "The Willows"

        print(" ... testing exact match")
        select_req = (
            bios.isql()
            .select()
            .from_context(context_name)
            .where("storeId in (30002, 30004)")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30002
        assert record0["zipCode"] == 94064
        assert record0["address"] == "The Willows"
        assert record0["numProducts"] == 17

        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30004
        assert record1["zipCode"] == 94050
        assert record1["address"] == "Santa Clara"
        assert record1["numProducts"] == 9

        print(" ... query not covered by index should be rejected")
        select_req = (
            bios.isql().select().from_context(context_name).where("storeId > 30002").build()
        )
        with pytest.raises(ServiceError) as exc_info:
            result = self.session.execute(select_req)
            pprint(result.get_records(), width=200)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT
        assert "No suitable features found for the specified query" in exc_info.value.message

    def test_index_with_multi_dimensions(self):
        context_name = "storeContextAttributeFromKey"
        context = copy.deepcopy(CONTEXT)
        context["contextName"] = context_name
        context["features"] = [
            {
                "featureName": "byZipCodeNumProducts",
                "dimensions": ["zipCode", "numProducts"],
                "attributes": ["address"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "RangeQuery",
            }
        ]

        print(f"\n ... creating context {context_name}")
        setup_context(self.session, context)

        print(" ... upserting entries")
        records = [
            "30001,94061,Redwood City,10",
            "30002,94064,The Willows,17",
            "30003,94061,Woodside,20",
            "30004,94050,Santa Clara,9",
        ]
        self.upsert_entries(context_name, records)

        print(" ... sleeping to wait for indexing done")
        time.sleep(35)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address")
            .from_context(context_name)
            .where("zipCode <= 94061")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 3

        print("Testing: additional dimension column is returned even when its not in attributes.")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "address", "numProducts")
            .from_context(context_name)
            .where("zipCode <= 94061")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 3

    def test_create_context_with_multiple_index(self):
        context_name = CONTEXT_WITH_MULTIPLE_INDEX.get("contextName")
        print(f"\n ... creating context {context_name}")
        setup_context(self.session, CONTEXT_WITH_MULTIPLE_INDEX)

    def test_retroactive_indexing(self):
        context_name = "storeContextRetroactiveIndexing"
        initial_context = copy.deepcopy(CONTEXT)
        initial_context["contextName"] = context_name
        print(f"\n ... creating context {context_name}")
        setup_context(self.session, initial_context)

        print(" ... upserting a few entries")
        records = ["30001,94061,Redwood City,10", "30002,94064,The Willows,17"]
        self.upsert_entries(context_name, records)

        print(f" ... updating context {context_name}")
        context = copy.deepcopy(CONTEXT)
        context["contextName"] = context_name
        context["auditEnabled"] = True
        context["features"] = [
            {
                "featureName": "byZipCode",
                "dimensions": ["zipCode"],
                "attributes": ["numProducts"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "RangeQuery",
            }
        ]
        self.session.update_context(context_name, context)

        print(" ... sleeping to wait for indexing done")
        time.sleep(35)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context_name)
            .where("zipCode > 94061")
            .build()
        )

        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 1
        record = select_resp.get_records()[0]
        assert record["storeId"] == 30002
        assert record["numProducts"] == 17
        assert record["zipCode"] == 94064

        print(" ... upserting additional entries")
        records = ["30003,94061,Woodside,25", "30004,94067,Palo Alto,9"]
        self.upsert_entries(context_name, records)

        print(" ... sleeping again to wait for indexing done")
        time.sleep(30)

        print(" ... verifying the updated index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context_name)
            .where("zipCode > 94061")
            .build()
        )

        for _ in range(30):
            select_resp = self.session.execute(select_req)
            if len(select_resp.get_records()) == 2:
                break
            time.sleep(1)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30002
        assert record0["numProducts"] == 17
        assert record0["zipCode"] == 94064
        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30004
        assert record1["numProducts"] == 9
        assert record1["zipCode"] == 94067

        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context_name)
            .where("zipCode <= 94061")
            .build()
        )

        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30001
        assert record0["numProducts"] == 10
        assert record0["zipCode"] == 94061
        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30003
        assert record1["numProducts"] == 25
        assert record1["zipCode"] == 94061

    def test_index_modification(self):
        context_name = "storeContextIndexModTestTest"
        initial_context = copy.deepcopy(CONTEXT)
        initial_context["contextName"] = context_name
        initial_context["auditEnabled"] = True
        initial_context["features"] = [
            {
                "featureName": "byZipCode",
                "dimensions": ["zipCode"],
                "attributes": ["numProducts"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "RangeQuery",
            }
        ]

        print(f"\n ... creating context {context_name}")
        setup_context(self.session, initial_context)

        print(" ... upserting a few entries")
        records = [
            "30001,94061,Redwood City,10",
            "30002,94064,The Willows,17",
            "30003,94061,Woodside,25",
            "30004,94067,Palo Alto,9",
        ]
        self.upsert_entries(context_name, records)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context_name)
            .where("zipCode > 94061")
            .build()
        )

        for _ in range(10):
            select_resp = self.session.execute(select_req)
            if len(select_resp.get_records()) == 2:
                break
            time.sleep(5)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2
        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30002
        assert record0["numProducts"] == 17
        assert record0["zipCode"] == 94064
        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30004
        assert record1["numProducts"] == 9
        assert record1["zipCode"] == 94067

        print(" ... modifying the index")
        context = copy.deepcopy(initial_context)
        context["features"] = [
            {
                "featureName": "byZipCode",
                "dimensions": ["address"],
                "attributes": ["zipCode", "numProducts"],
                "featureInterval": 15000,
                "indexed": True,
                "indexType": "ExactMatch",
            }
        ]
        self.session.update_context(context_name, context)

        print(" ... verifying the created index")
        select_req = (
            bios.isql()
            .select("storeId", "address", "zipCode", "numProducts")
            .from_context(context_name)
            .where("address in ('Redwood City', 'Palo Alto')")
            .build()
        )

        for _ in range(10):
            select_resp = self.session.execute(select_req)
            if len(select_resp.get_records()) == 2:
                break
            time.sleep(5)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 2

        record0 = select_resp.get_records()[0]
        assert record0["storeId"] == 30004
        assert record0["address"] == "Palo Alto"
        assert record0["numProducts"] == 9
        assert record0["zipCode"] == 94067
        record1 = select_resp.get_records()[1]
        assert record1["storeId"] == 30001
        assert record1["address"] == "Redwood City"
        assert record1["numProducts"] == 10
        assert record1["zipCode"] == 94061

    def test_create_context_with_index_with_upsert(self):
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["contextName"] = "storeContextWithIndexUpsertTest"
        self.addCleanup(self.session.delete_context, context["contextName"])
        self.session.create_context(context)
        records = ["123,411007,Aundh,10", "0,888888,Unknown,0"]
        self.upsert_entries(context["contextName"], records)
        self.update_entries(context["contextName"], [123], {"numProducts": 20})
        self.delete_entries(context["contextName"], [[0]])

        time.sleep(5)

        req = (
            bios.isql()
            .select()
            .from_signal(self.make_audit_signal_name(context["contextName"]))
            .time_range(bios.time.now(), -bios.time.minutes(1))
            .build()
        )
        resp = self.session.execute(req)
        self.assertIsNotNone(resp)
        pprint(resp)
        self.assertEqual(len(resp.data_windows), 1)
        self.assertEqual(len(resp.data_windows[0].records), 4)

        # wait for rollups
        time.sleep(35)

        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 0")
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records())
        self.assertEqual(len(select_resp.get_records()), 1)
        record = select_resp.get_records()[0]
        self.assertEqual(record["numProducts"], 20)
        self.assertEqual(record["storeId"], 123)
        self.assertEqual(record["zipCode"], 411007)

    def test_context_index_limit(self):
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["contextName"] = "storeContextIndexLimitTest"
        self.addCleanup(self.session.delete_context, context["contextName"])
        self.session.create_context(context)
        records = [
            "1231,10000,Addr1,10",
            "1232,10001,Addr2,20",
            "1233,10002,Addr3,30",
            "1234,10003,Addr4,40",
            "1235,10004,Addr5,50",
            "1236,10005,Addr6,60",
            "1237,10006,Addr7,70",
            "1238,10007,Addr8,80",
            "1239,10008,Addr9,90",
            "1240,10009,Addr10,100",
            "0,00001,Unknown,0",
        ]
        self.upsert_entries(context["contextName"], records)

        # wait for rollups
        time.sleep(35)

        with self.assertRaises(ServiceError) as error_context:
            select_req = (
                bios.isql()
                .select("storeId", "zipCode", "numProducts")
                .from_context(context["contextName"])
                .where("zipCode >= 10003")
                .limit(-5)
                .build()
            )
            self.session.execute(select_req)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(error_context.exception.message, ".*Limit can't be negative.*")

        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .limit(5)
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records())
        self.assertEqual(len(select_resp.get_records()), 5)
        for record in select_resp.get_records():
            self.assertGreaterEqual(record["zipCode"], 10003)

    def test_context_index_orderby(self):
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["contextName"] = "storeContextIndexOrderbyTest"
        self.addCleanup(self.session.delete_context, context["contextName"])
        self.session.create_context(context)
        records = [
            "1231,10000,Addr1,10",
            "1232,10001,Addr2,30",
            "1233,10002,Addr3,20",
            "1234,10003,Addr4,50",
            "1235,10004,Addr5,40",
            "1236,10005,Addr6,70",
            "1237,10006,Addr7,60",
            "1238,10007,Addr8,80",
            "1239,10008,Addr9,100",
            "1240,10009,Addr10,90",
            "0,11111,Unknown,0",
        ]
        self.upsert_entries(context["contextName"], records)

        # wait for rollups
        time.sleep(35)

        print("--- select context filtered by zipCode reversely ordered by zipCode")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("zipCode", reverse=True)
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records())
        for i, record in enumerate(select_resp.get_records()):
            current_zipcode = record["zipCode"]
            if i == 0:
                assert current_zipcode == 11111
            else:
                assert current_zipcode < last_zipcode
            last_zipcode = current_zipcode

        print("--- select context filtered by zipCode reversely ordered by zipCode limited to 3")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("zipCode", reverse=True)
            .limit(3)
            .build()
        )

        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        records = select_resp.get_records()
        assert len(records) == 3
        assert records[0].get("numProducts") == 0
        assert records[0].get("zipCode") == 11111
        assert records[1].get("numProducts") == 90
        assert records[1].get("zipCode") == 10009
        assert records[2].get("numProducts") == 100
        assert records[2].get("zipCode") == 10008

        for i, record in enumerate(select_resp.get_records()):
            current_zipcode = record["zipCode"]
            if i == 0:
                assert current_zipcode == 11111
            else:
                assert current_zipcode < last_zipcode
            last_zipcode = current_zipcode

        print("--- select context filtered by zipCode ordered by zipCode")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("zipCode")
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records())
        for i, record in enumerate(select_resp.get_records()):
            current_zipcode = record["zipCode"]
            if i == 0:
                assert current_zipcode == 10003
            else:
                assert current_zipcode > last_zipcode
            last_zipcode = current_zipcode

        print("--- select context filtered by zipCode ordered by zipCode limited to 3")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("zipCode")
            .limit(3)
            .build()
        )

        select_resp = self.session.execute(select_req)
        records = select_resp.get_records()
        pprint(records)
        assert len(records) == 3
        assert records[0].get("numProducts") == 50
        assert records[0].get("zipCode") == 10003
        assert records[1].get("numProducts") == 40
        assert records[1].get("zipCode") == 10004
        assert records[2].get("numProducts") == 70
        assert records[2].get("zipCode") == 10005

        print("--- select filtered by zip code reversely ordered by num products")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("numProducts", reverse=True)
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records())
        for i, record in enumerate(select_resp.get_records()):
            if i == 0:
                assert record["numProducts"] == 100
            else:
                assert record["numProducts"] < last_num_products
            last_num_products = record["numProducts"]

        print("--- select filtered by zip code reversely ordered by num products limit 3")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("numProducts", reverse=True)
            .limit(3)
            .build()
        )

        select_resp = self.session.execute(select_req)
        records = select_resp.get_records()
        pprint(records)
        assert len(records) == 3
        assert records[0].get("numProducts") == 100
        assert records[0].get("zipCode") == 10008
        assert records[1].get("numProducts") == 90
        assert records[1].get("zipCode") == 10009
        assert records[2].get("numProducts") == 80
        assert records[2].get("zipCode") == 10007

        print("--- select filtered by zip code reversely order by num products")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("numProducts")
            .build()
        )

        select_resp = self.session.execute(select_req)
        self.assertIsNotNone(select_resp)
        self.assertIsNotNone(select_resp.get_records())
        pprint(select_resp.get_records())
        for i, record in enumerate(select_resp.get_records()):
            if i == 0:
                assert record["numProducts"] == 0
            else:
                assert record["numProducts"] > last_num_products
            last_num_products = record["numProducts"]

        print("--- select filtered by zip code reversely order by num products limit 3")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("numProducts")
            .limit(3)
            .build()
        )

        select_resp = self.session.execute(select_req)
        records = select_resp.get_records()
        pprint(records)
        assert len(records) == 3
        assert records[0].get("numProducts") == 0
        assert records[0].get("zipCode") == 11111
        assert records[1].get("numProducts") == 40
        assert records[1].get("zipCode") == 10004
        assert records[2].get("numProducts") == 50
        assert records[2].get("zipCode") == 10003

        print("Testing: order_by should fail if the order_by attribute is not in the index.")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode <= 10006")
            .order_by("address")
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            select_resp = self.session.execute(select_req)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*No suitable features found.*",
        )

        print("Testing: order_by should succeed even if the attribute is not a dimension.")
        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode <= 10006")
            .order_by("storeId")
            .build()
        )
        select_resp = self.session.execute(select_req)
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == 7

    def test_context_index_on_the_fly_basic(self):
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["contextName"] = "storeContextIndexOnTheFlyBasic"
        self.addCleanup(self.session.delete_context, context["contextName"])
        self.session.create_context(context)
        select_req_on_the_fly = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10000")
            .on_the_fly()
            .build()
        )

        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10000")
            .build()
        )

        for i in range(1, 51):
            records = [
                f"{i},{10000 + i},Addr{i},{i * 10}",
            ]
            self.upsert_entries(context["contextName"], records)
            time.sleep(1)

            # On the fly should return all latest entries.
            select_resp_on_the_fly = self.session.execute(select_req_on_the_fly)
            print(f"\non-the-fly: {i}")
            pprint(select_resp_on_the_fly.get_records())
            self.assertEqual(len(select_resp_on_the_fly.get_records()), i)

            # Without on the fly, latest entries may or may not get returned.
            select_resp = self.session.execute(select_req)
            print(f"index: {i}")
            pprint(select_resp.get_records())
            assert len(select_resp.get_records()) <= i

            # Indexing should not lag too far behind.
            self.assertGreaterEqual(len(select_resp.get_records()), i - 40)

    def test_context_index_on_the_fly(self):
        context = copy.deepcopy(CONTEXT_WITH_INDEX)
        context["contextName"] = "storeContextIndexOnTheFly"
        self.addCleanup(self.session.delete_context, context["contextName"])
        self.session.create_context(context)
        records = [
            "11,10001,Addr1,10",
            "12,10002,Addr2,20",
            "13,10003,Addr3,30",
            "14,10004,Addr4,40",
            "15,10005,Addr5,50",
            "16,10006,Addr6,60",
            "17,10007,Addr7,70",
            "18,10008,Addr8,80",
            "19,10009,Addr9,90",
        ]
        self.upsert_entries(context["contextName"], records)

        # Wait for rollups and then insert / delete some entries.
        time.sleep(35)
        records = [
            "21,10001,Addr1,10",
            "22,10002,Addr2,20",
            "23,10003,Addr3,30",
            "24,10004,Addr4,40",
            "25,10005,Addr5,50",
            "26,10006,Addr6,60",
            "27,10007,Addr7,70",
            "28,10008,Addr8,80",
            "29,10009,Addr9,90",
        ]
        self.upsert_entries(context["contextName"], records)
        self.delete_entries(context["contextName"], [[12], [18]])

        select_req_on_the_fly = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("zipCode", reverse=True)
            .on_the_fly()
            .build()
        )

        select_req = (
            bios.isql()
            .select("storeId", "zipCode", "numProducts")
            .from_context(context["contextName"])
            .where("zipCode >= 10003")
            .order_by("zipCode")
            .build()
        )

        # On the fly should return new entries and should not return deleted entries.
        select_resp_on_the_fly = self.session.execute(select_req_on_the_fly)
        print("\nFirst on-the-fly select")
        pprint(select_resp_on_the_fly.get_records())
        store_ids = {13, 14, 15, 16, 17, 19, 23, 24, 25, 26, 27, 28, 29}
        assert len(select_resp_on_the_fly.get_records()) == len(store_ids)
        for record in select_resp_on_the_fly.get_records():
            assert record.get("storeId") in store_ids

        # Without on the fly, even deleted entries may get returned and newer entries
        # may not get returned.
        select_resp = self.session.execute(select_req)
        print("First regular select")
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) <= len(select_resp_on_the_fly.get_records())

        # Wait for rollups and then insert some more entries.
        for _ in range(10):
            select_resp = self.session.execute(select_req)
            if len(select_resp.get_records()) == len(store_ids):
                break
            time.sleep(5)
        print("After rollup")
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) == len(store_ids)
        for record in select_resp.get_records():
            assert record.get("storeId") in store_ids

        records = [
            "31,10001,Addr1,10",
            "32,10002,Addr2,20",
            "33,10003,Addr3,30",
            "34,10004,Addr4,40",
            "35,10005,Addr5,50",
            "36,10006,Addr6,60",
            "37,10007,Addr7,70",
            "38,10008,Addr8,80",
            "39,10009,Addr9,90",
        ]
        self.upsert_entries(context["contextName"], records)

        # On the fly should return newly inserted entries.
        store_ids.update({33, 34, 35, 36, 37, 38, 39})
        select_resp_on_the_fly = self.session.execute(select_req_on_the_fly)
        print("Second on-the-fly select")
        pprint(select_resp_on_the_fly.get_records())
        assert len(select_resp_on_the_fly.get_records()) == len(store_ids)
        for record in select_resp_on_the_fly.get_records():
            assert record.get("storeId") in store_ids

        # Without on the fly, newly inserted entries may or may not get returned.
        select_resp = self.session.execute(select_req)
        print("Second regular select")
        pprint(select_resp.get_records())
        assert len(select_resp.get_records()) <= len(select_resp_on_the_fly.get_records())

        # Wait for rollups and verify the indexes are updated
        for _ in range(10):
            select_resp = self.session.execute(select_req)
            if len(select_resp.get_records()) == len(store_ids):
                break
            time.sleep(5)
        assert len(select_resp.get_records()) == len(store_ids), select_resp.get_records()
        for record in select_resp.get_records():
            assert record.get("storeId") in store_ids


if __name__ == "__main__":
    pytest.main(sys.argv)
