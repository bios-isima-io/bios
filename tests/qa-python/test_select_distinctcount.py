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
import pprint
import sys
import time
import unittest

import bios
import pytest
from bios import ServiceError
from bios.errors import ErrorCode
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosSelectDistinctcountTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIGNAL_DISTINCT = {
    "signalName": "distinct",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "ward", "type": "Integer"},
        {"attributeName": "city", "type": "String"},
        {"attributeName": "state", "type": "String"},
        {"attributeName": "country", "type": "String"},
        {"attributeName": "population_prcnt", "type": "Decimal"},
        {"attributeName": "pin", "type": "Integer"},
        {"attributeName": "UnionTerri", "type": "Boolean"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "DistinctStr",
                "attributes": ["country", "state"],
                "dataSketches": ["DistinctCount"],
                "featureInterval": 60000,
            },
            {
                "featureName": "DistinctInt",
                "attributes": ["ward"],
                "dataSketches": ["DistinctCount"],
                "featureInterval": 60000,
            },
            {
                "featureName": "DistinctDecimal",
                "attributes": ["population_prcnt"],
                "dataSketches": ["DistinctCount"],
                "featureInterval": 60000,
            },
            {
                "featureName": "byWardCityStateCountry",
                "attributes": [],
                "dimensions": ["ward", "city", "state", "country"],
                "featureInterval": 60000,
            },
        ]
    },
}

cities_per_country = {
    "India": 4,
    "USA": 2,
    "France": 1,
    "Canada": 4,
}

wards_per_country = {
    "India": 4,
    "USA": 3,
    "France": 1,
    "Canada": 4,
}


class SelectDistinctcountTest(unittest.TestCase):
    """Test cases that were ported from tfos-sdk to test extract operations"""

    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_signal(SIGNAL_DISTINCT)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        records = [
            "330,Delhi,Delhi,India,30.4,21474836690,True",
            "674,Chandigarh,Punjab,India,25.5,21474836694,False",
            "102,Bhopal,MP,India,10.3,21474836691,False",
            "102,Redwood City,California,USA,45.3,21474836692,True",
            "332,Raipur,CS,India,7.8,21474836693,False",
            "564,Arlington,California,USA,45.3,21474836692,True",
            "67,frnc,Paris,France,23.4,21474836695,True",
            "674,Chandigarh,Punjab,India,25.5,21474836696,True",
            "330,Toronto,Ontario,Canada,30.4,21474836690,True",
            "674,Victoria,British Columbia,Canada,25.5,21474836694,False",
            "564,Redwood City,California,USA,45.3,21474836692,True",
            "102,Bhopal,MP,India,10.3,21474936691,True",
            "67,frnc,Paris,France,23.4,31474836695,False",
            "330,Redwood City,California,USA,45.3,21474836692,True",
            "102,Edmonton,Alberta,Canada,10.3,21474836691,False",
            "332,Vancouver,British Columbia,Canada,7.8,21474836693,False",
        ]
        cls.start = cls.insert_into_distinct_signal(records)
        cls.wait_for_rollup(cls.start)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
        cls.session.close()

    @classmethod
    def wait_for_rollup(cls, origin):
        """Method to sleep until the next rollup interval.

        The method sleeps until the next rollup window from the origin time

        Args:
            origin (int): Origin milliseconds.
        """
        interval = 60000
        sleep_time = (origin + interval) / 1000 - time.time() + 10
        if sleep_time > 0:
            print(f"sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)

    @classmethod
    def insert_into_distinct_signal(cls, records):
        if not isinstance(records, list):
            records = [records]
        request = (
            bios.isql().insert().into(SIGNAL_DISTINCT["signalName"]).csv_bulk(records).build()
        )
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_distinct_str(self):
        request = (
            bios.isql()
            .select("distinctcount(country)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 1)
        self.assertAlmostEqual(records[0].get("distinctcount(country)"), 4, places=2)

    def test_distinct_int(self):
        request = (
            bios.isql()
            .select("distinctcount(ward)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 1)
        self.assertAlmostEqual(records[0].get("distinctcount(ward)"), 6, places=2)

    def test_distinct_dec(self):
        request = (
            bios.isql()
            .select("distinctcount(population_prcnt)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 1)
        self.assertAlmostEqual(records[0].get("distinctcount(population_prcnt)"), 6, places=2)

    def test_distinct_multiple(self):
        request = (
            bios.isql()
            .select("distinctcount(country)", "distinctcount(state)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 1)
        self.assertAlmostEqual(records[0].get("distinctcount(country)"), 4, places=2)
        self.assertAlmostEqual(records[0].get("distinctcount(state)"), 9, places=2)

    def test_distinct_string_with_groups(self):
        request = (
            bios.isql()
            .select("distinctcount(city)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .group_by("country")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == len(cities_per_country)
        for record in records:
            country = record.get("country")
            distinct_cities = record.get("distinctcount(city)")
            assert country in cities_per_country
            assert distinct_cities == cities_per_country[country]

    def test_distinct_int_with_groups(self):
        request = (
            bios.isql()
            .select("distinctcount(ward)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .group_by("country")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == len(wards_per_country)
        for record in records:
            country = record.get("country")
            distinct_wards = record.get("distinctcount(ward)")
            assert country in wards_per_country
            assert distinct_wards == wards_per_country[country]

    def test_distinct_without_groups_orthogonal_filter(self):
        request = (
            bios.isql()
            .select("distinctcount(city)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .where("state in ('California', 'Ontario')")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == 1
        assert records[0].get("distinctcount(city)") == 3

    def test_distinct_with_groups_orthogonal_filter(self):
        request = (
            bios.isql()
            .select("distinctcount(city)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .where("state = 'California'")
            .group_by("country")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == 1
        for record in records:
            country = record.get("country")
            distinct_cities = record.get("distinctcount(city)")
            assert country == "USA"
            assert distinct_cities == 2

    def test_distinct_with_groups_filter_relevant_to_dimension(self):
        request = (
            bios.isql()
            .select("distinctcount(city)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .where("country in ('India', 'USA')")
            .group_by("country")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == 2
        for record in records:
            country = record.get("country")
            distinct_cities = record.get("distinctcount(city)")
            assert country in cities_per_country
            assert distinct_cities == cities_per_country[country]

    def test_distinct_without_groups_with_relevant_filter(self):
        cities = [
            f"'{city}'"
            for city in [
                "Delhi",
                "Chandigarh",
                "Bhopal",
                "Raipur",
                "Arlington",
                "frnc",
                "Toronto",
                "Victoria",
                "Edmonton",
                "Vancouver",
            ]
        ]
        request = (
            bios.isql()
            .select("distinctcount(city)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .where(f"city in ({', '.join(cities)})")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == 1
        assert records[0].get("distinctcount(city)") == 10

    def test_distinct_with_groups_and_filter_relevant_to_dictinctcount(self):
        cities = [
            f"'{city}'"
            for city in [
                "Delhi",
                "Chandigarh",
                "Bhopal",
                "Raipur",
                "Arlington",
                "frnc",
                "Toronto",
                "Victoria",
                "Edmonton",
                "Vancouver",
            ]
        ]
        request = (
            bios.isql()
            .select("distinctcount(city)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .where(f"city in ({', '.join(cities)})")
            .group_by("country")
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        windows = resp.get_data_windows()
        assert len(windows) == 1
        records = windows[0].records
        pprint.pprint(records)
        assert len(records) == len(cities_per_country)
        for record in records:
            country = record.get("country")
            distinct_cities = record.get("distinctcount(city)")
            assert country in cities_per_country
            original_count = cities_per_country[country]
            expected = original_count - 1 if country == "USA" else original_count
            assert distinct_cities == expected, country

    def test_distinct_multiple_with_aggregates(self):
        request = (
            bios.isql()
            .select("distinctcount(country)", "count()")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(self.start, bios.time.minutes(2))
            .build()
        )

        resp = self.session.execute(request)
        self.assertIsNotNone(resp)
        windows = resp.get_data_windows()
        self.assertEqual(len(windows), 1)
        records = windows[0].records
        self.assertEqual(len(records), 1)
        self.assertAlmostEqual(records[0].get("distinctcount(country)"), 4, places=2)
        self.assertEqual(records[0].get("count()"), 16)

    def test_distinctcount_empty_key(self):
        with self.assertRaises(ServiceError) as error_context:
            (
                bios.isql()
                .select("distinctcount()")
                .from_signal(SIGNAL_DISTINCT["signalName"])
                .tumbling_window(bios.time.minutes(2))
                .time_range(bios.time.now(), -bios.time.minutes(2))
                .build()
            )
        self.assertEqual(error_context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        self.assertEqual(
            error_context.exception.message, "DISTINCTCOUNT function must specify a parameter"
        )

    def test_distinctcount_invalid_key(self):
        request = (
            bios.isql()
            .select("distinctcount(invalidKey)")
            .from_signal(SIGNAL_DISTINCT["signalName"])
            .tumbling_window(bios.time.minutes(2))
            .time_range(bios.time.now(), -bios.time.minutes(2))
            .build()
        )

        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            "Invalid value: Query.*Non existing attribute*",
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
