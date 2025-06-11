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

import pytest

from bios import ServiceError, ErrorCode
import bios
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME, setup_tenant_config
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
COUNTRY_CONTEXT = {
    "contextName": "countryContext",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "countryCode", "type": "String"},
        {"attributeName": "name", "type": "String"},
        {"attributeName": "population", "type": "Integer"},
        {
            "attributeName": "continent",
            "type": "String",
            "allowedValues": [
                "Asia",
                "Africa",
                "Europe",
                "Australia",
                "North America",
                "South America",
            ],
        },
    ],
    "primaryKey": ["countryCode"],
    "dataSynthesisStatus": "Disabled",
}

INITIAL_COUNTRIES = [
    "AF,Afghanistan,4,Asia",
    "AL,Albania,8,Europe",
    "DZ,Algeria,12,Africa",
    "IN,India,356,Asia",
    "AD,Andorra,20,Europe",
    "JP,日本,58,Asia",
]

TEST_SIGNAL = {
    "signalName": "covidDataSignal",
    "version": 1596544411442,
    "biosVersion": 1596544411442,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "reportedDate", "type": "String"},
        {"attributeName": "countryCode", "type": "String"},
        {"attributeName": "reportedCases", "type": "Integer"},
        {"attributeName": "reportedDeaths", "type": "Integer"},
    ],
    "enrich": {
        "missingLookupPolicy": "StoreFillInValue",
        "enrichments": [
            {
                "enrichmentName": "countryJoin",
                "foreignKey": ["countryCode"],
                "missingLookupPolicy": "Reject",
                "contextName": "countryContext",
                "contextAttributes": [
                    {"attributeName": "name", "as": "countryName"},
                    {"attributeName": "population"},
                    {"attributeName": "continent"},
                ],
            }
        ],
    },
    "postStorageStage": {
        "features": [
            {
                "featureName": "by_country",
                "dimensions": ["countryCode"],
                "attributes": ["reportedCases", "reportedDeaths", "population"],
                "featureInterval": 60000,
            },
            {
                "featureName": "by_continent",
                "dimensions": ["continent"],
                "attributes": ["reportedCases", "reportedDeaths", "population"],
                "featureInterval": 60000,
            },
        ]
    },
}


class BiosSelectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.contexts = []
        cls.signals = []

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            try:
                admin.delete_signal(TEST_SIGNAL.get("signalName"))
            except ServiceError:
                pass
            try:
                admin.delete_context(COUNTRY_CONTEXT.get("contextName"))
            except ServiceError:
                pass
            admin.create_context(COUNTRY_CONTEXT)
            cls.contexts.append(COUNTRY_CONTEXT.get("contextName"))
            ctx_upsert_request = (
                bios.isql().upsert().into("countryContext").csv_bulk(INITIAL_COUNTRIES).build()
            )
            admin.execute(ctx_upsert_request)

            admin.create_signal(TEST_SIGNAL)
            cls.signals.append(TEST_SIGNAL.get("signalName"))

            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,AF,234,21").build()
            )
            response = admin.execute(request)

            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,AL,123,5").build()
            )
            admin.execute(request)

            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,IN,225,31").build()
            )
            admin.execute(request)

            request = (
                bios.isql().insert().into("covidDataSignal").csv("30/06/2020,JP,101,3").build()
            )
            admin.execute(request)

            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,IN,225,5").build()
            )
            admin.execute(request)

        cls.INSERT_TIME = response.records[0].timestamp
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        for signal in cls.signals:
            try:
                cls.session.delete_signal(signal)
            except ServiceError:
                pass
        for context in cls.contexts:
            try:
                cls.session.delete_context(context)
            except ServiceError:
                pass
        cls.session.close()

    @classmethod
    def time_delta(cls, delta):
        return int(time.time() * 1000), -delta * 60 * 1000

    @classmethod
    def push_random_data(cls):
        covid_data = [
            "13/06/2020,DZ,109,10",
            "05/06/2020,AF,787,17",
            "21/06/2020,AL,128,33",
        ]
        signal_request = bios.isql().insert().into("covidDataSignal").csv_bulk(covid_data).build()
        cls.session.execute(signal_request)

    def test_basic_select(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql().select().from_signal("covidDataSignal").time_range(start, delta).build()
        )
        resp = self.session.execute(select_request)
        countries = ["Afghanistan", "Albania", "India", "日本", "India"]
        deaths = [21, 5, 31, 3, 5]

        assert len(resp.get_data_windows()) == 1

        window0 = resp.get_data_windows()[0]
        assert len(window0.records) == len(countries)
        for i, record in enumerate(window0.records):
            self.assertEqual(record.get("countryName"), countries[i])
            self.assertEqual(record.get("reportedDeaths"), deaths[i])

    def test_select_country(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("countryName = 'India'")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 1
        for win in resp.get_data_windows():
            assert len(win.records) == 2
            for record in win.records:
                self.assertEqual(record.get("countryName"), "India")
                self.assertIn(record.get("reportedDeaths"), [31, 5])

    def test_select_deaths(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("reportedDeaths > 10 AND reportedDeaths < 30")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 1
        for win in resp.get_data_windows():
            assert len(win.records) == 1
            for record in win.records:
                self.assertEqual(record.get("countryName"), "Afghanistan")
                self.assertEqual(record.get("reportedDeaths"), 21)

    def test_order_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("reportedDeaths < 25")
            .order_by("CountryCode")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 1
        for win in resp.get_data_windows():
            records = win.records
            assert len(records) == 4
            self.assertEqual(records[0].get("countryName"), "Afghanistan")
            self.assertEqual(records[1].get("countryName"), "Albania")
            self.assertEqual(records[2].get("countryName"), "India")
            self.assertEqual(records[3].get("countryName"), "日本")

    def test_group_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("continent", "countryName", "sum(reportedDeaths)")
            .from_signal("covidDataSignal")
            .where("reportedDeaths >= 5")
            .group_by(["continent", "countryName"])
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)

        expected = {
            "Afghanistan": {"sum": 21, "continent": "Asia"},
            "Albania": {"sum": 5, "continent": "Europe"},
            "India": {"sum": 36, "continent": "Asia"},
        }

        assert len(resp.get_data_windows()) == 1
        records = resp.get_data_windows()[0].records
        assert len(records) == 3
        for record in records:
            expected_entry = expected.get(record.get("countryName"))
            assert expected_entry is not None
            assert record.get("sum(reportedDeaths)") == expected_entry.get("sum")
            assert record.get("continent") == expected_entry.get("continent")

    def test_last(self):
        start, delta = self.time_delta(120)
        select_request = (
            bios.isql()
            .select("last(reportedDeaths)")
            .from_signal("covidDataSignal")
            .where("countryCode = 'IN'")
            .group_by(["countryCode"])
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].get("last(reportedDeaths)"), 5)

    def test_order_by_case_sensitive(self):
        start, delta = self.time_delta(120)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("reportedDeaths < 25")
            .order_by("CountryCode", case_sensitive=True)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(len(records), 4)

    def test_order_by_reverse(self):
        start, delta = self.time_delta(120)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("reportedDeaths < 25")
            .order_by("CountryCode", reverse=True)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(records[0].get("countryName"), "日本")
        self.assertEqual(records[1].get("countryName"), "India")
        self.assertEqual(records[2].get("countryName"), "Albania")
        self.assertEqual(records[3].get("countryName"), "Afghanistan")
        self.assertEqual(len(records), 4)

    def test_multi_execute(self):
        start, delta = self.time_delta(120)
        select_request1 = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("reportedDeaths < 25")
            .order_by("CountryCode", case_sensitive=True)
            .time_range(start, delta)
            .build()
        )
        select_request2 = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("reportedDeaths < 25")
            .order_by("CountryCode", reverse=True)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.multi_execute(select_request1, select_request2)
        win1 = resp[0].get_data_windows()[-1]
        records1 = win1.records
        self.assertEqual(len(records1), 4)
        self.assertEqual(records1[0].get("countryName"), "Afghanistan")
        win2 = resp[1].get_data_windows()[-1]
        records2 = win2.records
        self.assertEqual(len(records2), 4)
        self.assertEqual(records2[0].get("countryName"), "日本")

    def test_filter_by_utf8(self):
        start, delta = self.time_delta(120)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .where("countryName = '日本'")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win1 = resp.get_data_windows()[-1]
        records1 = win1.records
        self.assertEqual(len(records1), 1)

    def test_global_limit(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .limit(3)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        countries = ["Afghanistan", "Albania", "India", "日本", "India"]
        deaths = [21, 5, 31, 3, 5]

        assert len(resp.get_data_windows()) == 1

        window0 = resp.get_data_windows()[0]
        assert len(window0.records) == 3
        for i, record in enumerate(window0.records):
            self.assertEqual(record.get("countryName"), countries[i])
            self.assertEqual(record.get("reportedDeaths"), deaths[i])

    def test_global_limit_by_more_than_available_entries(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .limit(10)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        countries = ["Afghanistan", "Albania", "India", "日本", "India"]
        deaths = [21, 5, 31, 3, 5]

        assert len(resp.get_data_windows()) == 1

        window0 = resp.get_data_windows()[0]
        assert len(window0.records) == len(countries)
        for i, record in enumerate(window0.records):
            self.assertEqual(record.get("countryName"), countries[i])
            self.assertEqual(record.get("reportedDeaths"), deaths[i])

    def test_tumbling_limit(self):
        self._wait_for_rollup()
        start = self.INSERT_TIME
        delta = bios.time.minutes(1)
        select_request = (
            bios.isql()
            .select("max(population)", "sum(reportedDeaths)")
            .from_signal("covidDataSignal")
            .group_by(["countryCode"])
            .limit(1)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(len(records), 1)

    def test_tumbling_limit_by_more_than_entries(self):
        self._wait_for_rollup()
        start = self.INSERT_TIME
        delta = bios.time.minutes(1)
        select_request = (
            bios.isql()
            .select("max(population)", "sum(reportedDeaths)")
            .from_signal("covidDataSignal")
            .group_by(["countryCode"])
            .limit(10)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(len(records), 4)

    def test_hopping_limit(self):
        self._wait_for_rollup()
        origin = self.INSERT_TIME
        delta = bios.time.minutes(1)
        select_request = (
            bios.isql()
            .select("max(population)", "sum(reportedDeaths)")
            .from_signal("covidDataSignal")
            .group_by(["countryCode"])
            .limit(2)
            .hopping_window(bios.time.minutes(1), 3)
            .time_range(origin, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(len(records), 2)

    def test_hopping_limit_by_more_than_entries(self):
        self._wait_for_rollup()
        origin = self.INSERT_TIME
        delta = bios.time.minutes(1)
        select_request = (
            bios.isql()
            .select("max(population)", "sum(reportedDeaths)")
            .from_signal("covidDataSignal")
            .group_by(["countryCode"])
            .limit(10)
            .hopping_window(bios.time.minutes(1), 3)
            .time_range(origin, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        win = resp.get_data_windows()[-1]
        records = win.records
        self.assertEqual(len(records), 4)

    def test_query_old_time_range_simple(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            statement = (
                bios.isql()
                .select()
                .from_signal("_allClientMetrics")
                .time_range(3600000, 7200000 - 3600000)
                .build()
            )
            result = session.execute(statement).to_dict()
            assert len(result) == 0

    def test_query_old_time_range_tumbling_window(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            statement = (
                bios.isql()
                .select("count()")
                .from_signal("_allClientMetrics")
                .tumbling_window(bios.time.minutes(5))
                .time_range(3600000, 7200000 - 3600000)
                .build()
            )
            result = session.execute(statement).to_dict()
            assert len(result) == 0

    def test_query_old_time_range_tumbling_window_with_dimensions(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            statement = (
                bios.isql()
                .select("sum(latencySum)", "sum(numSuccessfulOperations)")
                .from_signal("_allClientMetrics")
                .where("request IN ('INSERT', 'SELECT', 'UPSERT', 'SELECT_CONTEXT')")
                .tumbling_window(bios.time.minutes(5))
                .time_range(3600000, 7200000 - 3600000)
                .build()
            )
            result = session.execute(statement).to_dict()
            assert len(result) == 0

    def _wait_for_rollup(self):
        sleep_until = self.INSERT_TIME / 1000 + 125
        sleep_time = sleep_until - time.time()
        if sleep_time > 0:
            print(f"\nSleeping for {sleep_time} seconds to wait for rolling up to run")
            time.sleep(sleep_time)


if __name__ == "__main__":
    pytest.main(sys.argv)
