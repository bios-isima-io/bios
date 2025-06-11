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
import sys
import time
import unittest

import bios
import pytest
from bios import ServiceError
from bios.errors import ErrorCode
from bios.models.metric import Metric
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
TEST_SIGNAL1 = {
    "signalName": "covidDataSignal",
    "version": 1596544411442,
    "biosVersion": 1596544411442,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "reportedDate", "type": "String"},
        {"attributeName": "countryCode", "type": "String"},
        {"attributeName": "stateCode", "type": "String"},
        {"attributeName": "reportedCases", "type": "Integer"},
        {"attributeName": "reportedDeaths", "type": "Integer"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "by_country",
                "dimensions": ["countryCode", "stateCode"],
                "attributes": [
                    "reportedCases",
                    "reportedDeaths",
                ],
                "featureInterval": 60000,
            }
        ]
    },
    "dataSynthesisStatus": "Disabled",
}

TEST_SIGNAL2 = {
    "signalName": "scoreSignal",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "group", "type": "Integer"},
        {"attributeName": "name", "type": "string"},
        {"attributeName": "gender", "type": "String", "allowedValues": ["MALE", "FEMALE"]},
        {"attributeName": "score", "type": "Integer"},
    ],
}


class BiosSelectValidateTest(unittest.TestCase):
    """
    Test cases to validate select signal request validation
    """

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.signals = []

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_signal(TEST_SIGNAL1)
            cls.signals.append(TEST_SIGNAL1.get("signalName"))

            request = (
                bios.isql()
                .insert()
                .into("covidDataSignal")
                .csv("29/06/2020,AF,BDN,234,21")
                .build()
            )
            admin.execute(request)
            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,AL,BR,123,5").build()
            )
            admin.execute(request)
            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,IN,AR,225,31").build()
            )
            admin.execute(request)
            request = (
                bios.isql().insert().into("covidDataSignal").csv("29/06/2020,IN,CT,225,5").build()
            )
            admin.execute(request)
            request = (
                bios.isql().insert().into("covidDataSignal").csv("30/06/2020,JP,AOM,101,3").build()
            )
            admin.execute(request)

            admin.create_signal(TEST_SIGNAL2)
            cls.signals.append(TEST_SIGNAL2.get("signalName"))

            request = bios.isql().insert().into("scoreSignal").csv("0,brad,MALE,61").build()
            admin.execute(request)
            request = bios.isql().insert().into("scoreSignal").csv("1,carl,MALE,91").build()
            admin.execute(request)
            request = bios.isql().insert().into("scoreSignal").csv("1,sandra,FEMALE,58").build()
            admin.execute(request)
            request = bios.isql().insert().into("scoreSignal").csv("0,monica,FEMALE,83").build()
            response = admin.execute(request)

            cls.INSERT_TIME = response.records[0].timestamp
            print("Wait for rollup to run")
            time.sleep(90)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        for signal in cls.signals:
            try:
                cls.session.delete_signal(signal)
            except ServiceError:
                pass
        cls.session.close()

    @classmethod
    def time_delta(cls, delta):
        return cls.INSERT_TIME - delta * 30 * 1000, delta * 60 * 1000

    @classmethod
    def push_random_data(cls):
        covid_data = [
            "13/06/2020,DZ,109,10",
            "05/06/2020,AF,787,17",
            "21/06/2020,AL,128,33",
        ]
        signal_request = bios.isql().insert().into("covidDataSignal").csv_bulk(covid_data).build()
        cls.session.execute(signal_request)

    def test_select_attr_with_metric(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("countryCode", "sum(reportedCases)")
            .from_signal("covidDataSignal")
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Cannot select non-GroupBy attribute(s) along with metric(s)",
            error_context.exception.message,
        )

    def test_select_all_attr_with_group_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .time_range(start, delta)
            .build()
        )
        # TODO(BIOS-4981) - enable me
        # with self.assertRaises(ServiceError) as error_context:
        #     self.session.execute(select_request)
        # self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        # self.assertIn(
        #     "Attribute/aggregate(s) must be specified when group by clause is used",
        #     error_context.exception.message)
        resp = self.session.execute(select_request)
        for window in resp.get_data_windows():
            for record in window.records:
                self.assertIsNotNone(record.get("countryCode"))
                self.assertIsNotNone(record.get("reportedDate"))
                self.assertIsNotNone(record.get("reportedCases"))
                self.assertIsNotNone(record.get("reportedDeaths"))

    def test_select_attr_with_group_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("countryCode")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .time_range(start, delta)
            .build()
        )
        self.session.execute(select_request)

    def test_select_metric_with_group_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select(Metric("sum(reportedCases)"), Metric("max(reportedDeaths)"))
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 4)
        # TODO(BIOS-4981) - enable me
        # for record in resp.get_data_windows()[0].records:
        #     self.assertIsNone(record.get("countryCode"))

    def test_select_attr_with_order_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("reportedCases", "reportedDeaths")
            .from_signal("covidDataSignal")
            .order_by("countryCode", reverse=True)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 5)
        # TODO(BIOS-4981) - enable me
        # for record in resp.get_data_windows()[0].records:
        #     self.assertIsNone(record.get("countryCode"))

    def test_select_attr_with_order_by_group_key(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("countryCode", reverse=True)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertEqual(len(resp.get_data_windows()), 1)
        assert len(resp.get_data_windows()[0].records) == 4
        assert resp.get_data_windows()[0].records[0].get("countryCode") == "JP"
        assert resp.get_data_windows()[0].records[1].get("countryCode") == "IN"
        assert resp.get_data_windows()[0].records[2].get("countryCode") == "AL"
        assert resp.get_data_windows()[0].records[3].get("countryCode") == "AF"
        # TODO(BIOS-4981) - enable me
        # for record in resp.get_data_windows()[0].records:
        #     self.assertIsNone(record.get("countryCode"))

    def test_select_attr_with_order_by_irrelevant_key(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("stateCode", reverse=True)
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Order key attribute stateCode must present in group by clause",
            error_context.exception.message,
        )

    def test_select_metric_with_group_and_order_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select(Metric("sum(reportedCases)"), Metric("max(reportedDeaths)"))
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("countryCode", reverse=True)
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 4)
        # TODO(BIOS-4981) - enable me
        # for record in resp.get_data_windows()[0].records:
        #     self.assertIsNone(record.get("countryCode"))

    def test_select_other_attr_with_group_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("reportedCases")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "attribute: reportedCases is not part of group by clause",
            error_context.exception.message,
        )

    def test_select_attr_metric_with_order_by(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("countryCode", "sum(reportedCases)")
            .from_signal("covidDataSignal")
            .order_by("sum(reportedCases)", reverse=True)
            .group_by("countryCode")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 4)
        record = resp.get_data_windows()[0].records[0]
        self.assertEqual(record.get("sum(reportedCases)"), 450)
        self.assertEqual(record.get("countryCode"), "IN")
        record = resp.get_data_windows()[0].records[3]
        self.assertEqual(record.get("countryCode"), "JP")

    def test_select_with_tumbling_window_order_by_uncovered_attribute(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("reportedDate", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Order key attribute reportedDate must present in group by clause",
            error_context.exception.message,
        )

    def test_select_order_by_selected_dimension(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("countryCode")
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 15
        data_window = None
        for window in resp.get_data_windows():
            if len(window.records) > 0:
                data_window = window
                break
        assert data_window is not None
        assert len(data_window.records) == 4
        assert data_window.records[0].get("countryCode") == "AF"
        assert data_window.records[1].get("countryCode") == "AL"
        assert data_window.records[2].get("countryCode") == "IN"
        assert data_window.records[3].get("countryCode") == "JP"

    def test_select_order_by_attribute_not_in_dimension(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("stateCode")
            .order_by("countryCode")
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Order key attribute countryCode must present in group by clause",
            error_context.exception.message,
        )

    def test_select_order_by_attribute_in_dimension(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode", "stateCode")
            .order_by("stateCode")
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 15
        data_window = None
        for window in resp.get_data_windows():
            if len(window.records) > 0:
                data_window = window
                break
        assert data_window is not None
        assert len(data_window.records) == 5
        assert data_window.records[0].get("stateCode") == "AOM"
        assert data_window.records[1].get("stateCode") == "AR"
        assert data_window.records[2].get("stateCode") == "BDN"
        assert data_window.records[3].get("stateCode") == "BR"
        assert data_window.records[4].get("stateCode") == "CT"

    def test_select_order_by_selected_metric(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("sum(reportedCases)", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 15
        data_window = None
        for window in resp.get_data_windows():
            if len(window.records) > 0:
                data_window = window
                break
        assert data_window is not None
        assert len(data_window.records) == 4
        assert data_window.records[0].get("countryCode") == "IN"
        assert data_window.records[1].get("countryCode") == "AF"
        assert data_window.records[2].get("countryCode") == "AL"
        assert data_window.records[3].get("countryCode") == "JP"

    def test_select_order_by_non_selected_metric(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("avg(reportedCases)", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 15
        data_window = None
        for window in resp.get_data_windows():
            if len(window.records) > 0:
                data_window = window
                break
        assert data_window is not None
        assert len(data_window.records) == 4
        assert data_window.records[0].get("countryCode") == "AF"
        assert data_window.records[1].get("countryCode") == "IN"
        assert data_window.records[2].get("countryCode") == "AL"
        assert data_window.records[3].get("countryCode") == "JP"

    def test_select_order_by_non_existing_attribute(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("nosuch", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Order key attribute nosuch must present in group by clause",
            error_context.exception.message,
        )

    def test_select_with_tumbling_window_order_without_group(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .order_by("countryCode", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Order key attribute countryCode must present in group by clause",
            error_context.exception.message,
        )

    def test_select_order_by_aggregate_of_non_existing_attribute(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("min(nosuch)", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Order attribute nosuch does not exist",
            error_context.exception.message,
        )

    def test_select_order_by_invalid_attribute(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("not(closed", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Invalid order key: Syntax error",
            error_context.exception.message,
        )

    def test_select_order_by_empty_attribute(self):
        start, delta = self.time_delta(15)
        with self.assertRaises(ServiceError) as error_context:
            (
                bios.isql()
                .select("sum(reportedCases)")
                .from_signal("covidDataSignal")
                .group_by("countryCode")
                .order_by("", reverse=True)
                .tumbling_window(bios.time.minutes(1))
                .time_range(start, delta)
                .build()
            )
        self.assertEqual(error_context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        self.assertIn(
            "Parameter 'sort_spec' may not be empty",
            error_context.exception.message,
        )

    def test_select_order_by_blank_attribute(self):
        start, delta = self.time_delta(15)
        with self.assertRaises(ServiceError) as error_context:
            (
                bios.isql()
                .select("sum(reportedCases)")
                .from_signal("covidDataSignal")
                .group_by("countryCode")
                .order_by(" ", reverse=True)
                .tumbling_window(bios.time.minutes(1))
                .time_range(start, delta)
                .build()
            )
        self.assertEqual(error_context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        self.assertIn(
            "Parameter 'sort_spec' may not be empty",
            error_context.exception.message,
        )

    def test_select_order_by_non_existing_function(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("fourier(reportedCase)", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Invalid order key: Unknown function: fourier",
            error_context.exception.message,
        )

    def test_select_order_by_sum_of_nothing(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("sum()", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Invalid order key: Function 'sum' requires a parameter",
            error_context.exception.message,
        )

    def test_select_order_by_count_of_something(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("count(reportedCases)", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        assert len(resp.get_data_windows()) == 15
        data_window = None
        for window in resp.get_data_windows():
            if len(window.records) > 0:
                data_window = window
                break
        assert data_window is not None
        assert len(data_window.records) == 4
        assert data_window.records[0].get("countryCode") == "IN"
        assert data_window.records[1].get("countryCode") == "AF"
        assert data_window.records[2].get("countryCode") == "AL"
        assert data_window.records[3].get("countryCode") == "JP"

    def test_select_where_with_enum(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("scoreSignal")
            .where("gender > 'MALE'")
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn("Cannot use <,> operator on ENUM types", error_context.exception.message)

    def test_select_invalid_limit(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("scoreSignal")
            .limit(-1)
            .where("gender = 'MALE'")
            .time_range(start, delta)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn(
            "Limit (-1) must be greater than or equal to zero", error_context.exception.message
        )

    def test_select_limit_zero(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select()
            .from_signal("scoreSignal")
            .limit(0)
            .where("gender = 'MALE'")
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertEqual(len(resp.get_data_windows()), 1)
        length = len(resp.get_data_windows()[0].records)
        assert length == 0 or length == 2

    def test_select_agg_limit_zero(self):
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select(Metric("sum(reportedCases)"), Metric("sum(reportedDeaths)"))
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .limit(0)
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        for window in resp.get_data_windows():
            assert len(window.records) >= 0

    def test_select_windowed_group_by(self):
        # TODO(BIOS-4981): remove me
        start, delta = self.time_delta(15)
        select_request = (
            bios.isql()
            .select("count()")
            .from_signal("covidDataSignal")
            .group_by("countryCode")
            .order_by("count()")
            .tumbling_window(bios.time.minutes(1))
            .time_range(start, delta)
            .build()
        )
        resp = self.session.execute(select_request)
        for window in resp.get_data_windows():
            for record in window.records:
                self.assertIsNotNone(record.get("count()"))
                self.assertIsNotNone(record.get("countryCode"))


if __name__ == "__main__":
    pytest.main(sys.argv)
