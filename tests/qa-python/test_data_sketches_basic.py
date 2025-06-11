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
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from setup_common import read_file, update_stream_expect_error

TEST_TENANT_NAME = "biosDataSketchesBasicTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

DATA_SKETCHES_SIGNAL_NAME = "dataSketchesSignal"
DATA_SKETCHES_SIGNAL_BIOS = "../resources/data-sketches-signal-bios.json"

DATA_SKETCHES_CONTEXT_NAME = "dataSketchesContext"
DATA_SKETCHES_CONTEXT = {
    "contextName": "dataSketchesContext",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "orderId", "type": "Integer"},
        {
            "attributeName": "orderAmount",
            "type": "Decimal",
            "tags": {
                "category": "Quantity",
                "kind": "Money",
                "unit": "USD",
                "positiveIndicator": "High",
                "firstSummary": "AVG",
                "secondSummary": "SUM",
            },
        },
        {
            "attributeName": "deliveryTime",
            "type": "Integer",
            "tags": {
                "category": "Quantity",
                "kind": "Duration",
                "unit": "Minute",
                "positiveIndicator": "Low",
                "firstSummary": "AVG",
            },
        },
        {"attributeName": "warehouse", "type": "String", "tags": {"category": "Dimension"}},
        {"attributeName": "region", "type": "String", "tags": {"category": "Dimension"}},
        {"attributeName": "zipcode", "type": "String"},
        {
            "attributeName": "orderDescription",
            "type": "String",
            "tags": {"category": "Description"},
        },
        {"attributeName": "timeSpent", "type": "Decimal"},
    ],
    "primaryKey": ["orderId"],
    "auditEnabled": True,
}


class BiosDataSketchesBasicTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            session.create_signal(read_file(DATA_SKETCHES_SIGNAL_BIOS))
            session.create_context(DATA_SKETCHES_CONTEXT)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        self.assertIsNotNone(self.session)

    def tearDown(self):
        # try_delete_stream(self.session, DATA_SKETCHES_SIGNAL_NAME, "signal")
        self.session.close()

    def test_verify_schema(self):
        signal = self.session.get_signal(DATA_SKETCHES_SIGNAL_NAME)
        self.assertEqual(signal["signalName"], DATA_SKETCHES_SIGNAL_NAME)
        self.assertEqual(signal["attributes"][0]["attributeName"], "orderId")
        self.assertEqual(signal["attributes"][1]["tags"]["category"], "Quantity")
        self.assertEqual(signal["attributes"][1]["tags"]["kind"], "Money")
        self.assertEqual(signal["attributes"][1]["tags"]["unit"], "USD")
        self.assertEqual(signal["attributes"][1]["tags"]["positiveIndicator"], "High")
        self.assertEqual(signal["attributes"][1]["tags"]["secondSummary"], "SUM")
        self.assertEqual(signal["attributes"][2]["tags"]["category"], "Quantity")
        self.assertEqual(signal["attributes"][2]["tags"]["unit"], "Minute")
        self.assertEqual(signal["attributes"][2]["tags"]["positiveIndicator"], "Low")
        self.assertEqual(signal["attributes"][2]["tags"]["firstSummary"], "AVG")
        self.assertEqual(signal["attributes"][3]["tags"]["category"], "Dimension")
        self.assertEqual(signal["attributes"][4]["tags"]["category"], "Dimension")
        self.assertEqual(signal["attributes"][6]["tags"]["category"], "Description")
        self.assertEqual(signal["postStorageStage"]["features"][0]["dataSketches"][0], "Moments")
        self.assertFalse("dimensions" in signal["postStorageStage"]["features"][0])

        context = self.session.get_context(DATA_SKETCHES_CONTEXT_NAME)
        self.assertEqual(context["contextName"], DATA_SKETCHES_CONTEXT_NAME)
        self.assertEqual(context["attributes"][0]["attributeName"], "orderId")
        self.assertEqual(context["attributes"][1]["tags"]["category"], "Quantity")
        self.assertEqual(context["attributes"][1]["tags"]["kind"], "Money")
        self.assertEqual(context["attributes"][1]["tags"]["unit"], "USD")
        self.assertEqual(context["attributes"][1]["tags"]["positiveIndicator"], "High")
        self.assertEqual(context["attributes"][1]["tags"]["secondSummary"], "SUM")
        self.assertEqual(context["attributes"][2]["tags"]["category"], "Quantity")
        self.assertEqual(context["attributes"][2]["tags"]["unit"], "Minute")
        self.assertEqual(context["attributes"][2]["tags"]["positiveIndicator"], "Low")
        self.assertEqual(context["attributes"][2]["tags"]["firstSummary"], "AVG")
        self.assertEqual(context["attributes"][3]["tags"]["category"], "Dimension")
        self.assertEqual(context["attributes"][4]["tags"]["category"], "Dimension")
        self.assertEqual(context["attributes"][6]["tags"]["category"], "Description")

    def test_modify_invalid1(self):
        signal = self.session.get_signal(DATA_SKETCHES_SIGNAL_NAME)
        signal["attributes"][3]["tags"]["category"] = "Quantity"
        update_stream_expect_error(
            self.session,
            "signal",
            DATA_SKETCHES_SIGNAL_NAME,
            signal,
            ErrorCode.BAD_INPUT,
            "Constraint violation: signalConfig.attributes[3].tags.category: "
            "Only numeric types can be of category 'Quantity'",
        )

        context = self.session.get_context(DATA_SKETCHES_CONTEXT_NAME)
        context["attributes"][3]["tags"]["category"] = "Quantity"
        update_stream_expect_error(
            self.session,
            "context",
            DATA_SKETCHES_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Constraint violation: contextConfig.attributes[3].tags.category: "
            "Only numeric types can be of category 'Quantity'",
        )

    def test_modify_invalid2(self):
        signal = self.session.get_signal(DATA_SKETCHES_SIGNAL_NAME)
        signal["attributes"][7]["tags"] = {}
        signal["attributes"][7]["tags"]["category"] = "Description"
        signal["attributes"][7]["tags"]["positiveIndicator"] = "Neutral"
        update_stream_expect_error(
            self.session,
            "signal",
            DATA_SKETCHES_SIGNAL_NAME,
            signal,
            ErrorCode.BAD_INPUT,
            "Constraint violation: signalConfig.attributes[7].tags.category: "
            "category must be 'Quantity' in order to specify any tags other than summaries;",
        )

        context = self.session.get_context(DATA_SKETCHES_CONTEXT_NAME)
        context["attributes"][7]["tags"] = {}
        context["attributes"][7]["tags"]["category"] = "Description"
        context["attributes"][7]["tags"]["positiveIndicator"] = "Neutral"
        update_stream_expect_error(
            self.session,
            "context",
            DATA_SKETCHES_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Constraint violation: contextConfig.attributes[7].tags.category: "
            "category must be 'Quantity' in order to specify any tags other than summaries;",
        )

    def test_query_validation_signal(self):
        start = bios.time.now()
        delta = -bios.time.minutes(5)
        statement = (
            bios.isql()
            .select("p99(orderAmount)")
            .from_signal("dataSketchesSignal")
            .time_range(start, delta)
            .build()
        )
        # Should fail because sketches on signals require windowed queries.
        with self.assertRaises(ServiceError) as context:
            self.session.execute(statement)
        self.assertEqual(context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn("requires a windowed query", context.exception.message)

    def test_query_validation_context(self):
        statement = (
            bios.isql()
            .select("p99(orderAmount)")
            .from_context("dataSketchesContext")
            .group_by("warehouse")
            .build()
        )
        with self.assertRaises(ServiceError) as context:
            self.session.execute(statement)
        self.assertEqual(context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertIn("Data Sketches do not support this query yet", context.exception.message)


if __name__ == "__main__":
    pytest.main(sys.argv)
