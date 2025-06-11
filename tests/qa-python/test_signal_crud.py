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
import json
import pprint
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios_tutils import get_minimum_signal
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

TEST_TENANT_NAME = "biosSignalCrudTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
SIGNAL_ONE_ATTRIBUTE = {
    "signalName": "maximum",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "one", "type": "String"}],
}

SIGNAL_FOR_CREATE_TEST = {
    "signalName": "create_test_signal",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "foo", "type": "string"}],
}

SIGNAL_FOR_CREATE_TEST_JSON_FILE = "../resources/bios-signal-schema.json"


class BiosSignalCrudTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.SIGNAL_MINIMUM = get_minimum_signal()

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})
            sadmin.set_property("prop.minFeatureIntervalMillis", "")
            sadmin.set_property("prop.maxFeatureIntervalMillis", "")

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_signal(cls.SIGNAL_MINIMUM)
            admin.create_signal(SIGNAL_ONE_ATTRIBUTE)

        with open(SIGNAL_FOR_CREATE_TEST_JSON_FILE, "r") as json_file:
            cls.LOADED_SIGNAL = json.loads(json_file.read())

        # This sleep is necessary to ensure the inferred tags being created
        time.sleep(20)
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.maxDiff = None

    def test_get_signals(self):
        signals = self.session.get_signals()
        self.assertIsNotNone(signals)
        self.assertEqual(len(signals), 2)
        self.assertEqual(
            set(map(lambda entry: entry["signalName"], signals)),
            set(["minimum", "maximum"]),
        )

    def test_get_signals_with_include_internal(self):
        signals = self.session.get_signals(include_internal=True)
        self.assertIsNotNone(signals)
        self.assertEqual(len(signals), 5)
        self.assertEqual(
            {signal["signalName"] for signal in signals},
            {"minimum", "maximum", "_usage", "_clientMetrics", "_operationFailure"},
        )

    def test_get_signals_with_name_filter(self):
        signals = self.session.get_signals(names=["minimum"])
        self.assertIsNotNone(signals)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signalName"], "minimum")

        signals = self.session.get_signals(names=["maximum"])
        self.assertIsNotNone(signals)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signalName"], "maximum")

        signals = self.session.get_signals(names=["minimum", "maximum"])
        self.assertEqual(len(signals), 2)
        self.assertEqual(
            set(map(lambda entry: entry["signalName"], signals)),
            set(["minimum", "maximum"]),
        )

    def test_get_signal_with_detail_option(self):
        signals = self.session.get_signals(detail=True)
        self.assertEqual(len(signals), 2, json.dumps(signals))
        signal0 = signals[0]
        self.assertGreater(len(signal0), 1)
        self.assertEqual(signal0["missingAttributePolicy"], "Reject")
        self.assertEqual(len(signal0["attributes"]), 1)
        self.assertIsNone(signal0["attributes"][0].get("inferredTags"))
        # all signals are not internal when include_internal is not set
        for signal in signals:
            assert signal.get("isInternal") is False

    def test_get_signals_with_include_internal_and_detail_options(self):
        signals = self.session.get_signals(include_internal=True, detail=True)
        self.assertIsNotNone(signals)
        self.assertEqual(len(signals), 5)
        self.assertEqual(
            {signal["signalName"]: signal["isInternal"] for signal in signals},
            {
                "minimum": False,
                "maximum": False,
                "_usage": True,
                "_clientMetrics": True,
                "_operationFailure": True,
            },
        )

    def test_get_signal_with_include_inferred_tags_option(self):
        signals = self.session.get_signals(detail=True, inferred_tags=True)
        self.assertEqual(len(signals), 2, json.dumps(signals))
        signal0 = signals[0]
        self.assertGreater(len(signal0), 1)
        self.assertEqual(signal0["missingAttributePolicy"], "Reject")
        self.assertEqual(len(signal0["attributes"]), 1)
        self.assertIsNotNone(signal0["attributes"][0].get("inferredTags"))

    def test_get_signals_via_get_tenant(self):
        tenant = self.session.get_tenant(detail=True)
        signals = tenant["signals"]
        self.assertGreater(len(signals), 1)
        signal0 = signals[0]
        self.assertGreaterEqual(len(signal0["attributes"]), 1)
        self.assertIsNone(signal0["attributes"][0].get("inferredTags"))

    def test_get_signals_with_inferred_tags_option_via_get_tenant(self):
        tenant = self.session.get_tenant(detail=True, inferred_tags=True)
        signals = tenant["signals"]
        self.assertGreater(len(signals), 1)
        signal0 = signals[0]
        self.assertGreaterEqual(len(signal0["attributes"]), 1)
        self.assertIsNotNone(signal0["attributes"][0].get("inferredTags"))

    def test_get_signals_with_non_existent_signal(self):
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(names=["mum"])
            self.assertEqual(context.exception.error_code, ErrorCode.NO_SUCH_STREAM)

    def test_get_signal(self):
        signal = self.session.get_signal("maximum")
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], "maximum")
        self.assertGreater(len(signal), 1)
        self.assertIsNone(signal["attributes"][0].get("inferredTags"))
        assert signal["isInternal"] is False

    def test_get_internal_signal(self):
        signal = self.session.get_signal("_usage")
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], "_usage")
        self.assertGreater(len(signal), 1)
        self.assertIsNone(signal["attributes"][0].get("inferredTags"))
        assert signal["isInternal"] is True

    def test_get_signal_with_inferred_tags(self):
        signal = self.session.get_signal("maximum", inferred_tags=True)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], "maximum")
        self.assertGreater(len(signal), 1)
        self.assertIsNotNone(signal["attributes"][0].get("inferredTags"))

    def test_create_delete_signal(self):
        signal_name = SIGNAL_FOR_CREATE_TEST["signalName"]
        try:
            # test creation
            result = self.session.create_signal(SIGNAL_FOR_CREATE_TEST)
            self.assertIsNotNone(result)
            self.assertEqual(result["signalName"], signal_name)
            self.assertIsNotNone(result["version"])
            self.assertGreater(result["version"], 0)

            signals = self.session.get_signals()
            self.assertEqual(len(signals), 3)

            # test deletion
            response = self.session.delete_signal(signal_name)
            self.assertIsNone(response)
            signals = self.session.get_signals()
            self.assertEqual(len(signals), 2)
        finally:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                pass

    def test_create_detailed_signal(self):
        signal_name = self.LOADED_SIGNAL["signalName"]
        try:
            result = self.session.create_signal(self.LOADED_SIGNAL)
            self.assertIsNotNone(result)
            self.assertEqual(signal_name, self.LOADED_SIGNAL["signalName"])
        finally:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                pass

    def test_update_signal(self):
        signal_name = self.LOADED_SIGNAL["signalName"]
        try:
            original = self.session.create_signal(self.LOADED_SIGNAL)

            updated_signal = self.LOADED_SIGNAL.copy()
            updated_signal["attributes"].append(
                {"attributeName": "age", "type": "Integer", "default": -1}
            )
            result = self.session.update_signal(signal_name, updated_signal)
            self.assertIsNotNone(result)
            self.assertEqual(result["signalName"], original["signalName"])
            self.assertGreater(result["version"], original["version"])
            self.assertEqual(len(original["attributes"]), 10)
            self.assertEqual(len(result["attributes"]), 11)
            self.assertEqual(result["attributes"][10]["attributeName"], "age")
        finally:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                pass

    def test_update_signal_with_enum(self):
        signal_name = "stringEnum"

        # modify the signal to add enum
        signal = copy.deepcopy(self.SIGNAL_MINIMUM)
        signal["signalName"] = signal_name
        signal["attributes"][0]["allowedValues"] = ["ONE", "TWO", "THREE"]

        try:
            original = self.session.create_signal(signal)

            updated_signal = copy.deepcopy(signal)
            updated_signal["attributes"][0]["allowedValues"].append("FOUR")
            result = self.session.update_signal(signal_name, updated_signal)
            self.assertIsNotNone(result)
            self.assertEqual(result["signalName"], signal_name)
            self.assertGreater(result["version"], original["version"])
            self.assertEqual(result["biosVersion"], original["biosVersion"])
            self.assertEqual(
                result["attributes"][0]["allowedValues"],
                ["ONE", "TWO", "THREE", "FOUR"],
            )
        finally:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                pass

    # regression BB-1245
    def test_feature_name_conflict(self):
        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal["postStorageStage"]["features"].append(
            {
                "featureName": "by_country_state",
                "dimensions": ["country", "state"],
                "attributes": ["stay_length", "orderId"],
                "featureInterval": 300000,
            }
        )

        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(signal)

        self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
        self.assertEqual(
            context.exception.message,
            "Constraint violation:"
            " signalConfig.postStorageStage.features[1].featureName:"
            " The name conflicts with another feature entry;"
            " tenantName=biosSignalCrudTest,"
            " signalName=signal_with_postprocess_example,"
            " featureName=by_country_state",
        )

    # regression BB-1246
    def test_feature_negative_rollup(self):
        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal["postStorageStage"]["features"].append(
            {
                "featureName": "xxx",
                "dimensions": ["country", "state"],
                "attributes": ["stay_length", "orderId"],
                "featureInterval": -300000,
            }
        )

        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(signal)

        self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
        self.assertEqual(
            context.exception.message,
            "Invalid value:"
            " signalConfig.postStorageStage.features[1].featureInterval:"
            " featureInterval must be chosen from among:"
            " 50/100/500 ms, 1/5/15/30 second, and 1/5/15/30 minute;"
            " tenantName=biosSignalCrudTest,"
            " signalName=signal_with_postprocess_example,"
            " featureName=xxx, featureInterval=-300000",
        )

    def test_feature_disallowed_feature_interval(self):
        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal["postStorageStage"]["features"].append(
            {
                "featureName": "xxx",
                "dimensions": ["country", "state"],
                "attributes": ["stay_length", "orderId"],
                "featureInterval": 10000,
            }
        )

        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(signal)

        self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
        self.assertEqual(
            context.exception.message,
            "Invalid value:"
            " signalConfig.postStorageStage.features[1].featureInterval:"
            " featureInterval must be chosen from among:"
            " 50/100/500 ms, 1/5/15/30 second, and 1/5/15/30 minute;"
            " tenantName=biosSignalCrudTest,"
            " signalName=signal_with_postprocess_example,"
            " featureName=xxx, featureInterval=10000",
        )

    def test_feature_update_to_disallowed_feature_interval(self):
        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal_name = "disallowedFeatureInterval"
        signal["signalName"] = signal_name
        signal["postStorageStage"]["features"].append(
            {
                "featureName": "xxx",
                "dimensions": ["country", "state"],
                "attributes": ["stay_length", "orderId"],
                "featureInterval": 300000,
            }
        )

        self.session.create_signal(signal)

        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal["signalName"] = signal_name
        signal["postStorageStage"]["features"].append(
            {
                "featureName": "xxx",
                "dimensions": ["country", "state"],
                "attributes": ["stay_length", "orderId"],
                "featureInterval": 11000,
            }
        )

        with self.assertRaises(ServiceError) as context:
            self.session.update_signal(signal_name, signal)

        self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
        self.assertEqual(
            context.exception.message,
            "Invalid value:"
            " signalConfig.postStorageStage.features[1].featureInterval:"
            " featureInterval must be chosen from among:"
            " 50/100/500 ms, 1/5/15/30 second, and 1/5/15/30 minute;"
            " tenantName=biosSignalCrudTest,"
            " signalName=disallowedFeatureInterval,"
            " featureName=xxx, featureInterval=11000",
        )
        self.session.delete_signal(signal_name)

    # regression BB-1247
    def test_feature_attribute_too_long(self):
        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal["attributes"].append(
            {
                "attributeName": "0123456789012345678901234567890123456789x",
                "type": "string",
            }
        )

        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(signal)

        self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
        self.assertEqual(
            context.exception.message,
            "Invalid value:"
            " signalConfig.attributes[10].attributeName:"
            " Length of a name may not exceed 40;"
            " tenantName=biosSignalCrudTest,"
            " signalName=signal_with_postprocess_example,"
            " attributeName=0123456789012345678901234567890123456789x",
        )

    # regression BB-1248
    def test_duplicate_attribute_name(self):
        signal = copy.deepcopy(self.LOADED_SIGNAL)
        signal["attributes"].append({"attributeName": "orderId", "type": "string"})

        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(signal)

        self.assertTrue(context.exception.error_code == ErrorCode.BAD_INPUT)
        self.assertEqual(
            context.exception.message,
            "Constraint violation:"
            " signalConfig.attributes[10].attributeName:"
            " Attribute name conflict;"
            " tenantName=biosSignalCrudTest,"
            " signalName=signal_with_postprocess_example,"
            " attributeName=orderId",
        )

    def test_update_signal_enrichment_context_attributes(self):
        signal = {
            "signalName": "covidDataSignal",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "reportedDate", "type": "String"},
                {"attributeName": "countryCode", "type": "String"},
                {"attributeName": "reportedCases", "type": "Integer"},
                {"attributeName": "kind", "type": "String"},
                {"attributeName": "reportedDeaths", "type": "Integer"},
            ],
            "enrich": {
                "missingLookupPolicy": "StoreFillInValue",
                "enrichments": [
                    {
                        "enrichmentName": "countryJoin",
                        "foreignKey": ["countryCode"],
                        "missingLookupPolicy": "StoreFillInValue",
                        "contextName": "countryContext",
                        "contextAttributes": [
                            {
                                "attributeName": "name",
                                "as": "countryName",
                                "fillIn": "NA",
                            },
                            {"attributeName": "population", "fillIn": 0},
                            {"attributeName": "continent", "fillIn": "Asia"},
                        ],
                    }
                ],
            },
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "by_country",
                        "dimensions": ["countryCode"],
                        "attributes": ["reportedCases", "reportedDeaths"],
                        "featureInterval": 60000,
                    },
                    {
                        "featureName": "by_continent",
                        "dimensions": ["continent"],
                        "attributes": ["reportedCases", "reportedDeaths"],
                        "featureInterval": 60000,
                    },
                ]
            },
            "dataSynthesisStatus": "Disabled",
        }
        context = {
            "contextName": "countryContext",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "countryCode", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "majorityKind", "type": "String"},
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
        self.session.create_context(context)
        self.session.create_signal(signal)

        signal["enrich"]["enrichments"][0]["contextAttributes"][0]["as"] = "cn"
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["enrich"]["enrichments"][0]["contextAttributes"][0]["as"],
            ret["enrich"]["enrichments"][0]["contextAttributes"][0]["as"],
        )

        signal["enrich"]["enrichments"][0]["enrichmentName"] = "cJ"
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["enrich"]["enrichments"][0]["enrichmentName"],
            ret["enrich"]["enrichments"][0]["enrichmentName"],
        )

        signal["enrich"]["enrichments"][0]["foreignKey"] = ["kind"]
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["enrich"]["enrichments"][0]["foreignKey"],
            ret["enrich"]["enrichments"][0]["foreignKey"],
        )

        signal["enrich"]["enrichments"][0]["contextAttributes"][0]["fillIn"] = "IN"
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["enrich"]["enrichments"][0]["contextAttributes"][0]["fillIn"],
            ret["enrich"]["enrichments"][0]["contextAttributes"][0]["fillIn"],
        )

        signal["postStorageStage"]["features"][0]["featureName"] = "by"
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["postStorageStage"]["features"][0]["featureName"],
            ret["postStorageStage"]["features"][0]["featureName"],
        )

        signal["postStorageStage"]["features"][0]["dimensions"] = ["kind"]
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["postStorageStage"]["features"][0]["featureName"],
            ret["postStorageStage"]["features"][0]["featureName"],
        )

        signal["postStorageStage"] = {
            "features": [
                {
                    "featureName": "by_country",
                    "dimensions": ["countryCode"],
                    "attributes": ["reportedCases"],
                    "featureInterval": 60000,
                },
            ]
        }
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["postStorageStage"]["features"][0]["featureName"],
            ret["postStorageStage"]["features"][0]["featureName"],
        )

        signal["postStorageStage"]["features"][0]["featureInterval"] = 1800000
        ret = self.session.update_signal("covidDataSignal", signal)
        self.assertEqual(
            signal["postStorageStage"]["features"][0]["featureInterval"],
            ret["postStorageStage"]["features"][0]["featureInterval"],
        )

        # Looks like alerts are not yet implemented
        # ['alerts'] = [

        # ]
        signal["postStorageStage"] = {
            "features": [
                {
                    "featureName": "by_country",
                    "dimensions": ["countryCode"],
                    "attributes": ["reportedCases", "reportedDeaths"],
                    "featureInterval": 60000,
                },
                {
                    "featureName": "by_continent",
                    "dimensions": ["continent"],
                    "attributes": ["reportedCases", "reportedDeaths"],
                    "alerts": [
                        {
                            "alertName": "alertForAnomalyStayLength",
                            "condition": "(1 AND 1)",
                            "webhookUrl": "https://webhook.site/"
                            "99743393-3a47-473f-8676-319e8c5d9422",
                        }
                    ],
                    "featureInterval": 60000,
                },
            ]
        }
        ret = self.session.update_signal("covidDataSignal", signal)

        signal["postStorageStage"]["features"][1]["alerts"][0][
            "webhookUrl"
        ] = "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9433"

        ret = self.session.update_signal("covidDataSignal", signal)

        self.assertEqual(
            signal["postStorageStage"]["features"][0]["featureName"],
            ret["postStorageStage"]["features"][0]["featureName"],
        )

    def test_cud_internal_signal(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                # create
                signal = {
                    "signalName": "_test",
                    "missingAttributePolicy": "Reject",
                    "attributes": [
                        {"attributeName": "one", "type": "string"},
                        {"attributeName": "two", "type": "string"},
                    ],
                }
                with self.assertRaises(ServiceError) as cm:
                    self.session.create_signal(signal)
                self.assertEqual(cm.exception.error_code, ErrorCode.BAD_INPUT)
                sadmin.set_property("prop.allowInternalStreamName", "true")
                self.session.create_signal(signal)

                # update
                signal["attributes"].append(
                    {"attributeName": "three", "type": "string", "default": ""}
                )
                self.session.update_signal("_test", signal)
                sadmin.set_property("prop.allowInternalStreamName", "")
                signal["attributes"].append(
                    {"attributeName": "four", "type": "string", "default": ""}
                )
                with self.assertRaises(ServiceError) as cm:
                    self.session.update_signal("_test", signal)
                self.assertEqual(cm.exception.error_code, ErrorCode.BAD_INPUT)

                # delete
                with self.assertRaises(ServiceError) as cm:
                    self.session.update_signal("_test", signal)
                self.assertEqual(cm.exception.error_code, ErrorCode.BAD_INPUT)

            finally:
                sadmin.set_property("prop.allowInternalStreamName", "true")
                try:
                    self.session.delete_signal("_test")
                except ServiceError:
                    pass
                sadmin.set_property("prop.allowInternalStreamName", "")

    def test_negative_create_signal_with_unsupported_feature_attribute(self):
        signal_name = "alertBios4464"
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "reportTime",
                    "type": "Integer",
                    "tags": {
                        "category": "Quantity",
                        "kind": "Timestamp",
                        "unit": "UnixMillisecond",
                    },
                },
                {"attributeName": "tenant", "type": "String"},
                {"attributeName": "signal", "type": "String"},
                {"attributeName": "feature", "type": "String"},
                {"attributeName": "alert", "type": "String"},
                {"attributeName": "condition", "type": "String"},
                {"attributeName": "event", "type": "String"},
                {
                    "attributeName": "windowStartTime",
                    "type": "Integer",
                    "tags": {
                        "category": "Quantity",
                        "kind": "Timestamp",
                        "unit": "UnixMillisecond",
                    },
                },
                {
                    "attributeName": "windowLength",
                    "type": "Integer",
                    "tags": {"category": "Quantity", "kind": "Duration", "unit": "Millisecond"},
                },
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "byMultiple",
                        "dimensions": ["tenant", "signal", "feature", "alert", "condition"],
                        "attributes": ["event"],
                        "featureInterval": 60000,
                    }
                ]
            },
        }
        try:
            self.session.delete_signal(signal_name)
        except ServiceError as err:
            if err.error_code is not ErrorCode.NO_SUCH_STREAM:
                raise

        self.session.create_signal(signal)

    @unittest.skip("BIOS-5154")
    def test_rename_signal(self):
        signal = copy.deepcopy(SIGNAL_FOR_CREATE_TEST)
        self.session.create_signal(SIGNAL_FOR_CREATE_TEST)
        self.addCleanup(self.session.delete_signal, SIGNAL_FOR_CREATE_TEST["signalName"])
        signal["signalName"] = signal["signalName"] + "new"
        with self.assertRaises(ServiceError) as error_context:
            self.session.update_signal(SIGNAL_FOR_CREATE_TEST["signalName"], signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_IMPLEMENTED)

    def test_setting_internal_flag_should_be_ignored(self):
        signal_name = "signalSettingInternalFlag"
        signal = copy.deepcopy(SIGNAL_ONE_ATTRIBUTE)
        signal["signalName"] = signal_name
        created = self.session.create_signal(signal)
        assert created.get("isInternal") is False
        retrieved = self.session.get_signal(signal_name)
        assert retrieved.get("isInternal") is False

    def test_modifying_missing_attribute_policy(self):
        signal_name = "modifySignalMAP"
        signal = copy.deepcopy(SIGNAL_ONE_ATTRIBUTE)
        signal["signalName"] = signal_name
        self.session.create_signal(signal)

        # Change missing attribute policy
        signal["missingAttributePolicy"] = "StoreDefaultValue"
        signal["attributes"][0]["default"] = "MISSING"
        self.session.update_signal(signal_name, signal)
        updated = self.session.get_signal(signal_name)
        assert updated["missingAttributePolicy"] == signal["missingAttributePolicy"]
        assert updated["attributes"] == signal["attributes"]

        # Revert missing attribute policy with keeping default
        signal["missingAttributePolicy"] = "Reject"
        self.session.update_signal(signal_name, signal)
        updated2 = self.session.get_signal(signal_name)
        assert updated2["missingAttributePolicy"] == signal["missingAttributePolicy"]
        assert updated2["attributes"] == signal["attributes"]

        # Try to set StoreDefaultValue to an attribute without default
        signal["attributes"][0]["missingAttributePolicy"] = "StoreDefaultValue"
        del signal["attributes"][0]["default"]
        with pytest.raises(ServiceError) as error_context:
            self.session.update_signal(signal_name, signal)
        assert error_context.value.error_code == ErrorCode.BAD_INPUT
        assert "Constraint violation" in error_context.value.message

        # Set the default value
        signal["attributes"][0]["default"] = "N/A"
        self.session.update_signal(signal_name, signal)
        updated3 = self.session.get_signal(signal_name)
        assert updated3["missingAttributePolicy"] == signal["missingAttributePolicy"]
        assert updated3["attributes"] == signal["attributes"]

    def test_modifying_missing_lookup_policy(self):
        signal_name = "changeMlp"
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "reportedDate", "type": "String"},
                {"attributeName": "countryCode", "type": "String"},
                {"attributeName": "reportedCases", "type": "Integer"},
                {"attributeName": "kind", "type": "String"},
                {"attributeName": "reportedDeaths", "type": "Integer"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "countryJoin",
                        "foreignKey": ["countryCode"],
                        "missingLookupPolicy": "Reject",
                        "contextName": "mlpChangeRemote",
                        "contextAttributes": [
                            {
                                "attributeName": "name",
                                "as": "countryName",
                            },
                            {"attributeName": "population"},
                            {"attributeName": "continent"},
                        ],
                    }
                ],
            },
        }
        context = {
            "contextName": "mlpChangeRemote",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "countryCode", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "majorityKind", "type": "String"},
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
                        "Unknown",
                    ],
                },
            ],
            "primaryKey": ["countryCode"],
            "dataSynthesisStatus": "Disabled",
        }
        context2 = {
            "contextName": "mlpChangeRemote2",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "countryCode", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "majorityKind", "type": "String"},
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
                        "Unknown",
                    ],
                },
            ],
            "primaryKey": ["countryCode"],
            "dataSynthesisStatus": "Disabled",
        }
        self.session.create_context(context)
        self.session.create_context(context2)
        created = self.session.create_signal(signal)
        pprint.pprint(created)

        # Change missing lookup policy
        update = copy.deepcopy(signal)
        update["enrich"]["enrichments"][0]["missingLookupPolicy"] = "StoreFillInValue"
        update["enrich"]["enrichments"][0]["contextAttributes"][0]["fillIn"] = "MISSING"
        update["enrich"]["enrichments"][0]["contextAttributes"][1]["fillIn"] = 0
        update["enrich"]["enrichments"][0]["contextAttributes"][2]["fillIn"] = "Unknown"

        updated = self.session.update_signal(signal_name, update)
        assert updated["enrich"] == update["enrich"]
        assert self.session.get_signal(signal_name)["enrich"] == update["enrich"]

        # Change the missing lookup policy but leave the default values
        update2 = copy.deepcopy(update)
        update2["enrich"]["enrichments"][0]["missingLookupPolicy"] = "Reject"
        updated2 = self.session.update_signal(signal_name, update2)
        assert updated2["enrich"] == update2["enrich"]
        fetched2 = self.session.get_signal(signal_name)
        assert fetched2["enrich"] == updated2["enrich"]

        # Revert the signal back to the second state
        updated3 = self.session.update_signal(signal_name, update)
        assert updated3["enrich"] == update["enrich"]
        assert self.session.get_signal(signal_name)["enrich"] == updated["enrich"]

        # Change the default values
        update4 = copy.deepcopy(update)
        update4["enrich"]["enrichments"][0]["contextAttributes"][0]["fillIn"] = "n/a"
        update4["enrich"]["enrichments"][0]["contextAttributes"][1]["fillIn"] = -1
        update4["enrich"]["enrichments"][0]["contextAttributes"][2]["fillIn"] = "Asia"
        updated4 = self.session.update_signal(signal_name, update4)
        assert updated4["enrich"] == update4["enrich"]
        assert self.session.get_signal(signal_name)["enrich"] == update4["enrich"]

        # Back to the original
        updated5 = self.session.update_signal(signal_name, signal)
        assert updated5["enrich"] == signal["enrich"]
        assert self.session.get_signal(signal_name)["enrich"] == signal["enrich"]

        # Change remote context name
        update6 = copy.deepcopy(signal)
        update6["enrich"]["enrichments"][0]["contextName"] = "mlpChangeRemote2"
        updated6 = self.session.update_signal(signal_name, update6)
        assert updated6["enrich"] == update6["enrich"]
        assert self.session.get_signal(signal_name)["enrich"] == update6["enrich"]

    def test_update_signal_with_different_case(self):
        signal_name = "signalMixedCase"
        signal = copy.deepcopy(SIGNAL_ONE_ATTRIBUTE)
        signal["signalName"] = signal_name
        self.session.create_signal(signal)

        # update signal
        signal["attributes"].append(
            {"attributeName": "the_third", "type": "Integer", "default": 0}
        )
        signal["signalName"] = signal_name.upper()
        self.session.update_signal(signal_name.lower(), signal)
        updated = self.session.get_signal(signal_name)

        assert updated.get("signalName") == signal.get("signalName")
        assert updated.get("attributes") == signal.get("attributes")

    def test_removing_added_default(self):
        signal_name = "testRemovingAddedDefault"
        signal = copy.deepcopy(SIGNAL_ONE_ATTRIBUTE)
        signal["signalName"] = signal_name
        try:
            self.session.create_signal(signal)

            # update signal
            signal["attributes"].append(
                {"attributeName": "the_third", "type": "Integer", "default": -1}
            )
            self.session.update_signal(signal_name, signal)

            # try to remove default value, should be rejected
            del signal["attributes"][1]["default"]
            with pytest.raises(ServiceError) as exc_info:
                self.session.update_signal(signal_name, signal)
            assert exc_info.value.error_code == ErrorCode.BAD_INPUT
            assert (
                "Constraint violation: Default value must be configured for an adding attribute;"
                in exc_info.value.message
            )
        finally:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                pass

    def test_changing_missing_attribute_policy(self):
        signal_name = "testChangingMissingAttributePolicy"
        signal = copy.deepcopy(SIGNAL_ONE_ATTRIBUTE)
        signal["signalName"] = signal_name
        signal["attributes"].append(
            {
                "attributeName": "the_third",
                "type": "Integer",
                "missingAttributePolicy": "StoreDefaultValue",
                "default": -1,
            }
        )
        try:
            self.session.create_signal(signal)

            time0 = bios.time.now()
            self.session.execute(bios.isql().insert().into(signal_name).csv("hello,").build())

            # change missing attribute policy
            del signal["attributes"][1]["missingAttributePolicy"]
            del signal["attributes"][1]["default"]
            self.session.update_signal(signal_name, signal)

            with pytest.raises(ServiceError) as exc_info:
                self.session.execute(bios.isql().insert().into(signal_name).csv("one,").build())
            assert exc_info.value.error_code == ErrorCode.BAD_INPUT

            now = bios.time.now()
            records = self.session.execute(
                bios.isql()
                .select()
                .from_signal(signal_name)
                .time_range(time0, now - time0)
                .build()
            ).to_dict()
            assert len(records) == 1
            assert records[0].get("the_third") == -1
        finally:
            try:
                self.session.delete_signal(signal_name)
            except ServiceError:
                pass


if __name__ == "__main__":
    pytest.main(sys.argv)
