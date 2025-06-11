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
import sys
import unittest
from typing import Tuple

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import setup_context, setup_signal
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as endpoint
from tsetup import sadmin_pass, sadmin_user

TEST_TENANT_NAME = "contextAudit"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

PRODUCT_CONTEXT = {
    "contextName": "placeholder",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "product_id", "type": "integer"},
        {"attributeName": "product_name", "type": "string"},
        {"attributeName": "product_price", "type": "decimal"},
    ],
    "primaryKey": ["product_id"],
}


class ContextAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    def tearDown(self):
        self.session.close()

    @classmethod
    def _make_audit_signal_name(cls, context_name: str) -> str:
        """Generates audit signal name from context name"""
        return "audit" + context_name[0:1].upper() + context_name[1:]

    def _make_context_config(
        self, context_name: str, audit_enabled: bool = None
    ) -> Tuple[dict, str]:
        """Makes a context config from the PRODUCT_CONTEXT template, returns the context
        and its audit signal name"""
        context = copy.deepcopy(PRODUCT_CONTEXT)
        context["contextName"] = context_name
        if audit_enabled is not None:
            context["auditEnabled"] = audit_enabled
        return context, self._make_audit_signal_name(context_name)

    def _create_initial_context(self, context: dict) -> dict:
        """Create an initial context, returns the created context"""
        context_name = context.get("contextName")
        try:
            self.session.delete_context(context_name)
        except ServiceError:
            pass
        try:
            self.session.delete_signal(self._make_audit_signal_name(context_name))
        except ServiceError:
            pass

        self.session.create_context(context)
        return self.session.get_context(context_name)

    def test_audit_context_create_and_delete(self):
        context_name = "auditCreateTest"
        context, audit_signal_name = self._make_context_config(context_name, audit_enabled=True)

        created_context = self._create_initial_context(context)
        self.assertIsNotNone(created_context)
        self.assertEqual(created_context["contextName"], context_name)
        self.assertGreater(len(created_context), 1)

        signal = self.session.get_signal(audit_signal_name)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], audit_signal_name)
        attributes = signal.get("attributes")
        self.assertEqual(len(attributes), 7)
        self.assertEqual(attributes[0].get("attributeName"), "_operation")
        self.assertEqual(attributes[0].get("type"), "String")
        self.assertEqual(attributes[1].get("attributeName"), "product_id")
        self.assertEqual(attributes[1].get("type"), "Integer")
        self.assertEqual(attributes[2].get("attributeName"), "prevProduct_id")
        self.assertEqual(attributes[2].get("type"), "Integer")
        self.assertEqual(attributes[3].get("attributeName"), "product_name")
        self.assertEqual(attributes[3].get("type"), "String")
        self.assertEqual(attributes[4].get("attributeName"), "prevProduct_name")
        self.assertEqual(attributes[4].get("type"), "String")
        self.assertEqual(attributes[5].get("attributeName"), "product_price")
        self.assertEqual(attributes[5].get("type"), "Decimal")
        self.assertEqual(attributes[6].get("attributeName"), "prevProduct_price")
        self.assertEqual(attributes[6].get("type"), "Decimal")
        self.assertIsNotNone(signal.get("postStorageStage"))
        self.assertIsNotNone(signal["postStorageStage"].get("features"))
        self.assertEqual(len(signal["postStorageStage"]["features"]), 1)
        feature = signal["postStorageStage"]["features"][0]
        self.assertEqual(feature.get("featureName"), "byOperation")
        self.assertEqual(feature.get("dimensions"), ["_operation"])
        self.assertEqual(feature.get("attributes"), [])
        self.assertEqual(feature.get("featureInterval"), 300000)

        # Delete
        self.session.delete_context(context_name)
        with self.assertRaises(ServiceError) as ec:
            self.session.get_context(context_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

        with self.assertRaises(ServiceError) as ec:
            self.session.get_signal(audit_signal_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

        created_context = self._create_initial_context(context)

    def test_recreate_context(self):
        context_name = "auditRecreateTest"
        context, _ = self._make_context_config(context_name, audit_enabled=True)
        self._create_initial_context(context)
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_context(context)
        self.assertEqual(error_context.exception.error_code, ErrorCode.STREAM_ALREADY_EXISTS)
        self.session.delete_context(context_name)

    def test_force_setting_audit(self):
        context_name = "productToEnableAudit"
        context, audit_signal_name = self._make_context_config(context_name)

        # Create  base Context without Auditing
        created_context = self._create_initial_context(context)
        self.assertIsNotNone(created_context)
        self.assertEqual(created_context["contextName"], context_name)
        # audit is force set
        self.assertTrue(created_context.get("auditEnabled"))

        # Verify that the audit signal exists
        signal = self.session.get_signal(audit_signal_name)
        self.assertEqual(signal["signalName"], audit_signal_name)
        self.assertGreater(len(signal), 1)

        # Delete
        self.session.delete_context(context_name)
        with self.assertRaises(ServiceError) as ec:
            self.session.get_context(context_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

        with self.assertRaises(ServiceError) as ec:
            self.session.get_signal(audit_signal_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

    def test_deleting_context_with_enabled_audit(self):
        context_name = "productToDeleteWithEnabledAudit"
        context, audit_signal_name = self._make_context_config(context_name, audit_enabled=False)

        # Create  base Context without Auditing
        created_context = self._create_initial_context(context)
        self.assertIsNotNone(created_context)
        self.assertEqual(created_context["contextName"], context_name)
        # audit is always set true
        self.assertTrue(created_context["auditEnabled"])

        # ensure signal is created
        signal = self.session.get_signal(audit_signal_name)
        self.assertEqual(signal["signalName"], audit_signal_name)
        self.assertGreater(len(signal), 1)

        # Delete Signal -- should fail
        with self.assertRaises(ServiceError) as ec:
            self.session.delete_signal(audit_signal_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.BAD_INPUT)
        assert "Constraint violation" in error.message

        # Delete
        self.session.delete_context(context_name)
        with self.assertRaises(ServiceError) as ec:
            self.session.get_context(context_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

        # Verify that the audit signal is deleted, too
        with self.assertRaises(ServiceError) as ec:
            self.session.get_signal(audit_signal_name)
        error = ec.exception
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

    def test_modify_context_with_enabled_audit(self):
        context_name = "productToModifyWithEnabledAudit"
        context, audit_signal_name = self._make_context_config(context_name, audit_enabled=True)

        created_context = self._create_initial_context(context)
        self.assertIsNotNone(created_context)
        self.assertEqual(created_context["contextName"], context_name)
        self.assertGreater(len(created_context), 1)

        # Update the context
        context["attributes"].append({"attributeName": "gtin", "type": "Integer", "default": 0})
        self.session.update_context(context_name, context)
        modified_context = self.session.get_context(context_name)
        self.assertIsNotNone(modified_context)
        self.assertEqual(modified_context["attributes"][3]["attributeName"], "gtin")
        self.assertEqual(modified_context["attributes"][3]["type"], "Integer")
        self.assertEqual(modified_context["attributes"][3]["default"], 0)

        # Verify that the audit signal is also updated
        signal = self.session.get_signal(audit_signal_name)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], audit_signal_name)
        attributes = signal.get("attributes")
        self.assertEqual(len(attributes), 9)
        self.assertEqual(attributes[0].get("attributeName"), "_operation")
        self.assertEqual(attributes[0].get("type"), "String")
        self.assertEqual(attributes[1].get("attributeName"), "product_id")
        self.assertEqual(attributes[1].get("type"), "Integer")
        self.assertEqual(attributes[2].get("attributeName"), "prevProduct_id")
        self.assertEqual(attributes[2].get("type"), "Integer")
        self.assertEqual(attributes[3].get("attributeName"), "product_name")
        self.assertEqual(attributes[3].get("type"), "String")
        self.assertEqual(attributes[4].get("attributeName"), "prevProduct_name")
        self.assertEqual(attributes[4].get("type"), "String")
        self.assertEqual(attributes[5].get("attributeName"), "product_price")
        self.assertEqual(attributes[5].get("type"), "Decimal")
        self.assertEqual(attributes[6].get("attributeName"), "prevProduct_price")
        self.assertEqual(attributes[6].get("type"), "Decimal")
        self.assertEqual(attributes[7].get("attributeName"), "gtin")
        self.assertEqual(attributes[7].get("type"), "Integer")
        self.assertEqual(attributes[7].get("default"), 0)
        self.assertEqual(attributes[8].get("attributeName"), "prevGtin")
        self.assertEqual(attributes[8].get("type"), "Integer")
        self.assertEqual(attributes[8].get("default"), 0)

    def test_fail_modifying_context_with_enabled_audit(self):
        context_name = "productFailToModifyWithEnabledAudit"
        context, audit_signal_name = self._make_context_config(context_name, audit_enabled=True)

        created_context = self._create_initial_context(context)
        self.assertIsNotNone(created_context)
        self.assertEqual(created_context["contextName"], context_name)
        self.assertGreater(len(created_context), 1)

        # Keep the original audit signal
        signal = self.session.get_signal(audit_signal_name)

        # Try updating the context which fails
        context["attributes"][2]["type"] = "string"
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_context(context_name, context)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT

        # Verify that the audit signal did not change
        signal2 = self.session.get_signal(audit_signal_name)

        assert signal == signal2

    def test_modify_context_with_modified_audit(self):
        context_name = "productModWithModifiedAudit"
        context, audit_signal_name = self._make_context_config(context_name, audit_enabled=True)
        context["attributes"].append({"attributeName": "category_id", "type": "Integer"})

        self._create_initial_context(context)

        # Update the audit signal
        last_n_context_name = f"{context_name}_lastN"
        last_n_context_config = {
            "contextName": last_n_context_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product_id", "type": "integer"},
                {"attributeName": "productNameCollection", "type": "string"},
            ],
            "primaryKey": ["product_id"],
        }
        setup_context(self.session, last_n_context_config)

        category_context_name = f"{context_name}_lastN"
        category_context_config = {
            "contextName": category_context_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "category_id", "type": "integer"},
                {"attributeName": "category_name", "type": "string"},
            ],
            "primaryKey": ["category_id"],
        }
        setup_context(self.session, category_context_config)

        signal = self.session.get_signal(audit_signal_name)
        signal_to_modify = copy.deepcopy(signal)
        signal_to_modify["enrich"] = {
            "enrichments": [
                {
                    "enrichmentName": "category",
                    "foreignKey": ["category_id"],
                    "missingLookupPolicy": "Reject",
                    "contextName": category_context_name,
                    "contextAttributes": [{"attributeName": "category_name", "fillIn": "MISSING"}],
                }
            ]
        }
        signal_to_modify["postStorageStage"]["features"].append(
            {
                "featureName": "lastProductName",
                "dimensions": ["product_id"],
                "attributes": ["category_name"],
                "featureInterval": 300000,
                "materializedAs": "LastN",
                "items": 5,
                "ttl": 15 * 24 * 3600 * 1000,
                "featureAsContextName": last_n_context_name,
            }
        )
        signal_to_modify["postStorageStage"]["features"].append(
            {
                "featureName": "priceByCategory",
                "dimensions": ["category_id"],
                "attributes": ["product_price"],
                "featureInterval": 300000,
            }
        )
        self.session.update_signal(audit_signal_name, signal_to_modify)

        # Update the context - add attributes
        context["attributes"].append({"attributeName": "gtin", "type": "Integer", "default": 0})
        self.session.update_context(context_name, context)
        modified_context = self.session.get_context(context_name)
        self.assertIsNotNone(modified_context)
        self.assertEqual(modified_context["attributes"][4]["attributeName"], "gtin")
        self.assertEqual(modified_context["attributes"][4]["type"], "Integer")
        self.assertEqual(modified_context["attributes"][4]["default"], 0)

        # Verify that the audit signal is also updated, but the modified features remain
        signal = self.session.get_signal(audit_signal_name)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["signalName"], audit_signal_name)
        attributes = signal.get("attributes")
        self.assertEqual(len(attributes), 11)
        self.assertEqual(attributes[0].get("attributeName"), "_operation")
        self.assertEqual(attributes[0].get("type"), "String")
        self.assertEqual(attributes[1].get("attributeName"), "product_id")
        self.assertEqual(attributes[1].get("type"), "Integer")
        self.assertEqual(attributes[2].get("attributeName"), "prevProduct_id")
        self.assertEqual(attributes[2].get("type"), "Integer")
        self.assertEqual(attributes[3].get("attributeName"), "product_name")
        self.assertEqual(attributes[3].get("type"), "String")
        self.assertEqual(attributes[4].get("attributeName"), "prevProduct_name")
        self.assertEqual(attributes[4].get("type"), "String")
        self.assertEqual(attributes[5].get("attributeName"), "product_price")
        self.assertEqual(attributes[5].get("type"), "Decimal")
        self.assertEqual(attributes[6].get("attributeName"), "prevProduct_price")
        self.assertEqual(attributes[6].get("type"), "Decimal")
        self.assertEqual(attributes[7].get("attributeName"), "category_id")
        self.assertEqual(attributes[7].get("type"), "Integer")
        self.assertEqual(attributes[8].get("attributeName"), "prevCategory_id")
        self.assertEqual(attributes[8].get("type"), "Integer")
        self.assertEqual(attributes[9].get("attributeName"), "gtin")
        self.assertEqual(attributes[9].get("type"), "Integer")
        self.assertEqual(attributes[9].get("default"), 0)
        self.assertEqual(attributes[10].get("attributeName"), "prevGtin")
        self.assertEqual(attributes[10].get("type"), "Integer")
        self.assertEqual(attributes[10].get("default"), 0)

        assert signal.get("enrich") == signal_to_modify.get("enrich")
        assert signal.get("postStorageStage") == signal_to_modify.get("postStorageStage")

        # Update the context again - remove attributes
        context["attributes"] = list(
            filter(lambda attr: attr["attributeName"] != "product_price", context["attributes"])
        )
        self.session.update_context(context_name, context)

        # Verify that the audit signal is also updated, affected features should be removed
        signal2 = self.session.get_signal(audit_signal_name)
        expected_features = list(
            filter(
                lambda feature: feature["featureName"] != "priceByCategory",
                signal_to_modify["postStorageStage"]["features"],
            )
        )
        assert signal2["enrich"] == signal_to_modify["enrich"]
        assert signal2["postStorageStage"]["features"] == expected_features

        # Update the context again - remove another attribute that affects enrichment
        context["attributes"] = list(
            filter(lambda attr: attr["attributeName"] != "category_id", context["attributes"])
        )
        self.session.update_context(context_name, context)

        # Verify that the audit signal is also updated, affected enrichment and feature should be
        # removed
        signal3 = self.session.get_signal(audit_signal_name)
        expected_features = list(
            filter(
                lambda feature: feature["featureName"] != "lastProductName",
                signal2["postStorageStage"]["features"],
            )
        )
        assert not signal3.get("enrich") or not signal3["enrich"].get("enrichments")
        assert signal3["postStorageStage"]["features"] == expected_features

    def test_creating_context_with_pre_existing_consistent_audit(self):
        context_name = "createContextWithExistingAudit"
        context, audit_signal_name = self._make_context_config(context_name)
        context["auditEnabled"] = True
        try:
            self.session.delete_context(context_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise
        expected_audit_attributes = [
            {"attributeName": "_operation", "type": "String"},
            {"attributeName": "product_id", "type": "Integer"},
            {"attributeName": "prevProduct_id", "type": "Integer"},
            {"attributeName": "product_name", "type": "String"},
            {"attributeName": "prevProduct_name", "type": "String"},
            {"attributeName": "product_price", "type": "Decimal"},
            {"attributeName": "prevProduct_price", "type": "Decimal"},
        ]
        preexisting_audit_signal = {
            "signalName": audit_signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": expected_audit_attributes,
        }
        setup_signal(self.session, preexisting_audit_signal)

        # Create  base Context without Auditing
        created_context = self.session.create_context(context)
        assert created_context.get("contextName") == context_name

        audit_signal = self.session.get_signal(audit_signal_name)
        assert audit_signal.get("attributes") == expected_audit_attributes

    def test_creating_context_with_pre_existing_inconsistent_audit(self):
        context_name = "createContextWithBadAudit"
        context, audit_signal_name = self._make_context_config(context_name)
        context["auditEnabled"] = True
        try:
            self.session.delete_context(context_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise
        preexisting_audit_signal = {
            "signalName": audit_signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "one", "type": "String"},
                {"attributeName": "two", "type": "String"},
            ],
        }
        setup_signal(self.session, preexisting_audit_signal)

        # Create  base Context without Auditing
        with pytest.raises(ServiceError) as error_info:
            self.session.create_context(context)
        assert error_info.value.error_code == ErrorCode.BAD_INPUT


if __name__ == "__main__":
    pytest.main(sys.argv)
