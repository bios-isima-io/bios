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
import os
import sys
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import read_file
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

RESOURCES_DIR = f"{os.path.dirname(os.path.realpath(__file__))}/../resources"
CONTEXT_WITH_INDEXES = f"{RESOURCES_DIR}/bios-context-with-indexes.json"
CONTEXT_WITH_INDEXES_INVALID = f"{RESOURCES_DIR}/bios-context-with-indexes-invalid.json"
CONTEXT_WITH_FEATURES = f"{RESOURCES_DIR}/bios-context-with-features.json"
CONTEXT_WITH_FEATURES_INVALID = f"{RESOURCES_DIR}/bios-context-with-features-invalid.json"


TEST_TENANT_NAME = "biosContextCrudTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIMPLE_CONTEXT_NAME = "simpleContext"
SIMPLE_CONTEXT = {
    "contextName": SIMPLE_CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "Integer"},
        {"attributeName": "the_value", "type": "String"},
    ],
    "primaryKey": ["the_key"],
}

STORE_CONTEXT = {
    "contextName": "storeContext",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "storeId",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "zipCode", "type": "Integer", "default": -1},
        {"attributeName": "updatedTime", "type": "Integer", "default": 0},
        {"attributeName": "address", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["storeId"],
    "versionAttribute": "updatedTime",
    "ttl": 86400000,
}

ANOTHER_SIMPLE_CONTEXT = {
    "contextName": "context_example",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "userName", "type": "String"},
        {
            "attributeName": "name",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "n/a",
        },
    ],
    "primaryKey": ["userName"],
}


class BiosContextCrudTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_context(SIMPLE_CONTEXT)
            admin.create_context(STORE_CONTEXT)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    def tearDown(self):
        self.session.close()

    def test_get_context(self):
        self.assertIsNotNone(self.session)
        context = self.session.get_context(SIMPLE_CONTEXT_NAME)
        self.assertIsNotNone(context)
        self.assertEqual(context["contextName"], SIMPLE_CONTEXT_NAME)
        self.assertGreater(len(context), 1)
        self.assertIsNone(context["attributes"][0].get("inferredTags"))
        # audit should be enabled always
        self.assertTrue(context.get("auditEnabled"))

    def test_get_contexts_default(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts()
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 2)
        self.assertEqual(len(contexts[0]), 1)
        self.assertEqual(len(contexts[1]), 1)
        self.assertEqual(
            {entry.get("contextName") for entry in contexts},
            {SIMPLE_CONTEXT["contextName"], STORE_CONTEXT["contextName"]},
        )

    def test_get_contexts_including_internals(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts(include_internal=True)
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 4)
        self.assertEqual(
            {entry.get("contextName") for entry in contexts},
            {
                SIMPLE_CONTEXT["contextName"],
                STORE_CONTEXT["contextName"],
                "_ip2geo",
                "_ipBlacklist",
            },
        )

    def test_get_contexts_including_internals_with_detail(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts(include_internal=True, detail=True)
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 4)
        self.assertEqual(
            {entry.get("contextName"): entry.get("isInternal") for entry in contexts},
            {
                SIMPLE_CONTEXT["contextName"]: False,
                STORE_CONTEXT["contextName"]: False,
                "_ip2geo": True,
                "_ipBlacklist": True,
            },
        )

    def test_get_contexts_with_name(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts(names=[SIMPLE_CONTEXT_NAME])
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 1)
        self.assertEqual(len(contexts[0]), 1)
        self.assertEqual(contexts[0]["contextName"], SIMPLE_CONTEXT_NAME)

    def test_get_contexts_with_names(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts(
            names=[SIMPLE_CONTEXT_NAME, STORE_CONTEXT["contextName"]]
        )
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 2)
        names = {context["contextName"] for context in contexts}
        assert names == {SIMPLE_CONTEXT_NAME, STORE_CONTEXT["contextName"]}

    def test_get_contexts_with_internal_name(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts(names=["_ip2geo"])
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 1)
        self.assertEqual(len(contexts[0]), 1)
        self.assertEqual(contexts[0]["contextName"], "_ip2geo")

    def test_get_contexts_with_detail(self):
        self.assertIsNotNone(self.session)
        contexts = self.session.get_contexts(detail=True)
        self.assertIsNotNone(contexts)
        self.assertEqual(len(contexts), 2)
        self.assertGreater(len(contexts[0]), 1)
        self.assertGreater(len(contexts[1]), 1)

        store_context = list(
            filter(
                lambda context: (context["contextName"] == STORE_CONTEXT["contextName"]),
                contexts,
            )
        )[0]
        self.assertEqual(len(store_context["attributes"]), 4)
        self.assertIsNone(store_context["attributes"][0].get("inferredTags"))
        self.assertEqual(store_context["ttl"], 86400000)

        # verify all contexts has audit enabled
        # also verify all contexts are not internal
        for context in contexts:
            assert context.get("auditEnabled") is True
            assert context.get("isInternal") is False

    def test_get_contexts_via_get_tenant(self):
        tenant = self.session.get_tenant(detail=True)
        contexts = tenant["contexts"]
        self.assertGreater(len(contexts), 1)
        context0 = contexts[0]
        self.assertGreaterEqual(len(context0["attributes"]), 2)
        self.assertIsNone(context0["attributes"][0].get("inferredTags"))

    def test_invalid_context_update_change_primary_key(self):
        updated_context = copy.deepcopy(STORE_CONTEXT)
        updated_context["primaryKey"] = ["zipCode"]
        with self.assertRaises(ServiceError) as cm:
            self.session.update_context(STORE_CONTEXT["contextName"], updated_context)
        error = cm.exception
        self.assertEqual(error.error_code, ErrorCode.BAD_INPUT)

    def test_invalid_context_update_change_version_attribute(self):
        updated_context = copy.deepcopy(STORE_CONTEXT)
        updated_context["versionAttribute"] = "zipCode"
        with self.assertRaises(ServiceError) as cm:
            self.session.update_context(STORE_CONTEXT["contextName"], updated_context)
        error = cm.exception
        self.assertEqual(error.error_code, ErrorCode.BAD_INPUT)

    def test_crud_context(self):
        context_name = ANOTHER_SIMPLE_CONTEXT["contextName"]
        try:
            # Create
            response = self.session.create_context(ANOTHER_SIMPLE_CONTEXT)
            self.assertIsNotNone(response)
            self.assertGreater(response["version"], 0)
            self.assertTrue(response["auditEnabled"])
            original = copy.deepcopy(ANOTHER_SIMPLE_CONTEXT)
            # the server set isInternal flag
            original["isInternal"] = False

            temp = response.copy()
            del temp["version"]
            del temp["biosVersion"]
            del temp["auditEnabled"]
            self.assertEqual(temp, original)

            # Read
            fetched = self.session.get_context(context_name)
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched["biosVersion"], response["version"])
            self.assertEqual(fetched, response)

            # Update
            to_modify = copy.deepcopy(original)
            to_modify["attributes"].append(
                {
                    "attributeName": "gender",
                    "type": "String",
                    "tags": {"category": "Dimension"},
                    "allowedValues": ["UNKNOWN", "MALE", "FEMALE"],
                    "default": "UNKNOWN",
                }
            )
            to_modify["ttl"] = 3600000
            updated_resp = self.session.update_context(context_name, to_modify)
            self.assertIsNotNone(updated_resp)
            self.assertGreater(updated_resp["version"], fetched["version"])
            self.assertEqual(updated_resp["biosVersion"], fetched["biosVersion"])
            self.assertEqual(updated_resp["ttl"], 3600000)
            self.assertTrue(updated_resp["auditEnabled"])

            fetched_again = self.session.get_context(context_name)
            self.assertIsNotNone(fetched_again)
            self.assertEqual(fetched_again, updated_resp)

            # Delete
            self.session.delete_context(context_name)
            with self.assertRaises(ServiceError) as cm:
                self.session.get_context(context_name)
            error = cm.exception
            self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

        finally:
            try:
                self.session.delete_context(context_name)
            except ServiceError:
                pass

    # BIOS-5638 audit should be enabled even when the user tries to disable it.
    # We'll remove the property eventually.
    def test_force_enabling_audit(self):
        context_name = "testForceEnablingAudit"
        context_config = copy.deepcopy(ANOTHER_SIMPLE_CONTEXT)
        context_config["contextName"] = context_name
        context_config["auditEnabled"] = False
        try:
            # Create
            response = self.session.create_context(context_config)
            self.assertIsNotNone(response)
            self.assertGreater(response["version"], 0)
            temp = response.copy()
            del temp["version"]
            del temp["biosVersion"]

            expected = copy.deepcopy(context_config)
            expected["auditEnabled"] = True
            expected["isInternal"] = False
            self.assertEqual(temp, expected)

            # Read
            fetched = self.session.get_context(context_name)
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched["biosVersion"], response["version"])
            self.assertEqual(fetched, response)

            # Update, the spec has audit disabled
            to_modify = copy.deepcopy(context_config)
            to_modify["attributes"].append(
                {
                    "attributeName": "gender",
                    "type": "String",
                    "tags": {"category": "Dimension"},
                    "allowedValues": ["UNKNOWN", "MALE", "FEMALE"],
                    "default": "UNKNOWN",
                }
            )
            to_modify["ttl"] = 3600000
            updated_resp = self.session.update_context(context_name, to_modify)
            self.assertIsNotNone(updated_resp)
            self.assertGreater(updated_resp["version"], fetched["version"])
            self.assertEqual(updated_resp["biosVersion"], fetched["biosVersion"])
            self.assertEqual(updated_resp["ttl"], 3600000)
            self.assertTrue(updated_resp["auditEnabled"])

            fetched_again = self.session.get_context(context_name)
            self.assertIsNotNone(fetched_again)
            self.assertEqual(fetched_again, updated_resp)

            # Delete
            self.session.delete_context(context_name)
            with self.assertRaises(ServiceError) as cm:
                self.session.get_context(context_name)
            error = cm.exception
            self.assertEqual(error.error_code, ErrorCode.NO_SUCH_STREAM)

        finally:
            try:
                self.session.delete_context(context_name)
            except ServiceError:
                pass

    def test_cud_internal_context(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                # create
                context = {
                    "contextName": "_test",
                    "missingAttributePolicy": "Reject",
                    "attributes": [
                        {"attributeName": "one", "type": "string"},
                        {"attributeName": "two", "type": "string"},
                    ],
                    "primaryKey": ["one"],
                }
                with self.assertRaises(ServiceError) as cm:
                    self.session.create_context(context)
                self.assertEqual(cm.exception.error_code, ErrorCode.BAD_INPUT)
                sadmin.set_property("prop.allowInternalStreamName", "true")
                self.session.create_context(context)

                # update
                context["attributes"].append(
                    {"attributeName": "three", "type": "string", "default": ""}
                )
                self.session.update_context("_test", context)
                sadmin.set_property("prop.allowInternalStreamName", "")
                context["attributes"].append(
                    {"attributeName": "four", "type": "string", "default": ""}
                )
                with self.assertRaises(ServiceError) as cm:
                    self.session.update_context("_test", context)
                self.assertEqual(cm.exception.error_code, ErrorCode.BAD_INPUT)

                # delete
                with self.assertRaises(ServiceError) as cm:
                    self.session.update_context("_test", context)
                self.assertEqual(cm.exception.error_code, ErrorCode.BAD_INPUT)

            finally:
                sadmin.set_property("prop.allowInternalStreamName", "true")
                try:
                    self.session.delete_context("_test")
                except ServiceError:
                    pass
                sadmin.set_property("prop.allowInternalStreamName", "")

    def test_create_context_with_features(self):
        try:
            self.session.create_context(read_file(CONTEXT_WITH_FEATURES))
        finally:
            try:
                self.session.delete_context("product")
            except ServiceError:
                pass

    def test_create_context_with_features_invalid(self):
        with self.assertRaises(ServiceError) as err_context:
            self.session.create_context(read_file(CONTEXT_WITH_FEATURES_INVALID))
        self.assertEqual(err_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_create_context_with_indexes(self):
        try:
            self.session.create_context(read_file(CONTEXT_WITH_INDEXES))
        finally:
            try:
                self.session.delete_context("product")
            except ServiceError:
                pass

    def test_create_context_with_indexes_invalid(self):
        with self.assertRaises(ServiceError) as err_context:
            self.session.create_context(read_file(CONTEXT_WITH_INDEXES_INVALID))
        self.assertEqual(err_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_create_context_with_same_config(self):
        with self.assertRaises(ServiceError) as err_context:
            self.session.create_context(SIMPLE_CONTEXT)
        self.assertEqual(err_context.exception.error_code, ErrorCode.STREAM_ALREADY_EXISTS)

    def test_rename_context(self):
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = SIMPLE_CONTEXT_NAME + "new"
        with self.assertRaises(ServiceError) as err_context:
            self.session.update_context(SIMPLE_CONTEXT_NAME, context)
        self.assertEqual(err_context.exception.error_code, ErrorCode.NOT_IMPLEMENTED)

    def test_create_without_context_name(self):
        """should fail for bad input"""
        context = copy.deepcopy(SIMPLE_CONTEXT)
        del context["contextName"]
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_context(context)
        assert error_context.exception.error_code == ErrorCode.BAD_INPUT

    def test_setting_internal_flag_should_be_ignored(self):
        context_name = "contextSettingInternalFlag"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        created = self.session.create_context(context)
        assert created.get("isInternal") is False
        retrieved = self.session.get_context(context_name)
        assert retrieved.get("isInternal") is False

    #
    # NOTE: Valid index configurations are tested in test_context_index.py
    #

    def test_making_index_without_dimensions(self):
        """Context index without dimensions should be rejected, nothing to index without it"""
        context_name = "indexWithoutDimensions"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        context["features"] = [
            {
                "featureName": "noDimensions",
                "dimensions": [],
                "attributes": ["the_value"],
                "indexed": True,
                "indexType": "ExactMatch",
                "featureInterval": 300000,
            }
        ]
        with pytest.raises(ServiceError) as excinfo:
            self.session.create_context(context)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert excinfo.value.message == (
            "Constraint violation: contextConfig.features[0]: "
            "Property 'dimensions' must be set when indexed is true; "
            "tenantName=biosContextCrudTest, contextName=indexWithoutDimensions,"
            " featureName=noDimensions"
        )

    def test_making_index_without_index_type(self):
        """Index type must be specified when index is eanbled"""
        context_name = "indexWithoutIndexType"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        context["features"] = [
            {
                "featureName": "noIndexType",
                "dimensions": ["the_key"],
                "attributes": ["the_value"],
                "indexed": True,
                "featureInterval": 300000,
            }
        ]
        with pytest.raises(ServiceError) as excinfo:
            self.session.create_context(context)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert excinfo.value.message == (
            "Constraint violation: contextConfig.features[0]: "
            "Property 'indexType' must be set when indexed is true;"
            " tenantName=biosContextCrudTest, contextName=indexWithoutIndexType,"
            " featureName=noIndexType"
        )

    def test_making_exact_match_index_to_primary_key(self):
        """Making exact match index to primary key should be rejected, it's meaningless"""
        context_name = "pkIndexNegative"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        context["features"] = [
            {
                "featureName": "pkExactMatchIndex",
                "dimensions": ["the_key"],
                "attributes": [],
                "indexed": True,
                "indexType": "ExactMatch",
                "featureInterval": 300000,
            }
        ]
        with pytest.raises(ServiceError) as excinfo:
            self.session.create_context(context)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert excinfo.value.message == (
            "Constraint violation: contextConfig.features[0]:"
            " ExactMatch index to primary key cannot be set; tenantName=biosContextCrudTest,"
            " contextName=pkIndexNegative, featureName=pkExactMatchIndex"
        )

    #
    # NOTE: Valid features are tested in test_context_feature.py
    #

    def test_making_unsupported_write_time_indexing(self):
        """Context index without dimensions should be rejected, nothing to index without it"""
        context_name = "unsupportedFeatures"
        context = copy.deepcopy(STORE_CONTEXT)
        context["contextName"] = context_name
        context["features"] = [
            {
                "featureName": "indexOnInsert",
                "dimensions": ["zipCode"],
                "attributes": ["storeId", "updatedTime"],
                "indexOnInsert": True,
            }
        ]
        with pytest.raises(ServiceError) as excinfo:
            self.session.create_context(context)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert (
            excinfo.value.message
            == "Constraint violation: Write time indexing is not supported for a context;"
            " tenant=biosContextCrudTest, context=unsupportedFeatures, feature=indexOnInsert"
        )

    def test_update_context_with_different_case(self):
        context_name = "contextMixedCase"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        self.session.create_context(context)

        # update context
        context["attributes"].append(
            {"attributeName": "the_thrid", "type": "Integer", "default": 0}
        )
        context["contextName"] = context_name.upper()
        self.session.update_context(context_name.lower(), context)
        updated = self.session.get_context(context_name)

        assert updated.get("contextName") == context.get("contextName")
        assert updated.get("attributes") == context.get("attributes")

    def test_empty_attributes(self):
        context_name = "contextNoAttributes"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        del context["attributes"]

        with pytest.raises(ServiceError) as exc_info:
            self.session.create_context(context)
        assert exc_info.value.error_code == ErrorCode.BAD_INPUT

    def test_create_context_with_single_attribute(self):
        single_attribute_context = copy.deepcopy(SIMPLE_CONTEXT)
        single_attribute_context["contextName"] = "singleAttributeContext"
        del single_attribute_context["attributes"][1]
        response = self.session.create_context(single_attribute_context)
        self.assertIsNotNone(response)
        self.assertGreater(response["version"], 0)
        self.session.delete_context(single_attribute_context["contextName"])

    def test_removing_added_default(self):
        try:
            context_name = "testContextRemovingAddedDefault"
            context = copy.deepcopy(SIMPLE_CONTEXT)
            context["contextName"] = context_name
            self.session.create_context(context)

            # update context
            context["attributes"].append(
                {"attributeName": "third", "type": "Integer", "default": -1}
            )
            self.session.update_context(context_name, context)

            # try to remove default value, should be rejected
            del context["attributes"][2]["default"]
            with pytest.raises(ServiceError) as exc_info:
                self.session.update_context(context_name, context)
            assert exc_info.value.error_code == ErrorCode.BAD_INPUT
            assert (
                "Constraint violation: Default value must be configured for an adding attribute;"
                in exc_info.value.message
            )
        finally:
            try:
                self.session.delete_context(context_name)
            except ServiceError:
                pass

    def test_changing_missing_attribute_policy(self):
        context_name = "testCtxChangingMissingAttributePolicy"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        context["attributes"].append(
            {
                "attributeName": "the_third",
                "type": "Integer",
                "missingAttributePolicy": "StoreDefaultValue",
                "default": -1,
            }
        )
        try:
            self.session.create_context(context)
            self.session.execute(bios.isql().upsert().into(context_name).csv("1,hello,").build())

            # change missing attribute policy
            del context["attributes"][2]["missingAttributePolicy"]
            del context["attributes"][2]["default"]
            self.session.update_context(context_name, context)

            with pytest.raises(ServiceError) as exc_info:
                self.session.execute(bios.isql().upsert().into(context_name).csv("2,hi,").build())
            assert exc_info.value.error_code == ErrorCode.BAD_INPUT

            records = self.session.execute(
                bios.isql().select().from_context(context_name).where(keys=[[1]]).build()
            ).to_dict()
            assert len(records) == 1
            assert records[0].get("the_third") == -1
        finally:
            try:
                self.session.delete_context(context_name)
            except ServiceError:
                pass


if __name__ == "__main__":
    pytest.main(sys.argv)
