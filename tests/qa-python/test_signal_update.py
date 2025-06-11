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

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user
from tutils import b64enc

from setup_common import setup_context, setup_signal

TEST_TENANT_NAME = "biosSignalCrudTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

# ported from TFOS blob_support_admin.py
BLOB1 = bytearray.fromhex("feedfacecafebeef")
BLOB2 = bytearray.fromhex("31a571cf1a54")
BLOB_CONTEXT_1 = {
    "contextName": "blob_context_1",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "blobAttribute1",
            "type": "blob",
            "default": b64enc(BLOB1),
        },
        {"attributeName": "intAttribute1", "type": "integer", "default": 123},
        {
            "attributeName": "blobAttribute_extra",
            "type": "blob",
            "default": b64enc(BLOB2),
        },
    ],
    "primaryKey": ["blobAttribute1"],
}

CONTEXT_FOR_ENRICHMENT = {
    "contextName": "ctx_update_context",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "intAttribute", "type": "Integer"},
        {"attributeName": "intAttribute1", "type": "Integer"},
        {"attributeName": "stringAttribute", "type": "String"},
        {"attributeName": "doubleAttribute", "type": "decimal"},
        {"attributeName": "doubleAttribute1", "type": "decimal"},
        {"attributeName": "booleanAttribute", "type": "boolean"},
        {
            "attributeName": "enumAttribute",
            "type": "string",
            "allowedValues": [
                "enum_const_1",
                "enum_const_2",
                "enum_const_3",
                "enum_const_4",
            ],
        },
        {"attributeName": "blobAttribute", "type": "blob"},
    ],
    "primaryKey": ["intAttribute"],
}

SIMPLE_SIGNAL1_TEMPLATE = {
    "signalName": "dummy",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "stringAttribute", "type": "String"},
        {"attributeName": "int", "type": "Integer"},
        {"attributeName": "boolean", "type": "Boolean"},
        {"attributeName": "double", "type": "Decimal"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "view_24",
                "dimensions": ["stringAttribute", "boolean"],
                "attributes": ["int", "double"],
                "featureInterval": 300000,
            }
        ]
    },
}

ENRICHED_SIGNAL1_TEMPLATE = {
    "signalName": "dummy",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "stringAttribute", "type": "String"},
        {"attributeName": "int", "type": "Integer"},
        {"attributeName": "boolean", "type": "Boolean"},
        {"attributeName": "double", "type": "Decimal"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "ctx_update_pp",
                "missingLookupPolicy": "Reject",
                "foreignKey": ["int"],
                "contextName": "ctx_update_context",
                "contextAttributes": [
                    {"attributeName": "intAttribute1"},
                    {"attributeName": "doubleAttribute"},
                ],
            }
        ]
    },
    "postStorageStage": {
        "features": [
            {
                "featureName": "view_25",
                "dimensions": ["stringAttribute", "boolean"],
                "attributes": ["int", "doubleAttribute"],
                "featureInterval": 300000,
            }
        ]
    },
}


class BiosSignalCrudTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            setup_context(admin, CONTEXT_FOR_ENRICHMENT)

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

    def insert_into_signal(self, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = self.session.execute(request)
        return resp.records[0].timestamp

    # ported from test_rollup_admin.py in TFOS tests
    def test_add_multiple_features_with_multiple_dimensions(self):
        """Test to add a signal that has multiple features with multiple dimensions"""
        signal = {
            "signalName": "alltypes_rollup1",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "intAttribute", "type": "Integer"},
                {"attributeName": "intAttribute1", "type": "Integer"},
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "doubleAttribute", "type": "Decimal"},
                {"attributeName": "doubleAttribute1", "type": "Decimal"},
                {"attributeName": "booleanAttribute", "type": "Boolean"},
                {
                    "attributeName": "enumAttribute",
                    "type": "String",
                    "allowedValues": [
                        "enum_const_1",
                        "enum_const_2",
                        "enum_const_3",
                        "enum_const_4",
                    ],
                },
                {"attributeName": "blobAttribute", "type": "Blob"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "groupByStringInt",
                        "dimensions": [
                            "stringAttribute",
                            "intAttribute",
                        ],
                        "attributes": [
                            "intAttribute1",
                            "doubleAttribute1",
                        ],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEnumDouble",
                        "dimensions": ["enumAttribute", "doubleAttribute"],
                        "attributes": [
                            "intAttribute1",
                            "doubleAttribute1",
                        ],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByBooleanString",
                        "dimensions": ["booleanAttribute", "stringAttribute"],
                        "attributes": [
                            "intAttribute1",
                            "doubleAttribute1",
                        ],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByBlobInt",
                        "dimensions": ["blobAttribute", "intAttribute"],
                        "attributes": [
                            "intAttribute1",
                            "doubleAttribute1",
                        ],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByString",
                        "dimensions": ["stringAttribute"],
                        "attributes": [],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEmpty",
                        "dimensions": [],
                        "attributes": [],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEmptyInt",
                        "dimensions": [],
                        "attributes": ["intAttribute1"],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEmptyIntDouble",
                        "dimensions": [],
                        "attributes": [
                            "intAttribute1",
                            "doubleAttribute1",
                        ],
                        "featureInterval": 300000,
                    },
                ],
            },
        }
        setup_signal(self.session, signal)

    def test_update_feature(self):
        """verify updating a feature signal"""
        signal1 = {
            "signalName": "postprocess_stream",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "string"},
                {"attributeName": "int", "type": "integer"},
                {"attributeName": "number", "type": "integer"},
                {"attributeName": "double", "type": "decimal"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "EmptyAttributeViewGroupIntNum",
                        "dimensions": ["int", "number"],
                        "attributes": [],
                        "featureInterval": 300000,
                    }
                ]
            },
        }

        signal2 = {
            "signalName": "postprocess_stream",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "string"},
                {"attributeName": "int", "type": "integer"},
                {"attributeName": "number", "type": "integer"},
                {"attributeName": "double", "type": "decimal"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "groupByIntAttributesNumDouble",
                        "dimensions": ["int"],
                        "attributes": ["number", "double"],
                        "featureInterval": 300000,
                    }
                ]
            },
        }
        self.session.create_signal(signal1)
        self.session.update_signal("postprocess_stream", signal2)
        retrieved = self.session.get_signal("postprocess_stream")

        assert retrieved.get("postStorageStage").get("features")[0].get("dimensions") == ["int"]

    def test_removing_enrichment_and_feature(self):
        signal1 = {
            "signalName": "signal_27",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "boolean", "type": "boolean"},
                {"attributeName": "decimal", "type": "decimal"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "missingLookupPolicy": "Reject",
                        "enrichmentName": "ctx_update_pp",
                        "foreignKey": ["int"],
                        "contextName": "ctx_update_context",
                        "contextAttributes": [
                            {
                                "attributeName": "intAttribute1",
                            },
                            {
                                "attributeName": "doubleAttribute",
                            },
                        ],
                    }
                ]
            },
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "view_27",
                        "dimensions": ["stringAttribute", "boolean"],
                        "attributes": ["int", "doubleAttribute"],
                        "featureInterval": 300000,
                    }
                ]
            },
        }

        signal2 = {
            "signalName": "signal_27",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "boolean", "type": "boolean"},
                {"attributeName": "decimal", "type": "decimal"},
            ],
        }
        setup_signal(self.session, signal1)
        self.session.update_signal("signal_27", signal2)

        retrieved = self.session.get_signal("signal_27")

        assert not retrieved.get("enrich")
        assert not retrieved.get("postStorageStage")

    def test_update_signal_with_feature_name_conflict(self):
        signal1 = {
            "signalName": "signal_27",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "boolean", "type": "boolean"},
                {"attributeName": "decimal", "type": "decimal"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "missingLookupPolicy": "Reject",
                        "enrichmentName": "ctx_update_context",
                        "foreignKey": ["int"],
                        "contextName": "ctx_update_context",
                        "contextAttributes": [
                            {
                                "attributeName": "intAttribute1",
                            },
                            {
                                "attributeName": "doubleAttribute",
                            },
                        ],
                    }
                ]
            },
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "view_27",
                        "dimensions": ["stringAttribute", "boolean"],
                        "attributes": ["int", "doubleAttribute"],
                        "featureInterval": 300000,
                    }
                ]
            },
        }

        setup_signal(self.session, signal1)

        # Duplicate View should throw exception
        signal2 = copy.deepcopy(signal1)
        signal2["postStorageStage"]["features"].append(
            {
                "featureName": "view_27",
                "dimensions": ["stringAttribute", "decimal"],
                "attributes": ["int"],
                "featureInterval": 300000,
            }
        )
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_signal(signal2.get("signalName"), signal2)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        err_message = (
            "Constraint violation: signalConfig.postStorageStage.features[1].featureName:"
            " The name conflicts with another feature entry; tenantName=biosSignalCrudTest,"
            " signalName=signal_27, featureName=view_27"
        )
        assert error.message == err_message

    def test_update_features_negative_misc(self):
        """TCS-ID : ap_roll_up_32, 33, 34, 36, & 37"""
        signal1 = {
            "signalName": "signal_33",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "number", "type": "Integer"},
                {"attributeName": "double", "type": "Decimal"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "ctx_update_pp",
                        "missingLookupPolicy": "Reject",
                        "foreignKey": ["int"],
                        "contextName": "ctx_update_context",
                        "contextAttributes": [
                            {"attributeName": "intAttribute1"},
                            {"attributeName": "doubleAttribute"},
                        ],
                    }
                ]
            },
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "view_33",
                        "dimensions": ["stringAttribute", "number"],
                        "attributes": ["int", "doubleAttribute"],
                        "featureInterval": 300000,
                    }
                ]
            },
        }
        setup_signal(self.session, signal1)

        # Non-existing attributes in dimensions should cause an exception
        signal2 = copy.deepcopy(signal1)
        signal2.get("postStorageStage").get("features")[0].get("dimensions").append(
            "doubleAttribute4"
        )
        expected_message = (
            "Constraint violation: signalConfig.postStorageStage.features[0].dimensions[2]:"
            " 'doubleAttribute4' in feature dimensions not found in the stream attribute names;"
            " tenantName=biosSignalCrudTest, signalName=signal_33, featureName=view_33"
        )
        self.update_and_expect_constraint_violation(signal2, expected_message)

        # Non-Existing aggregation attributes should cause an exception
        signal3 = copy.deepcopy(signal1)
        signal3.get("postStorageStage").get("features")[0].get("attributes").append(
            "doubleAttribute4"
        )
        expected_message = (
            "Constraint violation: signalConfig.postStorageStage.features[0].attributes[2]:"
            " 'doubleAttribute4' in feature attributes not found in the stream attribute names;"
            " tenantName=biosSignalCrudTest, signalName=signal_33, featureName=view_33"
        )
        self.update_and_expect_constraint_violation(signal3, expected_message)

        # Duplicate attributes in dimension should cause an exception
        signal4 = copy.deepcopy(signal1)
        signal4.get("postStorageStage").get("features")[0].get("dimensions").append("number")
        expected_message = (
            "Constraint violation: Feature properties 'dimensions' and 'attributes' "
            "may not have duplicates; tenant=biosSignalCrudTest, signal=signal_33, "
            "feature=view_33, attribute=number"
        )
        self.update_and_expect_constraint_violation(signal4, expected_message)

        # Duplicate aggregation attributes should cause an exception
        signal5 = copy.deepcopy(signal1)
        signal5.get("postStorageStage").get("features")[0].get("attributes").append("int")
        expected_message = (
            "Constraint violation: Feature properties 'dimensions' and 'attributes' "
            "may not have duplicates; tenant=biosSignalCrudTest, signal=signal_33,"
            " feature=view_33, attribute=int"
        )
        self.update_and_expect_constraint_violation(signal5, expected_message)

        # Same Attributes in dimensions and attributes should cause an exception
        signal6 = copy.deepcopy(signal1)
        signal6.get("postStorageStage").get("features")[0].get("dimensions").append("number")
        expected_message = (
            "Constraint violation: Feature properties 'dimensions' and 'attributes' "
            "may not have duplicates; tenant=biosSignalCrudTest, signal=signal_33, "
            "feature=view_33, attribute=number"
        )
        self.update_and_expect_constraint_violation(signal6, expected_message)

    def test_update_features_with_empty_or_null_fields(self):
        """TCS-ID : ap_roll_up_31
        Updating rollup config with any mandatory element is missing/empty/none
        should throw exception.
        """
        signal1 = {
            "signalName": "signal_31",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "number", "type": "Integer"},
                {"attributeName": "double", "type": "Decimal"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "view_31",
                        "dimensions": ["stringAttribute", "number"],
                        "attributes": ["int", "double"],
                        "featureInterval": 300000,
                    }
                ]
            },
        }
        setup_signal(self.session, signal1)

        #  missing feature name should cause an exception.
        signal2 = copy.deepcopy(signal1)
        del signal2.get("postStorageStage").get("features")[0]["featureName"]
        self.update_and_expect_constraint_violation(signal2)

        #   missing aggregate element should throw exception.
        signal4 = copy.deepcopy(signal1)
        del signal4.get("postStorageStage").get("features")[0]["attributes"]
        self.update_and_expect_constraint_violation(signal4)

        # missing feature interval should throw exception
        signal5 = copy.deepcopy(signal1)
        del signal5.get("postStorageStage").get("features")[0]["featureInterval"]
        self.update_and_expect_constraint_violation(signal5)

        # A blank attribute name in dimensions should cause an exception.
        signal6 = copy.deepcopy(signal1)
        signal6.get("postStorageStage").get("features")[0]["dimensions"].append("")
        expected_message = (
            "Constraint violation: signalConfig.postStorageStage.features[0].dimensions[2]:"
            " Entry in feature dimensions may not be null or blank;"
            " tenantName=biosSignalCrudTest, signalName=signal_31, featureName=view_31"
        )
        self.update_and_expect_constraint_violation(signal6, expected_message)

        # A blank attribute name in feature attributes should cause an exception.
        signal7 = copy.deepcopy(signal1)
        signal7.get("postStorageStage").get("features")[0]["attributes"].append("")
        expected_message = (
            "Constraint violation: signalConfig.postStorageStage.features[0].attributes[2]:"
            " Entry in feature attributes may not be null or blank;"
            " tenantName=biosSignalCrudTest, signalName=signal_31, featureName=view_31"
        )
        self.update_and_expect_constraint_violation(signal7, expected_message)

        # null feature name should be rejected
        signal8 = copy.deepcopy(signal1)
        signal8.get("postStorageStage").get("features")[0]["featureName"] = None
        self.update_and_expect_constraint_violation(signal8)

        # null in dimensions should be rejected
        signal9 = copy.deepcopy(signal1)
        signal9.get("postStorageStage").get("features")[0]["dimensions"].insert(1, None)
        expected_message = (
            "Constraint violation: signalConfig.postStorageStage.features[0].dimensions[1]:"
            " Entry in feature dimensions may not be null or blank;"
            " tenantName=biosSignalCrudTest, signalName=signal_31, featureName=view_31"
        )
        self.update_and_expect_constraint_violation(signal9, expected_message)

        # null attributes should be rejected
        signal10 = copy.deepcopy(signal1)
        signal10.get("postStorageStage").get("features")[0]["attributes"] = None
        self.update_and_expect_constraint_violation(signal10)

        # null in attributes should be rejected
        signal11 = copy.deepcopy(signal1)
        signal11.get("postStorageStage").get("features")[0]["attributes"] = ["int", "double", None]
        self.update_and_expect_constraint_violation(signal11)

    def test_adding_feature(self):
        """TCS-ID : ap_roll_up_22
        Updating signal stream ( no pre-process, no post-process),
        with rollup config should succeed.
        """
        signal1 = {
            "signalName": "signal_22",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "boolean", "type": "Boolean"},
                {"attributeName": "double", "type": "Decimal"},
            ],
        }

        setup_signal(self.session, signal1)

        signal2_features = {
            "features": [
                {
                    "featureName": "EmptyAttributeView_22",
                    "dimensions": ["int", "boolean"],
                    "attributes": [],
                    "featureInterval": 300000,
                }
            ]
        }
        signal2 = copy.deepcopy(signal1)
        signal2["postStorageStage"] = signal2_features
        self.session.update_signal(signal2.get("signalName"), signal2)
        updated = self.session.get_signal(signal2.get("signalName"))
        assert updated.get("postStorageStage") == signal2_features

        signal3_features = {
            "features": [
                {
                    "featureName": "stringValueInFeatureAttr",
                    "dimensions": ["int", "boolean"],
                    "attributes": ["stringAttribute"],
                    "featureInterval": 300000,
                }
            ]
        }
        signal3 = copy.deepcopy(signal1)
        signal3["postStorageStage"] = signal3_features
        expected_message = (
            "Constraint violation: Type of a feature attribute is not supported by the"
            " data sketch; tenant=biosSignalCrudTest, signal=signal_22,"
            " feature=stringValueInFeatureAttr, attribute=stringAttribute, attributeType=String,"
            " dataSketch=Moments"
        )
        created = self.session.update_signal(signal3.get("signalName"), signal3)
        assert created.get("postStorageStage") == signal3.get("postStorageStage")

    def test_add_feature_with_enriched_attributes(self):
        """TCS-ID : ap_roll_up_23
        Updating signal stream ( pre-process, no post-process),
        with rollup config should succeed.
        """
        signal1 = {
            "signalName": "signal_23",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "boolean", "type": "Boolean"},
                {"attributeName": "double", "type": "Decimal"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "ctx_update_pp",
                        "missingLookupPolicy": "Reject",
                        "foreignKey": ["int"],
                        "contextName": "ctx_update_context",
                        "contextAttributes": [
                            {"attributeName": "intAttribute1"},
                            {"attributeName": "doubleAttribute"},
                        ],
                    }
                ]
            },
        }

        signal2_features = {
            "features": [
                {
                    "featureName": "view_23",
                    "dimensions": ["stringAttribute", "boolean"],
                    "attributes": ["int", "doubleAttribute"],
                    "featureInterval": 300000,
                }
            ]
        }

        setup_signal(self.session, signal1)

        signal2 = copy.deepcopy(signal1)
        signal2["postStorageStage"] = signal2_features
        self.session.update_signal(signal2.get("signalName"), signal2)

        updated = self.session.get_signal(signal2.get("signalName"))
        assert updated.get("postStorageStage") == signal2_features

    def test_changing_feature_interval(self):
        signal_name = "signal_24"
        signal1 = copy.deepcopy(SIMPLE_SIGNAL1_TEMPLATE)
        signal1["signalName"] = signal_name
        setup_signal(self.session, signal1)

        signal2 = copy.deepcopy(signal1)
        signal2["postStorageStage"]["features"][0]["featureInterval"] = 900000
        self.session.update_signal(signal2.get("signalName"), signal2)

        updated = self.session.get_signal(signal2.get("signalName"))

        assert updated.get("postStorageStage") == signal2.get("postStorageStage")

    def test_truncating_feature_attributes(self):
        signal_name = "signal_25"
        signal1 = copy.deepcopy(ENRICHED_SIGNAL1_TEMPLATE)
        signal1["signalName"] = signal_name

        setup_signal(self.session, signal1)

        signal2 = copy.deepcopy(signal1)
        signal2["postStorageStage"]["features"][0]["attributes"] = []
        self.session.update_signal(signal2.get("signalName"), signal2)

        updated = self.session.get_signal(signal2.get("signalName"))

        assert updated.get("postStorageStage") == signal2["postStorageStage"]

    def test_removing_feature(self):
        signal_name = "signal_26"
        signal1 = copy.deepcopy(SIMPLE_SIGNAL1_TEMPLATE)
        signal1["signalName"] = signal_name

        setup_signal(self.session, signal1)

        signal2 = copy.deepcopy(signal1)
        del signal2["postStorageStage"]
        self.session.update_signal(signal_name, signal2)

        updated = self.session.get_signal(signal_name)

        assert updated.get("postStorageStage") is None

    def test_trying_to_change_to_identical_signal_with_features(self):
        signal_name = "signal_29"
        signal1 = copy.deepcopy(SIMPLE_SIGNAL1_TEMPLATE)
        signal1["signalName"] = signal_name

        setup_signal(self.session, signal1)

        with pytest.raises(ServiceError) as excinfo:
            self.session.update_signal(signal_name, signal1)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        expected_message = (
            "Requested config for change is identical to existing:"
            " tenant=biosSignalCrudTest stream=signal_29"
        )
        assert error.message == expected_message

    def test_adding_multiple_features(self):
        signal_name = "basic_stream"
        signal1 = {
            "signalName": "basic_stream",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "int", "type": "Integer"},
                {"attributeName": "boolean", "type": "Boolean"},
                {"attributeName": "double", "type": "Decimal"},
            ],
        }
        setup_signal(self.session, signal1)

        signal2 = {
            "signalName": "basic_stream",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "intAttribute", "type": "Integer", "default": 0},
                {"attributeName": "intAttribute1", "type": "Integer", "default": 0},
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "doubleAttribute", "type": "Decimal", "default": 0.0},
                {"attributeName": "doubleAttribute1", "type": "Decimal", "default": 0.0},
                {"attributeName": "booleanAttribute", "type": "Boolean", "default": False},
                {
                    "attributeName": "enumAttribute",
                    "type": "String",
                    "allowedValues": [
                        "enum_const_1",
                        "enum_const_2",
                        "enum_const_3",
                        "enum_const_4",
                    ],
                    "default": "enum_const_1",
                },
                {"attributeName": "blobAttribute", "type": "Blob", "default": ""},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "groupByStringInt",
                        "dimensions": [
                            "stringAttribute",
                            "intAttribute",
                        ],
                        "attributes": ["intAttribute1", "doubleAttribute1"],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEnumDoubleBoolean",
                        "dimensions": ["enumAttribute", "doubleAttribute", "booleanAttribute"],
                        "attributes": ["intAttribute1", "doubleAttribute1"],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByBoolean",
                        "dimensions": ["booleanAttribute"],
                        "attributes": ["intAttribute1", "doubleAttribute1"],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByBlob",
                        "dimensions": ["blobAttribute"],
                        "attributes": ["intAttribute1", "doubleAttribute1"],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByString",
                        "dimensions": ["stringAttribute"],
                        "attributes": [],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEmpty",
                        "dimensions": [],
                        "attributes": [],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEmptyWithIntAttr",
                        "dimensions": [],
                        "attributes": ["intAttribute1"],
                        "featureInterval": 300000,
                    },
                    {
                        "featureName": "groupByEmptyWithIntDoubleAttr",
                        "dimensions": [],
                        "attributes": ["intAttribute1", "doubleAttribute1"],
                        "featureInterval": 300000,
                    },
                ]
            },
        }
        self.session.update_signal(signal_name, signal2)

    def test_blob_foreign_key_using_empty_default_value(self):
        """Configuring condition blob signal with empty default should be rejected."""
        setup_context(self.session, BLOB_CONTEXT_1)
        signal_config = {
            "signalName": "blob_signal_29",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "blobAttribute1",
                    "type": "blob",
                    "missingAttributePolicy": "StoreDefaultValue",
                    "default": "",
                },
                {"attributeName": "blobAttribute2", "type": "blob"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "pp1",
                        "foreignKey": ["blobAttribute1"],
                        "missingLookupPolicy": "Reject",
                        "contextName": "blob_context_1",
                        "contextAttributes": [{"attributeName": "blobAttribute_extra"}],
                    }
                ]
            },
        }

        with pytest.raises(ServiceError) as excinfo:
            self.session.create_signal(signal_config)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        expected_message = (
            "Constraint violation: Default value of blob type foreignKey[0] may not be empty;"
            " tenant=biosSignalCrudTest, signal=blob_signal_29, preprocess=pp1,"
            " attribute=blobAttribute1, context=blob_context_1"
        )
        assert error.message == expected_message

    def test_select_after_signal_update(self):
        signal = {
            "signalName": "studentSchemaChange",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "name", "type": "String"},
                {"attributeName": "class", "type": "String"},
                {"attributeName": "roll_no", "type": "Integer"},
                {"attributeName": "score", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "student_view_by_class",
                        "dimensions": ["class"],
                        "attributes": [],
                        "featureInterval": 300000,
                    }
                ]
            },
        }
        self.session.create_signal(signal)

        start = self.insert_into_signal(
            "studentSchemaChange",
            ["John,STD-I,1,35", "Steve,STD-I,2,45", "Willey,STD-I,3,35", "Nick,STD-I,4,65"],
        )

        signal2 = copy.deepcopy(signal)
        signal2["postStorageStage"]["features"][0]["attributes"].append("score")
        self.session.update_signal("studentSchemaChange", signal2)

        self.insert_into_signal(
            "studentSchemaChange",
            ["Riken,STD-I,5,35", "Siken,STD-I,6,45", "Mike,STD-II,1,35", "Dinda,STD-II,2,65"],
        )

        signal3 = copy.deepcopy(signal2)
        signal3["attributes"].append(
            {"attributeName": "subject", "type": "String", "default": "Math"}
        )
        self.session.update_signal("studentSchemaChange", signal3)

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal("studentSchemaChange", ["Chris,STD-II,2,65"])
        self.assertEqual(error_context.exception.error_code, ErrorCode.SCHEMA_MISMATCHED)

        select_statement = (
            bios.isql()
            .select()
            .from_signal("studentSchemaChange")
            .time_range(start, bios.time.now() - start)
            .build()
        )

        resp = self.session.execute(select_statement)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        records = resp.get_data_windows()[0].records
        self.assertEqual(len(records), 8)
        for record in records:
            self.assertEqual(record.get("subject"), "Math")

    def update_and_expect_constraint_violation(self, signal, expected_message=None):
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_signal(signal.get("signalName"), signal)
        error = excinfo.value
        assert error.error_code == ErrorCode.BAD_INPUT
        if expected_message:
            assert error.message == expected_message


if __name__ == "__main__":
    pytest.main(sys.argv)
