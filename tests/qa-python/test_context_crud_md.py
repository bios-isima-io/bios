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
import pprint
import sys
from typing import List, Tuple

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import read_file, setup_tenant_config

CONTEXT_WITH_INDEXES = "../resources/bios-context-with-indexes.json"
CONTEXT_WITH_INDEXES_INVALID = "../resources/bios-context-with-indexes-invalid.json"
CONTEXT_WITH_FEATURES = "../resources/bios-context-with-features.json"
CONTEXT_WITH_FEATURES_INVALID = "../resources/bios-context-with-features-invalid.json"


ADMIN_USER = admin_user + "@" + TENANT_NAME

SIMPLE_CONTEXT_NAME = "simpleContext"
SIMPLE_CONTEXT = {
    "contextName": SIMPLE_CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "keyOne", "type": "Integer"},
        {"attributeName": "keyTwo", "type": "String"},
        {"attributeName": "the_value", "type": "String"},
    ],
    "primaryKey": ["keyOne", "keyTwo"],
}

PRIMARY_KEY_ONLY_CONTEXT = {
    "contextName": "storeContext",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {"attributeName": "area", "type": "String", "default": "NO AREA"},
        {
            "attributeName": "storeId",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
    ],
    "primaryKey": ["area", "storeId"],
    "ttl": 86400000,
}

SINGLE_STRING_KEY_CONTEXT = {
    "contextName": "singleStringKey",
    "missingAttributePolicy": "Reject",
    "attributes ": [
        {"attributeName": "theKey", "type": "String"},
        {
            "attributeName": "theValue",
            "type": "Decimal",
        },
    ],
}

SINGLE_INTEGER_KEY_CONTEXT = {
    "contextName": "singleIntegerKey",
    "missingAttributePolicy": "Reject",
    "attributes ": [
        {"attributeName": "theKey", "type": "Integer"},
        {
            "attributeName": "theValue",
            "type": "Decimal",
        },
    ],
}

CREATED_CONTEXTS = []


@pytest.fixture(scope="module")
def set_up_class():
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        setup_tenant_config(TENANT_NAME)


@pytest.fixture(autouse=True)
def session(set_up_class):
    admin = bios.login(ep_url(), ADMIN_USER, admin_pass)

    yield admin

    for context in CREATED_CONTEXTS:
        admin.delete_context(context)
    CREATED_CONTEXTS.clear()


def test_create_delete_fundamental(session):
    context_name = "ctxMultiPKeyFundamental"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    _test_fundamental(session, context_name, context)


def test_create_delete_default_primary_key_value(session):
    context_name = "ctxMultiPKeyFundamental"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["missingAttributePolicy"] = "StoreDefaultValue"
    context["attributes"][0]["default"] = -1
    context["attributes"][1]["default"] = "MISSING"
    context["attributes"][2]["default"] = "MISSING"
    _test_fundamental(session, context_name, context, default_key=[-1, "MISSING"])


def test_create_delete_reverse_attributes(session):
    context_name = "ctxMultiPKeyReverseAttrs"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["attributes"].reverse()
    pprint.pprint(context)

    _test_fundamental(session, context_name, context)


def test_create_delete_primary_key_with_different_cases(session):
    context_name = "ctxMultiPKeyAllCapital"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["attributes"].reverse()
    modified = copy.deepcopy(context)
    modified["primaryKey"] = [key.upper() for key in modified["primaryKey"]]
    pprint.pprint(modified)

    _test_fundamental(session, context_name, context, expected_context=context)


def test_mod_fundamental(session):
    context_name = "ctxMultiPKeyModFundamental"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    _test_modification(session, context_name, context)


def test_mod_reverse_attributes(session):
    context_name = "ctxMultiPKeyModReverseAttrs"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["attributes"].reverse()
    pprint.pprint(context)

    _test_modification(session, context_name, context)


def test_invalid_create_without_attributes(session):
    context_name = "ctxMultiPKeyEmptyPrimaryKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    del context["attributes"]
    del context["primaryKey"]

    with pytest.raises(ServiceError) as exc_info:
        session.create_context(context)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: Attributes are not set in the specified context"
    )


def test_invalid_create_with_no_primary_key(session):
    context_name = "ctxMultiPKeyEmptyPrimaryKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["primaryKey"] = []

    with pytest.raises(ServiceError) as exc_info:
        session.create_context(context)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: contextConfig.primaryKey: The values must be set;"
    )


def test_invalid_create_with_non_existing_primary_key(session):
    context_name = "ctxMultiPKeyNonExistingPrimaryKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["primaryKey"] = ["keyOne", "keyThree"]

    with pytest.raises(ServiceError) as exc_info:
        session.create_context(context)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: contextConfig.primaryKey[1]: Primary key must exist in attributes;"
    )


def test_invalid_create_with_primary_key_including_none(session):
    context_name = "ctxMultiPKeyNonExistingPrimaryKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["primaryKey"] = [None, "keyTwo"]

    with pytest.raises(ServiceError) as exc_info:
        session.create_context(context)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: contextConfig.primaryKey[0]: The value must be set;"
    )


def test_invalid_create_with_empty_primary_key_default(session):
    context_name = "ctxMultiPKeyEmptyStringDefault"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    context["missingAttributePolicy"] = "StoreDefaultValue"
    context["attributes"][0]["default"] = -1
    context["attributes"][1]["default"] = ""
    context["attributes"][2]["default"] = "MISSING"

    with pytest.raises(ServiceError) as exc_info:
        session.create_context(context)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: Default value of primary key may not be empty;"
    )


def test_invalid_context_update_decrease_primary_key(session):
    context_name = "ctxMultiDecreasePKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    try:
        session.delete_context(context_name)
    except ServiceError as err:
        if err.error_code != ErrorCode.NO_SUCH_STREAM:
            raise
    session.create_context(context)

    to_update = copy.deepcopy(context)
    to_update["primaryKey"] = to_update["primaryKey"][1:]

    with pytest.raises(ServiceError) as exc_info:
        session.update_context(context_name, to_update)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: Primary key attributes may not be modified;"
    )


def test_invalid_context_update_increase_primary_key(session):
    context_name = "ctxMultiIncreasePKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    try:
        session.delete_context(context_name)
    except ServiceError as err:
        if err.error_code != ErrorCode.NO_SUCH_STREAM:
            raise
    session.create_context(context)

    to_update = copy.deepcopy(context)
    to_update["primaryKey"] = to_update["primaryKey"][1:]

    with pytest.raises(ServiceError) as exc_info:
        session.update_context(context_name, to_update)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: Primary key attributes may not be modified;"
    )


def test_invalid_context_update_change_primary_key(session):
    context_name = "ctxMultiChangePKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    try:
        session.delete_context(context_name)
    except ServiceError as err:
        if err.error_code != ErrorCode.NO_SUCH_STREAM:
            raise
    session.create_context(context)

    to_update = copy.deepcopy(context)
    to_update["primaryKey"][1] = "the_value"

    with pytest.raises(ServiceError) as exc_info:
        session.update_context(context_name, to_update)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: Primary key attributes may not be modified;"
    )


def test_invalid_context_update_reverse_primary_key(session):
    context_name = "ctxMultiReversePKey"
    context = copy.deepcopy(SIMPLE_CONTEXT)
    context["contextName"] = context_name
    try:
        session.delete_context(context_name)
    except ServiceError as err:
        if err.error_code != ErrorCode.NO_SUCH_STREAM:
            raise
    session.create_context(context)

    to_update = copy.deepcopy(context)
    to_update["primaryKey"].reverse()

    with pytest.raises(ServiceError) as exc_info:
        session.update_context(context_name, to_update)
    assert exc_info.value.error_code == ErrorCode.BAD_INPUT
    assert exc_info.value.message.startswith(
        "Constraint violation: Primary key attributes may not be modified;"
    )


def test_primary_key_only_context(session):
    context = copy.deepcopy(PRIMARY_KEY_ONLY_CONTEXT)
    context_name = context.get("contextName")
    try:
        session.delete_context(context_name)
    except ServiceError as err:
        if err.error_code != ErrorCode.NO_SUCH_STREAM:
            raise
    session.create_context(context)

    upsert_statement = (
        bios.isql().upsert().into(context_name).csv_bulk(["area A,2", "area B,4"]).build()
    )
    session.execute(upsert_statement)

    select_statement = (
        bios.isql()
        .select()
        .from_context(context_name)
        .where(
            keys=[
                ["area A", 0],
                ["area A", 1],
                ["area A", 2],
                ["area A", 3],
                ["area A", 4],
                ["area A", 5],
                ["area B", 1],
                ["area B", 2],
                ["area B", 3],
                ["area B", 4],
                ["area B", 5],
            ]
        )
        .build()
    )
    response = session.execute(select_statement)
    records = response.to_dict()
    assert records == [{"area": "area A", "storeId": 2}, {"area": "area B", "storeId": 4}]


def _test_fundamental(
    session,
    context_name,
    context,
    default_key: Tuple[int, str] = None,
    expected_context: dict = None,
):
    session.create_context(context)

    retrieved = session.get_context(context_name)
    assert retrieved.get("version") is not None
    assert retrieved.get("biosVersion") is not None
    assert retrieved.get("auditEnabled") is True
    assert retrieved.get("isInternal") is False
    del retrieved["version"]
    del retrieved["biosVersion"]
    del retrieved["auditEnabled"]
    del retrieved["isInternal"]

    if expected_context is None:
        expected_context = context
    assert retrieved == expected_context

    # test simple insertion and extraction
    entries = [
        {"keyOne": 111, "keyTwo": "Apple", "the_value": "Fuji"},
        {"keyOne": 222, "keyTwo": "Orange", "the_value": "Kara Kara"},
    ]
    if default_key:
        entries.append({"keyOne": None, "keyTwo": "can't test yet", "the_value": "misc"})
    csv = _make_csv_bulk(context, entries)
    upsert_statement = bios.isql().upsert().into(context_name).csv_bulk(csv).build()
    session.execute(upsert_statement)

    keys = [[111, "Apple"], [222, "Orange"]]
    if default_key:
        keys.append([default_key[0], "can't test yet"])
    select_statement = bios.isql().select().from_context(context_name).where(keys=keys).build()
    response = session.execute(select_statement)
    print(response.get_records())
    records = response.get_records()
    if default_key:
        assert len(records) == 3
    else:
        assert len(records) == 2
    assert records[0].get("keyOne") == 111
    assert records[0].get("keyTwo") == "Apple"
    assert records[0].get("the_value") == "Fuji"
    assert records[1].get("keyOne") == 222
    assert records[1].get("keyTwo") == "Orange"
    assert records[1].get("the_value") == "Kara Kara"
    if default_key:
        assert records[2].get("keyOne") == default_key[0]
        assert records[2].get("keyTwo") == "can't test yet"
        assert records[2].get("the_value") == "misc"

    select_statement2 = (
        bios.isql()
        .select()
        .from_context(context_name)
        .where(keys=[[222, "Apple"], [111, "Orange"]])
        .build()
    )
    response2 = session.execute(select_statement2)
    print(response2.get_records())
    records2 = response2.get_records()
    assert len(records2) == 0

    # Delete the context
    session.delete_context(context_name)

    with pytest.raises(ServiceError) as exc_info:
        session.get_context(context_name)
    assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def _test_modification(session, context_name, context):
    # create the initial context
    session.create_context(context)

    # upsert a few before context mod
    entries = [
        {"keyOne": 111, "keyTwo": "Apple", "the_value": "Fuji"},
        {"keyOne": 222, "keyTwo": "Orange", "the_value": "Cara Cara"},
    ]
    csv = _make_csv_bulk(context, entries)
    upsert_statement = bios.isql().upsert().into(context_name).csv_bulk(csv).build()
    session.execute(upsert_statement)

    # add an attribute
    to_update = copy.deepcopy(context)
    to_update["attributes"].append({"attributeName": "price", "type": "Decimal", "default": 0.0})

    # update context
    session.update_context(context_name, to_update)
    retrieved = session.get_context(context_name)
    assert retrieved.get("version") is not None
    assert retrieved.get("biosVersion") is not None
    assert retrieved.get("auditEnabled") is True
    assert retrieved.get("isInternal") is False
    del retrieved["version"]
    del retrieved["biosVersion"]
    del retrieved["auditEnabled"]
    del retrieved["isInternal"]
    assert retrieved == to_update

    # upsert a few more after context mod
    entries = [
        {"keyOne": 333, "keyTwo": "Persimmon", "the_value": "Jiro", "price": 3.5},
        {"keyOne": 222, "keyTwo": "Orange", "the_value": "Navel", "price": 2.0},
    ]
    csv = _make_csv_bulk(retrieved, entries)
    upsert_statement = bios.isql().upsert().into(context_name).csv_bulk(csv).build()
    session.execute(upsert_statement)

    select_statement = (
        bios.isql()
        .select()
        .from_context(context_name)
        .where(keys=[[111, "Apple"], [222, "Orange"], [333, "Persimmon"]])
        .build()
    )
    response = session.execute(select_statement)
    print(response.get_records())
    records = response.get_records()
    assert len(records) == 3
    assert records[0].get("keyOne") == 111
    assert records[0].get("keyTwo") == "Apple"
    assert records[0].get("the_value") == "Fuji"
    assert records[0].get("price") == pytest.approx(0.0)
    assert records[1].get("keyOne") == 222
    assert records[1].get("keyTwo") == "Orange"
    assert records[1].get("the_value") == "Navel"
    assert records[1].get("price") == pytest.approx(2.0)
    assert records[2].get("keyOne") == 333
    assert records[2].get("keyTwo") == "Persimmon"
    assert records[2].get("the_value") == "Jiro"
    assert records[2].get("price") == pytest.approx(3.5)

    select_statement2 = (
        bios.isql()
        .select()
        .from_context(context_name)
        .where(keys=[[333, "Apple"], [111, "Orange"], [222, "Persimmon"]])
        .build()
    )
    response2 = session.execute(select_statement2)
    print(response2.get_records())
    records2 = response2.get_records()
    assert len(records2) == 0

    # Delete the context
    session.delete_context(context_name)

    with pytest.raises(ServiceError) as exc_info:
        session.get_context(context_name)
    assert exc_info.value.error_code == ErrorCode.NO_SUCH_STREAM


def _make_csv_bulk(context: dict, entries: List[dict]) -> List[str]:
    csv_bulk = []
    for entry in entries:
        values = []
        for attribute in context["attributes"]:
            value = entry.get(attribute["attributeName"])
            values.append(str(value) if value is not None else "")
        csv_bulk.append(",".join(values))
    return csv_bulk


# def test_create_context_with_features(self):
#     try:
#         self.session.create_context(read_file(CONTEXT_WITH_FEATURES))
#     finally:
#         try:
#             self.session.delete_context("product")
#         except ServiceError:
#             pass
#
# def test_create_context_with_features_invalid(self):
#     with self.assertRaises(ServiceError) as err_context:
#         self.session.create_context(read_file(CONTEXT_WITH_FEATURES_INVALID))
#     self.assertEqual(err_context.exception.error_code, ErrorCode.BAD_INPUT)
#
# def test_create_context_with_indexes(self):
#     try:
#         self.session.create_context(read_file(CONTEXT_WITH_INDEXES))
#     finally:
#         try:
#             self.session.delete_context("product")
#         except ServiceError:
#             pass
#
# def test_create_context_with_indexes_invalid(self):
#     with self.assertRaises(ServiceError) as err_context:
#         self.session.create_context(read_file(CONTEXT_WITH_INDEXES_INVALID))
#     self.assertEqual(err_context.exception.error_code, ErrorCode.BAD_INPUT)
#
# def test_create_context_with_same_config(self):
#     with self.assertRaises(ServiceError) as err_context:
#         self.session.create_context(SIMPLE_CONTEXT)
#     self.assertEqual(err_context.exception.error_code, ErrorCode.STREAM_ALREADY_EXISTS)
#
# def test_rename_context(self):
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     context["contextName"] = SIMPLE_CONTEXT_NAME + "new"
#     with self.assertRaises(ServiceError) as err_context:
#         self.session.update_context(SIMPLE_CONTEXT_NAME, context)
#     self.assertEqual(err_context.exception.error_code, ErrorCode.NOT_IMPLEMENTED)
#
# def test_create_without_context_name(self):
#     """should fail for bad input"""
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     del context["contextName"]
#     with self.assertRaises(ServiceError) as error_context:
#         self.session.create_context(context)
#     assert error_context.exception.error_code == ErrorCode.BAD_INPUT
#
# def test_setting_internal_flag_should_be_ignored(self):
#     context_name = "contextSettingInternalFlag"
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     context["contextName"] = context_name
#     created = self.session.create_context(context)
#     assert created.get("isInternal") is False
#     retrieved = self.session.get_context(context_name)
#     assert retrieved.get("isInternal") is False
#
# #
# # NOTE: Valid index configurations are tested in test_context_index.py
# #
#
# def test_making_index_without_dimensions(self):
#     """Context index without dimensions should be rejected, nothing to index without it"""
#     context_name = "indexWithoutDimensions"
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     context["contextName"] = context_name
#     context["features"] = [
#         {
#             "featureName": "noDimensions",
#             "dimensions": [],
#             "attributes": ["the_value"],
#             "indexed": True,
#             "indexType": "ExactMatch",
#             "featureInterval": 300000,
#         }
#     ]
#     with pytest.raises(ServiceError) as excinfo:
#         self.session.create_context(context)
#     assert excinfo.value.error_code == ErrorCode.BAD_INPUT
#     assert excinfo.value.message == (
#         "Constraint violation: contextConfig.features[0]: "
#         "Property 'dimensions' must be set when indexed is true; "
#         "tenantName=biosContextCrudTest, contextName=indexWithoutDimensions,"
#         " featureName=noDimensions"
#     )
#
# def test_making_index_without_index_type(self):
#     """Index type must be specified when index is eanbled"""
#     context_name = "indexWithoutIndexType"
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     context["contextName"] = context_name
#     context["features"] = [
#         {
#             "featureName": "noIndexType",
#             "dimensions": ["the_key"],
#             "attributes": ["the_value"],
#             "indexed": True,
#             "featureInterval": 300000,
#         }
#     ]
#     with pytest.raises(ServiceError) as excinfo:
#         self.session.create_context(context)
#     assert excinfo.value.error_code == ErrorCode.BAD_INPUT
#     assert excinfo.value.message == (
#         "Constraint violation: contextConfig.features[0]: "
#         "Property 'indexType' must be set when indexed is true;"
#         " tenantName=biosContextCrudTest, contextName=indexWithoutIndexType,"
#         " featureName=noIndexType"
#     )
#
# def test_making_exact_match_index_to_primary_key(self):
#     """Making exact match index to primary key should be rejected, it's meaningless"""
#     context_name = "pkIndexNegative"
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     context["contextName"] = context_name
#     context["features"] = [
#         {
#             "featureName": "pkExactMatchIndex",
#             "dimensions": ["the_key"],
#             "attributes": [],
#             "indexed": True,
#             "indexType": "ExactMatch",
#             "featureInterval": 300000,
#         }
#     ]
#     with pytest.raises(ServiceError) as excinfo:
#         self.session.create_context(context)
#     assert excinfo.value.error_code == ErrorCode.BAD_INPUT
#     assert excinfo.value.message == (
#         "Constraint violation: contextConfig.features[0]:"
#         " ExactMatch index to primary key cannot be set; tenantName=biosContextCrudTest,"
#         " contextName=pkIndexNegative, featureName=pkExactMatchIndex"
#     )
#
# #
# # NOTE: Valid features are tested in test_context_feature.py
# #
#
# def test_making_unsupported_write_time_indexing(self):
#     """Context index without dimensions should be rejected, nothing to index without it"""
#     context_name = "unsupportedFeatures"
#     context = copy.deepcopy(STORE_CONTEXT)
#     context["contextName"] = context_name
#     context["features"] = [
#         {
#             "featureName": "indexOnInsert",
#             "dimensions": ["zipCode"],
#             "attributes": ["storeId", "updatedTime"],
#             "indexOnInsert": True,
#         }
#     ]
#     with pytest.raises(ServiceError) as excinfo:
#         self.session.create_context(context)
#     assert excinfo.value.error_code == ErrorCode.BAD_INPUT
#     assert (
#         excinfo.value.message
#         == "Constraint violation: Write time indexing is not supported for a context;"
#         " tenant=biosContextCrudTest, context=unsupportedFeatures, feature=indexOnInsert"
#     )
#
# def test_update_context_with_different_case(self):
#     context_name = "contextMixedCase"
#     context = copy.deepcopy(SIMPLE_CONTEXT)
#     context["contextName"] = context_name
#     self.session.create_context(context)
#
#     # update context
#     context["attributes"].append(
#         {"attributeName": "the_thrid", "type": "Integer", "default": 0}
#     )
#     context["contextName"] = context_name.upper()
#     self.session.update_context(context_name.lower(), context)
#     updated = self.session.get_context(context_name)
#
#     assert updated.get("contextName") == context.get("contextName")
#     assert updated.get("attributes") == context.get("attributes")
#
#
if __name__ == "__main__":
    pytest.main(sys.argv)
