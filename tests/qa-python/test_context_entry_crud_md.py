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

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios.models import AttributeType, ContextRecords
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
STORE_CONTEXT = {
    "contextName": "storeContextMd",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "storeType",
            "type": "String",
            "missingAttributePolicy": "Reject",
        },
        {
            "attributeName": "storeId",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "zipCode", "type": "Integer", "default": -1},
        {"attributeName": "address", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["storeType", "storeId"],
}

CONTEXT_WITH_STRING_KEY = {
    "contextName": "stringKeysContext",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "objectId", "type": "Integer"},
        {"attributeName": "category", "type": "String", "allowedValues": ["catA", "catB", "catC"]},
        {"attributeName": "subCategory", "type": "String"},
        {"attributeName": "projectCode", "type": "Integer"},
        {"attributeName": "title", "type": "String"},
    ],
    "primaryKey": ["category", "subCategory", "objectId"],
}

CONTEXT_STRING_AND_INT = {
    "contextName": "intIntAndString",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key_1", "type": "string"},
        {"attributeName": "the_key_2", "type": "integer"},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key_1", "the_key_2"],
}

CONTEXT_PRIMARY_KEYS = {
    "contextName": "primaryKeysTest",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key_1", "type": "integer"},
        {"attributeName": "the_key_2", "type": "string", "allowedValues": ["ONE", "TWO", "THREE"]},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key_1", "the_key_2"],
}


@pytest.fixture(scope="module")
def session():
    setup_tenant_config()

    session = bios.login(ep_url(), ADMIN_USER, admin_pass)
    setup_context(session, STORE_CONTEXT)
    setup_context(session, CONTEXT_WITH_STRING_KEY)
    setup_context(session, CONTEXT_STRING_AND_INT)
    setup_context(session, CONTEXT_PRIMARY_KEYS)

    yield session

    session.close()


def test_bios_context_crud(session):
    upsert_statement = (
        bios.isql()
        .upsert()
        .into(STORE_CONTEXT["contextName"])
        .csv_bulk(["typeA,0,10,rcb", "typeB,1,11,vk"])
        .build()
    )
    session.execute(upsert_statement)

    select_request = (
        bios.isql()
        .select()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA", 0], ["typeB", 1]])
        .build()
    )

    reply = session.execute(select_request)

    records = reply.get_records()
    assert len(records) == 2
    record0 = records[0]
    assert record0.get_primary_key() == ["typeA", 0]
    assert record0.get("storeType") == "typeA"
    assert record0.get("storeId") == 0
    assert record0.get("zipCode") == 10
    assert record0.get("address") == "rcb"

    record1 = records[1]
    assert record1.get_primary_key() == ["typeB", 1]
    assert record1.get("storeType") == "typeB"
    assert record1.get("storeId") == 1
    assert record1.get("zipCode") == 11
    assert record1.get("address") == "vk"

    assert record0.get_type("storeType") == AttributeType.STRING
    assert record0.get_type("storeId") == AttributeType.INTEGER
    assert record0.get_type("zipCode") == AttributeType.INTEGER
    assert record0.get_type("address") == AttributeType.STRING

    # Test invalid update statements:
    # empty key
    invalid_update_statement = (
        bios.isql()
        .update(STORE_CONTEXT["contextName"])
        .set({"zipCode": 14, "address": "CSK"})
        .where(key=[])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(invalid_update_statement)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    # Test invalid update statements:
    # too few key elements
    invalid_update_statement = (
        bios.isql()
        .update(STORE_CONTEXT["contextName"])
        .set({"zipCode": 14, "address": "CSK"})
        .where(key=["typeA"])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(invalid_update_statement)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    # too many key elements
    invalid_update_statement = (
        bios.isql()
        .update(STORE_CONTEXT["contextName"])
        .set({"zipCode": 14, "address": "CSK"})
        .where(key=["typeA", 0, 1])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(invalid_update_statement)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    # including none
    invalid_update_statement = (
        bios.isql()
        .update(STORE_CONTEXT["contextName"])
        .set({"zipCode": 14, "address": "CSK"})
        .where(key=["typeA", None])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(invalid_update_statement)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    # including empty string
    invalid_update_statement = (
        bios.isql()
        .update(STORE_CONTEXT["contextName"])
        .set({"zipCode": 14, "address": "CSK"})
        .where(key=["", 0])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(invalid_update_statement)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    update_statement = (
        bios.isql()
        .update(STORE_CONTEXT["contextName"])
        .set({"zipCode": 14, "address": "CSK"})
        .where(key=["typeA", 0])
        .build()
    )
    session.execute(update_statement)

    select_statement = (
        bios.isql()
        .select()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA", 0], ["typeB", 1]])
        .build()
    )
    reply = session.execute(select_statement)

    records = reply.get_records()
    assert len(records) == 2
    record0 = records[0]
    assert record0.get_primary_key() == ["typeA", 0]
    assert record0.get("storeId") == 0
    assert record0.get("zipCode") == 14
    assert record0.get("address") == "CSK"
    record1 = records[1]
    assert record1.get_primary_key() == ["typeB", 1]
    assert record1.get("storeId") == 1
    assert record1.get("zipCode") == 11
    assert record1.get("address") == "vk"

    # deleting with empty keys
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeB", 1], []])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(request)
    error = excinfo.value
    assert error.error_code == ErrorCode.BAD_INPUT
    # deletion shouldn't have happened
    reply = session.execute(select_statement)
    records = reply.get_records()
    assert len(records) == 2

    # deleting with keys with fewer elements
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA"], ["typeB", 1]])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(request)
    error = excinfo.value
    assert error.error_code == ErrorCode.BAD_INPUT
    # deletion shouldn't have happened
    reply = session.execute(select_statement)
    records = reply.get_records()
    assert len(records) == 2

    # deleting with keys with more elements
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA", 0], ["typeB", 1, 2]])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(request)
    error = excinfo.value
    assert error.error_code == ErrorCode.BAD_INPUT
    # deletion shouldn't have happened
    reply = session.execute(select_statement)
    records = reply.get_records()
    assert len(records) == 2

    # deleting with keys with None elements
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA", None], ["typeB", 1]])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(request)
    error = excinfo.value
    assert error.error_code == ErrorCode.BAD_INPUT
    # deletion shouldn't have happened
    reply = session.execute(select_statement)
    records = reply.get_records()
    assert len(records) == 2

    # deleting with keys with empty string elements
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["", 0], ["typeB", 1]])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(request)
    error = excinfo.value
    assert error.error_code == ErrorCode.BAD_INPUT
    # deletion shouldn't have happened
    reply = session.execute(select_statement)
    records = reply.get_records()
    assert len(records) == 2

    # deleting non-existing entry should be ignored silently
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA", 0], ["typeB", 1], ["typeC", 2]])
        .build()
    )
    session.execute(request)

    reply = session.execute(select_request)
    assert len(reply.get_records()) == 0

    # double deletion is fine
    request = (
        bios.isql()
        .delete()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeA", 0], ["typeB", 1]])
        .build()
    )
    session.execute(request)

    reply = session.execute(select_request)
    assert len(reply.get_records()) == 0


def test_bios_upsert_context_error(session):
    statement = (
        bios.isql()
        .upsert()
        .into(STORE_CONTEXT["contextName"])
        .csv_bulk(["typeA,0,10,rcb", "typeB,1,11"])
        .build()
    )
    with pytest.raises(ServiceError) as context:
        session.execute(statement)
    assert context.value.error_code == ErrorCode.SCHEMA_MISMATCHED
    assert context.value.message == (
        "Source event string has less values than pre-defined;"
        " tenant=biosPythonQA, context=storeContextMd"
    )


def test_context_select_should_not_have_invalid_attributes(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[["typeA", 1]])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_empty_keys(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_keys_includes_empty(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[["typeA", 1], []])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_key_too_few_elements(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[["typeA", 1], ["typeB"]])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_key_too_many_elements(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[["typeA", 1], ["typeB", 1, 2.3]])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_keys_includes_empty_string(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[["typeA", 1], ["", 2]])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_keys_includes_none(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[[None, 1], ["typeB", 2]])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_with_keys_type_mismatch(session):
    with pytest.raises(ServiceError) as context:
        req = (
            bios.isql()
            .select("attribute")
            .from_context(STORE_CONTEXT["contextName"])
            .where(keys=[["typeA", 1], [False, "typeB"]])
            .build()
        )
        reply = session.execute(req)
        print(reply.get_records())
    context.value.error_code == ErrorCode.BAD_INPUT


def test_context_select_all_primary_keys(session):
    context_name = STORE_CONTEXT["contextName"]
    upsert_statement = (
        bios.isql()
        .upsert()
        .into(context_name)
        .csv_bulk(["typeX,8,10,rcb", "typeY,9,11,vk", "typeX,9,12,sf"])
        .build()
    )
    session.execute(upsert_statement)

    session.feature_refresh(context_name)

    time.sleep(30)

    select_request = bios.isql().select("storeType", "storeId").from_context(context_name).build()
    for _ in range(10):
        reply = session.execute(select_request)
        if len(reply.get_records()) == 1 and reply.get_records()[0].get("count()") == 3:
            break
        time.sleep(3)

    records = reply.get_records()
    assert len(records) == 3
    record0 = records[0]
    assert record0.get_primary_key() == ["typeX", 8]
    assert record0.get("storeType") == "typeX"
    assert record0.get("storeId") == 8
    assert record0.get("zipCode") is None
    assert record0.get("address") is None
    record1 = records[1]
    assert record1.get_primary_key() == ["typeX", 9]
    assert record1.get("storeType") == "typeX"
    assert record1.get("storeId") == 9
    assert record1.get("zipCode") is None
    assert record1.get("address") is None
    record2 = records[2]
    assert record2.get_primary_key() == ["typeY", 9]
    assert record2.get("storeType") == "typeY"
    assert record2.get("storeId") == 9
    assert record2.get("zipCode") is None
    assert record2.get("address") is None

    select_request = bios.isql().select("count()").from_context(context_name).build()
    reply = session.execute(select_request)

    records = reply.get_records()
    assert len(records) == 1
    record0 = records[0]
    assert record0.get("count()") == 3

    session.execute(
        bios.isql()
        .delete()
        .from_context(context_name)
        .where(keys=[["typeX", 8], ["typeX", 9], ["typeY", 9]])
        .build()
    )


def test_context_list_enum_primary_keys(session):
    context_name = CONTEXT_WITH_STRING_KEY["contextName"]
    upsert_statement = (
        bios.isql()
        .upsert()
        .into(context_name)
        .csv_bulk(
            [
                "2,catB,subX,89,one",
                "1,catB,subX,11,two",
                "9,catA,subX,12,three",
                "6,catC,subN,71,four",
            ]
        )
        .build()
    )
    session.execute(upsert_statement)

    # basic pattern
    select_request = (
        bios.isql()
        .select("category", "subCategory", "objectId")
        .from_context(context_name)
        .build()
    )
    reply = session.execute(select_request)
    pprint.pprint(reply.to_dict())
    _validate_primary_keys(reply)

    # primary key order not aligned with the original
    select_request = (
        bios.isql()
        .select("subCategory", "category", "objectId")
        .from_context(context_name)
        .build()
    )
    reply = session.execute(select_request)
    pprint.pprint(reply.to_dict())
    _validate_primary_keys(reply)

    # different cases
    select_request = (
        bios.isql()
        .select("category", "subcategory", "objectid")
        .from_context(context_name)
        .build()
    )
    reply = session.execute(select_request)
    pprint.pprint(reply.to_dict())
    _validate_primary_keys(reply)


def _validate_primary_keys(reply):
    expected_primary_keys = [
        ["catA", "subX", 9],
        ["catB", "subX", 1],
        ["catB", "subX", 2],
        ["catC", "subN", 6],
    ]

    records = reply.get_records()
    assert len(records) == 4
    for i, record in enumerate(records):
        expected = expected_primary_keys[i]
        assert record.get_primary_key() == expected
        assert record.get("category") == expected[0]
        assert record.get("subCategory") == expected[1]
        assert record.get("objectId") == expected[2]


def test_latter_wins_in_upserting_duplicate_primary_keys(session):
    upsert_statement = (
        bios.isql()
        .upsert()
        .into(STORE_CONTEXT["contextName"])
        .csv_bulk(["typeX,9000,10,rcb", "typeK,7,11,vk", "typeX,9000,41,pcb"])
        .build()
    )
    session.execute(upsert_statement)

    select_request = (
        bios.isql()
        .select()
        .from_context(STORE_CONTEXT["contextName"])
        .where(keys=[["typeX", 9000], ["typeK", 7]])
        .build()
    )
    reply = session.execute(select_request)
    records = reply.get_records()
    assert len(records) == 2
    assert records[0].get("zipCode") == 41
    assert records[0].get("address") == "pcb"
    assert records[1].get("zipCode") == 11
    assert records[1].get("address") == "vk"


def test_context_select_primary_keys(session):
    context_name = CONTEXT_PRIMARY_KEYS["contextName"]
    num_entries = 100000
    print(f"populating {num_entries} context entries")
    enum_values = ["ONE", "TWO", "THREE"]
    left = 0
    while left < num_entries:
        right = min(left + 2048, num_entries)
        entries = [
            f"{idx},{enum_values[idx % len(enum_values)]},{idx}" for idx in range(left, right)
        ]
        statement = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
        session.execute(statement)
        print(f"  ... {right}")
        left = right
        if right % 32768 == 0:
            _verify_listing_primary_keys(session, context_name, right)

    _verify_listing_primary_keys(session, context_name, num_entries)


def _verify_listing_primary_keys(session, context_name: str, num_entries: int):
    print(f"extracting primary keys, number of keys: {num_entries}")
    statement = bios.isql().select("the_key_1", "the_key_2").from_context(context_name).build()
    keys = session.execute(statement).to_dict()
    print("verifying")
    assert len(keys) == num_entries
    enum_values = ["ONE", "TWO", "THREE"]
    for idx, key in enumerate(keys):
        assert key.get("the_key_1") == idx
        assert key.get("the_key_2") == enum_values[idx % len(enum_values)]


def select_entries(session, keys, context_name):
    select_request = bios.isql().select().from_context(context_name).where(keys=keys).build()
    reply = session.execute(select_request)
    assert isinstance(reply, ContextRecords)
    return reply


def upsert_entries(session, entries, context_name):
    upsert_statement = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
    session.execute(upsert_statement)


def delete_entries(session, keys, context_name):
    delete_request = bios.isql().delete().from_context(context_name).where(keys=keys).build()
    session.execute(delete_request)


def test_update_regular(session):
    context_name = STORE_CONTEXT["contextName"]

    upsert_entries(session, ["typeM,0,10,abc-10", "typeL,1,11,def-11"], context_name)
    reply = select_entries(session, [["typeM", 0], ["typeL", 1]], context_name)
    records = reply.get_records()

    assert len(records) == 2
    assert records[0].get_primary_key() == ["typeM", 0]
    assert records[0].get("zipCode") == 10
    assert records[0].get("address") == "abc-10"
    assert records[1].get_primary_key() == ["typeL", 1]
    assert records[1].get("zipCode") == 11
    assert records[1].get("address") == "def-11"

    upsert_entries(session, ["typeM,0,20,abc-20", "typeL,1,5,def-5"], context_name)
    reply = select_entries(session, [["typeM", 0], ["typeL", 1]], context_name)
    records = reply.get_records()

    assert len(records) == 2
    assert records[0].get_primary_key() == ["typeM", 0]
    assert records[0].get("zipCode") == 20
    assert records[0].get("address") == "abc-20"
    assert records[1].get_primary_key() == ["typeL", 1]
    assert records[1].get("zipCode") == 5
    assert records[1].get("address") == "def-5"

    delete_entries(session, [["typeM", 0], ["typeL", 1]], context_name)
    reply = select_entries(session, [["typeM", 0], ["typeL", 1], ["typeM", 2]], context_name)
    assert len(reply.get_records()) == 0


def test_negative_upsert_empty_key(session):
    statement = (
        bios.isql()
        .upsert()
        .into(CONTEXT_WITH_STRING_KEY["contextName"])
        .csv_bulk(["typeC,hello,10,rcb", ",11,vk"])
        .build()
    )
    with pytest.raises(ServiceError) as excinfo:
        session.execute(statement)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT


def test_select_partial_attributes(session):
    """Regression BIOS-6620"""
    upsert_statement = (
        bios.isql()
        .upsert()
        .into(CONTEXT_STRING_AND_INT["contextName"])
        .csv_bulk(["roland,303,tb", "roland,909,tr"])
        .build()
    )
    session.execute(upsert_statement)

    select_all_request = (
        bios.isql()
        .select()
        .from_context(CONTEXT_STRING_AND_INT["contextName"])
        .where("the_key_1 = 'roland' AND the_key_2 in (303, 909)")
        .order_by("the_key_2")
        .build()
    )

    reply = session.execute(select_all_request)
    records = reply.get_records()
    assert len(records) == 2
    record0 = records[0]
    assert record0.get("the_key_1") == "roland"
    assert record0.get("the_key_2") == 303
    assert record0.get("the_value") == "tb"
    record1 = records[1]
    assert record1.get("the_key_1") == "roland"
    assert record1.get("the_key_2") == 909
    assert record1.get("the_value") == "tr"

    select_part_request = (
        bios.isql()
        .select("the_key_2", "the_value")
        .from_context(CONTEXT_STRING_AND_INT["contextName"])
        .where("the_key_1 = 'roland' AND the_key_2 in (303, 909)")
        .order_by("the_key_2")
        .build()
    )

    reply = session.execute(select_part_request)
    # records = reply.get_records()
    records = reply.to_dict()
    assert len(records) == 2
    record0 = records[0]
    assert len(record0) == 2
    assert record0.get("the_key_2") == 303
    assert record0.get("the_value") == "tb"
    record1 = records[1]
    assert len(record1) == 2
    assert record1.get("the_key_2") == 909
    assert record1.get("the_value") == "tr"

    # select all attributes again
    reply = session.execute(select_all_request)
    records = reply.get_records()
    assert len(records) == 2
    record0 = records[0]
    assert record0.get("the_key_1") == "roland"
    assert record0.get("the_key_2") == 303
    assert record0.get("the_value") == "tb"
    record1 = records[1]
    assert record1.get("the_key_1") == "roland"
    assert record1.get("the_key_2") == 909
    assert record1.get("the_value") == "tr"


if __name__ == "__main__":
    pytest.main(sys.argv)
