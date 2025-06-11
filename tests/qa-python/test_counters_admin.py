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

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

CONTEXT_COUNTER_SNAPSHOTS_NAME = "counterSnapshots"
CONTEXT_COUNTER_SNAPSHOTS = {
    "contextName": CONTEXT_COUNTER_SNAPSHOTS_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "fcType",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "fcId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "itemId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "timestamp",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Timestamp", "unit": "UuidTime"},
        },
        {
            "attributeName": "onHandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
        {
            "attributeName": "demandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
    ],
    "primaryKey": ["fcType", "fcId", "itemId"],
}

SIGNAL_COUNTER_UPDATES_NAME = "counterUpdates"
SIGNAL_COUNTER_UPDATES = {
    "signalName": SIGNAL_COUNTER_UPDATES_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "fcType",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "fcId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "itemId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "operation",
            "type": "String",
            "allowedValues": ["change", "set"],
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "onHandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
        {
            "attributeName": "demandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byAll",
                "dimensions": ["fcType", "fcId", "itemId", "operation"],
                "attributes": ["onHandValue", "demandValue"],
                "featureInterval": 5000,
                "materializedAs": "AccumulatingCount",
                "featureAsContextName": "counterSnapshots",
                "indexOnInsert": True,
            }
        ]
    },
}

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

CREATED_STREAMS = []


@pytest.fixture
def session():
    setup_tenant_config()

    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin_session:
        yield admin_session

        while CREATED_STREAMS:
            (stream_name, stream_type) = CREATED_STREAMS.pop()
            try:
                if stream_type == "signal":
                    admin_session.delete_signal(stream_name)
                else:
                    admin_session.delete_context(stream_name)
            except ServiceError:
                pass


def _cleanup(session, signal_name, context_name):
    if signal_name:
        try:
            session.delete_signal(signal_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise
    if context_name:
        try:
            session.delete_context(context_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise


def test_fundamental(session):
    _cleanup(session, SIGNAL_COUNTER_UPDATES_NAME, CONTEXT_COUNTER_SNAPSHOTS_NAME)

    session.create_context(CONTEXT_COUNTER_SNAPSHOTS)
    CREATED_STREAMS.append((CONTEXT_COUNTER_SNAPSHOTS_NAME, "context"))
    session.create_signal(SIGNAL_COUNTER_UPDATES)
    CREATED_STREAMS.append((SIGNAL_COUNTER_UPDATES_NAME, "signal"))
    retrieved_signal = session.get_signal(SIGNAL_COUNTER_UPDATES_NAME)
    pprint.pprint(retrieved_signal)
    assert retrieved_signal.get("postStorageStage") == SIGNAL_COUNTER_UPDATES.get(
        "postStorageStage"
    )


def test_delete_context_while_signal_exists(session):
    signal_name = "counterUpdatesDeletionTest"
    context_name = "counterSnapshotsDeletionTest"
    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))
    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    # Deleting the snapshot context should be rejected
    with pytest.raises(ServiceError) as excinfo:
        session.delete_context(context_name)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    # Remove the snapshot feature, then the context can be deleted
    del signal_config["postStorageStage"]
    session.update_signal(signal_name, signal_config)

    session.delete_context(context_name)
    with pytest.raises(ServiceError) as excinfo:
        session.get_context(context_name)
    assert excinfo.value.error_code == ErrorCode.NO_SUCH_STREAM


def test_invalid_schema(session):
    """Details are covered by unit testing. This test case verifies the validation errors are
    returned to the client properly"""
    signal_name = "counterUpdatesInvalidSchema"
    context_name = "counterSnapshotsInvalidSchema"
    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["attributes"][0]["type"] = "Integer"
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(signal_config)
        CREATED_STREAMS.append((signal_name, "signal"))

    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        "Constraint violation: AccumulatingCount: Dimension attribute type mismatch;"
        " tenant=biosPythonQA, signal=counterUpdatesInvalidSchema, feature=byAll,"
        " featureAsContextName=counterSnapshotsInvalidSchema,"
        " attribute=fcType, type(signal)=Integer, type(context)=String" == excinfo.value.message
    )


def test_context_missing(session):
    signal_name = "signalUpdatesContextMissing"
    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalname"] = signal_name

    _cleanup(session, signal_name, None)
    try:
        session.delete_signal(signal_name)
    except ServiceError as err:
        if err.error_code != ErrorCode.NO_SUCH_STREAM:
            raise

    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(signal_config)
        CREATED_STREAMS.append((signal_name, "signal"))
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        excinfo.value.message == "Constraint violation: AccumulatingCount:"
        " The context specified by featureAsContextName is missing: counterSnapshots;"
        " tenant=biosPythonQA, signal=counterUpdates, feature=byAll,"
        " featureAsContextName=counterSnapshots"
    )


def test_invalid_feature(session):
    """Test creating a signal with write time indexing"""
    signal_name = "counterUpdatesInvalidFeature"
    context_name = "counterSnapshotsInvalidFeature"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    CREATED_STREAMS.append((signal_name, "signal"))
    feature_name_missing = copy.deepcopy(signal_config)
    del feature_name_missing["postStorageStage"]["features"][0]["featureName"]
    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(feature_name_missing)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    wrong_dimensions = copy.deepcopy(signal_config)
    feature = wrong_dimensions["postStorageStage"]["features"][0]
    feature["dimensions"] = feature["dimensions"][0:2]
    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(wrong_dimensions)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    wrong_attributes = copy.deepcopy(signal_config)
    feature = wrong_attributes["postStorageStage"]["features"][0]
    feature["attributes"] = feature["attributes"][0:1]
    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(wrong_attributes)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        excinfo.value.message
        == "Constraint violation: AccumulatingCount: Unnecessary attributes are in the context;"
        " tenant=biosPythonQA, signal=counterUpdatesInvalidFeature, feature=byAll,"
        " featureAsContextName=counterSnapshotsInvalidFeature,"
        " unnecessaryAttributes=[demandValue]"
    )


def test_retrofit_snapshot_feature(session):
    """Test adding a snapshot feature afterwards"""
    signal_name = "counterUpdatesRetrofit"
    context_name = "counterSnapshotsRetrofit"

    _cleanup(session, signal_name, context_name)

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name

    feature = signal_config["postStorageStage"]["features"][0]
    feature["featureAsContextName"] = context_name
    del signal_config["postStorageStage"]["features"]

    CREATED_STREAMS.append((context_name, "context"))

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    signal_config["postStorageStage"]["features"] = [feature]

    with pytest.raises(ServiceError) as excinfo:
        session.update_signal(signal_name, signal_config)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name

    session.create_context(context_config)

    session.update_signal(signal_name, signal_config)


def test_retrofit_snapshot_feature2(session):
    """Test adding a snapshot feature afterwards by turning on the snapshot flag"""
    signal_name = "counterUpdatesRetrofit2"
    context_name = "counterSnapshotsRetrofit2"

    _cleanup(session, signal_name, context_name)

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name

    feature = signal_config["postStorageStage"]["features"][0]
    feature["featureAsContextName"] = context_name
    feature.pop("materializedAs", None)

    CREATED_STREAMS.append((context_name, "context"))

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    feature["materializedAs"] = "AccumulatingCount"

    with pytest.raises(ServiceError) as excinfo:
        session.update_signal(signal_name, signal_config)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name

    session.create_context(context_config)

    session.update_signal(signal_name, signal_config)


def test_add_enrichment_to_signal(session):
    """Test adding an enrichment to the updates signal"""
    signal_name = "counterUpdatesSignalEnrichment"
    context_name = "counterSnapshotsSignalEnrichment"
    enriching_context_name = "items"

    _cleanup(session, signal_name, context_name)
    _cleanup(session, None, enriching_context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    context_items = {
        "contextName": enriching_context_name,
        "missingAttributePolicy": "Reject",
        "attributes": [
            {"attributeName": "itemId", "type": "Integer"},
            {"attributeName": "itemName", "type": "String"},
        ],
        "primaryKey": ["itemId"],
    }
    session.create_context(context_items)
    CREATED_STREAMS.append((enriching_context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    signal_config["enrich"] = {
        "enrichments": [
            {
                "enrichmentName": "itemName",
                "foreignKey": ["itemId"],
                "missingLookupPolicy": "Reject",
                "contextName": "items",
                "contextAttributes": [
                    {"attributeName": "itemName", "as": "itemName", "fillIn": "MISSING"}
                ],
            }
        ]
    }

    session.update_signal(signal_name, signal_config)


def test_add_enrichment_to_context(session):
    """Test adding an enrichment to the snapshot context"""
    signal_name = "counterUpdatesContextEnrichment"
    context_name = "counterSnapshotsContextEnrichment"
    enriching_context_name = "items"

    _cleanup(session, signal_name, context_name)
    _cleanup(session, None, enriching_context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    context_items = {
        "contextName": enriching_context_name,
        "missingAttributePolicy": "Reject",
        "attributes": [
            {"attributeName": "itemId", "type": "Integer"},
            {"attributeName": "itemName", "type": "String"},
        ],
        "primaryKey": ["itemId"],
    }
    session.create_context(context_items)
    CREATED_STREAMS.append((enriching_context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    context_config["enrichments"] = [
        {
            "enrichmentName": "itemName",
            "foreignKey": ["itemId"],
            "missingLookupPolicy": "StoreFillInValue",
            "enrichedAttributes": [
                {
                    "value": f"{enriching_context_name}.itemName",
                    "as": "itemName",
                    "fillIn": "MISSING",
                }
            ],
        }
    ]

    session.update_context(context_name, context_config)


def test_add_feature_to_signal(session):
    """Test adding a feature to the updates signal"""
    signal_name = "counterUpdatesSignalFeature"
    context_name = "counterSnapshotsSignalFeature"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    signal_config["postStorageStage"]["features"].append(
        {
            "featureName": "byItemId",
            "dimensions": ["itemId"],
            "attributes": [],
            "featureInterval": 60000,
        }
    )

    session.update_signal(signal_name, signal_config)


def test_add_feature_to_context(session):
    """Test adding a feature to the snapshot context"""
    signal_name = "counterUpdatesContextFeature"
    context_name = "counterSnapshotsContextFeature"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    context_config["features"] = [
        {
            "featureName": "byAll",
            "dimensions": ["fcType", "fcId", "itemId"],
            "attributes": ["onHandValue", "demandValue"],
            "featureInterval": 60000,
        }
    ]

    session.update_context(context_name, context_config)


def test_update_signal_attribute_rejected(session):
    """Modifying or adding attribute to the signal would be rejected"""
    signal_name = "counterUpdatesSignalAttributeMod"
    context_name = "counterSnapshotsSignalAttributeMod"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name

    feature = signal_config["postStorageStage"]["features"][0]
    feature["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    signal_attribute_added = copy.deepcopy(signal_config)
    signal_attribute_added["attributes"].append(
        {"attributeName": "extraValue", "type": "Decimal", "default": 0.0}
    )
    signal_attribute_added["postStorageStage"]["features"][0]["attributes"].append("extraValue")

    with pytest.raises(ServiceError) as excinfo:
        session.update_signal(signal_name, signal_attribute_added)

    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        excinfo.value.message
        == "Constraint violation: AccumulatingCount: Required snapshot attribute"
        " 'extraValue' is missing in the context;"
        " tenant=biosPythonQA, signal=counterUpdatesSignalAttributeMod, feature=byAll,"
        " featureAsContextName=counterSnapshotsSignalAttributeMod"
    )

    signal_attribute_modified = copy.deepcopy(signal_config)
    signal_attribute_modified["attributes"][1]["type"] = "String"
    with pytest.raises(ServiceError) as excinfo:
        session.update_signal(signal_name, signal_attribute_modified)

    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        excinfo.value.message
        == "Constraint violation: AccumulatingCount: Dimension attribute type mismatch;"
        " tenant=biosPythonQA, signal=counterUpdatesSignalAttributeMod, feature=byAll,"
        " featureAsContextName=counterSnapshotsSignalAttributeMod,"
        " attribute=fcId, type(signal)=String, type(context)=Integer"
    )


def test_snapshot_context_attribute_rejected(session):
    """Modifying or adding attribute to the context would be rejected"""
    signal_name = "counterUpdatesContextAttributeMod"
    context_name = "counterSnapshotsContextAttributeMod"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name

    feature = signal_config["postStorageStage"]["features"][0]
    feature["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    context_attribute_added = copy.deepcopy(context_config)
    context_attribute_added["attributes"].append(
        {"attributeName": "extraSnapshot", "type": "Decimal", "default": 0.0}
    )

    with pytest.raises(ServiceError) as excinfo:
        session.update_context(context_name, context_attribute_added)

    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        excinfo.value.message
        == "Constraint violation: AccumulatingCount: Unnecessary attributes are in the context;"
        " tenant=biosPythonQA, signal=counterUpdatesContextAttributeMod, feature=byAll,"
        " featureAsContextName=counterSnapshotsContextAttributeMod,"
        " unnecessaryAttributes=[extraSnapshot]"
    )

    context_attribute_modified = copy.deepcopy(context_config)
    context_attribute_modified["attributes"][1]["type"] = "String"
    with pytest.raises(ServiceError) as excinfo:
        session.update_context(context_name, context_attribute_modified)

    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        excinfo.value.message
        == "Constraint violation: AccumulatingCount: Dimension attribute type mismatch;"
        " tenant=biosPythonQA, signal=counterUpdatesContextAttributeMod, feature=byAll,"
        " featureAsContextName=counterSnapshotsContextAttributeMod,"
        " attribute=fcId, type(signal)=Integer, type(context)=String"
    )


def test_add_write_time_indexing(session):
    """Test modifying a feature for disabling indexOnInsert"""
    signal_name = "counterUpdatesAddIndex"
    context_name = "counterSnapshotsAddIndex"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name

    session.create_signal(signal_config)
    CREATED_STREAMS.append((signal_name, "signal"))

    signal_config["postStorageStage"]["features"][0]["indexOnInsert"] = False

    session.update_signal(signal_name, signal_config)

    retrieved_signal = session.get_signal(signal_name)
    assert (
        retrieved_signal["postStorageStage"]["features"]
        == signal_config["postStorageStage"]["features"]
    )


def test_invalid_write_time_indexing(session):
    """Test adding a feature with write time indexing"""
    signal_name = "counterUpdatesInvalidIndex"
    context_name = "counterSnapshotsInvalidIndex"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name
    signal_config["postStorageStage"]["features"].append(
        {
            "featureName": "indexer",
            "dimensions": ["fcType", "fcId", "itemId"],
            "attributes": ["operation", "onHandValue", "demandValue"],
            "indexOnInsert": True,
            "featureInterval": 300000,
        }
    )

    CREATED_STREAMS.append((signal_name, "signal"))

    feature_name_missing = copy.deepcopy(signal_config)
    del feature_name_missing["postStorageStage"]["features"][1]["featureName"]
    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(feature_name_missing)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        "Constraint violation: signalConfig.postStorageStage.features[1].featureName:"
        " The value must be set" in excinfo.value.message
    )

    empty_dimensions = copy.deepcopy(signal_config)
    empty_dimensions["postStorageStage"]["features"][1]["dimensions"] = []
    with pytest.raises(ServiceError) as excinfo:
        session.create_signal(empty_dimensions)
    assert excinfo.value.error_code == ErrorCode.BAD_INPUT
    assert (
        "Constraint violation: Dimension may not be empty when 'indexOnInsert' is true"
        in excinfo.value.message
    )


def _insert(session, signal_name, event):
    statement = bios.isql().insert().into(signal_name).csv(event).build()
    session.execute(statement)


def test_signal_index(session):
    signal_name = "counterUpdatesIndexTest"
    context_name = "counterSnapshotsIndexTest"

    _cleanup(session, signal_name, context_name)

    context_config = copy.deepcopy(CONTEXT_COUNTER_SNAPSHOTS)
    context_config["contextName"] = context_name
    session.create_context(context_config)
    CREATED_STREAMS.append((context_name, "context"))

    signal_config = copy.deepcopy(SIGNAL_COUNTER_UPDATES)
    signal_config["signalName"] = signal_name
    signal_config["postStorageStage"]["features"][0]["featureAsContextName"] = context_name
    session.create_signal(signal_config)

    time0 = bios.time.now()
    _insert(session, signal_name, "typeA,10,100,set,10001,20001")
    _insert(session, signal_name, "typeA,10,100,change,10002,20002")
    _insert(session, signal_name, "typeA,10,100,set,10003,20003")
    _insert(session, signal_name, "typeA,10,100,change,10004,20004")
    time1 = bios.time.now()

    statement = (
        bios.isql()
        .select()
        .from_signal(signal_name)
        .where("fcType IN ('typeA') AND fcId IN (10) AND itemId IN (100)")
        .time_range(time0, time1 - time0)
        .build()
    )

    result = session.execute(statement)
    assert len(result.get_data_windows()) == 1
    records = result.get_data_windows()[0].get_records()
    print(records)
    assert len(records) == 4
    assert records[0].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "set",
        "onHandValue": 10001,
        "demandValue": 20001,
    }
    assert records[1].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHandValue": 10002,
        "demandValue": 20002,
    }
    assert records[2].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "set",
        "onHandValue": 10003,
        "demandValue": 20003,
    }
    assert records[3].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHandValue": 10004,
        "demandValue": 20004,
    }


if __name__ == "__main__":
    pytest.main(sys.argv)
