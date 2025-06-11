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
from setup_common import setup_signal, setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

SIGNAL_NAME = "indexOnInsert"
SIGNAL = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "fcType",
            "type": "String",
        },
        {
            "attributeName": "fcId",
            "type": "Integer",
        },
        {
            "attributeName": "itemId",
            "type": "Integer",
        },
        {
            "attributeName": "operation",
            "type": "String",
            "allowedValues": ["change", "set"],
        },
        {
            "attributeName": "onHand",
            "type": "Integer",
        },
        {
            "attributeName": "demand",
            "type": "Integer",
        },
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byAll",
                "dimensions": ["fcType", "fcId", "itemId", "operation"],
                "attributes": ["onHand", "demand"],
                "featureInterval": 1000,
                "featureAsContextName": "counterSnapshots",
                "indexOnInsert": True,
            }
        ]
    },
}

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

CREATED_STREAMS = []


@pytest.fixture(scope="module")
def session():
    setup_tenant_config()

    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin_session:
        setup_signal(admin_session, SIGNAL)
        CREATED_STREAMS.append((SIGNAL_NAME, "signal"))

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


def _insert(session, event):
    statement = bios.isql().insert().into(SIGNAL_NAME).csv(event).build()
    session.execute(statement)


def test_fundamental(session):
    time0 = bios.time.now()
    _insert(session, "typeA,10,100,set,10001,20001")
    _insert(session, "typeA,10,100,change,10002,20002")
    _insert(session, "typeA,10,100,set,10003,20003")
    _insert(session, "typeA,10,100,change,10004,20004")
    time1 = bios.time.now()

    statement = (
        bios.isql()
        .select()
        .from_signal(SIGNAL_NAME)
        .where(
            "fcType = 'typeA' AND fcId = 10 AND itemId = 100 AND operation in ('change', 'set')"
        )
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
        "onHand": 10001,
        "demand": 20001,
    }
    assert records[1].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHand": 10002,
        "demand": 20002,
    }
    assert records[2].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "set",
        "onHand": 10003,
        "demand": 20003,
    }
    assert records[3].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHand": 10004,
        "demand": 20004,
    }


def test_subset_values(session):
    time0 = bios.time.now()
    _insert(session, "typeA,10,100,set,10001,20001")
    _insert(session, "typeA,10,100,change,10002,20002")
    _insert(session, "typeA,10,100,set,10003,20003")
    _insert(session, "typeA,10,100,change,10004,20004")
    time1 = bios.time.now()

    statement = (
        bios.isql()
        .select("fcType", "fcId", "itemId", "operation", "onHand")
        .from_signal(SIGNAL_NAME)
        .where("fcType = 'typeA' AND fcId in (10) AND itemId = 100 AND operation in ('change')")
        .time_range(time0, time1 - time0)
        .build()
    )

    result = session.execute(statement)
    assert len(result.get_data_windows()) == 1
    records = result.get_data_windows()[0].get_records()
    print(records)
    assert len(records) == 2
    assert records[0].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHand": 10002,
    }
    assert records[1].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHand": 10004,
    }


def test_subset_dimensions(session):
    time0 = bios.time.now()
    _insert(session, "typeA,10,100,set,10001,20001")
    _insert(session, "typeA,10,100,change,10002,20002")
    _insert(session, "typeA,10,100,set,10003,20003")
    _insert(session, "typeA,10,100,change,10004,20004")
    time1 = bios.time.now()

    statement = (
        bios.isql()
        .select("fcType", "fcId", "itemId", "operation", "onHand")
        .from_signal(SIGNAL_NAME)
        .where("fcType = 'typeA' AND fcId = 10 AND itemId in (100)")
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
        "onHand": 10001,
    }
    assert records[1].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHand": 10002,
    }
    assert records[2].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "set",
        "onHand": 10003,
    }
    assert records[3].to_dict() == {
        "fcType": "typeA",
        "fcId": 10,
        "itemId": 100,
        "operation": "change",
        "onHand": 10004,
    }


if __name__ == "__main__":
    pytest.main(sys.argv)
