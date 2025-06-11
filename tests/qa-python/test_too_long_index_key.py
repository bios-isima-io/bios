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
import random
import string
import sys
import time

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_context, setup_signal, setup_tenant_config

ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIGNAL_NAME = "volcanoActivity"
SIGNAL = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "country", "type": "String"},
        {"attributeName": "state", "type": "String"},
        {"attributeName": "volcanoName", "type": "String"},
        {"attributeName": "category", "type": "Integer"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byCountryVolcanoName",
                "dimensions": ["country", "state", "volcanoName"],
                "attributes": [],
                "featureInterval": 60000,
                # "indexed": True,
            }
        ]
    },
}

SIGNAL_INDEXED_NAME = "volcanoActivityIndexed"
SIGNAL_INDEXED = copy.deepcopy(SIGNAL)
SIGNAL_INDEXED["signalName"] = SIGNAL_INDEXED_NAME
SIGNAL_INDEXED["postStorageStage"]["features"][0]["indexed"] = True

CONTEXT_NAME = "cityToContinent"
CONTEXT = {
    "contextName": CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "city", "type": "String"},
        {"attributeName": "continent", "type": "String"},
    ],
    "primaryKey": ["city"],
}


@pytest.fixture(scope="module")
def set_up_class():
    setup_tenant_config()
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        setup_signal(admin, SIGNAL_INDEXED)
        setup_signal(admin, SIGNAL)
        setup_context(admin, CONTEXT)


def generate_random_string(length):
    random_string = "".join(random.choices(string.ascii_letters + string.digits, k=length))
    return random_string


def test_too_long_signal_attributes(set_up_class):
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
        long_key = generate_random_string(30000)
        malformed_key = generate_random_string(40000)
        malformed_key2 = generate_random_string(32767)
        events = [
            "US,Hawaii,St. Helens,4",
            "Japan,Nagano,Mt. Asama,3",
            f'India,Maharashtra,"{long_key}",6',
            # "India,Maharashtra,Deccan Traps,6",
            "US,Wyoming,Yellowstone Caldera,3",
            "Japan,Shizuoka,Mt. Fuji,2",
            "India,Bengal,Barren Island,1",
            "US,Oregon,Mt. Shasta,3",
            "Japan,Kumamoto,Mt. Aso,8",
            "Japan,Hokkaido,Usudake,2",
        ]
        # insert valid events
        statement = bios.isql().insert().into(SIGNAL_NAME).csv_bulk(events).build()
        statement_indexed = bios.isql().insert().into(SIGNAL_INDEXED_NAME).csv_bulk(events).build()
        response = session.execute(statement)
        session.execute(statement_indexed)
        start = response.records[0].timestamp

        # try to insert invalid events
        statement = (
            bios.isql().insert().into(SIGNAL_NAME).csv(f'Japan,Iwate,"{malformed_key}",6').build()
        )
        with pytest.raises(ServiceError) as excinfo:
            session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        statement = (
            bios.isql()
            .insert()
            .into(SIGNAL_NAME)
            .csv_bulk([f'US,"{malformed_key2}","{malformed_key2}",6'])
            .build()
        )
        with pytest.raises(ServiceError) as excinfo:
            session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT

        end = bios.time.now()

        next_checkpoint = (int(end / 60000) + 1) * 60
        sleep_until = next_checkpoint + 20
        sleep_seconds = int(sleep_until - time.time())
        until = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sleep_until))
        print(f"sleeping for {sleep_seconds} seconds until {until}")
        time.sleep(sleep_seconds)

        check_result(session, SIGNAL_INDEXED_NAME, start, end)
        check_result(session, SIGNAL_NAME, start, end)


def check_result(session, signal_name, start, end):
    statement = (
        bios.isql()
        .select("count()")
        .from_signal(signal_name)
        .group_by("country")
        .tumbling_window(60000)
        .time_range(start, end + 60000 - start)
        .build()
    )
    events = session.execute(statement).to_dict()
    pprint.pprint(events)
    assert len(events) == 3
    assert events[0].get("country") == "India"
    assert events[0].get("count()") == 2
    assert events[1].get("country") == "Japan"
    assert events[1].get("count()") == 4
    assert events[2].get("country") == "US"
    assert events[2].get("count()") == 3


def test_too_long_context_primary_key(set_up_class):
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
        malformed_key = generate_random_string(40000)
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql().upsert().into(CONTEXT_NAME).csv(f'"{malformed_key}",America').build()
            )
            session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT


def test_too_long_context_attributes(set_up_class):
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
        malformed_key = generate_random_string(32767)
        with pytest.raises(ServiceError) as excinfo:
            statement = (
                bios.isql()
                .upsert()
                .into(CONTEXT_NAME)
                .csv_bulk([f'"{malformed_key}","{malformed_key}"'])
                .build()
            )
            session.execute(statement)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT


if __name__ == "__main__":
    pytest.main(sys.argv)
