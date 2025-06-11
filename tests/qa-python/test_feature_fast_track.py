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

import json
import pprint
import sys
import time

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_context, setup_signal, setup_tenant_config
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url
from tsetup import ingest_pass, ingest_user, sadmin_pass, sadmin_user

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"
INGEST_USER = f"{ingest_user}@{TENANT_NAME}"
EXTRACT_USER = f"{extract_user}@{TENANT_NAME}"

SIGNAL_NAME = "offerViews"
OFFER_VIEWS = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "sessionId", "type": "String"},
        {"attributeName": "offerId", "type": "Integer"},
        {"attributeName": "program", "type": "String"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byOffer",
                "dimensions": ["offerId"],
                "attributes": [],
                "featureInterval": 5000,
            },
            {
                "featureName": "byProgram",
                "dimensions": ["program"],
                "attributes": [],
                "featureInterval": 5000,
            },
            {
                "featureName": "byProgramOffer",
                "dimensions": ["program", "offerId"],
                "attributes": [],
                "featureInterval": 5000,
            },
            {
                "featureName": "bySession",
                "dimensions": ["sessionId"],
                "attributes": ["offerId"],
                "dataSketches": ["LastN"],
                "featureInterval": 5000,
                "featureAsContextName": "offerViews_offerIdBySessionId",
                "items": 15,
                "ttl": 604800000,
            },
        ]
    },
}

CONTEXT_NAME = "offerViews_offerIdBySessionId"
FAC = {
    "contextName": CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "sessionId", "type": "String"},
        {"attributeName": "offerIdCollection", "type": "String"},
    ],
    "primaryKey": ["sessionId"],
    "ttl": 5788800000,
}


@pytest.fixture(scope="module")
def set_up_class():
    setup_tenant_config()
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        print("\n *** setting the fast track signal")
        sadmin.set_property("prop.maintenance.fastTrackSignals", f"{TENANT_NAME}.{SIGNAL_NAME}")
        print(" *** sleeping for 15 seconds to wait for the config being effective")
        time.sleep(15)

    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        setup_context(admin, FAC)
        setup_signal(admin, OFFER_VIEWS)
        yield

        print("\n *** deleting the test signal")
        admin.delete_signal(SIGNAL_NAME)

    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        print(" *** clearing the fast track signal")
        sadmin.set_property("prop.maintenance.fastTrackSignals", "")


def _insert(session, event):
    statement = bios.isql().insert().into(SIGNAL_NAME).csv(event).build()
    session.execute(statement)


def test_fundamental(set_up_class):
    with bios.login(ep_url(), INGEST_USER, ingest_pass) as session:
        session_id = "f007ba11"
        session_id2 = "ba5eba11"
        print(" ... inserting the first set of events")
        _insert(session, f"{session_id},112233,programA")
        _insert(session, f"{session_id2},223344,programB")
        print(" ... sleeping for 5 seconds")
        time.sleep(5)
        print(" ... inserting the second event")
        event1 = f"{session_id},223344,programB"
        statement = bios.isql().insert().into(SIGNAL_NAME).csv(event1).build()
        session.execute(statement)

        print(" ... waiting for the FaC context entries are populated")

    with bios.login(ep_url(), EXTRACT_USER, extract_pass) as session:
        statement = (
            bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[session_id]]).build()
        )
        start = time.time()
        timeout = 15
        populated = False
        while time.time() <= start + timeout:
            fac_resp = session.execute(statement)
            if len(fac_resp.get_records()) > 0:
                record = fac_resp.get_records()[0]
                entry = json.loads(record.get("offerIdCollection"))
                collections = entry.get("c")
                if len(collections) == 2:
                    populated = True
                    elapsed = time.time() - start
                    break
            time.sleep(1)
        pprint.pprint(fac_resp.to_dict())
        assert populated
        print(f" ... elapsed seconds: {elapsed}")
        print(" ... verifying")
        assert collections[0].get("v") == 112233
        assert collections[1].get("v") == 223344

        print(" ... verifying hot offers")
        hot_offers = session.execute(
            bios.isql()
            .select("count()")
            .from_signal(SIGNAL_NAME)
            .group_by("offerId")
            .order_by("count()", reverse=True)
            .tumbling_window(bios.time.minutes(1))
            .time_range(bios.time.now(), bios.time.minutes(-1), bios.time.seconds(5))
            .build()
        )
        print(hot_offers)
        assert len(hot_offers.get_data_windows()) == 1
        records = hot_offers.get_data_windows()[0].get_records()
        assert len(records) == 2
        assert records[0].get("offerId") == 223344
        assert records[1].get("offerId") == 112233


if __name__ == "__main__":
    pytest.main(sys.argv)
