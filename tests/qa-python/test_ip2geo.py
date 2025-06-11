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
import unittest

import pytest

from bios import ServiceError, ErrorCode
import bios
from bios.models import ContextRecords, AttributeType

from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME,
    setup_context,
    setup_signal,
    setup_tenant_config,
)
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
SIGNAL = {
    "signalName": "ipLookup",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "ip", "type": "String"}],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "ip2geo",
                "foreignKey": ["ip"],
                "missingLookupPolicy": "StoreFillInValue",
                "contextName": "_ip2geo",
                "contextAttributes": [
                    {"attributeName": "city", "fillIn": "MISSING"},
                    {"attributeName": "country", "fillIn": "MISSING"},
                    {"attributeName": "continent", "fillIn": "MISSING"},
                    {"attributeName": "latitude", "fillIn": 0.0},
                    {"attributeName": "longitude", "fillIn": 0.0},
                    {"attributeName": "timezone", "fillIn": "MISSING"},
                ],
            }
        ]
    },
}


class Ip2GeoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            setup_signal(session, SIGNAL)

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            setup_signal(session, SIGNAL)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            session.delete_signal(SIGNAL.get("signalName"))

    def test_regular_tenant(self):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            self._test_ip2geo(session)

    def test_system_tenant(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            self._test_ip2geo(session)

    def _test_ip2geo(self, session):
        signal_name = SIGNAL.get("signalName")
        since = bios.time.now()
        insert_statement = bios.isql().insert().into(signal_name).csv("122.222.190.163").build()
        session.execute(insert_statement)
        until = bios.time.now()
        select_statement = (
            bios.isql().select().from_signal(signal_name).time_range(since, until - since).build()
        )
        records = session.execute(select_statement).to_dict()
        assert len(records) == 1
        record = records[0]
        assert record.get("city") != "MISSING"
        assert record.get("country") != "MISSING"
        assert record.get("continent") != "MISSING"
        assert record.get("latitude") != 0.0
        assert record.get("longitude") != 0.0
        assert record.get("timezone") != "MISSING"


if __name__ == "__main__":
    pytest.main(sys.argv)
