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
import sys
import unittest
import pytest

import bios
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url
from bios import ServiceError

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TENANT_NAME = "biosGeolocationEnrichTest"

ADMIN_USER = admin_user + "@" + TENANT_NAME
CONTEXT = {
    "contextName": "geolocation",
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {"attributeName": "ip", "type": "String", "default": "n/a"},
        {"attributeName": "city", "type": "String", "default": "n/a"},
        {"attributeName": "state", "type": "String", "default": "n/a"},
        {"attributeName": "country", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["ip"],
}

SIGNAL = {
    "signalName": "accesses",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "ip", "type": "String"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "ip_to_geolocation",
                "missingLookupPolicy": "StoreFillInValue",
                "foreignKey": ["ip"],
                "contextName": "geolocation",
                "contextAttributes": [
                    {"attributeName": "city", "fillIn": "No City"},
                    {"attributeName": "state", "fillIn": "No State"},
                    {"attributeName": "country", "fillIn": "No Country"},
                ],
            }
        ]
    },
}


class TestInsertEnrich(unittest.TestCase):
    """TestInsertEnrich"""

    @classmethod
    def setUpClass(cls):
        """Test setup class for creating tenant and start the admin
        session"""
        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        try:
            cls.sadmin.delete_tenant(TENANT_NAME)
        except ServiceError:
            pass
        cls.sadmin.create_tenant({"tenantName": TENANT_NAME})
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        cls.session.create_context(CONTEXT)
        cls.session.create_signal(SIGNAL)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.sadmin.delete_tenant(TENANT_NAME)
        except ServiceError:
            pass
        cls.sadmin.close()
        cls.session.close()

        cls.sadmin.close()
        cls.session.close()

    def upsert_entries(self, entries):
        statement = bios.isql().upsert().into(CONTEXT["contextName"]).csv_bulk(entries).build()
        self.session.execute(statement)

    def insert_entries(self, entries):
        statement = bios.isql().insert().into(SIGNAL["signalName"]).csv_bulk(entries).build()
        resp = self.session.execute(statement)
        self.assertIsNotNone(resp)
        return resp.records[0].timestamp

    def test_when_key_is_not_matching_with_signal(self):
        """
        Test to verify when the key is not matching with any value in context.
        """
        timestamp = self.insert_entries(["78.89.12.34"])

        select_st = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .time_range(timestamp, bios.time.seconds(10))
            .build()
        )
        select_resp = self.session.execute(select_st)
        self.assertIsNotNone(select_resp)
        self.assertEqual(len(select_resp.data_windows), 1)
        self.assertEqual(len(select_resp.data_windows[0].records), 1)
        record = select_resp.data_windows[0].records[0]
        self.assertEqual(record.get("ip"), "78.89.12.34")
        self.assertEqual(record.get("city"), "No City")
        self.assertEqual(record.get("state"), "No State")
        self.assertEqual(record.get("country"), "No Country")

    def test_simple_ingest_join(self):
        """To test simple ingest join feature"""
        self.upsert_entries(
            ["11.21.30.40,Sunnyvale,California,US", "12.20.32.41,Fremont,California,US"]
        )

        timestamp = self.insert_entries(["11.21.30.40"])

        select_st = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .time_range(timestamp, bios.time.seconds(10))
            .build()
        )
        select_resp = self.session.execute(select_st)
        self.assertIsNotNone(select_resp)
        self.assertEqual(len(select_resp.data_windows), 1)
        self.assertEqual(len(select_resp.data_windows[0].records), 1)
        record = select_resp.data_windows[0].records[0]
        self.assertEqual(record.get("ip"), "11.21.30.40")
        self.assertEqual(record.get("city"), "Sunnyvale")
        self.assertEqual(record.get("state"), "California")
        self.assertEqual(record.get("country"), "US")

    def test_ingest_join_with_multiple_events_matching_signal(self):
        """To test when the key is matching to two events in context stream"""
        # put a context and ingest signal
        self.upsert_entries(["10.20.30.44,Arlington,Texas,U.S.A."])
        timestamp1 = self.insert_entries(["10.20.30.44"])

        # put another context to the same key and ingest again
        self.upsert_entries(["10.20.30.44,Mumbai,Maharashtra,India"])
        self.insert_entries(["10.20.30.44"])

        select_st = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .time_range(timestamp1, bios.time.seconds(10))
            .build()
        )
        select_resp = self.session.execute(select_st)
        self.assertIsNotNone(select_resp)
        self.assertEqual(len(select_resp.data_windows), 1)
        self.assertEqual(len(select_resp.data_windows[0].records), 2)
        record0 = select_resp.data_windows[0].records[0]
        self.assertEqual(record0.get("ip"), "10.20.30.44")
        self.assertEqual(record0.get("city"), "Arlington")
        self.assertEqual(record0.get("state"), "Texas")
        self.assertEqual(record0.get("country"), "U.S.A.")
        record1 = select_resp.data_windows[0].records[1]
        self.assertEqual(record1.get("ip"), "10.20.30.44")
        self.assertEqual(record1.get("city"), "Mumbai")
        self.assertEqual(record1.get("state"), "Maharashtra")
        self.assertEqual(record1.get("country"), "India")


if __name__ == "__main__":
    pytest.main(sys.argv)
