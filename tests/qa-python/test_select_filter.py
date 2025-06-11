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
from bios import ServiceError
import bios
from bios.errors import ErrorCode
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url
from tutils import b64enc, b64dec


logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosSelectFilterTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIGNAL = {
    "signalName": "filterTest",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "sr_no", "type": "Integer"},
        {"attributeName": "name", "type": "String"},
        {"attributeName": "marks", "type": "Decimal"},
        {"attributeName": "pass", "type": "Boolean"},
        {"attributeName": "details", "type": "Blob"},
    ],
}


class SelectDistinctcountTest(unittest.TestCase):
    """Test cases that were ported from tfos-sdk to test extract operations"""

    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_signal(SIGNAL)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
        cls.session.close()

    @classmethod
    def insert_into_signal(cls, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(SIGNAL["signalName"]).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_filter_with_all_dtypes(self):
        records = [
            "12,yue,89.3,True," + b64enc(b"9e829a88-a2e0-11e8-a8a7-42010a800002"),
            "19,cds,23.3,False," + b64enc(b"a8098c1a-f86e-11da-bd1a-00112444be1e"),
            "64,dhf,63.3,True," + b64enc(b"6fa459ea-ee8a-3ca4-894e-db77e160355e"),
            "92,hsj,52.4,True," + b64enc(b"16fd2706-8baf-433b-82eb-8c7fada847da"),
        ]
        start = self.insert_into_signal(records)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("sr_no = 19 AND name = 'cds'")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 1)
        self.assertEqual(records1[0].get("sr_no"), 19)
        self.assertEqual(records1[0].get("name"), "cds")
        self.assertEqual(records1[0].get("marks"), 23.3)
        self.assertEqual(records1[0].get("pass"), False)
        self.assertEqual(records1[0].get("details"), b"a8098c1a-f86e-11da-bd1a-00112444be1e")

        request2 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("pass = True AND marks > 80")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp2 = self.session.execute(request2)
        self.assertIsNotNone(resp2)
        windows2 = resp2.get_data_windows()
        self.assertEqual(len(windows2), 1)
        records2 = windows2[0].records
        self.assertEqual(len(records2), 1)
        self.assertEqual(records2[0].get("sr_no"), 12)
        self.assertEqual(records2[0].get("name"), "yue")
        self.assertEqual(records2[0].get("marks"), 89.3)
        self.assertEqual(records2[0].get("pass"), True)
        self.assertEqual(records2[0].get("details"), b"9e829a88-a2e0-11e8-a8a7-42010a800002")

    def test_filter_blob(self):
        request3 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("details = '" + b64enc(b"6fa459ea-ee8a-3ca4-894e-db77e160355e") + "'")
            .time_range(bios.time.now(), -bios.time.seconds(5))
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(request3)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_filter_one_condition(self):
        records = [
            "12,yue,89.3,True," + b64enc(b"9e829a88-a2e0-11e8-a8a7-42010a800002"),
            "19,cds,23.3,False," + b64enc(b"a8098c1a-f86e-11da-bd1a-00112444be1e"),
            "64,dhf,63.3,True," + b64enc(b"6fa459ea-ee8a-3ca4-894e-db77e160355e"),
            "92,hsj,52.4,True," + b64enc(b"16fd2706-8baf-433b-82eb-8c7fada847da"),
        ]
        start = self.insert_into_signal(records)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("sr_no > 19")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 2)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("sr_no < 19")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 1)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("sr_no <= 19")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 2)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("sr_no >= 92")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 1)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("sr_no = 92")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 1)

        request1 = (
            bios.isql()
            .select()
            .from_signal(SIGNAL["signalName"])
            .where("marks >= 63.3")
            .time_range(start, bios.time.seconds(5))
            .build()
        )
        resp1 = self.session.execute(request1)
        self.assertIsNotNone(resp1)
        windows1 = resp1.get_data_windows()
        self.assertEqual(len(windows1), 1)
        records1 = windows1[0].records
        self.assertEqual(len(records1), 2)


if __name__ == "__main__":
    pytest.main(sys.argv)
