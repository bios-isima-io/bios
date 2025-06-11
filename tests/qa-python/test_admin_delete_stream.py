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
from bios.errors import ErrorCode
import bios
from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME,
    setup_signal,
    setup_tenant_config,
    try_delete_signal,
)
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

SIGNAL = {
    "signalName": "adminDeleteTestSignal",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "stringAttribute", "type": "String"}],
}


class AdminDeleteStreamTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        setup_signal(cls.session, SIGNAL)

    @classmethod
    def tearDownClass(cls):
        try_delete_signal(cls.session, SIGNAL["signalName"])
        cls.session.close()

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_insert_select_after_signal_delete(self):
        signal = {
            "signalName": "adminDeleteTestSignal1",
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "stringAttribute", "type": "String"}],
        }
        self.session.create_signal(signal)

        start = self.insert_into_signal(signal["signalName"], ["1", "2", "3"])

        select_request = (
            bios.isql()
            .select()
            .from_signal(signal["signalName"])
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 3)

        self.session.delete_signal(signal["signalName"])

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal(signal["signalName"], "4")
        self.assertEqual(error_context.exception.error_code, ErrorCode.NO_SUCH_STREAM)

        select_request = (
            bios.isql()
            .select()
            .from_signal(signal["signalName"])
            .time_range(start, bios.time.now() - start)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.NO_SUCH_STREAM)

    def test_insert_select_after_tenant_delete(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TENANT_NAME + "adminDeleteTest")
            except bios.ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TENANT_NAME + "adminDeleteTest"})
        session = bios.login(ep_url(), ADMIN_USER + "adminDeleteTest", admin_pass)
        self.addCleanup(session.close)
        signal = {
            "signalName": "signal1",
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "stringAttribute", "type": "String"}],
        }
        session.create_signal(signal)

        request = bios.isql().insert().into(signal["signalName"]).csv_bulk(["1", "2", "3"]).build()
        session.execute(request)

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.delete_tenant(TENANT_NAME + "adminDeleteTest")

        request = bios.isql().insert().into(signal["signalName"]).csv_bulk(["4"]).build()
        with self.assertRaises(ServiceError) as error_context:
            session.execute(request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.NO_SUCH_TENANT)

        select_request = (
            bios.isql()
            .select()
            .from_signal(signal["signalName"])
            .time_range(bios.time.now(), bios.time.seconds(-10))
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            session.execute(select_request)
        self.assertEqual(error_context.exception.error_code, ErrorCode.NO_SUCH_TENANT)

        session.close()


if __name__ == "__main__":
    pytest.main(sys.argv)
