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
import math
import unittest
import pytest
import bios
from bios import ErrorCode, ServiceError
from bios.models.metric import Metric
from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME,
    setup_context,
    setup_signal,
    setup_tenant_config,
    try_delete_context,
    try_delete_signal,
)
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tutils import b64enc

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"


class MissingAttributePolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    @classmethod
    def insert_into_signal(cls, stream, records):
        if not isinstance(records, list):
            records = [records]
        request = bios.isql().insert().into(stream).csv_bulk(records).build()
        resp = cls.session.execute(request)
        return resp.records[0].timestamp

    def test_map_set_to_reject(self):
        signal_config = {
            "signalName": "alltypes_map_reject",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "intAttribute", "type": "Integer"},
                {"attributeName": "doubleAttribute", "type": "Decimal"},
                {"attributeName": "booleanAttribute", "type": "Boolean"},
                {"attributeName": "blobAttribute", "type": "Blob"},
            ],
        }
        self.session.create_signal(signal_config)
        self.addCleanup(self.session.delete_signal, signal_config["signalName"])

        self.insert_into_signal("alltypes_map_reject", "hjk,24,45.7,False," + b64enc(b"OK"))

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal("alltypes_map_reject", "hjk")
        self.assertEqual(error_context.exception.error_code, ErrorCode.SCHEMA_MISMATCHED)
        self.assertRegex(
            error_context.exception.message,
            "Source event string has less values than pre-defined*",
        )

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal("alltypes_map_reject", "hjk,,45.7,False," + b64enc(b"OK"))
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal("alltypes_map_reject", "hjk,24,,False," + b64enc(b"OK"))
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

        # BIOS-5202
        # with self.assertRaises(ServiceError) as error_context:
        #     self.insert_into_signal("alltypes_map_reject", "hjk,24,45.7,," + b64enc(b"OK"))
        # self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

        self.insert_into_signal("alltypes_map_reject", "hjk,24,45.7,False," + b64enc(b"OK"))

    @unittest.skip("BIOS-5179")
    def test_map_set_to_reject_string_blob(self):
        signal_config = {
            "signalName": "alltypes_map_reject",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String"},
                {"attributeName": "intAttribute", "type": "Integer"},
                {"attributeName": "doubleAttribute", "type": "Decimal"},
                {"attributeName": "booleanAttribute", "type": "Boolean"},
                {"attributeName": "blobAttribute", "type": "Blob"},
            ],
        }
        self.session.create_signal(signal_config)
        self.addCleanup(self.session.delete_signal, signal_config["signalName"])

        self.insert_into_signal("alltypes_map_reject", "hjk,24,45.7,False," + b64enc(b"OK"))

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal("alltypes_map_reject", ",24,45.7,False," + b64enc(b"OK"))
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

        with self.assertRaises(ServiceError) as error_context:
            self.insert_into_signal("alltypes_map_reject", "hjk,24,45.7,False,")
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)

    def test_map_set_to_store_default(self):
        signal_config = {
            "signalName": "alltypes_map_storedefault",
            "missingAttributePolicy": "storeDefaultValue",
            "attributes": [
                {"attributeName": "stringAttribute", "type": "String", "default": "N/A"},
                {"attributeName": "intAttribute", "type": "Integer", "default": 99},
                {"attributeName": "doubleAttribute", "type": "Decimal", "default": math.e},
                {"attributeName": "booleanAttribute", "type": "Boolean", "default": True},
                {"attributeName": "blobAttribute", "type": "Blob", "default": b64enc(b"None")},
            ],
        }
        self.session.create_signal(signal_config)
        self.addCleanup(self.session.delete_signal, signal_config["signalName"])

        start = self.insert_into_signal("alltypes_map_storedefault", ",,,,")

        select_request = (
            bios.isql()
            .select()
            .from_signal("alltypes_map_storedefault")
            .time_range(start, bios.time.now() - start)
            .build()
        )
        resp = self.session.execute(select_request)
        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get_data_windows()), 1)
        self.assertEqual(len(resp.get_data_windows()[0].records), 1)
        record = resp.get_data_windows()[0].records[0]
        # BIOS-5179
        # self.assertEqual(record.get("stringAttribute"), "N/A")
        self.assertEqual(record.get("intAttribute"), 99)
        self.assertEqual(record.get("doubleAttribute"), math.e)
        self.assertEqual(record.get("booleanAttribute"), True)
        # BIOS-5179
        # self.assertEqual(record.get("blobAttribute"), "None")


if __name__ == "__main__":
    pytest.main(sys.argv)
