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
import random
import string
import sys
import unittest

import pytest

from bios import ServiceError, ErrorCode, InsertBulkError
import bios
from bios.isql_request import ISqlRequest
from bios_tutils import get_minimum_signal
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "biosInsertBulkTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIGNAL_MAXIMUM = {
    "signalName": "maximum",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "one", "type": "string"},
        {"attributeName": "three", "type": "string"},
        {"attributeName": "four", "type": "Decimal"},
        {"attributeName": "two", "type": "integer"},
    ],
}


def get_random_string(length=random.randint(4, 8)):
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for _ in range(length))
    return result_str


class BiosInsertBulkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

        cls.SIGNAL_MINIMUM = get_minimum_signal()

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            admin.create_signal(cls.SIGNAL_MINIMUM)
            admin.create_signal(SIGNAL_MAXIMUM)

        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass

        cls.session.close()

    def test_bios_insert_bulk(self):
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_MINIMUM["signalName"])
            .csv_bulk(["str", "another", "more"])
            .build()
        )
        ret = self.session.execute(request)
        self.assertEqual(len(ret.records), 3)

    def test_bios_insert_bulk_real_bulk_50(self):
        bulk_request = [get_random_string() for _ in range(50)]
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_MINIMUM["signalName"])
            .csv_bulk(bulk_request)
            .build()
        )
        ret = self.session.execute(request)
        self.assertEqual(len(ret.records), len(bulk_request))

    @unittest.skip("Time")
    def test_bios_insert_bulk_real_bulk_5000(self):
        bulk_request = [get_random_string() for _ in range(5000)]
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_MINIMUM["signalName"])
            .csv(bulk_request)
            .build()
        )
        ret = self.session.execute(request)
        self.assertEqual(len(ret), len(bulk_request))

    @unittest.skip("Time")
    def test_bios_insert_bulk_real_bulk_100000(self):
        bulk_request = [get_random_string() for _ in range(100)] * 1000
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_MINIMUM["signalName"])
            .csv(bulk_request)
            .build()
        )
        ret = self.session.execute(request)
        self.assertEqual(len(ret), len(bulk_request))

    def test_bios_insert_bulk_real_bulk_more_than_100k(self):
        bulk_request = [get_random_string() for _ in range(100)] * 1000
        bulk_request.append("some")
        with self.assertRaises(ServiceError) as context:
            request = (
                ISqlRequest()
                .insert()
                .into(self.SIGNAL_MINIMUM["signalName"])
                .csv(bulk_request)
                .build()
            )
            self.session.execute(request)
        self.assertEqual(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_bios_insert_bulk_partial_failure(self):
        request = (
            ISqlRequest()
            .insert()
            .into(self.SIGNAL_MINIMUM["signalName"])
            .csv_bulk(["str", "another", "more", "2,1,1", "a, s, d"])
            .build()
        )
        with self.assertRaises(InsertBulkError) as context:
            self.session.execute(request)
        exception = context.exception
        self.assertEqual(exception.error_code, ErrorCode.SCHEMA_MISMATCHED)
        errors = exception.get_errors()
        self.assertEqual(errors[3].error_code, ErrorCode.SCHEMA_MISMATCHED)
        print(errors[3].message)
        message = (
            "Source event string has more values than pre-defined;"
            " tenant=biosInsertBulkTest, signal=minimum"
        )
        self.assertEqual(errors[3].message, message)
        self.assertEqual(errors[4].error_code, ErrorCode.SCHEMA_MISMATCHED)
        self.assertEqual(errors[4].message, message)
        for _, error in errors.items():
            print(error)


if __name__ == "__main__":
    pytest.main(sys.argv)
