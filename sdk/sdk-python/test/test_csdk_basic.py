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
import sys
import unittest

import pytest

from bios._csdk import CSdkOperationId, CSdkSession
from bios._csdk._error_handler import _ErrorHandler
from bios.errors import ErrorCode, ServiceError


class CSdkBasicTest(unittest.TestCase):
    def test_operation_ids(self):
        """Iterate CSdkOperationId entries. The test ask C-SDK for the name of each operation ID.
        Then compare the returned name with the OperationId's own name. This verifies operation
        ID consistency between Python wrapper and C-SDK."""
        for op_id in CSdkOperationId:
            name = CSdkSession.get_operation_name(op_id)
            assert op_id.name == name

    def test_error_codes(self):
        """Iterate ErrorCode entries. The test ask C-SDK for the name of each status code.
        Then compare the returned name with the ServiceError's own name. This verifies error
        code consistency between Python wrapper and C-SDK."""
        error_codes = CSdkSession.list_error_codes()
        for error_code in error_codes:
            name = CSdkSession.get_status_name(error_code)
            self.assertEqual(error_code.name, name)

    def test_error_handler(self):
        handler = _ErrorHandler()
        self.assertEqual(
            handler.resolve_code(0x210004),
            ErrorCode.TENANT_ALREADY_EXISTS,
        )
        self.assertIsNone(handler.handle_response(0, "{}"))
        try:
            handler.handle_response(-1, "{}")
            self.fail("Exception is expected")
        except ServiceError:
            pass
        error = None
        try:
            handler.handle_response(
                2097153,
                '{"message":"Failed to verify server certificate: self signed certificate (18)"}',
            )
            self.fail("Exception is expected")
        except ServiceError as err:
            error = err
        self.assertIsNotNone(error)
        self.assertEqual(error.error_code, ErrorCode.SERVER_CONNECTION_FAILURE)
        self.assertEqual(
            error.message,
            "Failed to verify server certificate: self signed certificate (18)",
        )

    @unittest.skip("server dependency -- would be moved to IT")
    def test_async_call(self):
        # CSdkSession.set_log_level(logging.DEBUG)
        # logging.disable(logging.NOTSET)
        logging.basicConfig(level=logging.DEBUG)
        session = CSdkSession()
        session.start_session(
            "localhost",
            443,
            True,
            "/temp",
            "/home/naoki/workspace/tfos/test_data/cacerts.pem",
        )
        # print('SESSION:', session)
        session.login("dapi_test", "test_stream", "extract", "extract")
        session.close()


if __name__ == "__main__":
    pytest.main(sys.argv)
