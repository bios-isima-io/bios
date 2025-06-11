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

import unittest

from bios import ErrorCode, ServiceError


class ServiceErrorTest(unittest.TestCase):
    def test_basic_constructor(self):
        error = ServiceError(ErrorCode.INVALID_ARGUMENT, "Invalid argument")
        self.assertEqual(error.error_code, ErrorCode.INVALID_ARGUMENT)
        self.assertEqual(error.message, "Invalid argument")
        self.assertIsNone(error.endpoint)
        self.assertEqual(error.params, {})
        self.assertEqual(
            str(error),
            "error_code=INVALID_ARGUMENT, message=Invalid argument",
        )

    def test_set_endpoint(self):
        error = ServiceError(
            ErrorCode.GENERIC_SERVER_ERROR,
            "",
            endpoint="201.334.51.79:443",
        )
        self.assertEqual(error.error_code, ErrorCode.GENERIC_SERVER_ERROR)
        self.assertEqual(error.message, "")
        self.assertEqual(error.endpoint, "201.334.51.79:443")
        self.assertEqual(error.params, {})
        self.assertEqual(
            str(error),
            "error_code=GENERIC_SERVER_ERROR, endpoint=201.334.51.79:443," " message=",
        )

    def test_set_params(self):
        error = ServiceError(
            ErrorCode.BAD_INPUT,
            "wrong type",
            params={"hello": "world"},
        )
        self.assertEqual(error.error_code, ErrorCode.BAD_INPUT)
        self.assertEqual(error.message, "wrong type")
        self.assertIsNone(error.endpoint)
        self.assertEqual(error.params.get("hello"), "world")
        self.assertEqual(
            str(error),
            "error_code=BAD_INPUT, message=wrong type, params={'hello': 'world'}",
        )

    def test_set_all(self):
        error = ServiceError(
            ErrorCode.NO_SUCH_TENANT,
            "No such tenant; tenant=IDontExist",
            params={"name": "IDontExist"},
            endpoint="123.45.67.89:8080",
        )
        self.assertEqual(error.error_code, ErrorCode.NO_SUCH_TENANT)
        self.assertEqual(error.message, "No such tenant; tenant=IDontExist")
        self.assertEqual(error.endpoint, "123.45.67.89:8080")
        self.assertEqual(error.params.get("name"), "IDontExist")
        self.assertEqual(
            str(error),
            "error_code=NO_SUCH_TENANT, endpoint=123.45.67.89:8080,"
            " message=No such tenant; tenant=IDontExist, params={'name': 'IDontExist'}",
        )


if __name__ == "__main__":
    unittest.main()
