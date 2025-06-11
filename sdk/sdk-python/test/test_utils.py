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
import unittest

from bios._utils import check_str_enum
from bios.errors import ErrorCode, ServiceError


class UtilsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ALLOWED_VALUES = {"SanMateo", "SanRamon", "RedwoodCity"}

    def test_enum_check_case_insensitive(self):
        check_str_enum("sanmateo", "city", self.ALLOWED_VALUES, True)
        check_str_enum("SanRamon", "city", self.ALLOWED_VALUES, True)
        check_str_enum("REDWOODCITY", "city", self.ALLOWED_VALUES, True)
        with self.assertRaises(ServiceError) as ec:
            check_str_enum("SanFrancisco", "city", self.ALLOWED_VALUES, False)
        self.assertEqual(ec.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_enum_check_case_sensitive(self):
        check_str_enum("SanMateo", "city", self.ALLOWED_VALUES, False)
        with self.assertRaises(ServiceError) as ec:
            check_str_enum("sanmateo", "city", self.ALLOWED_VALUES, False)
        self.assertEqual(ec.exception.error_code, ErrorCode.INVALID_ARGUMENT)


if __name__ == "__main__":
    unittest.main()
