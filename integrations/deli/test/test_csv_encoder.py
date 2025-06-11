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

from deli.delivery_channel.bios_delivery_channel import CsvEncoder


class TestDataFeeder(unittest.TestCase):
    """Verifies CSV encoder"""

    def test_fundamental(self):
        attributes = {"one": None, "two": None}
        self.assertEqual(
            CsvEncoder.encode(attributes, {"one": "Hello", "two": "world"}), "Hello,world"
        )

    def test_non_string(self):
        attributes = {"integer": None, "decimal": None, "boolean": None}
        self.assertEqual(
            CsvEncoder.encode(attributes, {"integer": 1, "decimal": 2.1, "boolean": True}),
            "1,2.1,true",
        )

    def test_double_quotes(self):
        attributes = {
            "number": None,
            "dquotes": None,
            "comma": None,
            "lineFeed": None,
            "newLine": None,
        }
        self.assertEqual(
            CsvEncoder.encode(
                attributes,
                {
                    "number": 1.2,
                    "dquotes": 'he said, "hello world."',
                    "comma": "one,two",
                    "lineFeed": "abc\rdef",
                    "newLine": "123\n456",
                },
            ),
            '1.2,"he said, ""hello world.""","one,two","abc\rdef","123\n456"',
        )

    def test_missing_attributes(self):
        attributes = {"integer": None, "decimal": None, "boolean": None, "string": None}
        self.assertEqual(
            CsvEncoder.encode(attributes, {"integer": 1, "boolean": False}),
            "1,,false,",
        )

    def test_missing_attributes_with_default(self):
        attributes = {"integer": None, "decimal": -1.0, "boolean": None, "string": "MISSING"}
        self.assertEqual(
            CsvEncoder.encode(attributes, {"integer": 1, "boolean": False}),
            "1,-1.0,false,MISSING",
        )

    def test_empty_string(self):
        attributes = {
            "integer": None,
            "decimal": None,
            "boolean": None,
            "empty": None,
            "blank": None,
        }
        self.assertEqual(
            CsvEncoder.encode(
                attributes, {"integer": 1, "boolean": False, "empty": "", "blank": " "}
            ),
            '1,,false,""," "',
        )

    def test_extra_attributes(self):
        attributes = {"one": None, "two": None}
        self.assertEqual(
            CsvEncoder.encode(
                attributes,
                {"one": "hello", "two": 2, "extra1": "this shouldn't exist", "extra2": 123},
            ),
            "hello,2",
        )


if __name__ == "__main__":
    unittest.main()
