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

from deli.data_processor import AttributeFinder


class TestAttributeFinder(unittest.TestCase):
    """Tests AttributeFinder class"""

    def test_no_search_path(self):
        """Fundamental test without search path"""
        finder = AttributeFinder("")
        records = finder.search_records({"hello": "world", "hi": "there"})
        self.assertEqual(records, [{"hello": "world", "hi": "there"}])
        self.assertEqual(finder.find(records[0], "hello"), "world")
        self.assertEqual(finder.find(records[0], "hi"), "there")

    def test_search_path(self):
        """Fundamental test with search path"""
        with open("test/resources/double_nested.json", "r", encoding="utf-8") as file:
            message = json.load(file)
        finder = AttributeFinder("**/items/*")
        self.assertEqual(finder.find(message, "key_values/user"), "admin@webhookTest")
        records = finder.search_records(message)
        self.assertEqual(
            records,
            [
                {
                    "user": "Manish",
                    "item": "Crocin",
                    "quantity": "6",
                    "date_str": "2020-12-18",
                },
                {
                    "user": "Manish",
                    "item": "Dolo650",
                    "quantity": "10",
                    "date_str": "2020-12-18",
                },
                {
                    "user": "Manish",
                    "item": "Dolo650",
                    "quantity": "10",
                    "date_str": "2020-12-18",
                },
                {
                    "user": "Manish",
                    "item": "Betadine",
                    "quantity": "1",
                    "date_str": "2020-12-18",
                },
                {
                    "user": "Aditya",
                    "item": "Crocin",
                    "quantity": "5",
                    "date_str": "2020-12-18",
                },
                {
                    "user": "Aditya",
                    "item": "Dolo650",
                    "quantity": "12",
                    "date_str": "2020-12-18",
                },
                {
                    "user": "Aditya",
                    "item": "Betadine",
                    "quantity": "3",
                    "date_str": "2020-12-18",
                },
            ],
        )
        self.assertEqual(finder.find(records[6], "item"), "Betadine")
        self.assertEqual(finder.find(records[6], "user"), "Aditya")
        self.assertEqual(finder.find(records[6], "quantity"), "3")
        self.assertEqual(finder.find(records[6], "date_str"), "2020-12-18")


if __name__ == "__main__":
    unittest.main()
