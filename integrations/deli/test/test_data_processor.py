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
import pprint
import unittest

from bios import ErrorCode, ServiceError

from deli.configuration import DataPickupSpec
from deli.data_processor import DataProcessor, ImportedModules
from deli.event_spec import EventSpec


class TestDataProcessor(unittest.TestCase):
    """Tests data processor"""

    def test_simple(self):
        """Test a simple data processor."""
        pickup_config_dict = {
            "attributes": [
                {"sourceAttributeName": "hi"},
                {"sourceAttributeName": "hello", "as": "greetings"},
            ]
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)
        processor = DataProcessor("test_flow", pickup_spec)
        self.assertEqual(len(processor.units), 2)
        # source attributes are covered
        processed = processor.process({"hello": "world", "hi": "there", "howdy": "ok"})
        assert processed[0][0].attributes == {"greetings": "world", "hi": "there"}
        assert processed[0][0].operation == EventSpec.Operation.INSERT

        # source attribute are missing
        processed = processor.process({"hello": "world", "howdy": "ok"})
        assert processed[0][0].attributes == {"greetings": "world", "hi": None}
        assert processed[0][0].operation == EventSpec.Operation.INSERT

        processed = processor.process({"greetings": "hello", "hi": "there", "howdy": "ok"})
        assert processed[0][0].attributes == {"greetings": None, "hi": "there"}
        assert processed[0][0].operation == EventSpec.Operation.INSERT

        processed = processor.process({"howdy": "ok"})
        assert processed[0][0].attributes == {"greetings": None, "hi": None}
        assert processed[0][0].operation == EventSpec.Operation.INSERT

    def test_nested_attributes(self):
        """Test a simple data processor."""
        pickup_config_dict = {
            "attributeSearchPath": "event",
            "attributes": [
                {"sourceAttributeName": "/event/triggerTime", "as": "eventTime"},
                {"sourceAttributeName": "eventId"},
                {"sourceAttributeName": "visitor/visitorId", "as": "visitorId"},
                {"sourceAttributeName": "dimensions/eVar1/data/*", "as": "eVar1"},
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)
        processor = DataProcessor("test_flow", pickup_spec)
        self.assertEqual(len(processor.units), 4)

        input_data_normal = {
            "event": {
                "triggerTime": 12345,
                "eventId": "myEventId",
                "visitor": {"visitorId": 3344567},
                "dimensions": {
                    "eVar1": {
                        "type": "string",
                        "data": ["Test data evar1"],
                        "name": "eVar30",
                        "source": "session summary",
                    }
                },
            }
        }

        # source attributes are covered
        processed = processor.process(input_data_normal)
        assert processed[0][0].attributes == {
            "eventtime": 12345,
            "eventid": "myEventId",
            "visitorid": 3344567,
            "evar1": "Test data evar1",
        }
        assert processed[0][0].operation == EventSpec.Operation.INSERT

        input_data_empty_data_value = {
            "event": {
                "triggerTime": 12345,
                "eventId": "myEventId",
                "visitor": {"visitorId": 3344567},
                "dimensions": {
                    "eVar1": {
                        "type": "string",
                        "data": [],
                        "name": "eVar30",
                        "source": "session summary",
                    }
                },
            }
        }

        # source attributes are not covered
        processed = processor.process(input_data_empty_data_value)
        assert processed[0][0].attributes == {
            "eventtime": 12345,
            "eventid": "myEventId",
            "visitorid": 3344567,
            "evar1": None,
        }

    def test_transformations(self):
        """Verifies fundamental transformation behavior."""
        pickup_config_dict = {
            "attributes": [
                {
                    "sourceAttributeNames": ["hour", "minute"],
                    "transforms": [{"rule": "lambda h, m: f'{h}:{m}'", "as": "time"}],
                },
                {
                    "sourceAttributeName": "quantity",
                    "transforms": [
                        {"rule": "lambda x: x", "as": "quantity"},
                        {"rule": "lambda x: x * x", "as": "squaredQuantity"},
                        {"rule": "lambda x: x * x * x", "as": "cubedQuantity"},
                    ],
                },
            ]
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)
        processor = DataProcessor("test_flow", pickup_spec)
        self.assertEqual(
            processor.process({"hour": "13", "minute": "24", "quantity": 3})[0][0].attributes,
            {"time": "13:24", "quantity": 3, "squaredquantity": 9, "cubedquantity": 27},
        )
        # invalid input
        with self.assertRaises(ServiceError) as error_context:
            processor.process({"hour": 13, "minute": 24, "quantity": "3"})
        exception = error_context.exception
        self.assertEqual(exception.error_code, ErrorCode.BAD_INPUT)

    def test_processes(self):
        """Verifies attribute processors."""
        module_src = """
def calc_minutes(hours, minutes):
    return hours * 60 + minutes

def calc_millis(seconds, milliseconds):
    return seconds * 1000 + milliseconds

def convert_to_hex(src):
    return hex(src)
"""
        processor_configs = {"test_processor_module": module_src}
        pickup_config_dict = {
            "attributes": [
                {
                    "sourceAttributeNames": ["hour", "minute"],
                    "processes": [
                        {
                            "processorName": "test_processor_module",
                            "method": "calc_minutes",
                        }
                    ],
                    "as": "elapsedMinutes",
                },
                {
                    "sourceAttributeNames": ["seconds", "millis"],
                    "processes": [
                        {
                            "processorName": "test_processor_module",
                            "method": "calc_millis",
                        }
                    ],
                    "transforms": [{"rule": "lambda x: x/1000", "as": "seconds"}],
                },
                {
                    "sourceAttributeName": "int_value",
                    "processes": [
                        {"processorName": "test_processor_module", "method": "convert_to_hex"}
                    ],
                    "as": "hexValue",
                },
            ]
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)
        processor = DataProcessor(
            "test_flow", pickup_spec, ImportedModules(processor_configs).load()
        )
        processed = processor.process(
            {"hour": 2, "minute": 30, "seconds": 1, "millis": 320, "int_value": 238}
        )
        self.assertEqual(
            processed[0][0].attributes,
            {"elapsedminutes": 150, "seconds": 1.32, "hexvalue": "0xee"},
        )

    def test_filter_for_none(self):
        """Test a simple data processor."""
        pickup_config_dict = {
            "attributeSearchPath": "",
            "filters": [{"sourceAttributeName": "zero", "filter": "lambda value: value is None"}],
            "attributes": [
                {"sourceAttributeName": "one", "as": "testName"},
                {
                    "sourceAttributeName": "two",
                    "transforms": [{"rule": "lambda dummy: 'filter for none'", "as": "testParam"}],
                },
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)
        processor = DataProcessor("test_flow", pickup_spec)

        input_data_hit = {
            "zero": None,
            "one": "hello world",
            "user": "admin@test",
            "pass": "admin",
        }

        processed = processor.process(input_data_hit)
        assert processed[0][0].attributes == {
            "testname": "hello world",
            "testparam": "filter for none",
        }


if __name__ == "__main__":
    unittest.main()
