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
from bios.models import Metric, MetricFunction


class MetricParserTest(unittest.TestCase):
    def test_sum(self):
        metric = Metric("sum(abc)")
        self.assertEqual(metric.function, MetricFunction.SUM)
        self.assertEqual(metric.of, "abc")
        self.assertIsNone(metric.get_as())

    def test_count(self):
        metric = Metric("count()")
        self.assertEqual(metric.function, MetricFunction.COUNT)
        self.assertIsNone(metric.of)
        self.assertIsNone(metric.get_as())

    def test_min(self):
        metric = Metric("min(latency)").set_as("minimumLatency")
        self.assertEqual(metric.function, MetricFunction.MIN)
        self.assertEqual(metric.of, "latency")
        self.assertEqual(metric.get_as(), "minimumLatency")

    def test_max(self):
        metric = Metric("max(latency)")
        self.assertEqual(metric.function, MetricFunction.MAX)
        self.assertEqual(metric.of, "latency")

    def test_last(self):
        metric = Metric("last(status)")
        self.assertEqual(metric.function, MetricFunction.LAST)
        self.assertEqual(metric.of, "status")

    def test_invalid_count(self):
        with self.assertRaises(ServiceError) as ctx:
            Metric("count(sss)")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_invalid_sum(self):
        with self.assertRaises(ServiceError) as ctx:
            Metric("sum()")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_unknown_function(self):
        with self.assertRaises(ServiceError) as ctx:
            Metric("nosuch(abc)")
        self.assertEqual(ctx.exception.error_code, ErrorCode.INVALID_ARGUMENT)


if __name__ == "__main__":
    unittest.main()
