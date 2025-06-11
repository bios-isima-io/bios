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
import unittest.mock as mock

import bios
from bios._proto._bios.data_pb2 import MetricFunction
from bios.client import Client


class BiosSelectRequestTest(unittest.TestCase):
    @classmethod
    @mock.patch("bios.client.BiosCSdkSession")
    def setUpClass(cls, mock_CSdkSession):
        cls.session = Client("mock_host", 443, True, ".")
        cls.mock_csdk = mock_CSdkSession

    # TODO Add more test cases

    # regression BB-1256
    def test_last_operator(self):
        now = bios.time.now()
        statement = (
            bios.isql()
            .select("last(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by(["countryName"])
            .time_range(now, -bios.time.days(1))
            .build()
        )
        request = statement.request
        self.assertEqual(request.end_time, now)
        self.assertEqual(request.start_time, now - 86400000)
        self.assertEqual(len(request.metrics), 1)
        self.assertEqual(request.metrics[0].function, MetricFunction.LAST)
        self.assertEqual(request.metrics[0].of, "reportedCases")
        self.assertEqual(getattr(request, "from"), "covidDataSignal")
        self.assertEqual(len(request.group_by.dimensions), 1)
        self.assertEqual(request.group_by.dimensions[0], "countryName")

    def test_multi_execute(self):
        now = bios.time.now()
        statement1 = (
            bios.isql()
            .select("last(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by(["countryName"])
            .time_range(now, -bios.time.days(1))
            .build()
        )
        statement2 = (
            bios.isql()
            .select("sum(reportedCases)")
            .from_signal("covidDataSignal")
            .group_by(["continent"])
            .time_range(now, -bios.time.days(1))
            .build()
        )
        self.session.multi_execute(statement1, statement2)
        self.mock_csdk.assert_called()


if __name__ == "__main__":
    unittest.main()
