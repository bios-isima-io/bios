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
import uuid

import bios

from setup_common import (
    BIOS_QA_COMMON_ADMIN_USER as ADMIN_USER,
    setup_tenant_config,
)
from tsetup import get_endpoint_url as ep_url
from tsetup import admin_pass


class ReportsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    def tearDown(self):
        self.session.close()

    def test_reports(self):
        # Create 1st report and verify.
        report_id1 = uuid.uuid1()
        report1 = {}
        report1["reportId"] = str(report_id1)
        report1["reportName"] = "Test report 1"
        self.session.put_report_config(report1)
        reports = self.session.get_report_configs()
        self.assertGreaterEqual(len(reports["reportConfigs"]), 1)

        # Create 2nd report and verify.
        report_id2 = uuid.uuid1()
        report2 = {}
        report2["reportId"] = str(report_id2)
        report2["reportName"] = "Test report 2"
        self.session.put_report_config(report2)
        reports = self.session.get_report_configs()
        self.assertGreaterEqual(len(reports["reportConfigs"]), 2)

        reports = self.session.get_report_configs([str(report_id1)])
        self.assertEqual(len(reports["reportConfigs"]), 1)
        self.assertEqual(reports["reportConfigs"][0]["reportName"], "Test report 1")

        reports = self.session.get_report_configs([str(report_id2)])
        self.assertEqual(len(reports["reportConfigs"]), 1)
        self.assertEqual(reports["reportConfigs"][0]["reportName"], "Test report 2")

        # Delete 2nd report and verify.
        self.session.delete_report(str(report_id2))
        reports = self.session.get_report_configs([str(report_id2)])
        self.assertEqual(len(reports["reportConfigs"]), 0)

        # Get 1st report and verify.
        reports = self.session.get_report_configs([str(report_id1)])
        self.assertEqual(len(reports["reportConfigs"]), 1)
        self.assertEqual(reports["reportConfigs"][0]["reportName"], "Test report 1")


if __name__ == "__main__":
    unittest.main()
