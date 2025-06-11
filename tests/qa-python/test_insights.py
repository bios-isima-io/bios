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

import sys
import unittest
import uuid

import bios
import pytest
from tsetup import admin_pass
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_ADMIN_USER as ADMIN_USER
from setup_common import setup_tenant_config


class InsightsTest(unittest.TestCase):
    INSIGHT_NAME = "signal"

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        self.assertIsNotNone(self.session)

    def tearDown(self):
        self.session.close()

    def test_insights(self):
        # Create 1st insight and verify.
        insight_id1 = uuid.uuid1()
        insight1 = {}
        insight1["insightId"] = str(insight_id1)
        insight1["insightName"] = "Test insight 1"
        insight1["reportId"] = str(uuid.uuid1())
        insight1["fav"] = False
        self.session.put_insight_configs(self.INSIGHT_NAME, insight1)
        insight = self.session.get_insight_configs(self.INSIGHT_NAME)
        # Note: Get only returns the latest inserted insight.
        self.assertEqual(insight, insight1)

        # Create 2nd insight and verify.
        insight_id2 = uuid.uuid1()
        insight2 = {}
        insight2["insightId"] = str(insight_id2)
        insight2["insightName"] = "Test insight 2"
        insight2["reportId"] = str(uuid.uuid1())
        insight2["fav"] = True
        self.session.put_insight_configs(self.INSIGHT_NAME, insight2)
        insight = self.session.get_insight_configs(self.INSIGHT_NAME)
        # Note: Get only returns the latest inserted insight.
        self.assertEqual(insight, insight2)

        # Delete all insights and verify.
        self.session.delete_insights(self.INSIGHT_NAME)
        insights = self.session.get_insight_configs(self.INSIGHT_NAME)
        self.assertEqual(len(insights), 0)


if __name__ == "__main__":
    pytest.main(sys.argv)
