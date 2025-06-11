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
import os
import sys
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_signal, setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_rollup_intervals

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

INTERVAL_MS = get_rollup_intervals()
SIGNAL_NAME = "signal_rollup_attr_conflict"
SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "intAttribute", "type": "Integer"},
        {"attributeName": "intAttribute1", "type": "Integer"},
        {"attributeName": "count()", "type": "String"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "groupByInt",
                "dimensions": ["intAttribute"],
                "attributes": ["intAttribute1"],
                "featureInterval": INTERVAL_MS,
            },
        ]
    },
}


class TestRollupAttrConflict(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test(self):
        with self.assertRaises(ServiceError) as error_context:
            setup_signal(self.session, SIGNAL_CONFIG)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*string must start with an alphanumeric followed by alphanumerics or underscores*",
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
