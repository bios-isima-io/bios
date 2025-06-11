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
import pytest
import bios
from bios import ServiceError
from bios.errors import ErrorCode
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME, setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url


class TestRollupAdmin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ADMIN_USER = admin_user + "@" + TENANT_NAME
        setup_tenant_config()
        cls.session = bios.login(ep_url(), cls.ADMIN_USER, admin_pass)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_postprocess_without_feature_interval(self):
        signal = {
            "signalName": "RollupTest",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {"featureName": "priceByName", "dimensions": ["name"], "attributes": ["price"]}
                ]
            },
        }
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            "Constraint violation.*featureInterval: The value must be set*",
        )

    def test_postprocess_invalid_dimension(self):
        signal = {
            "signalName": "RollupTest",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "priceByName",
                        "dimensions": ["name_invalid"],
                        "attributes": ["price"],
                        "featureInterval": 60000,
                    }
                ]
            },
        }
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            "Constraint violation.*'name_invalid' in feature dimensions not found*",
        )

    def test_postprocess_invalid_attribute(self):
        signal = {
            "signalName": "RollupTest",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "priceByName",
                        "dimensions": ["name"],
                        "attributes": ["price_invalid"],
                        "featureInterval": 60000,
                    }
                ]
            },
        }
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            "Constraint violation.*'price_invalid' in feature attributes not found*",
        )

    def test_postprocess_duplicate_dimensions(self):
        signal = {
            "signalName": "RollupTest",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "priceByName",
                        "dimensions": ["product", "product"],
                        "attributes": [],
                        "featureInterval": 60000,
                    }
                ]
            },
        }
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*Feature properties 'dimensions' and 'attributes' may not have duplicates*",
        )

    def test_postprocess_duplicate_attributes(self):
        signal = {
            "signalName": "RollupTest",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "priceByName",
                        "dimensions": [],
                        "attributes": ["product", "product"],
                        "featureInterval": 60000,
                    }
                ]
            },
        }
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*Feature properties 'dimensions' and 'attributes' may not have duplicates*",
        )

    def test_postprocess_duplicates(self):
        signal = {
            "signalName": "RollupTest",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "priceByName",
                        "dimensions": ["product"],
                        "attributes": ["product"],
                        "featureInterval": 60000,
                    }
                ]
            },
        }
        with self.assertRaises(ServiceError) as error_context:
            self.session.create_signal(signal)
        self.assertEqual(error_context.exception.error_code, ErrorCode.BAD_INPUT)
        self.assertRegex(
            error_context.exception.message,
            ".*Feature properties 'dimensions' and 'attributes' may not have duplicates*",
        )


if __name__ == "__main__":
    pytest.main(sys.argv)
