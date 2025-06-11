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

from bios import ErrorCode, ServiceError
from bios.client import Client


@unittest.skip("the way to validate request has changed, deferring to QA tests")
class BiosClientValidationTest(unittest.TestCase):
    @classmethod
    @mock.patch("bios.client.BiosCSdkSession")
    def setUpClass(cls, mock_session):
        cls.session = Client("mock_host", 443, True, ".")
        cls.mock_csdk = mock_session

    def test_get_signals_names_parameter_validation(self):
        self.assertIsNotNone(self.session)
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(names=10)
        self.assertTrue("Invalid argument", context.exception)
        self.assertTrue(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(names=["minimum", 5])
        self.assertTrue("Invalid argument", context.exception)
        self.assertTrue(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_get_signals_detail_parameter_validation(self):
        self.assertIsNotNone(self.session)
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(detail=10)
        self.assertTrue("Invalid argument", context.exception)
        self.assertTrue(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(detail="minimum")
        self.assertTrue("Invalid argument", context.exception)

    def test_get_signals_include_internal_parameter_validation(self):
        self.assertIsNotNone(self.session)
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(include_internal=10)
        self.assertTrue("Invalid argument", context.exception)
        self.assertTrue(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        with self.assertRaises(ServiceError) as context:
            self.session.get_signals(include_internal="minimum")
        self.assertTrue("Invalid argument", context.exception)

    def test_get_signal_with_signal_name(self):
        self.assertIsNotNone(self.session)
        params = (10, [], {})
        self.assertIsNotNone(self.session)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.get_signal(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_create_signal_param_validation_invalid(self):
        self.assertIsNotNone(self.session)
        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(10)
        self.assertTrue("Invalid argument", context.exception)
        self.assertTrue(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)
        with self.assertRaises(ServiceError) as context:
            self.session.create_signal(["somesignal"])
        self.assertTrue("Invalid argument", context.exception)
        self.assertTrue(context.exception.error_code, ErrorCode.INVALID_ARGUMENT)

    def test_create_signal_param_validation_valid(self):
        self.assertIsNotNone(self.session)
        self.session.create_signal("foosignal")
        self.mock_csdk.assert_called()
        self.session.create_signal({"name": "foosignal"})
        self.mock_csdk.assert_called()

    def test_update_signal_param_validation_valid(self):
        self.assertIsNotNone(self.session)
        self.session.update_signal("name", "signalConfig")
        self.mock_csdk.assert_called()
        self.session.update_signal("name", {"signalName": "signalConfig"})
        self.mock_csdk.assert_called()

    def test_update_signal_param_validation_invalid(self):
        params = (
            (10, "signalConfig"),
            ("signal", 10),
            (10, 10),
            ("signal", ["list"]),
            (["list"], "signalConfig"),
            ({"d": "dict"}, "signalConfig"),
        )
        self.assertIsNotNone(self.session)
        for name, config in params:
            with self.subTest(name=name, config=config):
                with self.assertRaises(ServiceError) as context:
                    self.session.update_signal(name, config)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_delete_signal_param_validation_valid(self):
        self.assertIsNotNone(self.session)
        self.session.delete_signal("name")
        self.mock_csdk.assert_called()

    def test_delete_signal_param_validation_invalid(self):
        params = (10, [], {})
        self.assertIsNotNone(self.session)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.delete_signal(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_get_contexts_param_validation_valid(self):
        self.assertIsNotNone(self.session)
        self.session.get_contexts()
        self.mock_csdk.assert_called()
        params = (
            ("name", None),
            ("Name", True),
            ("Name", False),
            (None, True),
            (None, False),
        )
        for names, detail in params:
            with self.subTest(names=names, detail=detail):
                self.session.get_contexts(names=names, detail=detail)
                self.mock_csdk.assert_called()

    def test_get_contexts_param_validation_invalid(self):
        self.assertIsNotNone(self.session)
        params = (
            (10, None),
            (1, True),
            ({}, False),
            (None, "Trye"),
            (None, []),
        )
        for names, detail in params:
            with self.subTest(names=names, detail=detail):
                with self.assertRaises(ServiceError) as context:
                    self.session.get_contexts(names=names, detail=detail)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_get_context_param_validation_valid(self):
        params = ("some_name", "", "long_name_in_context")
        for name in params:
            with self.subTest(name=name):
                self.session.get_context(name)
                self.mock_csdk.assert_called()

    def test_get_context_param_validation_invalid(self):
        params = (10, [], {}, 12.5)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.get_context(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_create_context_param_validation_valid(self):
        params = (
            "some_name",
            "",
            "long_name_in_context",
            {"name": "somename"},
        )
        for name in params:
            with self.subTest(name=name):
                self.session.create_context(name)
                self.mock_csdk.assert_called()

    def test_create_context_param_validation_invalid(self):
        params = (10, [], 12.5)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.create_context(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_update_context_param_validation_valid(self):
        params = (
            ("context_name", "context_config"),
            ("context_name", {"con": "fig"}),
            ("", {}),
        )
        for name, context in params:
            with self.subTest(name=name, context=context):
                self.session.update_context(name, context)
                self.mock_csdk.assert_called()

    def test_update_context_param_validation_invalid(self):
        params = (
            (10, "context_config"),
            ("context_name", 10),
            (10, 12),
            ([], "config"),
            ("config", []),
            ({}, {}),
        )
        for name, context in params:
            with self.subTest(name=name, context=context):
                with self.assertRaises(ServiceError) as context:
                    self.session.update_context(name, context)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_delete_context_param_validation_valid(self):
        params = ("some_name", "", "long_name_in_context")
        for name in params:
            with self.subTest(name=name):
                self.session.delete_context(name)
                self.mock_csdk.assert_called()

    def test_delete_context_param_validation_invalid(self):
        params = (10, [], 12.5)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.delete_context(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_list_tenants_param_validation_valid(self):
        self.assertIsNotNone(self.session)
        self.session.list_tenants()
        self.mock_csdk.assert_called()
        params = ("name", "", ["name", "another_name"], None)
        for names in params:
            with self.subTest(names=names):
                self.session.list_tenants(names=names)
                self.mock_csdk.assert_called()

    def test_list_tenants_param_validation_invalid(self):
        self.assertIsNotNone(self.session)
        params = (10, 12.5, {})
        for names in params:
            with self.subTest(names=names):
                with self.assertRaises(ServiceError) as context:
                    self.session.list_tenants(names=names)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_get_tenant_param_validation_valid(self):
        params = (
            ("name", True, True),
            ("", False, True),
            ("name", False, False),
            ("name", True, False),
        )
        for name, detail, internal in params:
            with self.subTest(name=name, detail=detail, internal=internal):
                self.session.get_tenant(name, detail, internal)
                self.mock_csdk.assert_called()

    def test_get_tenant_param_validation_invalid(self):
        params = (
            ("name", True, ""),
            ("", "True", True),
            (10, False, False),
            (["name"], True, False),
            ("name", True, "False"),
        )
        for name, detail, internal in params:
            with self.subTest(name=name, detail=detail, internal=internal):
                with self.assertRaises(ServiceError) as context:
                    self.session.get_tenant(name, detail, internal)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_create_tenant_param_validation_valid(self):
        params = (
            "some_name",
            "",
            "long_name_in_tenant",
            {"name": "somename"},
        )
        for name in params:
            with self.subTest(name=name):
                self.session.create_tenant(name)
                self.mock_csdk.assert_called()

    def test_create_tenant_param_validation_invalid(self):
        params = (10, [], 12.5)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.create_tenant(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )

    def test_delete_tenant_param_validation_valid(self):
        params = ("some_name", "", "long_name_in_tenant")
        for name in params:
            with self.subTest(name=name):
                self.session.delete_tenant(name)
                self.mock_csdk.assert_called()

    def test_delete_tenant_param_validation_invalid(self):
        params = (10, [], 12.5)
        for name in params:
            with self.subTest(name=name):
                with self.assertRaises(ServiceError) as context:
                    self.session.delete_tenant(name)
                self.assertTrue("Invalid argument", context.exception)
                self.assertTrue(
                    context.exception.error_code,
                    ErrorCode.INVALID_ARGUMENT,
                )


if __name__ == "__main__":
    unittest.main()
