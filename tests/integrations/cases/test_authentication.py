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

import base64
import hashlib
import hmac
import json
import pprint
import unittest

from rest_client import HttpConnection, RestClient, OperationError
from tsetup import get_endpoint_url as ep_url

import bios
from tsetup import admin_user, admin_pass, ingest_user, ingest_pass
from tsetup import get_endpoint_url as ep_url
from tsetup import get_webhook_url


class TestAuthentication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.conn = HttpConnection(get_webhook_url())
        cls.WEBHOOK_PATH = "simple-signal/filter-test"
        cls.HEADER_AUTH_PATH = "header-auth"
        cls.HMAC_AUTH_PATH_ENCODED_DIGEST = "hmac-auth-encoded-digest"
        cls.HMAC_AUTH_PATH_UNENCODED_DIGEST = "hmac-auth-unencoded-digest"
        cls.AUTHORIZATION_AUTH_PATH = "authorization-auth"

    def test_in_message(self):
        test_name = "test correct credential inmessage auth"
        body = {
            "zero": "abc",
            "one": test_name,
            "user": f"admin@{self.TENANT_NAME}",
            "pass": "admin",
        }
        status, _ = RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=body)
        self.assertEqual(status, "OK\n")

    def test_incorrect_in_message_credential(self):
        test_name = "test incorrect credential inmessage auth"
        body = {
            "zero": "abc",
            "one": test_name,
            "user": f"admin1@{self.TENANT_NAME}",
            "pass": "admin1",
        }
        try:
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=body)
            self.fail()
        except OperationError as ex:
            self.assertEqual(ex.status, 401)

    def test_missing_in_message_credential(self):
        test_name = "test missing credential inmessage auth"
        body = {
            "zero": "abc",
            "one": test_name,
        }
        try:
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH, data=body)
            self.fail()
        except OperationError as ex:
            self.assertEqual(ex.status, 401)

    def test_valid_header_auth(self):
        test_name = "valid header auth"
        headers = {
            "user": f"{ingest_user}@{self.TENANT_NAME}",
            "pass": f"{ingest_pass}",
        }
        body = {"testName": test_name, "testParam": "valid"}
        status, _ = RestClient.call(
            self.conn, "POST", self.HEADER_AUTH_PATH, headers=headers, data=body
        )
        self.assertEqual(status, "OK\n")
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 1)

    def test_invalid_header_auth(self):
        test_name = "invalid header auth"
        headers = {
            "user": f"{ingest_user}@{self.TENANT_NAME}",
            "pass": "invalid password",
        }
        body = {"testName": test_name, "testParam": "valid"}
        try:
            RestClient.call(self.conn, "POST", self.HEADER_AUTH_PATH, headers=headers, data=body)
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)

        # make sure that the message really was not delivered
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)

    def test_header_auth_missing_header(self):
        test_name = "header auth missing header"
        headers = {
            "wrong-user-header": f"{ingest_user}@{self.TENANT_NAME}",
            "wrong-password-header": "{ingest_pass}",
        }
        body = {"testName": test_name, "testParam": "valid"}
        try:
            RestClient.call(self.conn, "POST", self.HEADER_AUTH_PATH, headers=headers, data=body)
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)

        # make sure that the message really was not delivered
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)

    def test_valid_hmac_auth_encoded_digest(self):
        test_name = "valid_hmac_auth_encoded_digest"
        body = {"testName": test_name, "testParam": "valid"}
        secret = "salt"
        req_data = json.dumps(body)
        digest = hmac.new(
            secret.encode("utf-8"), req_data.encode("utf-8"), hashlib.sha256
        ).digest()
        computed_hmac = base64.b64encode(digest)
        headers = {
            "x-shopify-hmac-sha256": computed_hmac,
            "user": f"{ingest_user}@{self.TENANT_NAME}",
            "pass": f"{ingest_pass}",
        }

        status, _ = RestClient.call(
            self.conn,
            "POST",
            self.HMAC_AUTH_PATH_ENCODED_DIGEST,
            headers=headers,
            data=body,
        )
        self.assertEqual(status, "OK\n")
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 1)

    def test_invalid_hmac_auth_encoded_digest(self):
        test_name = "invalid_hmac_auth_encoded_digest"
        body = {"testName": test_name, "testParam": "valid"}
        secret = "wrong-secret"
        req_data = json.dumps(body)
        digest = hmac.new(
            secret.encode("utf-8"), req_data.encode("utf-8"), hashlib.sha256
        ).digest()
        computed_hmac = base64.b64encode(digest)
        headers = {"x-shopify-hmac-sha256": computed_hmac}

        try:
            RestClient.call(
                self.conn,
                "POST",
                self.HMAC_AUTH_PATH_ENCODED_DIGEST,
                headers=headers,
                data=body,
            )
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)
        # make sure that event has not been delivered
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)

    def test_valid_hmac_auth_unencoded_digest(self):
        test_name = "valid_hmac_auth_unencoded_digest"
        body = {"testName": test_name, "testParam": "valid"}
        secret = "salt"
        req_data = json.dumps(body)
        digest = hmac.new(
            secret.encode("utf-8"), req_data.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        headers = {
            "x-shopify-hmac-sha256": digest,
            "user": f"{ingest_user}@{self.TENANT_NAME}",
            "pass": f"{ingest_pass}",
        }

        status, _ = RestClient.call(
            self.conn,
            "POST",
            self.HMAC_AUTH_PATH_UNENCODED_DIGEST,
            headers=headers,
            data=body,
        )
        self.assertEqual(status, "OK\n")
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 1)

    def test_invalid_hmac_auth_unencoded_digest(self):
        test_name = "invalid_hmac_auth_unencoded_digest"
        body = {"testName": test_name, "testParam": "valid"}
        secret = "wrong-secret"
        req_data = json.dumps(body)
        digest = hmac.new(
            secret.encode("utf-8"), req_data.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        headers = {"x-shopify-hmac-sha256": digest}

        try:
            RestClient.call(
                self.conn,
                "POST",
                self.HMAC_AUTH_PATH_UNENCODED_DIGEST,
                headers=headers,
                data=body,
            )
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)
        # make sure that event has not been delivered
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)

    def test_hmac_auth_header_missing(self):
        test_name = "hmac auth header missing"
        body = {"testName": test_name, "testParam": "valid"}
        secret = "salt"
        req_data = json.dumps(body)
        digest = hmac.new(
            secret.encode("utf-8"), req_data.encode("utf-8"), hashlib.sha256
        ).digest()
        computed_hmac = base64.b64encode(digest)
        headers = {"wrong-header": computed_hmac}

        try:
            RestClient.call(
                self.conn,
                "POST",
                self.HMAC_AUTH_PATH_ENCODED_DIGEST,
                headers=headers,
                data=body,
            )
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)
        # make sure that event has not been delivered
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)

    def test_valid_basic_authorization_auth(self):
        test_name = "valid authorization auth"
        body = {"testName": test_name, "testParam": "valid"}
        secret = f"{ingest_user}@{self.TENANT_NAME}:{ingest_pass}"
        cred = base64.b64encode(secret.encode("utf-8")).decode("utf-8")
        headers = {"Authorization": f"BASIC {cred}"}

        status, _ = RestClient.call(
            self.conn, "POST", self.AUTHORIZATION_AUTH_PATH, headers=headers, data=body
        )
        self.assertEqual(status, "OK\n")
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 1)

    def test_invalid_basic_authorization_auth(self):
        test_name = "invalid authorization auth"
        body = {"testName": test_name, "testParam": "valid"}
        secret = f"{ingest_user}@{self.TENANT_NAME}:bad_password"
        cred = base64.b64encode(secret.encode("utf-8")).decode("utf-8")
        headers = {"Authorization": f"BASIC {cred}"}

        try:
            RestClient.call(
                self.conn,
                "POST",
                self.AUTHORIZATION_AUTH_PATH,
                headers=headers,
                data=body,
            )
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)

        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)

    def test_basic_authorization_auth_header_missing(self):
        test_name = "authorization auth header missing"
        body = {"testName": test_name, "testParam": "valid"}
        secret = f"{ingest_user}@{self.TENANT_NAME}:{ingest_pass}"
        cred = base64.b64encode(secret.encode("utf-8")).decode("utf-8")
        headers = {"Authorization-Wrong": f"BASIC {cred}"}

        try:
            RestClient.call(
                self.conn,
                "POST",
                self.AUTHORIZATION_AUTH_PATH,
                headers=headers,
                data=body,
            )
            self.fail("exception must happen")
        except OperationError as ex:
            self.assertEqual(ex.status, 401)

        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            statement = (
                bios.isql()
                .select()
                .from_signal("simpleSignal")
                .where(f"testName = '{test_name}'")
                .time_range(bios.time.now(), -bios.time.seconds(10))
                .build()
            )
            reply = client.execute(statement)
            records = reply.data_windows[0].records
            # pprint.pprint(records)
            self.assertEqual(len(records), 0)


if __name__ == "__main__":
    unittest.main()
