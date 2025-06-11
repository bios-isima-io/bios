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

import pprint
import sys
import time
import unittest

import pytest

from rest_client import HttpConnection, RestClient

import bios
from tsetup import admin_user, admin_pass
from tsetup import get_endpoint_url as ep_url
from tsetup import get_webhook_url


class TestWebhookFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.CONTEXT_NAME = "simpleProductCatalog"
        cls.conn = HttpConnection(get_webhook_url())
        cls.WEBHOOK_PATH_SINGLE = "cms/single"
        cls.WEBHOOK_PATH_MULTI = "cms/multiple"
        cls.WEBHOOK_PATH_MULTI_BULK = "cms/multiple_bulk"

    def test_deletion_flat_records(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            body0 = {
                "productId": "111222333",
                "productName": "one",
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SINGLE, data=body0)
            body1 = {
                "productId": "333332211",
                "productName": "three",
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
                "operation": "INSERT",
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SINGLE, data=body1)

            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["111222333"], ["333332211"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 2

            body2 = {
                "productId": "333332211",
                "productName": "three",
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
                "operation": "DELETE",
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SINGLE, data=body2)
            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["111222333"], ["333332211"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 1
            assert records[0].get("productId") == "111222333"
            assert records[0].get("productName") == "one"

            # Repeating deletion shouldn't cause an error
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SINGLE, data=body2)
            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["111222333"], ["333332211"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 1
            assert records[0].get("productId") == "111222333"
            assert records[0].get("productName") == "one"

    def test_deletion_nested_records(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            body1 = {
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
                "products": [
                    {
                        "productId": "AAAAAAA",
                        "productName": "ai",
                        "operation": "INSERT",
                    },
                    {
                        "productId": "BBBB",
                        "productName": "bee",
                        "operation": "INSERT",
                    },
                ],
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_MULTI, data=body1)

            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["AAAAAAA"], ["BBBB"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 2

            body2 = {
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
                "products": [
                    {
                        "productId": "BBBB",
                        "productName": "bee",
                        "operation": "DELETE",
                    },
                    {
                        "productId": "CCCCCCCCC",
                        "productName": "see",
                        "operation": "INSERT",
                    },
                    {
                        "productId": "DD",
                        "productName": "dee",
                        "operation": "DELETE",
                    },
                ],
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_MULTI, data=body2)
            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["AAAAAAA"], ["BBBB"], ["CCCCCCCCC"], ["DD"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 2
            assert {record.get("productId") for record in records} == {"AAAAAAA", "CCCCCCCCC"}

            # Repeating deletion shouldn't cause an error
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_MULTI, data=body2)
            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["AAAAAAA"], ["BBBB"], ["CCCCCCCCC"], ["DD"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 2
            assert {record.get("productId") for record in records} == {"AAAAAAA", "CCCCCCCCC"}

    def test_bulk_deletion_nested_records(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as client:
            body1 = {
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
                "products": [
                    {
                        "productId": "xxxx",
                        "productName": "X",
                    },
                    {
                        "productId": "yy",
                        "productName": "Y",
                    },
                    {
                        "productId": "zzzzz",
                        "productName": "Z",
                    },
                ],
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_MULTI_BULK, data=body1)

            body2 = {
                "user": f"admin@{self.TENANT_NAME}",
                "password": "admin",
                "operation": "DELETE",
                "products": [
                    {
                        "productId": "wwwwwww",
                        "productName": "bee",
                    },
                    {
                        "productId": "xxxx",
                        "productName": "see",
                    },
                    {
                        "productId": "yy",
                        "productName": "dee",
                    },
                ],
            }
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_MULTI_BULK, data=body2)
            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["wwwwwww"], ["xxxx"], ["yy"], ["zzzzz"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 1
            assert {record.get("productId") for record in records} == {"zzzzz"}

            # Repeating deletion shouldn't cause an error
            RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_MULTI_BULK, data=body2)
            statement = (
                bios.isql()
                .select()
                .from_context(self.CONTEXT_NAME)
                .where(keys=[["wwwwwww"], ["xxxx"], ["yy"], ["zzzzz"]])
                .build()
            )
            reply = client.execute(statement)
            records = reply.to_dict()
            assert len(records) == 1
            assert {record.get("productId") for record in records} == {"zzzzz"}


if __name__ == "__main__":
    pytest.main(sys.argv)
