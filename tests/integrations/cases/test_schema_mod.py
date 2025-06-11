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

import copy
import json
import pprint
import sys
import unittest

import bios
import pytest
from bios import ServiceError
from rest_client import HttpConnection, RestClient
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_webhook_url


class TestSchemaModification(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.SIGNAL_NAME = "shoppingCart"
        cls.CONTEXT_NAME = "inventory"
        cls.conn = HttpConnection(get_webhook_url())
        cls.WEBHOOK_PATH_SHOPPING_CART = "cms/shopping-cart"
        cls.WEBHOOK_PATH_INVENTORY = "cms/inventory"
        cls.RESOURCES_DIR = "../resources/files/schema_change"

        with open(f"{cls.RESOURCES_DIR}/mod_cart.json", "r", encoding="utf-8") as file:
            cart_schema_info = json.load(file)
        cls.CART_SIGNAL_MOD = cart_schema_info.get("signal")
        cls.CART_FLOW_MOD = cart_schema_info.get("flow")
        cls.CART_SIGNAL_ORIG = copy.deepcopy(cls.CART_SIGNAL_MOD)
        cls.CART_SIGNAL_ORIG["attributes"] = cls.CART_SIGNAL_ORIG["attributes"][:-1]
        cls.CART_FLOW_ORIG = copy.deepcopy(cls.CART_FLOW_MOD)
        cls.CART_FLOW_ORIG["dataPickupSpec"]["attributes"] = cls.CART_FLOW_ORIG["dataPickupSpec"][
            "attributes"
        ][:-1]

        with open(f"{cls.RESOURCES_DIR}/mod_inventory.json", "r", encoding="utf-8") as file:
            inventory_schema_info = json.load(file)
        cls.INVENTORY_CONTEXT_MOD = inventory_schema_info.get("context")
        cls.INVENTORY_FLOW_MOD = inventory_schema_info.get("flow")
        cls.INVENTORY_CONTEXT_ORIG = copy.deepcopy(cls.INVENTORY_CONTEXT_MOD)
        cls.INVENTORY_CONTEXT_ORIG["attributes"] = cls.INVENTORY_CONTEXT_ORIG["attributes"][:-1]
        cls.INVENTORY_FLOW_ORIG = copy.deepcopy(cls.INVENTORY_FLOW_MOD)
        cls.INVENTORY_FLOW_ORIG["dataPickupSpec"]["attributes"] = cls.INVENTORY_FLOW_ORIG[
            "dataPickupSpec"
        ]["attributes"][:-1]

    def test_modify_signal(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as session:
            try:

                # test initial insertion
                time0 = bios.time.now()
                body0 = {
                    "visitorId": 123,
                    "productId": 456,
                    "category": "Electronics",
                    "quantity": 5,
                    "price": 5432,
                    "unit": "USD",
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SHOPPING_CART, data=body0)

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME)
                    .time_range(time0, bios.time.now() - time0)
                    .build()
                )
                reply = session.execute(statement)
                records = reply.to_dict()
                assert len(records) == 1

                # modify schema
                session.update_signal(self.SIGNAL_NAME, self.CART_SIGNAL_MOD)
                session.update_import_flow_spec(
                    self.CART_FLOW_MOD.get("importFlowId"), self.CART_FLOW_MOD
                )
                modified_signal = session.get_signal(self.SIGNAL_NAME)
                assert len(modified_signal.get("attributes")) == 4

                time1 = bios.time.now()
                body1 = {
                    "visitorId": 567,
                    "productId": 890,
                    "category": "Kitchen",
                    "quantity": 1,
                    "price": 12345,
                    "unit": "USD",
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SHOPPING_CART, data=body1)

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME)
                    .time_range(time1, bios.time.now() - time1)
                    .build()
                )
                reply = session.execute(statement)
                records = reply.to_dict()
                assert len(records) == 1
                assert records[0].get("price") in [0, 12345]

                # revert the schema
                pprint.pprint(self.CART_SIGNAL_ORIG)
                session.update_signal(self.SIGNAL_NAME, self.CART_SIGNAL_ORIG)
                session.update_import_flow_spec(
                    self.CART_FLOW_ORIG.get("importFlowId"), self.CART_FLOW_ORIG
                )
                reverted_signal = session.get_signal(self.SIGNAL_NAME)
                assert len(reverted_signal.get("attributes")) == 3

                time2 = bios.time.now()
                body2 = {
                    "visitorId": 912,
                    "productId": 100,
                    "category": "Hardware",
                    "quantity": 2,
                    "price": 1000,
                    "unit": "USD",
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_SHOPPING_CART, data=body2)

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME)
                    .time_range(time2, bios.time.now() - time2)
                    .build()
                )
                reply = session.execute(statement)
                records = reply.to_dict()
                assert len(records) == 1
                assert records[0].get("price") is None
            finally:
                try:
                    session.update_signal(self.SIGNAL_NAME, self.CART_SIGNAL_ORIG)
                except ServiceError:
                    pass
                session.update_import_flow_spec(
                    self.CART_FLOW_ORIG.get("importFlowId"), self.CART_FLOW_ORIG
                )

    def test_modify_context(self):
        with bios.login(ep_url(), f"{admin_user}@{self.TENANT_NAME}", admin_pass) as session:
            try:

                # test initial insertion
                body1 = {
                    "productId": 456,
                    "unitPrice": "34.56",
                    "quantity": 5,
                    "location": "Santa Clara",
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                    "clear": False,
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_INVENTORY, data=body1)

                statement = (
                    bios.isql()
                    .select()
                    .from_context(self.CONTEXT_NAME)
                    .where(keys=[["456"]])
                    .build()
                )
                reply = session.execute(statement)
                records = reply.to_dict()
                assert len(records) == 1

                # modify schema
                session.update_context(self.CONTEXT_NAME, self.INVENTORY_CONTEXT_MOD)
                session.update_import_flow_spec(
                    self.INVENTORY_FLOW_MOD.get("importFlowId"), self.INVENTORY_FLOW_MOD
                )
                modified_context = session.get_context(self.CONTEXT_NAME)
                assert len(modified_context.get("attributes")) == 4

                body2 = {
                    "productId": 789,
                    "unitPrice": "81.32",
                    "quantity": 1,
                    "location": "Fremont",
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                    "clear": False,
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_INVENTORY, data=body2)

                body3 = {
                    "productId": 456,
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                    "clear": True,
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_INVENTORY, data=body3)

                statement = (
                    bios.isql()
                    .select()
                    .from_context(self.CONTEXT_NAME)
                    .where(keys=[["456"], ["789"]])
                    .build()
                )
                reply = session.execute(statement)
                records = reply.to_dict()
                assert len(records) == 1
                assert records[0].get("productId") == 789
                assert records[0].get("location") in ["MISSING", "Fremont"]

                # revert the schema
                session.update_context(self.CONTEXT_NAME, self.INVENTORY_CONTEXT_ORIG)
                session.update_import_flow_spec(
                    self.INVENTORY_FLOW_ORIG.get("importFlowId"), self.INVENTORY_FLOW_ORIG
                )
                reverted_context = session.get_context(self.CONTEXT_NAME)
                assert len(reverted_context.get("attributes")) == 3

                body4 = {
                    "productId": 911,
                    "unitPrice": "81.32",
                    "quantity": 1,
                    "location": "Fremont",
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                    "clear": False,
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_INVENTORY, data=body4)

                body5 = {
                    "productId": 789,
                    "user": f"admin@{self.TENANT_NAME}",
                    "password": "admin",
                    "clear": True,
                }
                RestClient.call(self.conn, "POST", self.WEBHOOK_PATH_INVENTORY, data=body5)

                statement = (
                    bios.isql()
                    .select()
                    .from_context(self.CONTEXT_NAME)
                    .where(keys=[["789"], ["911"]])
                    .build()
                )
                reply = session.execute(statement)
                records = reply.to_dict()
                assert len(records) == 1
                assert records[0].get("productId") == 911
                assert records[0].get("location") is None
            finally:
                try:
                    session.update_context(self.CONTEXT_NAME, self.INVENTORY_CONTEXT_ORIG)
                except ServiceError:
                    pass
                session.update_import_flow_spec(
                    self.INVENTORY_FLOW_ORIG.get("importFlowId"), self.INVENTORY_FLOW_ORIG
                )


if __name__ == "__main__":
    pytest.main(sys.argv)
