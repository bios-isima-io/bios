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
import random

from pymongo import MongoClient
import bios
import pytest
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
import urllib.parse

TENANT_NAME = "test"
CART_ACTIVITY = "cartActivity"
WAREHOUSE_STOCK = "warehouseStock"

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

#
# Mongodb common params
#
USER = "mongoadmin"
PASSWORD = "Unbreakable!"
REPLICA_SET = "my-mongo-set"
#
# DB shopping
#
DB_SHOPPING = "shopping"
CART_ITEMS = "cart_items"
TABLE_STOCK = "stock"


@pytest.fixture(name="bios_session")
def make_bios_session():
    session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    yield session

    session.close()


@pytest.fixture(name="mongo_client")
def make_mongo_client():
    password = urllib.parse.quote(PASSWORD)
    connection_string = (
        f"mongodb://{USER}:{password}@localhost:27017/?ssl=false&replicaSet={REPLICA_SET}"
    )
    client = MongoClient(connection_string)

    yield client

    client.close()


def test_fundamental_operatoins(bios_session, mongo_client):

    db_shopping = mongo_client[DB_SHOPPING]
    collection_cart_items = db_shopping[CART_ITEMS]

    item_id = random.randint(1, 0xFFFF)
    product_id = "RR450020RFG"
    #
    # create an entry
    #
    start = bios.time.now()
    item = {
        "_id": item_id,
        "user_id": 1010,
        "product_id": product_id,
        "item_name": "candy",
        "quantity": 4,
        "price": 1099,
    }
    collection_cart_items.insert_one(item)

    # wait for the request being processed
    time.sleep(2)

    delta = bios.time.now() - start
    query = bios.isql().select().from_signal(CART_ACTIVITY).time_range(start, delta).build()
    events = bios_session.execute(query).to_dict()
    pprint.pprint(events)
    assert len(events) == 1
    assert events[0].get("userId") == 1010
    assert events[0].get("productId") == product_id
    assert events[0].get("itemName") == "candy"
    assert events[0].get("quantity") == 4
    assert events[0].get("price") == "10.99"
    assert events[0].get("activity") == "Create"

    #
    # update the entry
    #
    collection_cart_items.update_one({"_id": item_id}, {"$set": {"quantity": 5, "price": 1159}})

    # wait for the request being processed
    time.sleep(2)

    delta = bios.time.now() - start
    query = bios.isql().select().from_signal(CART_ACTIVITY).time_range(start, delta).build()
    events = bios_session.execute(query).to_dict()
    pprint.pprint(events)
    assert len(events) == 2
    assert events[1].get("userId") == 1010
    assert events[1].get("productId") == product_id
    assert events[1].get("itemName") == "candy"
    assert events[1].get("quantity") == 5
    assert events[1].get("price") == "11.59"
    assert events[1].get("activity") == "Update"

    #
    # replace the entry
    #
    replacement = {
        "_id": item_id,
        "user_id": 1011,
        "product_id": product_id,
        "item_name": "altoids",
        "quantity": 5,
        "price": 1159,
        "note": "to correct initial data",
    }
    collection_cart_items.replace_one({"_id": item_id}, replacement)

    # wait for the request being processed
    time.sleep(2)

    delta = bios.time.now() - start
    query = bios.isql().select().from_signal(CART_ACTIVITY).time_range(start, delta).build()
    events = bios_session.execute(query).to_dict()
    pprint.pprint(events)
    assert len(events) == 3
    assert events[2].get("userId") == 1011
    assert events[2].get("productId") == product_id
    assert events[2].get("itemName") == "altoids"
    assert events[2].get("quantity") == 5
    assert events[2].get("price") == "11.59"
    assert events[2].get("activity") == "Update"

    #
    # delete the entry
    #
    collection_cart_items.delete_one({"_id": item_id})

    # wait for the request being processed
    time.sleep(2)

    delta = bios.time.now() - start
    query = bios.isql().select().from_signal(CART_ACTIVITY).time_range(start, delta).build()
    events = bios_session.execute(query).to_dict()
    pprint.pprint(events)
    assert len(events) == 4
    assert events[3].get("userId") == 1011
    assert events[3].get("productId") == product_id
    assert events[3].get("itemName") == "altoids"
    assert events[3].get("quantity") == 5
    assert events[3].get("price") == "11.59"
    assert events[3].get("activity") == "Delete"


def test_operation_to_another_table(bios_session, mongo_client):

    db_shopping = mongo_client[DB_SHOPPING]
    collection_stock = db_shopping[TABLE_STOCK]

    item_id = random.randint(1, 0xFFFF)
    item_number = random.randint(1, 0xFFFF)
    item_number2 = random.randint(1, 0xFFFF)
    item_name = "Whiteboard Eraser"
    #
    # create an entry
    #
    item = {
        "_id": item_id,
        "item_number": item_number,
        "item_name": item_name,
        "quantity": 915,
    }
    collection_stock.insert_one(item)

    # wait for the request being processed
    time.sleep(2)

    query = bios.isql().select().from_context(WAREHOUSE_STOCK).where(keys=[[item_number]]).build()
    events = bios_session.execute(query).to_dict()
    assert len(events) == 1
    assert events[0].get("itemNumber") == item_number
    assert events[0].get("itemName") == item_name
    assert events[0].get("quantity") == 915

    #
    # update the entry
    #
    collection_stock.update_one({"_id": item_id}, {"$set": {"quantity": 2250}})

    # wait for the request being processed
    time.sleep(2)

    query = bios.isql().select().from_context(WAREHOUSE_STOCK).where(keys=[[item_number]]).build()
    events = bios_session.execute(query).to_dict()
    assert len(events) == 1
    assert events[0].get("itemNumber") == item_number
    assert events[0].get("itemName") == item_name
    assert events[0].get("quantity") == 2250

    #
    # replace the entry
    #
    replacement = {
        "_id": item_id,
        "item_number": item_number2,
        "item_name": item_name,
        "quantity": 2255,
        "note": "wrong item number",
    }
    collection_stock.replace_one({"_id": item_id}, replacement)

    # wait for the request being processed
    time.sleep(2)

    # Theck the result. The operation above would cause duplicate entries.
    # In order to avoid this issue, the context should use _id as the primary key
    query = (
        bios.isql()
        .select()
        .from_context(WAREHOUSE_STOCK)
        .where(keys=[[item_number], [item_number2]])
        .build()
    )
    events = bios_session.execute(query).to_dict()
    assert len(events) == 2
    assert events[0].get("itemNumber") == item_number
    assert events[0].get("itemName") == item_name
    assert events[0].get("quantity") == 2250
    assert events[1].get("itemNumber") == item_number2
    assert events[1].get("itemName") == item_name
    assert events[1].get("quantity") == 2255

    #
    # delete the entry
    #
    collection_stock.delete_one({"_id": item_id})

    # wait for the request being processed
    time.sleep(2)

    query = (
        bios.isql()
        .select()
        .from_context(WAREHOUSE_STOCK)
        .where(keys=[[item_number], [item_number2]])
        .build()
    )
    events = bios_session.execute(query).to_dict()
    assert len(events) == 1
    assert events[0].get("itemNumber") == item_number
    assert events[0].get("itemName") == item_name
    assert events[0].get("quantity") == 2250


def test_insert_to_wrong_database(bios_session, mongo_client):

    db_unrelated = mongo_client["unrelated_db"]
    collection_cart_items = db_unrelated[CART_ITEMS]

    item_id = random.randint(1, 0xFFFF)
    product_id = "RR420035NPX"
    #
    # create an entry
    #
    start = bios.time.now()
    item = {
        "_id": item_id,
        "user_id": 1010,
        "product_id": product_id,
        "item_name": "umbrella",
        "quantity": 1,
        "price": 2032,
    }
    collection_cart_items.insert_one(item)

    # wait for the request being processed
    time.sleep(2)

    delta = bios.time.now() - start
    query_ctx = bios.isql().select().from_signal(CART_ACTIVITY).time_range(start, delta).build()
    events = bios_session.execute(query_ctx).to_dict()
    pprint.pprint(events)
    assert len(events) == 0


if __name__ == "__main__":
    pytest.main(sys.argv)
