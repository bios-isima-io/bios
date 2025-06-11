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

import bios
import psycopg2
import pytest
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

TENANT_NAME = "test"
CONTEXT_NAME = "productCatalog"

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"


@pytest.fixture(name="bios_session")
def make_bios_session():
    session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    yield session

    session.close()


@pytest.fixture(name="sql_connection")
def make_sql_connection():
    connection = psycopg2.connect(
        host="localhost", user="postgres", password="postgrespw", database="products"
    )

    yield connection

    connection.close()


def test_operate_a_row(bios_session, sql_connection):
    # create an entry
    try:
        #
        # create an entry
        #
        product_id = "abc00001"
        with sql_connection.cursor() as cursor:
            sql = (
                "insert into product (product_id, product_name, price)"
                f" values ('{product_id}', 'toothbrush', 543)"
            )
            cursor.execute(sql)
        sql_connection.commit()

        # wait for the request being processed
        time.sleep(2)

        query_ctx = (
            bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[product_id]]).build()
        )
        entries = bios_session.execute(query_ctx).to_dict()
        pprint.pprint(entries)
        assert len(entries) == 1
        assert entries[0].get("productId") == product_id
        assert entries[0].get("productName") == "toothbrush"
        assert entries[0].get("price") == "5.43"

        #
        # update the entry
        #
        with sql_connection.cursor() as cursor:
            sql = f"update product set price = 499 where product_id = '{product_id}'"
            cursor.execute(sql)
        sql_connection.commit()

        # wait for the request being processed
        time.sleep(2)

        query_ctx = (
            bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[product_id]]).build()
        )
        entries = bios_session.execute(query_ctx).to_dict()
        pprint.pprint(entries)
        assert len(entries) == 1
        assert entries[0].get("productId") == product_id
        assert entries[0].get("productName") == "toothbrush"
        assert entries[0].get("price") == "4.99"

        #
        # delete the entry
        #
        with sql_connection.cursor() as cursor:
            sql = f"delete from product where product_id = '{product_id}'"
            cursor.execute(sql)
        sql_connection.commit()

        time.sleep(2)

        query_ctx = (
            bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[product_id]]).build()
        )
        entries = bios_session.execute(query_ctx).to_dict()
        assert len(entries) == 0

    except Exception:  # pylint: disable=broad-except
        with sql_connection.cursor() as cursor:
            cursor.execute("rollback")
            sql = "delete from product where product_id = 'abc00001'"
            cursor.execute(sql)
        sql_connection.commit()
        raise


if __name__ == "__main__":
    pytest.main(sys.argv)
