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
import pymysql
import pytest
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

TENANT_NAME = "test"
CONTEXT_NAME = "customer"
SIGNAL_NAME = "customerEnterAndExit"

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"


@pytest.fixture(name="bios_session")
def make_bios_session():
    session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    yield session

    session.close()


@pytest.fixture(name="sql_connection")
def make_sql_connection():
    connection = pymysql.connect(
        host="localhost",
        # port=4406,
        user="root",
        password="mysqlpw",
        database="inventory",
        cursorclass=pymysql.cursors.DictCursor,
    )

    yield connection

    connection.close()


def test_operate_a_row(bios_session, sql_connection):
    # create an entry
    try:
        #
        # create an entry
        #
        start = bios.time.now()
        with sql_connection.cursor() as cursor:
            sql = (
                "insert into customers (id, first_name, last_name, email)"
                " values (123, 'John', 'Doe', 'jd@example.com')"
            )
            cursor.execute(sql)
        sql_connection.commit()

        # wait for the request being processed
        time.sleep(2)

        query_ctx = bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[123]]).build()
        entries = bios_session.execute(query_ctx).to_dict()
        pprint.pprint(entries)
        assert len(entries) == 1
        assert entries[0].get("email") == "jd@example.com"
        assert entries[0].get("fullName") == "Doe, John"

        delta = bios.time.now() - start
        query_sig = bios.isql().select().from_signal(SIGNAL_NAME).time_range(start, delta).build()
        records = bios_session.execute(query_sig).to_dict()
        pprint.pprint(records)
        assert len(records) == 1
        assert records[0].get("email") == "jd@example.com"
        assert records[0].get("firstName") == "John"
        assert records[0].get("timestamp") is not None
        assert records[0].get("action") == "Enter"

        #
        # update the entry
        #
        with sql_connection.cursor() as cursor:
            sql = "update customers set first_name = 'Jane' where id = 123"
            cursor.execute(sql)
        sql_connection.commit()

        # wait for the request being processed
        time.sleep(2)

        query_ctx = bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[123]]).build()
        entries = bios_session.execute(query_ctx).to_dict()
        pprint.pprint(entries)
        assert len(entries) == 1
        assert entries[0].get("email") == "jd@example.com"
        assert entries[0].get("fullName") == "Doe, Jane"

        # The signal flow excludes UPDATE operations
        delta = bios.time.now() - start
        query_sig = bios.isql().select().from_signal(SIGNAL_NAME).time_range(start, delta).build()
        records = bios_session.execute(query_sig).to_dict()
        pprint.pprint(records)
        assert len(records) == 1
        assert records[0].get("email") == "jd@example.com"
        assert records[0].get("firstName") == "John"

        #
        # delete the entry
        #
        with sql_connection.cursor() as cursor:
            sql = "delete from customers where email = 'jd@example.com'"
            cursor.execute(sql)
        sql_connection.commit()

        time.sleep(2)

        query_ctx = bios.isql().select().from_context(CONTEXT_NAME).where(keys=[[123]]).build()
        entries = bios_session.execute(query_ctx).to_dict()
        assert len(entries) == 0

        delta = bios.time.now() - start
        query_sig = bios.isql().select().from_signal(SIGNAL_NAME).time_range(start, delta).build()
        records = bios_session.execute(query_sig).to_dict()
        pprint.pprint(records)
        assert len(records) == 2
        assert records[0].get("email") == "jd@example.com"
        assert records[0].get("firstName") == "John"
        assert records[1].get("email") == "jd@example.com"
        assert records[1].get("firstName") == "Jane"
        assert records[1].get("timestamp") > records[0].get("timestamp")
        assert records[1].get("action") == "Exit"

    finally:
        try:
            with sql_connection.cursor() as cursor:
                sql = "delete from customers where email = 'jd@example.com'"
                cursor.execute(sql)
            sql_connection.commit()
        except pymysql.err.IntegrityError:
            pass


if __name__ == "__main__":
    pytest.main(sys.argv)
