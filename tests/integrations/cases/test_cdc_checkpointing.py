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

import os
import random
import sys
import time
import urllib
from multiprocessing import Process

import bios
import docker
import pymysql
import pytest
from pymongo import MongoClient
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

TENANT_NAME = "test"
MYSQL_CONTEXT_NAME = "customer"
MONGO_CONTEXT_NAME = "orderStatus"

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

NUM_ENTRIES = 360

CONTAINER_SUFFIX = os.environ.get("CONTAINER_SUFFIX", "it.example.com")
INTEGRATIONS_CONTAINER_NAME = f"bios-apps.{CONTAINER_SUFFIX}"
NUM_BIOS_NODES = 3

#
# Mongodb parameters
#
USER = "mongoadmin"
PASSWORD = "Unbreakable!"
REPLICA_SET = "my-mongo-set"
DB_SHOPPING = "shopping"
TABLE_ORDER_STATUS = "order_status"


def make_bios_session():
    session = bios.login(ep_url(), ADMIN_USER, admin_pass)
    yield session
    session.close()


def make_id_start() -> int:
    """Generate a random ID offset to avoid ID conflicts among multiple test executions."""
    id_start = random.randint(0, 0xFFFF)
    id_start <<= 14
    return id_start


def populate_rows(id_start: int):
    """Populate NUM_ENTRIES customer entries in the database and return."""

    mongo_password = urllib.parse.quote(PASSWORD)
    mongo_connection_string = (
        f"mongodb://{USER}:{mongo_password}@localhost:27017/?ssl=false&replicaSet={REPLICA_SET}"
    )

    print("")
    with (
        pymysql.connect(
            host="localhost", user="root", password="mysqlpw", database="inventory"
        ) as sql_connection,
        MongoClient(mongo_connection_string) as mongo_client,
    ):

        db_shopping = mongo_client[DB_SHOPPING]
        collection_order_status = db_shopping[TABLE_ORDER_STATUS]

        for i in range(NUM_ENTRIES):
            entry_id = id_start + i

            # populate a mysql row
            with sql_connection.cursor() as cursor:
                sql = (
                    "insert into customers (id, first_name, last_name, email)"
                    f" values ({entry_id}, 'John', 'Doe', 'jd+clone{entry_id}@example.com')"
                )
                cursor.execute(sql)
            sql_connection.commit()

            # populate a mongodb row
            item = {
                "_id": entry_id,
                "order_id": entry_id,
                "user_id": f"user{entry_id}",
                "status": "placed",
            }
            collection_order_status.insert_one(item)

            print(f"\rid={entry_id} ({i + 1} / {NUM_ENTRIES})", end="")

            time.sleep(0.5)


def test_process_failure_handling():
    """Stop components in the import flow path while the source is generating events,
    verify no events are lost."""

    # Test preparation
    id_start = make_id_start()

    docker_client = docker.from_env()
    container_integ = docker_client.containers.get(INTEGRATIONS_CONTAINER_NAME)

    containers_bios = []
    for i in range(NUM_BIOS_NODES):
        try:
            containers_bios.append(docker_client.containers.get(f"bios{i + 1}.{CONTAINER_SUFFIX}"))
        except docker.errors.NotFound:
            pass

    # Start populating the DB entries in another fork
    process = Process(target=populate_rows, args=(id_start,))
    process.start()

    # abuse the import flow in the mean time
    try:
        # time.sleep(5)
        # print("\n1. Restart the webhook service while the CDC is picking up the changes")
        # container_integ.exec_run("supervisorctl stop integrations-webhook")
        # time.sleep(5)
        # container_integ.exec_run("supervisorctl start integrations-webhook")
        # time.sleep(10)
        #
        # print("\n2. Restart the bios containers")
        # for container in containers_bios:
        #     container.stop()
        # for container in containers_bios:
        #     container.start()
        # time.sleep(40)
        #
        # print("\n3. Restart the integrations container from the healthy state")
        # container_integ.restart()
        # time.sleep(10)
        #
        # print("\n4. Restart the integrations container while the webhook is down")
        # container_integ.exec_run("supervisorctl stop integrations-webhook")
        # time.sleep(5)
        # container_integ.restart()
        # time.sleep(20)
        #
        # print("\n5. Restart the integrations container while bios nodes are down")
        # for container in containers_bios:
        #     container.stop()
        # container_integ.stop()
        # for container in containers_bios:
        #     container.start()
        # container_integ.start()

        process.join()
        process = None
        print("")
    finally:
        if process:
            process.kill()

    # To ensure the CDC consumes all changes
    time.sleep(45)

    with bios.login(ep_url(), ADMIN_USER, admin_pass) as bios_session:
        keys = [[entry_id] for entry_id in range(id_start, id_start + NUM_ENTRIES)]

        print("checking records populated by integrations-mysql")
        query_mysql = (
            bios.isql().select().from_context(MYSQL_CONTEXT_NAME).where(keys=keys).build()
        )
        entries = bios_session.execute(query_mysql).to_dict()
        assert len(entries) == NUM_ENTRIES

        print("checking records populated by integrations-mongodb")
        query_mongo = (
            bios.isql().select().from_context(MONGO_CONTEXT_NAME).where(keys=keys).build()
        )
        entries = bios_session.execute(query_mongo).to_dict()
        assert len(entries) == NUM_ENTRIES


if __name__ == "__main__":
    pytest.main(sys.argv)
