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

from multiprocessing import Process, Pipe

import json
import os
import subprocess
import pprint
import sys
import time
import unittest
import uuid

import pytest

import docker

import bios
from bios import ServiceError, ErrorCode
from tsetup import (
    sadmin_user,
    sadmin_pass,
    admin_user,
    admin_pass,
    ingest_user,
    ingest_pass,
    extract_user,
    extract_pass,
)
from tsetup import get_endpoint_url as ep_url
from tsetup import get_multinode_endpoint as mendpoints
from tsetup import get_single_endpoint as endpoint
from tsetup import ingest_user, ingest_pass, extract_user, extract_pass, sadmin_user, sadmin_pass


class ServerRestartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT = "failure_recovery"
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            tenants = sadmin.list_tenants()
            if cls.TENANT not in tenants:
                sadmin.create_tenant({"tenantName": cls.TENANT})

        with bios.login(ep_url(), f"admin@{cls.TENANT}", "admin") as admin:
            pprint.pprint(admin.get_contexts())
            contexts = [context.get("contextName") for context in admin.get_contexts()]
            if "simpleContext" not in contexts:
                admin.create_context(
                    {
                        "contextName": "simpleContext",
                        "missingAttributePolicy": "Reject",
                        "attributes": [
                            {"attributeName": "one", "type": "String"},
                            {"attributeName": "two", "type": "String"},
                        ],
                        "primaryKey": ["one"],
                    }
                )

        cls.num_nodes = int(os.environ.get("NUM_NODES") or 6)
        cls.docker_client = docker.from_env()
        cls.docker_bios = [
            cls.docker_client.containers.get(f"bios{i + 1}") for i in range(cls.num_nodes)
        ]

    @classmethod
    def tearDownClass(cls):
        # try:
        #     cls.sadmin.delete_tenant(cls.TENANT)
        # except ServiceError:
        #     pass
        # cls.sadmin.close()
        pass

    def tearDown(self):
        try:
            self.docker_bios[1].start()
        except Exception:
            pass

    def push_context_entries(self, index: int):
        host1, port1, _, _, _ = mendpoints(0)
        with bios.login(f"https://{host1}:{port1}", f"admin@{self.TENANT}", "admin") as admin:
            i = 0
            while True:
                i += 1
                statement = bios.isql().upsert().into("simpleContext").csv(f"{i},{i}").build()
                admin.execute(statement)
                if i % 250 == 0:
                    print(f"        [{index}]: upserted {i} records")

    def test_server_restart(self):
        """test restarting a server, no failure report should occur"""

        start = bios.time.now()

        print("====> start upserts")
        procs = []
        for index in range(5):
            proc = Process(target=self.push_context_entries, args=(index,))
            procs.append(proc)
            proc.start()

        try:
            time.sleep(10)
            print("====> stop node 2")
            self.docker_bios[1].stop()
            time.sleep(20)
            print("====> start node 2")
            self.docker_bios[1].start()
            time.sleep(30)

            print("====> verify no failure report occurred")
            with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
                statement = (
                    bios.isql()
                    .select()
                    .from_signal("_failureReport")
                    .time_range(start, bios.time.now() - start)
                    .build()
                )
                reports = sadmin.execute(statement).to_dict()
                assert len(reports) == 0
        finally:
            for proc in procs:
                proc.terminate()
                proc.join()
                proc.close()


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv))
