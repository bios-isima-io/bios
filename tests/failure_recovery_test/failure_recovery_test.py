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

import json
import os
import subprocess
import sys
import time
import unittest
import uuid
from multiprocessing import Pipe, Process

import pytest
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_multinode_endpoint as mendpoints
from tsetup import get_single_endpoint as endpoint
from tsetup import ingest_pass, ingest_user, sadmin_pass, sadmin_user

import bios
import docker
from bios import ErrorCode, ServiceError


class FailureRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT = "failure_recovery"
        cls.LB_TENANT = "csdk_lb_tenant"
        cls.sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        try:
            cls.sadmin.delete_tenant(cls.TENANT)
            cls.sadmin.delete_tenant(cls.LB_TENANT)
        except ServiceError:
            pass
        cls.simple_signal = {
            "signalName": "simple",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "first", "type": "string"},
                {"attributeName": "second", "type": "integer"},
            ],
        }

        cls.sadmin.create_tenant({"tenantName": cls.TENANT})
        cls.sadmin.create_tenant({"tenantName": cls.LB_TENANT})
        cls.sadmin.set_property("prop.maintenance.fastTrackWorkerInterval", "1000")
        cls.sadmin.set_property("prop.maintenance.fastTrackSignals", f"{cls.TENANT}.simpleContext")

        cls.admin_session = bios.login(ep_url(), f"{admin_user}@{cls.LB_TENANT}", admin_pass)
        print(f"creating signal {cls.simple_signal.get('signalName')}")
        cls.admin_session.create_signal(cls.simple_signal)
        cls.BIOS_VERSION = os.environ.get("BIOS_VERSION")
        cls.docker_client = docker.from_env()
        cls.docker_proxy = cls.docker_client.containers.get("biosproxy")
        cls.docker_db = cls.docker_client.containers.get("bios-storage")
        cls.num_nodes = int(os.environ.get("NUM_NODES") or 6)
        cls.docker_bios = [
            cls.docker_client.containers.get(f"bios{i + 1}") for i in range(cls.num_nodes)
        ]

    @classmethod
    def tearDownClass(cls):
        # try:
        #     cls.sadmin.delete_tenant(cls.TENANT)
        # except ServiceError:
        #     pass
        cls.sadmin.close()

    def setUp(self):
        pass

    def tearDown(self):
        try:
            self.docker_proxy.start()
        except Exception:
            pass
        try:
            self.docker_bios[1].start()
        except Exception:
            pass

    def test_failure_recovery(self):
        """test recreating a stream; new stream has more attributes"""
        context_name = "my_context"
        signal_name = "simple"
        context = {
            "contextName": context_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "the_key", "type": "integer"},
                {"attributeName": "the_value", "type": "string"},
            ],
            "primaryKey": ["the_key"],
        }
        first = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "first", "type": "string"},
                {"attributeName": "second", "type": "integer"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "lookup",
                        "foreignKey": ["second"],
                        "missingLookupPolicy": "reject",
                        "contextName": context_name,
                        "contextAttributes": [{"attributeName": "the_value"}],
                    }
                ],
            },
        }
        second = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "first", "type": "string"},
                {"attributeName": "second", "type": "integer"},
                {"attributeName": "three", "type": "string", "default": -1},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "lookup",
                        "foreignKey": ["second"],
                        "missingLookupPolicy": "reject",
                        "contextName": context_name,
                        "contextAttributes": [{"attributeName": "the_value"}],
                    }
                ],
            },
        }

        # Test normal case
        print("\n====> create initial streams")
        admin = bios.login(ep_url(), f"{admin_user}@{self.TENANT}", admin_pass)
        admin.create_context(context)
        admin.create_signal(first)
        admin.delete_signal(signal_name)

        # Add a few contexts
        admin.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(["1,ichi", "2,ni", "3,san", "4,yon", "5,go", "6,roku", "7,nana"])
            .build()
        )

        # Put many context entries and load them to Cache for each service node.
        context_count = 500
        keys = []
        entries = []
        print("====> put initial {} context entries via Python SDK".format(context_count))
        for i in range(context_count):
            key = str(i) + "00"
            entry = key + "," + str(i) + "a"
            keys.append([key])
            entries.append(entry)
        admin.execute(bios.isql().upsert().into(context_name).csv_bulk(entries).build())
        host0, port0, _, _, _ = mendpoints(0)
        admin1 = bios.login(f"https://{host0}:{port0}", f"{admin_user}@{self.TENANT}", admin_pass)
        admin1.execute(bios.isql().select().from_context(context_name).where(keys=keys).build())
        host1, port1, _, _, _ = mendpoints(1)
        admin2 = bios.login(f"https://{host1}:{port1}", f"{admin_user}@{self.TENANT}", admin_pass)
        admin2.execute(bios.isql().select().from_context(context_name).where(keys=keys).build())
        host2, port2, _, _, _ = mendpoints(2)
        admin3 = bios.login(f"https://{host2}:{port2}", f"{admin_user}@{self.TENANT}", admin_pass)

        proxy_url = "https://172.18.0.10:8443"

        entries = admin3.execute(
            bios.isql().select().from_context(context_name).where(keys=keys).build()
        ).to_dict()
        for i in range(context_count):
            assert entries[i].get("the_value") == str(i) + "a"

        # Start putting contexts via Java SDK
        print("====> start context updates via Java SDK")
        classpath = (
            "./tests/failure_recovery_test:./sdk/sdk-java/target/"
            f"bios-sdk-{self.BIOS_VERSION}-jar-with-dependencies.jar"
        )
        proc = subprocess.Popen(["java", "-cp", classpath, "PutContexts", str(context_count), "b"])

        # fork a subprocess to test node 3 with extenrl connection (i.e., no load balancing)
        parent_conn, child_conn = Pipe()
        proc2 = Process(
            target=self.verify_node_3_reloading, args=(host2, port2, signal_name, child_conn)
        )
        proc2.start()

        print("====> destroying connectivity to BIOS node 3 by stopping the proxy")
        self.docker_proxy.stop()

        print("====> verify that admin login would be successful when nodes are partially down")
        with bios.login(f"https://{host1}:{port1}", f"{admin_user}@{self.TENANT}", admin_pass):
            pass

        admin.create_signal(first)

        # In the forked process, the client connects to the node 3 as an external connection.
        # "in maintenance", "recovered already", or None is returned here
        print("====> verify that the node 3 enters maintenance mode")
        parent_conn.poll(70)
        result1 = parent_conn.recv()
        assert result1 in ("in maintenance", "recovered already")

        # The second node must be marked down
        if result1 == "in maintenance":
            failed_endpoints = self.sadmin.get_property("failed_endpoints")
            endpoints = failed_endpoints.split("\n")
            assert len(endpoints) == 1
            assert endpoints[0] == proxy_url

        admin.execute(bios.isql().upsert().into(context_name).csv("2,dos").build())

        print("====> get the connection to bios3 back")
        self.docker_proxy.start()

        # Verify the added signal can be retrieved properly
        print("====> verify that the signal config is still retrievable")
        out = admin.get_signal(signal_name)
        assert out is not None
        assert out.get("signalName") == signal_name
        print(out)
        assert len(out.get("attributes")) == 2

        # In the forked process, the client checks if the node 3 recovers
        print("====> verify that the node 3 has been recovered from maintenance mode")
        parent_conn.poll(70)
        result2 = parent_conn.recv()
        assert result2 == "Recovered"

        admin2.close()

        # Verify the down mark has been removed
        print("====> verify that there are no fail marks in the endpoint list")
        for _ in range(20):
            endpoints = self.sadmin.get_property("failed_endpoints")
            if not endpoints:
                break
            time.sleep(1)
        assert endpoints == ""

        # Try ingest
        ingest1 = bios.login(ep_url(), f"{ingest_user}@{self.TENANT}", ingest_pass)
        response = ingest1.execute(bios.isql().insert().into(signal_name).csv("one,1").build())

        # Stop the node 1 service  and modify
        print("====> stop node 1")
        self.docker_bios[0].stop()
        time.sleep(1)

        admin.update_signal(signal_name, second)
        start = time.time()
        while time.time() - start < 10:
            admin.execute(bios.isql().upsert().into(context_name).csv("3,tres").build())

        ingest2 = bios.login(
            f"https://{host1}:{port1}", f"{ingest_user}@{self.TENANT}", ingest_pass
        )
        print("====> verify that ingestions go fine")
        ingest2.execute(bios.isql().insert().into(signal_name).csv("two,2,8199").build())
        ingest2.execute(bios.isql().insert().into(signal_name).csv("three,3,9878").build())
        ingest2.execute(bios.isql().insert().into(signal_name).csv("four,4,10029").build())
        ingest2.execute(bios.isql().insert().into(signal_name).csv("five,5,317").build())
        ingest2.execute(bios.isql().insert().into(signal_name).csv("six,6,7198").build())

        # Restart the node 1
        print("====> rstart node 1")
        self.docker_bios[0].start()

        print("====> verify ingestion again")
        ingest1.execute(bios.isql().insert().into(signal_name).csv("seven,7,9123").build())

        # Try extracting via node 1
        print("====> data verification -- from Python SDK")
        for _ in range(30):
            try:
                extract1 = bios.login(
                    f"https://{host0}:{port0}", f"{extract_user}@{self.TENANT}", extract_pass
                )
                break
            except ServiceError as err:
                pass

        start = response.records[0].get_timestamp()
        events = extract1.execute(
            bios.isql()
            .select()
            .from_signal(signal_name)
            .time_range(start, int(time.time() * 1000 - start))
            .build()
        ).to_dict()
        assert len(events) == 7
        assert events[0].get("the_value") == "ichi"
        assert events[1].get("the_value") == "dos"
        assert events[2].get("the_value") == "tres"
        assert events[3].get("the_value") == "yon"
        assert events[4].get("the_value") == "go"
        assert events[5].get("the_value") == "roku"
        assert events[6].get("the_value") == "nana"

        print("====> wait for the upsertions from Java SDK completed")
        proc.wait()
        # Verify the put context executions from the Java SDK
        print("====> data verification -- from Java SDK")
        self.sadmin.send_log_message("Asking for failed endpoints")
        for _ in range(30):
            failed_endpoints = self.sadmin.get_property("failed_endpoints")
            if not failed_endpoints:
                break
            time.sleep(1)
        self.sadmin.send_log_message("Asked")
        assert failed_endpoints == ""
        for i in range(context_count):
            entries = admin.execute(
                bios.isql()
                .select()
                .from_context(context_name)
                .where(keys=[[str(i) + "00"]])
                .build()
            ).to_dict()
            assert entries[0].get("the_value") == str(i) + "b"

    def test_operate_signal_with_a_stopped_node(self):
        """test inserting into a stream with the node 1 down"""
        insert_session = bios.login(ep_url(), f"{ingest_user}@{self.LB_TENANT}", ingest_pass)
        extract_session = bios.login(ep_url(), f"{extract_user}@{self.LB_TENANT}", extract_pass)
        try:
            self.docker_bios[0].stop()
            time.sleep(2)
            # There is only one signal in this config and its down
            response = insert_session.execute(
                bios.isql().insert().into("simple").csv("hello,11051918").build()
            )
            ingest_timestamp = response.records[0].get_timestamp()

            events = extract_session.execute(
                bios.isql()
                .select()
                .from_signal("simple")
                .time_range(ingest_timestamp, bios.time.now() - ingest_timestamp)
                .build()
            ).to_dict()
            assert len(events) == 1
            assert events[0].get("first") is not None, events[0]
            assert events[0].get("first") == "hello", events[0]
            assert events[0].get("second") is not None, events[0]
            assert events[0].get("second") == 11051918, events[0]
        finally:
            self.docker_bios[0].start()
            insert_session.close()
            extract_session.close()

    def test_rollup_restart(self):
        """Test restarting rollups on failure recovery"""

        simple_context = {
            "contextName": "simpleContext",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "the_key", "type": "string"},
                {"attributeName": "the_value", "type": "string"},
            ],
            "primaryKey": ["the_key"],
        }
        simple_feature = {
            "signalName": "simpleFeature",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "key", "type": "string"},
                {"attributeName": "value", "type": "integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "byKey",
                        "dimensions": ["key"],
                        "attributes": ["value"],
                        "featureInterval": 5000,
                    },
                ],
            },
        }

        signal_name = simple_feature.get("signalName")
        context_name = simple_context.get("contextName")

        with bios.login(ep_url(), f"{admin_user}@{self.TENANT}", admin_pass) as admin:
            print(f"creating context {simple_context.get('contextName')}")
            admin.create_context(simple_context)
            print(f"creating signal {simple_feature.get('signalName')}")
            admin.create_signal(simple_feature)

            num_initial_inserts = 30
            for idx in range(num_initial_inserts):
                admin.execute(bios.isql().insert().into(signal_name).csv(f"one,{idx}").build())

            time.sleep(5)
            for _ in range(40):
                resp = admin.execute(
                    bios.isql()
                    .select("count()")
                    .from_signal(signal_name)
                    .group_by("key")
                    .tumbling_window(bios.time.minutes(1))
                    .snapped_time_range(
                        bios.time.now(), bios.time.minutes(-1), bios.time.seconds(5)
                    )
                    .build()
                )
                data_windows = resp.get_data_windows()
                if len(data_windows) > 0 and len(data_windows[0].get_records()) > 0:
                    break
                time.sleep(1)
            records = data_windows[0].get_records()
            assert len(records) == 1
            assert records[0].get("count()") == num_initial_inserts

            start = bios.time.now()

            print("====> start populating signals in a separate thread")
            proc = Process(target=self.populate_feature_signal, args=(self.TENANT, signal_name))
            proc.start()

            time.sleep(5)
            print("====> destroying connectivity to BIOS node 3 by stopping the proxy")
            self.docker_proxy.stop()
            time.sleep(10)
            admin.execute(bios.isql().upsert().into(context_name).csv("hello,world").build())
            self.docker_proxy.start()

            proc.join()
            print("====> populating is done, waiting for the signal rolled up")

            time.sleep(10)
            for _ in range(40):
                resp = admin.execute(
                    bios.isql()
                    .select("count()")
                    .from_signal(signal_name)
                    .group_by("key")
                    .tumbling_window(bios.time.minutes(2))
                    .snapped_time_range(start, bios.time.minutes(2), bios.time.seconds(5))
                    .build()
                )
                data_windows = resp.get_data_windows()
                if len(data_windows) > 0 and len(data_windows[0].get_records()) == 5:
                    min_count = 1000
                    for record in data_windows[0].get_records():
                        min_count = min(min_count, record.get("count()"))
                    if min_count == 20:
                        break
                time.sleep(1)

            print("====> verifying rolled up data")
            assert len(data_windows[0].get_records()) == 5
            for record in data_windows[0].get_records():
                assert record.get("key") in {"0", "1", "2", "3", "4"}
                assert record.get("count()") == 20

    def populate_feature_signal(self, tenant_name, signal_name):
        with bios.login(ep_url(), f"{admin_user}@{tenant_name}", admin_pass) as admin:
            num_inserts = 100
            for idx in range(num_inserts):
                key = idx % 5
                admin.execute(bios.isql().insert().into(signal_name).csv(f"{key},{idx}").build())
                time.sleep(0.3)

    def get_proxy_endpoint(self):
        return os.environ.get("BIOS_PROXY_ENDPOINT") or "https://localhost:9443"

    def verify_node_3_reloading(self, host2, port2, signal_name, child_conn):
        if "BIOS_CONNECTION_TYPE" in os.environ:
            del os.environ["BIOS_CONNECTION_TYPE"]
        try:
            with bios.login(
                f"https://{host2}:{port2}", f"{admin_user}@{self.TENANT}", admin_pass
            ) as admin:
                # Verify the node 3 enters maintenance mode
                # print("++++> verify that the node 3 enters maintenance mode")
                time0 = time.time()
                stream_not_found = True
                first_result = None
                while time.time() < time0 + 60 and stream_not_found:
                    try:
                        admin.get_signal(signal_name)
                        stream_not_found = False
                        first_result = "recovered already"
                    except ServiceError as err:
                        if err.error_code == ErrorCode.NO_SUCH_STREAM:
                            time.sleep(0.2)
                            continue
                        stream_not_found = False
                        if err.error_code in (
                            ErrorCode.SERVICE_UNAVAILABLE,
                            ErrorCode.SERVER_CONNECTION_FAILURE,
                        ):
                            first_result = "in maintenance"
                        else:
                            first_result = f"unexpected error: {err}"
                        break
                elapsed_time = time.time() - time0
                print(f"first_result={first_result} time taken={elapsed_time}")
                child_conn.send(first_result)

                # Test if the service recovers in 40 seconds
                time1 = time.time()
                recovered = "Recovery failed"
                while time.time() < time1 + 80:
                    try:
                        out3 = admin.get_signal(signal_name)
                        assert out3 is not None
                        assert out3.get("signalName") == signal_name
                        print(out3)
                        assert len(out3.get("attributes")) == 2
                        recovered = "Recovered"
                        break
                    except ServiceError as err:
                        if err.error_code in (
                            ErrorCode.SERVICE_UNAVAILABLE,
                            ErrorCode.SERVER_CONNECTION_FAILURE,
                        ):
                            continue
                        print(f"ERROR: {err}")
                        break
                child_conn.send(recovered)

        except Exception as err:
            print(f"ERROR in the forked process: {err}")
            child_conn.send(False)
            child_conn.send(False)


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv))
