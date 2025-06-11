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

import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user
from tutils import run_negative_test


class TestEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            cls.saved_endpoints = session.list_endpoints()

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as session:
            for endpoint in session.list_endpoints():
                session.delete_endpoint(endpoint)
            for endpoint in cls.saved_endpoints:
                session.add_endpoint(endpoint)

    def setUp(self):
        self.admin = bios.login(ep_url(), sadmin_user, sadmin_pass)

    def tearDown(self):
        self.admin.close()

    def test_endpoints_operations(self):
        """add, list, and remove endpoints"""

        endpoints0 = self.admin.list_endpoints()

        # we cannot test this in single node environment
        if not endpoints0:
            return

        # test add conflict -- should be ignored
        self.admin.add_endpoint(endpoints0[0])
        endpoints = self.admin.list_endpoints()
        self.assertEqual(len(endpoints), len(endpoints0))
        for endpoint in endpoints:
            self.assertTrue(endpoint, endpoint in endpoints0)

        # test deleting endpoints
        index = int(len(endpoints0) / 2)
        self.admin.delete_endpoint(endpoints0[index])
        endpoints = self.admin.list_endpoints()
        self.assertEqual(len(endpoints), len(endpoints0) - 1)
        for i in range(len(endpoints)):
            if i != index:
                self.assertTrue(endpoints[i], endpoints[i] in endpoints0)

        # test delete conflict
        self.admin.delete_endpoint(endpoints0[index])
        endpoints = self.admin.list_endpoints()
        self.assertEqual(len(endpoints), len(endpoints0) - 1)
        for i in range(len(endpoints)):
            if i != index:
                self.assertTrue(endpoints[i], endpoints[i] in endpoints0)

        # test adding back
        self.admin.add_endpoint(endpoints0[index])
        endpoints = self.admin.list_endpoints()
        endpoints = self.admin.list_endpoints()
        self.assertEqual(len(endpoints), len(endpoints0))
        for endpoint in endpoints:
            self.assertTrue(endpoint, endpoint in endpoints0)

    def test_add_endpoint_args(self):
        """JIRA-ID : TFOS-624
        Verify argument checkers
        """
        run_negative_test(
            self,
            lambda: self.admin.add_endpoint(None),
            ErrorCode.INVALID_ARGUMENT,
            "Parameter 'endpoint' must be of type <class 'str'>, "
            "but <class 'NoneType'> was specified",
        )

        run_negative_test(
            self,
            lambda: self.admin.add_endpoint(""),
            ErrorCode.INVALID_ARGUMENT,
            "Parameter 'endpoint' may not be empty",
        )

        run_negative_test(
            self,
            lambda: self.admin.add_endpoint(123),
            ErrorCode.INVALID_ARGUMENT,
            "Parameter 'endpoint' must be of type <class 'str'>, but <class 'int'> was specified",
        )

    def test_delete_endpoint_args(self):
        """JIRA-ID : TFOS-624
        Verify argument checkers
        """
        run_negative_test(
            self,
            lambda: self.admin.delete_endpoint(None),
            ErrorCode.INVALID_ARGUMENT,
            "Parameter 'endpoint' must be of type <class 'str'>, "
            "but <class 'NoneType'> was specified",
        )

        run_negative_test(
            self,
            lambda: self.admin.delete_endpoint(""),
            ErrorCode.INVALID_ARGUMENT,
            "Parameter 'endpoint' may not be empty",
        )

        run_negative_test(
            self,
            lambda: self.admin.delete_endpoint(123),
            ErrorCode.INVALID_ARGUMENT,
            "Parameter 'endpoint' must be of type <class 'str'>, but <class 'int'> was specified",
        )

    def test_endpoint_node_id_mapping(self):
        """BB-433 regression; Try deleting and adding an endpoint several times and verify the
        mapping stays the same. This test cannot verify the mapping is really correct (there's
        no way to know the right node ID for an endpoint from client side), but this test would
        fail if the symptom BB-433 exists.
        """
        endpoints = self.admin.list_endpoints()
        if not endpoints or len(endpoints) < 2:
            # we cannot execute this test when number of registered endpoints are less than two
            return
        ep = endpoints[0]
        initial_node_id = self.admin.get_property(f"endpoint:{ep}")
        for i in range(20):
            # insert a dummy operation to prevent the test cycle being synchronized with the SDK
            # load balancer. This would break the "thythm."
            if i % 2:
                self.admin.list_endpoints()
            self.admin.delete_endpoint(ep)

            # Verify that the node lookup entry has been removed.
            # We'll retry a few times in case the DB content has not changed, since the
            # backend DB may delay in replicating the change and the request may reach such
            # a node.
            deleted_node_id = initial_node_id
            for i in range(3):
                deleted_node_id = self.admin.get_property(f"endpoint:{ep}")
                print(deleted_node_id)
                if not deleted_node_id:
                    break
                time.sleep(1)

            # deleted endpoint should not have EP - nodeID mapping
            self.assertEqual(deleted_node_id, "")
            self.admin.add_endpoint(ep)
            # EP - nodeID mapping should not change after recreating the endpoint
            current_node_id = ""
            for i in range(3):
                current_node_id = self.admin.get_property(f"endpoint:{ep}")
                if current_node_id:
                    break
                time.sleep(1)
            self.assertEqual(current_node_id, initial_node_id)


if __name__ == "__main__":
    pytest.main(sys.argv)
