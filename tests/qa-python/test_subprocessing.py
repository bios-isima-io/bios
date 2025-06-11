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
import sys
import threading
import time
import unittest

import bios

from bios_tutils import get_minimum_signal
from setup_common import (
    setup_tenant_config,
    setup_signal,
    BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME,
)
from tsetup import admin_pass, admin_user, sadmin_pass, sadmin_user
from tsetup import get_endpoint_url as ep_url


ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME
SIGNAL_NAME = "subProcessingTest"


class SubProcessingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config(clear_tenant=False)
        signal_config = get_minimum_signal(signal_name=SIGNAL_NAME)
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
            setup_signal(admin, signal_config)

    def test_fundamental(self):
        parent_conn, child_conn = Pipe()

        def _run():
            with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin2:
                child_conn.send(sadmin2.list_tenants())

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            tenants = sadmin.list_tenants()
            self.assertIsInstance(tenants, list)
            proc2 = Process(target=_run)
            proc2.start()
            tenants2 = parent_conn.recv()
            self.assertEqual(tenants2, tenants)

        proc3 = Process(target=_run)
        proc3.start()
        tenants3 = parent_conn.recv()
        self.assertEqual(tenants3, tenants)

        parent_conn.close()
        child_conn.close()

    def test_bulk_insertions_fork(self):
        parent_conn1, child_conn1 = Pipe()
        parent_conn2, child_conn2 = Pipe()
        parent_conn3, child_conn3 = Pipe()
        parent_conn4, child_conn4 = Pipe()

        proc1 = Process(target=self.subtask, args=(child_conn1,))
        proc2 = Process(target=self.subtask, args=(child_conn2,))
        proc3 = Process(target=self.subtask, args=(child_conn3,))
        proc4 = Process(target=self.subtask, args=(child_conn4,))

        proc1.start()
        proc2.start()
        proc3.start()
        proc4.start()

        proc1.join()
        proc2.join()
        proc3.join()
        proc4.join()

        self.assertEqual(parent_conn1.recv(), "ok")
        self.assertEqual(parent_conn2.recv(), "ok")
        self.assertEqual(parent_conn3.recv(), "ok")
        self.assertEqual(parent_conn4.recv(), "ok")

    def test_create_sessions_on_multi_threads(self):
        try:
            parent_conn1, child_conn1 = Pipe()
            parent_conn2, child_conn2 = Pipe()
            parent_conn3, child_conn3 = Pipe()
            parent_conn4, child_conn4 = Pipe()

            thread1 = threading.Thread(target=self.subtask, args=(child_conn1,))
            thread2 = threading.Thread(target=self.subtask, args=(child_conn2,))
            thread3 = threading.Thread(target=self.subtask, args=(child_conn3,))
            thread4 = threading.Thread(target=self.subtask, args=(child_conn4,))

            thread1.start()
            thread2.start()
            thread3.start()
            thread4.start()

            thread1.join()
            thread2.join()
            thread3.join()
            thread4.join()

            self.assertEqual(parent_conn1.recv(), "ok")
            self.assertEqual(parent_conn2.recv(), "ok")
            self.assertEqual(parent_conn3.recv(), "ok")
            self.assertEqual(parent_conn4.recv(), "ok")
        finally:
            bios.disable_threads()

    def test_share_session_by_multi_threads(self):
        """Bad idea, but user may do it"""
        bios.enable_threads()
        try:
            session = bios.login(ep_url(), ADMIN_USER, admin_pass)

            def insert(conn):
                try:
                    timestamp = int(time.time() * 1000)
                    statement = (
                        bios.isql()
                        .insert()
                        .into(SIGNAL_NAME)
                        .csv_bulk(
                            [
                                str(timestamp),
                                str(timestamp + 1),
                                str(timestamp + 2),
                                str(timestamp + 3),
                            ]
                        )
                        .build()
                    )
                    session.execute(statement)
                    conn.send("ok")
                except Exception as err:
                    conn.send("error: " + str(err))

            parent_conn1, child_conn1 = Pipe()
            parent_conn2, child_conn2 = Pipe()
            parent_conn3, child_conn3 = Pipe()
            parent_conn4, child_conn4 = Pipe()

            thread1 = threading.Thread(target=insert, args=(child_conn1,))
            thread2 = threading.Thread(target=insert, args=(child_conn2,))
            thread3 = threading.Thread(target=insert, args=(child_conn3,))
            thread4 = threading.Thread(target=insert, args=(child_conn4,))

            thread1.start()
            thread2.start()
            thread3.start()
            thread4.start()

            thread1.join()
            thread2.join()
            thread3.join()
            thread4.join()

            self.assertEqual(parent_conn1.recv(), "ok")
            self.assertEqual(parent_conn2.recv(), "ok")
            self.assertEqual(parent_conn3.recv(), "ok")
            self.assertEqual(parent_conn4.recv(), "ok")
        finally:
            bios.disable_threads()
            session.close()

    def test_negative_attempt_to_multithread_in_wrong_mode(self):
        parent_conn1, child_conn1 = Pipe()

        session = bios.login(ep_url(), ADMIN_USER, admin_pass)

        print("OK")

        def insert(conn):
            try:
                print("START", file=sys.stderr)
                timestamp = int(time.time() * 1000)
                statement = (
                    bios.isql()
                    .insert()
                    .into(SIGNAL_NAME)
                    .csv_bulk(
                        [
                            str(timestamp),
                            str(timestamp + 1),
                            str(timestamp + 2),
                            str(timestamp + 3),
                        ]
                    )
                    .build()
                )
                session.execute(statement)
                conn.send("ok")
            except Exception as err:
                conn.send("error: " + str(err))

        thread1 = threading.Thread(target=insert, args=(child_conn1,))

        thread1.start()

        thread1.join()

        session.close()

        self.assertEqual(
            parent_conn1.recv(),
            "error: Multi-threading is disabled."
            " Call 'bios.enable_threads() before starting sessions in multi-threads.",
        )

    def subtask(self, conn, expect_error=False):
        try:
            with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
                start = None
                for _ in range(100):
                    timestamp = int(time.time() * 1000)
                    if not start:
                        start = timestamp
                    statement = (
                        bios.isql()
                        .insert()
                        .into(SIGNAL_NAME)
                        .csv_bulk(
                            [
                                str(timestamp),
                                str(timestamp + 1),
                                str(timestamp + 2),
                                str(timestamp + 3),
                            ]
                        )
                        .build()
                    )
                    session.execute(statement)
                    # print()
            conn.send("ok")
        except Exception as err:
            conn.send("error: " + str(err))
            if not expect_error:
                raise


if __name__ == "__main__":
    unittest.main()
