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

import logging
import time
import unittest

from bios._csdk import CSdkSession


class CsdkBasicTest(unittest.TestCase):
    """Verifies behavior of class CSddkSession where are not dependent on the server."""

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging.INFO,
        )
        # datefmt='%Y-%m-%d %H:%M:%S')
        cls.logger = logging.getLogger(__name__)

    def test_constructors(self):
        session1 = CSdkSession()
        session2 = CSdkSession()

        self.assertGreaterEqual(session1._session_handle, 0)
        self.assertEqual(session2._session_handle, session1._session_handle + 1)
        self.assertNotEqual(session1._fdr, session2._fdr)
        reply = session1._loop.run_until_complete(session1._echo("hello", 10))
        self.logger.info("reply=%s", reply)
        self.assertEqual(reply[1], "hello")

        session1._loop.run_until_complete(self.run_echos(session1))

    async def run_echos(self, session1):
        # verify no cross talks
        self.logger.info("begin echo methods")
        start_time = time.time()
        future11 = session1._would_echo("session1.op1", 200)
        future12 = session1._would_echo("session1.op2", 100)
        future21 = session1._would_echo("session1.op3", 250)
        future22 = session1._would_echo(b"\x01\x01\x10\x10\x11", 50, True)
        sent_time = time.time()
        self.logger.info("sent all requests")
        self.assertLess(sent_time - start_time, 0.1)
        result11 = await future11
        self.logger.info("result11: %s", result11)
        result12 = await future12
        self.logger.info("result12: %s", result12)
        result21 = await future21
        self.logger.info("result21: %s", result21)
        result22 = await future22
        self.logger.info("result22: %s", result22)
        done_time = time.time()
        self.assertEqual(result11[1], "session1.op1")
        self.assertEqual(result12[1], "session1.op2")
        self.assertEqual(result21[1], "session1.op3")
        self.assertEqual(result22[1], b"\x01\x01\x10\x10\x11")
        elapsed = done_time - start_time
        self.logger.info("elapsed=%s", elapsed)
        self.assertGreater(elapsed, 0.25)
        self.assertLess(elapsed, 0.35)


if __name__ == "__main__":
    unittest.main()
