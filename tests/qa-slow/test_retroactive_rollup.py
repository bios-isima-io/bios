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
import time
import unittest

import bios
from bios import ServiceError
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user
from tutils import get_next_checkpoint

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config


class TestRetroactiveRollup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.INTERVAL = 60000
        cls.SIGNAL_NAME = "retroactiveRollupTest"
        cls.SIGNAL_CONFIG = {
            "signalName": cls.SIGNAL_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
        }

        cls.SIGNAL_CONFIG2 = {
            "signalName": cls.SIGNAL_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "price", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "priceByName",
                        "dimensions": ["name"],
                        "attributes": ["price"],
                        "featureInterval": cls.INTERVAL,
                    },
                    {
                        "featureName": "quantitybyProduct",
                        "dimensions": ["product"],
                        "attributes": ["quantity"],
                        "featureInterval": cls.INTERVAL * 5,
                    },
                ]
            },
        }

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("prop.rollupDebugSignal", cls.SIGNAL_NAME)

        cls.ADMIN_USER = admin_user + "@" + TENANT_NAME

        setup_tenant_config()

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            try:
                admin.delete_signal(cls.SIGNAL_NAME)
            except ServiceError:
                pass
            admin.create_signal(cls.SIGNAL_CONFIG)
            # admin.update_signal(cls.SIGNAL_NAME, cls.SIGNAL_CONFIG2)

            print(f"tenant={TENANT_NAME}, signal={cls.SIGNAL_NAME}")

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("prop.rollupDebugSignal", "")

    @classmethod
    def _skip_interval(cls, origin, interval=None):
        """Method to sleep until the next rollup interval.

        The method sleeps until the next rollup window from the origin time

        Args:
            origin (int): Origin milliseconds.
            interval (int): Interval milliseconds to skip.  The default interval is used
                if not speicfied.
        """
        if not interval:
            interval = 60000
        sleep_time = (origin + interval) / 1000 - time.time()
        if sleep_time > 0:
            print(f"-- Sleeping until {cls._ts2date((origin + interval) / 1000)}")
            time.sleep(sleep_time)

    def setUp(self):
        self.client = bios.login(ep_url(), f"{extract_user}@{TENANT_NAME}", extract_pass)

    def tearDown(self):
        self.client.close()

    def _summarize_checkpoint_floor(self, timestamp, interval):
        return int(timestamp / interval) * interval

    def _dump(self, method, summarize_timestamp, start, end, res):
        """Dump test data in case of failure

        Args:
            method (obj): test method
            summarize_timestamp (int): Time when the summarize operation happened
            res (obj): Summarize response
        """
        print("failed:", method.__doc__)
        print("extract time={}", self._ts2date(summarize_timestamp))
        print(
            "start={} ({}), end={} ({})".format(
                self._ts2date(start / 1000),
                start,
                self._ts2date(end / 1000),
                end,
            )
        )
        for data_window in res.get_data_windows():
            print(f"timestamp: {data_window.window_begin_time}")
            for record in data_window.records:
                pprint.pprint(record)

    @classmethod
    def _ts2date(self, timestamp):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

    def test_summarize(self):
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            # wait if current time is close to the next boundary
            postprocess_interval = self.INTERVAL * 5 / 1000
            now = time.time()
            five_minutes_boundary = get_next_checkpoint(now, postprocess_interval)
            print(
                "now: {}, next_checktpoint: {}".format(
                    self._ts2date(now), self._ts2date(five_minutes_boundary)
                )
            )
            eta = five_minutes_boundary - now
            if eta > self.INTERVAL / 1000 * 2:
                sleep_sec = eta - self.INTERVAL / 1000 * 2 + 10
                print("-- Sleeping until", self._ts2date(sleep_sec + time.time()))
                time.sleep(sleep_sec)
            elif eta < self.INTERVAL / 1000:
                print("-- Sleeping until", self._ts2date(eta + 190 + time.time()))
                time.sleep(eta + 190)
            print("now:", self._ts2date(time.time()))
            print("-- Inserting the initial tier of events")

            # ingest a cluster of events to the stream
            initial_events = [
                "mobile,jack,10,1000",  # ...0
                "laptop,paul,20,2000",  # ...1
                "smarttv,nancy,30,3000",  # .2
                "battery,paul,15,4000",  # ..3
                "tablet,paul,40,5000",  # ...4
                "tv,nancy,50,2000",  # ......5
                "tablet,nick,60,6000",  # ...6
                "tv,anne,15,100",  # ........7
                "battery,paul,15,1000",  # ..8
                "tv,eddy,45,8000",  # .......9
                "mobile,paul,80,2500",  # ..10
                "battery,anne,15,300",  # ..11
                "smarttv,gary,90,10000",  # 12
            ]
            responses = [
                admin.execute(bios.isql().insert().into(self.SIGNAL_NAME).csv(event).build())
                for event in initial_events
            ]

            start = responses[0].records[0].timestamp
            timestamps_first = [response.records[0].timestamp for response in responses]
            print(f"start: {start}")

            # skip until the next rollup cycle
            self._skip_interval(start, interval=self.INTERVAL)

            print("now:", self._ts2date(time.time()))
            print("-- Inserting the second tier of events")
            # ingest another cluster of events to the stream
            second_events = [
                "smarttv,michael,3,9000",  # ..0
                "smarttv,nancy,2,8000",  # ...1
                "laptop,nancy,1,4000",  # .....2
                "smartphone,anne,5,3000",  # ..3
                "mobile,eddy,8,1000",  # ......4
                "battery,michael,4,100",  # ...5
                "mobile,nancy,3,800",  # ......6
                "battery,june,3,90",  # .......7
                "smarttv,jack,4,8000",  # .....8
                "smartphone,jack,7,5000",  # ..9
            ]

            responses2 = [
                admin.execute(bios.isql().insert().into(self.SIGNAL_NAME).csv(event).build())
                for event in second_events
            ]

            # skip until the next of next rollup cycle
            sleep_time = 120
            print("-- Sleeping until", self._ts2date(sleep_time + time.time()))
            time.sleep(sleep_time)

            print("now:", self._ts2date(time.time()))
            print("-- Inserting the third tier of events")
            # ingest another cluster of events to the stream
            third_events = [
                "battery,nancy,5,65",  # ..0
                "smartphone,nancy,1,6500",  # ...1
                "laptop,paul,1,10000",  # .....2
                "smarttv,paul,2,8500",  # ..3
            ]

            responses3 = [
                admin.execute(bios.isql().insert().into(self.SIGNAL_NAME).csv(event).build())
                for event in third_events
            ]

            end = responses3[-1].records[0].timestamp
            timestamps_third = [response.records[0].timestamp for response in responses3]

            print(f"end: {end}")

            admin.update_signal(self.SIGNAL_NAME, self.SIGNAL_CONFIG2)

            # wait for rollup to complete
            now = time.time()
            five_minutes_boundary = get_next_checkpoint(now, self.INTERVAL * 5 / 1000)
            sleep_time = five_minutes_boundary + 20 - time.time()
            if sleep_time > 0:
                print(f"-- Sleeping until {self._ts2date(time.time() + sleep_time)}")
                time.sleep(sleep_time)

        # adjust the time to lower boundary of interval
        start_time = start
        delta_1min = self.INTERVAL * 4
        delta_5mins = self.INTERVAL * 10

        # query for the 1 minute feature
        statement = (
            bios.isql()
            .select("name", "min(price)")
            .from_signal(self.SIGNAL_NAME)
            .group_by("name")
            .tumbling_window(self.INTERVAL)
            .time_range(start_time, delta_1min, self.INTERVAL)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = self._summarize_checkpoint_floor(start, self.INTERVAL)

            self.assertEqual(len(result.data_windows), 4)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 7)
            self.assertEqual(result.data_windows[0].records[0].get("name"), "anne")
            self.assertEqual(result.data_windows[0].records[0].get("min(price)"), 100)
            self.assertEqual(result.data_windows[0].records[1].get("name"), "eddy")
            self.assertEqual(result.data_windows[0].records[1].get("min(price)"), 8000)
            self.assertEqual(result.data_windows[0].records[2].get("name"), "gary")
            self.assertEqual(result.data_windows[0].records[2].get("min(price)"), 10000)
            self.assertEqual(result.data_windows[0].records[3].get("name"), "jack")
            self.assertEqual(result.data_windows[0].records[3].get("min(price)"), 1000)
            self.assertEqual(result.data_windows[0].records[4].get("name"), "nancy")
            self.assertEqual(result.data_windows[0].records[4].get("min(price)"), 2000)
            self.assertEqual(result.data_windows[0].records[5].get("name"), "nick")
            self.assertEqual(result.data_windows[0].records[5].get("min(price)"), 6000)
            self.assertEqual(result.data_windows[0].records[6].get("name"), "paul")
            self.assertEqual(result.data_windows[0].records[6].get("min(price)"), 1000)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + self.INTERVAL
            )
            self.assertEqual(len(result.data_windows[1].records), 6)
            self.assertEqual(result.data_windows[1].records[0].get("name"), "anne")
            self.assertEqual(result.data_windows[1].records[0].get("min(price)"), 3000)
            self.assertEqual(result.data_windows[1].records[1].get("name"), "eddy")
            self.assertEqual(result.data_windows[1].records[1].get("min(price)"), 1000)
            self.assertEqual(result.data_windows[1].records[2].get("name"), "jack")
            self.assertEqual(result.data_windows[1].records[2].get("min(price)"), 5000)
            self.assertEqual(result.data_windows[1].records[3].get("name"), "june")
            self.assertEqual(result.data_windows[1].records[3].get("min(price)"), 90)
            self.assertEqual(result.data_windows[1].records[4].get("name"), "michael")
            self.assertEqual(result.data_windows[1].records[4].get("min(price)"), 100)
            self.assertEqual(result.data_windows[1].records[5].get("name"), "nancy")
            self.assertEqual(result.data_windows[1].records[5].get("min(price)"), 800)

            self.assertEqual(
                result.data_windows[2].window_begin_time, expected_initial_ts + self.INTERVAL * 2
            )
            self.assertEqual(len(result.data_windows[2].records), 0)

            self.assertEqual(
                result.data_windows[3].window_begin_time, expected_initial_ts + self.INTERVAL * 3
            )
            self.assertEqual(len(result.data_windows[3].records), 2)
            self.assertEqual(result.data_windows[3].records[0].get("name"), "nancy")
            self.assertEqual(result.data_windows[3].records[0].get("min(price)"), 65)
            self.assertEqual(result.data_windows[3].records[1].get("name"), "paul")
            self.assertEqual(result.data_windows[3].records[1].get("min(price)"), 8500)
        except Exception:
            self._dump(self.test_summarize, summarize_timestamp, start, end, result)
            raise

        # query for the 5 minute feature
        statement = (
            bios.isql()
            .select("product", "sum(quantity)")
            .from_signal(self.SIGNAL_NAME)
            .group_by("product")
            .tumbling_window(self.INTERVAL * 5)
            .time_range(start_time, delta_5mins, self.INTERVAL * 5)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()
        try:
            expected_initial_ts = self._summarize_checkpoint_floor(start, self.INTERVAL * 5)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 7)
            self.assertEqual(result.data_windows[0].records[0].get("product"), "battery")
            self.assertEqual(result.data_windows[0].records[0].get("sum(quantity)"), 52)
            self.assertEqual(result.data_windows[0].records[1].get("product"), "laptop")
            self.assertEqual(result.data_windows[0].records[1].get("sum(quantity)"), 21)
            self.assertEqual(result.data_windows[0].records[2].get("product"), "mobile")
            self.assertEqual(result.data_windows[0].records[2].get("sum(quantity)"), 101)
            self.assertEqual(result.data_windows[0].records[3].get("product"), "smartphone")
            self.assertEqual(result.data_windows[0].records[3].get("sum(quantity)"), 12)
            self.assertEqual(result.data_windows[0].records[4].get("product"), "smarttv")
            self.assertEqual(result.data_windows[0].records[4].get("sum(quantity)"), 129)
            self.assertEqual(result.data_windows[0].records[5].get("product"), "tablet")
            self.assertEqual(result.data_windows[0].records[5].get("sum(quantity)"), 100)
            self.assertEqual(result.data_windows[0].records[6].get("product"), "tv")
            self.assertEqual(result.data_windows[0].records[6].get("sum(quantity)"), 110)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + self.INTERVAL * 5
            )
            self.assertEqual(len(result.data_windows[1].records), 4)
            self.assertEqual(result.data_windows[1].records[0].get("product"), "battery")
            self.assertEqual(result.data_windows[1].records[0].get("sum(quantity)"), 5)
            self.assertEqual(result.data_windows[1].records[1].get("product"), "laptop")
            self.assertEqual(result.data_windows[1].records[1].get("sum(quantity)"), 1)
            self.assertEqual(result.data_windows[1].records[2].get("product"), "smartphone")
            self.assertEqual(result.data_windows[1].records[2].get("sum(quantity)"), 1)
            self.assertEqual(result.data_windows[1].records[3].get("product"), "smarttv")
            self.assertEqual(result.data_windows[1].records[3].get("sum(quantity)"), 2)
        except Exception:
            self._dump(self.test_summarize, summarize_timestamp, start, end, result)
            raise


if __name__ == "__main__":
    unittest.main()
