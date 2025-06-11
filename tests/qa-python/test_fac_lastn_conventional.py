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

import copy
import json
import pprint
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_context, setup_signal, setup_tenant_config
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url
from tutils import get_next_checkpoint


class TestFeatureAsContextLastN(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.INTERVAL = 60000
        cls.SIGNAL_NAME = "featureAsContextLastNSignal"
        cls.SIGNAL_CONFIG = {
            "signalName": cls.SIGNAL_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "product", "type": "String"},
                {"attributeName": "name", "type": "String"},
                {"attributeName": "quantity", "type": "Integer"},
                {"attributeName": "phone", "type": "Integer"},
            ],
            "postStorageStage": {
                "features": [
                    {
                        "featureName": "byProduct",
                        "dimensions": ["product"],
                        "attributes": ["phone"],
                        "dataSketches": ["LastN"],
                        "featureInterval": cls.INTERVAL,
                        "items": 4,
                    },
                    {
                        "featureName": "quantityByProduct",
                        "dimensions": ["product"],
                        "attributes": ["quantity"],
                        "featureInterval": cls.INTERVAL * 5,
                    },
                ]
            },
        }
        cls.FEATURE_BY_PHONE = {
            "featureName": "byPhone",
            "dimensions": ["phone"],
            "attributes": ["name"],
            "dataSketches": ["LastN"],
            "featureInterval": cls.INTERVAL,
            "items": 15,
        }

        cls.PHONE_BY_PRODUCT_NAME = cls.SIGNAL_NAME + "_byProduct"
        cls.PHONE_BY_PRODUCT = {
            "contextName": cls.PHONE_BY_PRODUCT_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "productId", "type": "String"},
                {"attributeName": "phoneNumbers", "type": "String"},
            ],
            "primaryKey": ["productId"],
        }

        cls.NAME_BY_PHONE_NAME = cls.SIGNAL_NAME + "_byPhone"
        cls.NAME_BY_PHONE = {
            "contextName": cls.NAME_BY_PHONE_NAME,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "customerPhone", "type": "Integer"},
                {"attributeName": "customerNames", "type": "String"},
            ],
            "primaryKey": ["customerPhone"],
        }

        atc_schema_file = "../resources/fac_lastn.json"
        with open(atc_schema_file, "r") as file_atc:
            atc_schema = json.load(file_atc)

        cls.SIGNAL_ATC = atc_schema.get("signals")[0]
        cls.CONTEXT_ATC = atc_schema.get("contexts")[0]

        cls.FEATURE_ATC_LAST_N = {
            "featureName": "last15ProductIdbyObjectId",
            "dimensions": ["objectId"],
            "attributes": ["productId"],
            "dataSketches": ["LastN"],
            "featureInterval": cls.INTERVAL,
            "items": 100,
            "ttl": 10 * 365 * 24 * 3600 * 1000,
        }

        cls.ADMIN_USER = admin_user + "@" + TENANT_NAME

        setup_tenant_config()

        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            try:
                setup_context(admin, cls.PHONE_BY_PRODUCT)
                setup_context(admin, cls.NAME_BY_PHONE)
                setup_signal(admin, cls.SIGNAL_CONFIG)

                setup_context(admin, cls.CONTEXT_ATC)
                setup_signal(admin, cls.SIGNAL_ATC)
            except ServiceError:
                cls.tearDownClass()
                raise

            print(f"tenant={TENANT_NAME}, signal={cls.SIGNAL_NAME}")

            # wait if current time is close to the next boundary
            postprocess_interval = cls.INTERVAL * 2 / 1000
            now = time.time()
            next_checkpoint = get_next_checkpoint(now, postprocess_interval)
            print(
                "now: {}, next_checktpoint: {}".format(
                    cls._ts2date(now), cls._ts2date(next_checkpoint)
                )
            )
            eta = next_checkpoint - now
            if eta < postprocess_interval / 2 + 10:
                print("-- Sleeping until", cls._ts2date(eta + 10 + time.time()))
                time.sleep(eta + 10)
            print("now:", cls._ts2date(time.time()))
            print("-- Inserting initial events")

            # ingest a cluster of events to the stream
            initial_events = [
                "mobile,jack,10,9991234567",  # ...0
                "laptop,paul,20,9991234566",  # ...1
                "smarttv,nancyA,30,9991234565",  # 2
                "battery,paul,15,9991234569",  # ..3
                "tablet,paul,40,9991234566",  # ...4
                "tv,nancyB,50,9991234565",  # .....5
                "tablet,nick,60,9991234564",  # ...6
                "tv,anne,15,9991234569",  # .......7
                "battery,paul,15,9991234569",  # ..8
                "tv,eddy,45,9991234563",  # .......9
                "mobile,paul,80,9991234566",  # ..10
                "battery,anne,15,9991234569",  # .11
                "smarttv,gary,90,9991234562",  # .12
            ]
            responses = [
                admin.execute(bios.isql().insert().into(cls.SIGNAL_NAME).csv(event).build())
                for event in initial_events
            ]

            cls.start = responses[0].records[0].timestamp
            cls.timestamps_first = [response.records[0].timestamp for response in responses]
            print(f"start: {cls.start}")

            with open("../resources/atc_signal.json", "r") as atc_signal_file:
                cls.ATC_SIGNAL_DATA = json.load(atc_signal_file)
            for i in range(60, 64):
                event = cls.ATC_SIGNAL_DATA[i]
                csv = ",".join([str(attribute) for attribute in event.values()])
                admin.execute(
                    bios.isql().insert().into(cls.SIGNAL_ATC.get("signalName")).csv(csv).build()
                )

            with open("../resources/atc_context.json", "r") as atc_context_file:
                cls.ATC_CONTEXT_DATA = json.load(atc_context_file)
            for entry in cls.ATC_CONTEXT_DATA:
                collection = entry.get("productIdCollection").replace('"', '""')
                csv = f'{entry.get("objectId")},"{collection}"'
                admin.execute(
                    bios.isql().upsert().into(cls.CONTEXT_ATC.get("contextName")).csv(csv).build()
                )

            # Add the second feature -- we do this to test retroactive rollup
            print("-- Updating the signal to add another feature as context")
            updated_signal = copy.deepcopy(cls.SIGNAL_CONFIG)
            updated_signal.get("postStorageStage").get("features").append(cls.FEATURE_BY_PHONE)
            admin.update_signal(cls.SIGNAL_NAME, updated_signal)

            cls.SIGNAL_ATC.get("postStorageStage").get("features").append(cls.FEATURE_ATC_LAST_N)
            admin.update_signal(cls.SIGNAL_ATC.get("signalName"), cls.SIGNAL_ATC)

            # wait for rollup to complete
            cls._skip_interval(cls.start, interval=cls.INTERVAL)

            print("now:", cls._ts2date(time.time()))
            print("-- Inserting second events")
            # ingest another cluster of events to the stream
            second_events = [
                "smarttv,michael,3,8881234565",  # ...0
                "smarttv,oliver,2,8881234566",  # ....1
                "laptop,nancyC,1,9991234565",  # .....2
                "smartphone,anne,5,9991234569",  # ...3
                "mobile,eddy,8,9991234563",  # .......4
                "battery,michaelJr,2,8881234565",  # .5
                "mobile,oliver,3,8881234566",  # .....6
                "battery,june,3,8881234565",  # ......7
                "smarttv,paul,4,9991234566",  # ......8
                "smartphone,jack,7,9991234567",  # ...9
            ]

            responses2 = [
                admin.execute(bios.isql().insert().into(cls.SIGNAL_NAME).csv(event).build())
                for event in second_events
            ]

            cls.end = responses2[-1].records[0].timestamp
            cls.timestamps_second = [response.records[0].timestamp for response in responses2]

            print(f"end: {cls.end}")

            # wait for rollup to complete
            sleep_until = (
                int((cls.end + cls.INTERVAL - 1) / cls.INTERVAL) * cls.INTERVAL / 1000 + 20
            )
            print(f"-- Sleeping until {cls._ts2date(sleep_until)}")
            sleep_time = sleep_until - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            admin.delete_signal(cls.SIGNAL_ATC.get("signalName"))
            admin.delete_context(cls.CONTEXT_ATC.get("contextName"))
            admin.delete_signal(cls.SIGNAL_CONFIG.get("signalName"))
            admin.delete_context(cls.PHONE_BY_PRODUCT.get("contextName"))
            admin.delete_context(cls.NAME_BY_PHONE.get("contextName"))

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

    def _dump(self, method, summarize_timestamp, res):
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
                self._ts2date(self.start / 1000),
                self.start,
                self._ts2date(self.end / 1000),
                self.end,
            )
        )
        pprint.pprint(res)

    @classmethod
    def _ts2date(self, timestamp):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

    def test_schema(self):
        signal = self.client.get_signal(self.SIGNAL_NAME)
        pprint.pprint(signal)
        features = signal["postStorageStage"]["features"]
        assert features == [
            {
                "featureName": "byProduct",
                "dimensions": ["product"],
                "attributes": ["phone"],
                "materializedAs": "LastN",
                "featureInterval": self.INTERVAL,
                "items": 4,
                "ttl": 1296000000,
            },
            {
                "featureName": "quantityByProduct",
                "dimensions": ["product"],
                "attributes": ["quantity"],
                "featureInterval": self.INTERVAL * 5,
            },
            {
                "featureName": "byPhone",
                "dimensions": ["phone"],
                "attributes": ["name"],
                "materializedAs": "LastN",
                "featureInterval": self.INTERVAL,
                "items": 15,
                "ttl": 1296000000,
            },
        ]

    def test_summarize(self):
        # adjust the time to lower boundary of interval
        start_time = self.start
        delta = self.INTERVAL * 2

        # query for min(phone) grouped by product would be rejected since it's covered by
        # the feature byProduct which is for LastN data sketches that does not cover regular
        # moments aggregation
        statement = (
            bios.isql()
            .select("min(phone)")
            .from_signal(self.SIGNAL_NAME)
            .group_by("product")
            .tumbling_window(self.INTERVAL)
            .time_range(start_time, delta, self.INTERVAL)
            .build()
        )
        with self.assertRaises(ServiceError) as error_context:
            self.client.execute(statement)
        error = error_context.exception
        self.assertEqual(error.error_code, ErrorCode.BAD_INPUT)
        self.assertTrue("No suitable features found" in error.message)

        # but you can summarize if the query is not grouped; DataSketches covers this
        statement = (
            bios.isql()
            .select("min(phone)")
            .from_signal(self.SIGNAL_NAME)
            .tumbling_window(self.INTERVAL)
            .time_range(start_time, delta, self.INTERVAL)
            .build()
        )
        result = self.client.execute(statement)
        summarize_timestamp = time.time()

        try:
            expected_initial_ts = self._summarize_checkpoint_floor(self.start, self.INTERVAL)

            self.assertEqual(len(result.data_windows), 2)

            self.assertEqual(result.data_windows[0].window_begin_time, expected_initial_ts)
            self.assertEqual(len(result.data_windows[0].records), 1)
            self.assertEqual(result.data_windows[0].records[0].get("min(phone)"), 9991234562)

            self.assertEqual(
                result.data_windows[1].window_begin_time, expected_initial_ts + self.INTERVAL
            )
            self.assertEqual(len(result.data_windows[1].records), 1)
            self.assertEqual(result.data_windows[1].records[0].get("min(phone)"), 8881234565)
        except Exception:
            self._dump(self.test_summarize, summarize_timestamp, result)
            raise

    def test_last_four_phone_numbers_on_product_id(self):
        """Test LastN data sketches with a string key"""

        statement = (
            bios.isql()
            .select()
            .from_context(self.PHONE_BY_PRODUCT_NAME)
            .where(keys=[["smarttv"], ["laptop"], ["smartphone"], ["tv"], ["mobile"]])
            .build()
        )
        result = self.client.execute(statement)
        records = result.to_dict()

        try:
            self.assertEqual(len(records), 5)
            # smarttv
            self.assertEqual(records[0].get("productId"), "smarttv")
            values0 = json.loads(records[0].get("phoneNumbers")).get("c")
            self.assertEqual(len(values0), 4)
            self.assertEqual(values0[0].get("t"), self.timestamps_first[12])
            self.assertEqual(values0[0].get("v"), 9991234562)
            self.assertEqual(values0[1].get("t"), self.timestamps_second[0])
            self.assertEqual(values0[1].get("v"), 8881234565)
            self.assertEqual(values0[2].get("t"), self.timestamps_second[1])
            self.assertEqual(values0[2].get("v"), 8881234566)
            self.assertEqual(values0[3].get("t"), self.timestamps_second[8])
            self.assertEqual(values0[3].get("v"), 9991234566)

            # laptop
            self.assertEqual(records[1].get("productId"), "laptop")
            values1 = json.loads(records[1].get("phoneNumbers")).get("c")
            self.assertEqual(len(values1), 2)
            self.assertEqual(values1[0].get("t"), self.timestamps_first[1])
            self.assertEqual(values1[0].get("v"), 9991234566)
            self.assertEqual(values1[1].get("t"), self.timestamps_second[2])
            self.assertEqual(values1[1].get("v"), 9991234565)

            # smartphone
            self.assertEqual(records[2].get("productId"), "smartphone")
            values2 = json.loads(records[2].get("phoneNumbers")).get("c")
            self.assertEqual(len(values2), 2)
            self.assertEqual(values2[0].get("t"), self.timestamps_second[3])
            self.assertEqual(values2[0].get("v"), 9991234569)
            self.assertEqual(values2[1].get("t"), self.timestamps_second[9])
            self.assertEqual(values2[1].get("v"), 9991234567)

            # tv
            self.assertEqual(records[3].get("productId"), "tv")
            values3 = json.loads(records[3].get("phoneNumbers")).get("c")
            self.assertEqual(len(values3), 3)
            self.assertEqual(values3[0].get("t"), self.timestamps_first[5])
            self.assertEqual(values3[0].get("v"), 9991234565)
            self.assertEqual(values3[1].get("t"), self.timestamps_first[7])
            self.assertEqual(values3[1].get("v"), 9991234569)
            self.assertEqual(values3[2].get("t"), self.timestamps_first[9])
            self.assertEqual(values3[2].get("v"), 9991234563)

            # mobile
            self.assertEqual(records[4].get("productId"), "mobile")
            values4 = json.loads(records[4].get("phoneNumbers")).get("c")
            self.assertEqual(len(values4), 4)
            self.assertEqual(values4[0].get("t"), self.timestamps_first[0])
            self.assertEqual(values4[0].get("v"), 9991234567)
            self.assertEqual(values4[1].get("t"), self.timestamps_first[10])
            self.assertEqual(values4[1].get("v"), 9991234566)
            self.assertEqual(values4[2].get("t"), self.timestamps_second[4])
            self.assertEqual(values4[2].get("v"), 9991234563)
            self.assertEqual(values4[3].get("t"), self.timestamps_second[6])
            self.assertEqual(values4[3].get("v"), 8881234566)

        except Exception:
            pprint.pprint(records)
            raise

    def test_last_fifteen_names_on_phone_number(self):
        """Test LastN data sketches with a integer key, this case also tests retroactive rollup"""

        statement = (
            bios.isql()
            .select()
            .from_context(self.NAME_BY_PHONE_NAME)
            .where(keys=[[9991234565], [9991234562], [8881234565]])
            .build()
        )
        result = self.client.execute(statement)
        records = result.to_dict()

        try:
            self.assertEqual(len(records), 3)
            # smarttv
            self.assertEqual(records[0].get("customerPhone"), 9991234565)
            values0 = json.loads(records[0].get("customerNames")).get("c")
            self.assertEqual(len(values0), 3)
            self.assertEqual(values0[0].get("t"), self.timestamps_first[2])
            self.assertEqual(values0[0].get("v"), "nancyA")
            self.assertEqual(values0[1].get("t"), self.timestamps_first[5])
            self.assertEqual(values0[1].get("v"), "nancyB")
            self.assertEqual(values0[2].get("t"), self.timestamps_second[2])
            self.assertEqual(values0[2].get("v"), "nancyC")

            # laptop
            self.assertEqual(records[1].get("customerPhone"), 9991234562)
            values1 = json.loads(records[1].get("customerNames")).get("c")
            self.assertEqual(len(values1), 1)
            self.assertEqual(values1[0].get("t"), self.timestamps_first[12])
            self.assertEqual(values1[0].get("v"), "gary")

            # smartphone
            self.assertEqual(records[2].get("customerPhone"), 8881234565)
            values2 = json.loads(records[2].get("customerNames")).get("c")
            self.assertEqual(len(values2), 3)
            self.assertEqual(values2[0].get("t"), self.timestamps_second[0])
            self.assertEqual(values2[0].get("v"), "michael")
            self.assertEqual(values2[1].get("t"), self.timestamps_second[5])
            self.assertEqual(values2[1].get("v"), "michaelJr")
            self.assertEqual(values2[2].get("t"), self.timestamps_second[7])
            self.assertEqual(values2[2].get("v"), "june")

        except Exception:
            pprint.pprint(records)
            raise

    @unittest.skip("BIOS-5898")
    def test_script_to_feature_takeover(self):
        """Test actual data and see if the added feature works fine on top of existing data
        generated by the script"""
        self.maxDiff = None
        keys = [[entry.get("objectId")] for entry in self.ATC_CONTEXT_DATA]
        result = self.client.execute(
            bios.isql()
            .select()
            .from_context(self.CONTEXT_ATC.get("contextName"))
            .where(keys=keys)
            .build()
        )
        retrieved_entries = result.to_dict()
        self.assertEqual(retrieved_entries[0:60], self.ATC_CONTEXT_DATA[0:60])

        # the last four entries should be updated
        for i, entry in enumerate(retrieved_entries[60:64]):
            original = self.ATC_CONTEXT_DATA[60 + i]
            original_collection = json.loads(original.get("productIdCollection")).get("c")
            retrieved_collection = json.loads(entry.get("productIdCollection")).get("c")
            self.assertEqual(len(retrieved_collection), len(original_collection) + 1)
            self.assertEqual(
                retrieved_collection[: len(retrieved_collection) - 1], original_collection
            )
            appended = retrieved_collection[-1]
            self.assertEqual(appended.get("v"), self.ATC_SIGNAL_DATA[60 + i].get("productId"))

    def test_fetching_fac_schemas(self):
        name_by_phone = self.client.get_context(self.NAME_BY_PHONE_NAME)
        assert name_by_phone.get("isInternal") is True
        audit_name = f"audit{self.NAME_BY_PHONE_NAME}".lower()
        audit_name_by_phone = self.client.get_signal(audit_name)
        assert audit_name_by_phone.get("isInternal") is True

        contexts = self.client.get_contexts()
        assert self.NAME_BY_PHONE_NAME not in [ctx.get("contextName") for ctx in contexts]

        contexts = self.client.get_contexts(include_internal=True)
        assert self.NAME_BY_PHONE_NAME in [ctx.get("contextName") for ctx in contexts]

        signals = self.client.get_signals()
        assert audit_name not in [sig.get("signalName").lower() for sig in signals]

        signals = self.client.get_signals(include_internal=True)
        assert audit_name in [sig.get("signalName").lower() for sig in signals]

    def test_deleting_context(self):
        with bios.login(ep_url(), f"{admin_user}@{TENANT_NAME}", admin_pass) as session:
            with pytest.raises(ServiceError) as exc_info:
                session.delete_context(self.PHONE_BY_PRODUCT_NAME)
            print(exc_info.value)
            assert exc_info.value.error_code == ErrorCode.BAD_INPUT
            assert (
                "Constraint violation: Context cannot be deleted"
                " because other streams depend on it" in exc_info.value.message
            )

    def test_schema_creation_deletion(self):
        signal_name = "lastNCrud"
        context_name = "lastNCrud_byProduct"
        signal = copy.deepcopy(self.SIGNAL_CONFIG)
        signal["signalName"] = signal_name
        context = copy.deepcopy(self.PHONE_BY_PRODUCT)
        context["contextName"] = context_name

        with bios.login(ep_url(), f"{admin_user}@{TENANT_NAME}", admin_pass) as session:
            session.create_context(context)
            session.create_signal(signal)
            session.delete_signal(signal_name)
            session.delete_context(context_name)


if __name__ == "__main__":
    pytest.main(sys.argv)
