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
import logging
import os
import sys
import time
import unittest
from time import localtime, strftime

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios_tutils import load_signal_json, next_checkpoint
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TEST_TENANT_NAME
from setup_common import setup_tenant_config
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

SIGNAL_NAME = "indexedFeatureTest"


class SelectWithViewTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.SIGNAL_CONFIG = load_signal_json("../resources/indexed_features.json")

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def insert_records(cls, session, signal_name, inputs):
        first_insertion_time = None
        for product_id, user_id, user_category, clicks in inputs:
            csv_text = (
                f"10,11,false,false,asource,false,{product_id},false,true,false,aplatform,"
                f"true,asessionId,false,{product_id},{user_id},{user_category},{clicks}"
            )
            insert_statement = bios.isql().insert().into(signal_name).csv(csv_text).build()
            response = session.execute(insert_statement)
            if not first_insertion_time:
                first_insertion_time = response.records[0].timestamp
        return first_insertion_time

    def test_index_basic(self):
        """Test select using index table of a signal that has an indexed feature"""
        signal_name = SIGNAL_NAME + "_basic"
        with (
            bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin,
            bios.login(ep_url(), ADMIN_USER, admin_pass) as session,
        ):
            signal = copy.deepcopy(self.SIGNAL_CONFIG)
            signal["signalName"] = signal_name
            try:
                session.delete_signal(signal_name)
            except ServiceError:
                pass
            print(f"Test tenant     : {TEST_TENANT_NAME}")
            print(f" ... Creating signal : {signal_name}")
            session.create_signal(signal)

            # avoid the edge of the window
            if int(time.time()) % 60 > 50:
                print(" ... Sleeping for 15 seconds to avoid a rollup edge")
                time.sleep(15)

            # insert first-tier records
            print(" ... Inserting first-tier records")
            user_names = ["user_a", "user_b", "user_c"]
            user_categories = ["GROUP_A", "GROUP_B", "GROUP_C", "GROUP_D"]
            inputs = []
            for i in range(30):
                inputs.append((i + 1, user_names[i % 3], user_categories[i % 4], i + 2))
            insert_time = self.insert_records(session, signal_name, inputs)
            insert_time_str = strftime("%Y-%m-%dT%H:%M:%S", localtime(insert_time / 1000.0))
            print(f" ... Insertion time: {insert_time_str}")

            print(" ... Sleeping for 60 seconds")
            time.sleep(60)

            # insert second-tier records
            print(" ... Inserting second-tier records")
            inputs = []
            for i in range(30):
                inputs.append((i + 31, user_names[i % 3], user_categories[(i + 30) % 4], i + 32))
            self.insert_records(session, signal_name, inputs)

            # wait for the first rollup being completed
            now = time.time()
            estimated_first_rollup = int(((insert_time / 1000) + 60) / 60) * 60 + 15
            if now < estimated_first_rollup:
                sleep_time = estimated_first_rollup - now
                until = strftime("%Y-%m-%dT%H:%M:%S", localtime(estimated_first_rollup))
                print(
                    f" ... Sleeping for {sleep_time:.1f} seconds"
                    f" until {until} to wait for the first rollup"
                )
                time.sleep(sleep_time)

            # Run select with view table, the second tier is not available, so only the first-tier
            # records are picked up

            # select attributes
            print(" ... Selecting indexed attributes (first)")
            query_time = bios.time.now()
            statement_indexed_select_product_id = (
                bios.isql()
                .select("productId")
                .from_signal(signal_name)
                .where("userId = 'user_a'")
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            num_trials = 6
            retried = False
            for i in range(num_trials):
                response_product_id_first = session.execute(statement_indexed_select_product_id)
                if i < num_trials - 1 and len(response_product_id_first.to_dict()) < 10:
                    print(
                        " ...   Response has not reached expected size, will retry after 5 seconds"
                    )
                    time.sleep(5)
                    retried = True
                    estimated_first_rollup += 5
                    continue
                break
            self.assertEqual(len(response_product_id_first.to_dict()), 10)
            for i in range(10):
                record = response_product_id_first.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), i * 3 + 1)

            statement_indexed_select_group_by = (
                bios.isql()
                .select("userId", "eventCategory", "sum(clicks)")
                .from_signal(signal_name)
                .where("userId IN ('user_a', 'user_c')")
                .group_by("userId", "eventCategory")
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            response_group_by = session.execute(statement_indexed_select_group_by)
            self.assertEqual(len(response_group_by.to_dict()), 8)
            expected_values = {}
            expected_values["user_a " + "GROUP_A"] = 42  # 2 + 14 + 26
            expected_values["user_a " + "GROUP_B"] = 34  # 11 + 23
            expected_values["user_a " + "GROUP_C"] = 28  # 8 + 20
            expected_values["user_a " + "GROUP_D"] = 51  # 5 + 17 + 29
            # expected_values['user_b ' + 'GROUP_A'] = 54     # 6 + 18 + 30
            # expected_values['user_b ' + 'GROUP_B'] = 45     # 3 + 15 + 27
            # expected_values['user_b ' + 'GROUP_C'] = 36     # 12 + 24
            # expected_values['user_b ' + 'GROUP_D'] = 30     # 9 + 21
            expected_values["user_c " + "GROUP_A"] = 32  # 10 + 22
            expected_values["user_c " + "GROUP_B"] = 57  # 7 + 19 + 31
            expected_values["user_c " + "GROUP_C"] = 48  # 4 + 16 + 28
            expected_values["user_c " + "GROUP_D"] = 38  # 13 + 25
            for i in range(len(response_group_by.get_data_windows()[0].records)):
                record = response_group_by.get_data_windows()[0].records[i]
                subset = {
                    record.get("userId")
                    + " "
                    + record.get("eventCategory"): record.get("sum(clicks)")
                }
                self.assertEqual(expected_values, {**expected_values, **subset})
                del expected_values[record.get("userId") + " " + record.get("eventCategory")]
            self.assertEqual(len(expected_values), 0)

            if not retried:
                inst_statement = (
                    bios.isql()
                    .select()
                    .from_signal("_query")
                    .where("tenant = 'biosPythonQA'")
                    .time_range(query_time - 10, bios.time.minutes(10))
                    .build()
                )
                time.sleep(2)
                inst_result = sadmin.execute(inst_statement).to_dict()
                self.assertEqual(len(inst_result), 2)
                self.assertEqual(inst_result[0].get("numDbQueries"), 1)
                self.assertEqual(inst_result[0].get("numDbRowsRead"), 10)
                self.assertGreater(inst_result[0].get("dbTotalLatency"), 0)

            # select attributes (indexed, two dimensional)
            time.sleep(1)
            print(" ... Selecting indexed attributes (2 dimensional, first)")
            query_time = bios.time.now()
            statement_indexed2_select_product_id = (
                bios.isql()
                .select("productId", "eventCategory")
                .from_signal(signal_name)
                .where("userId = 'user_a' and eventCategory = 'GROUP_A'")
                .order_by(":timestamp", reverse=True)
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            response_index2_first = session.execute(statement_indexed2_select_product_id)
            self.assertEqual(len(response_index2_first.to_dict()), 3)
            for i in range(3):
                record = response_index2_first.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), (2 - i) * 12 + 1)
                self.assertEqual(record.get("eventCategory"), "GROUP_A")

            inst_statement = (
                bios.isql()
                .select()
                .from_signal("_query")
                .where("tenant = 'biosPythonQA'")
                .time_range(query_time, bios.time.minutes(10))
                .build()
            )
            time.sleep(2)
            inst_result = sadmin.execute(inst_statement).to_dict()
            self.assertEqual(len(inst_result), 1)
            self.assertEqual(inst_result[0].get("numDbQueries"), 1)
            self.assertEqual(inst_result[0].get("numDbRowsRead"), 3)
            self.assertGreater(inst_result[0].get("dbTotalLatency"), 0)

            # select attribute (indexed, range query)
            print(" ... Selecting range-query-indexed attributes (first)")
            query_timex = bios.time.now()
            statement_indexed_select_product_id_range = (
                bios.isql()
                .select("productId", "clicks")
                .from_signal(signal_name)
                .where("productId >= 26 and productId < 36")
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            response_clicks_first = session.execute(statement_indexed_select_product_id_range)
            self.assertEqual(len(response_clicks_first.to_dict()), 5)
            for i in range(5):
                record = response_clicks_first.get_data_windows()[0].records[i]
                self.assertEqual(record.get("clicks"), i + 27)

            inst_statement = (
                bios.isql()
                .select()
                .from_signal("_query")
                .where("tenant = 'biosPythonQA'")
                .time_range(query_timex, bios.time.minutes(10))
                .build()
            )
            time.sleep(2)
            inst_result = sadmin.execute(inst_statement).to_dict()
            self.assertEqual(len(inst_result), 1)
            self.assertEqual(inst_result[0].get("numDbQueries"), 1)
            self.assertEqual(inst_result[0].get("numDbRowsRead"), 5)
            self.assertGreater(inst_result[0].get("dbTotalLatency"), 0)

            # try on-the-fly
            print(" ... Selecting range-query-indexed attributes on the fly")
            statement_indexed_select_product_id_range_on_the_fly = (
                bios.isql()
                .select("productId", "clicks")
                .from_signal(signal_name)
                .where("productId >= 26 and productId < 36")
                .time_range(insert_time, query_time - insert_time)
                .on_the_fly()
                .build()
            )
            response_clicks_on_the_fly = session.execute(
                statement_indexed_select_product_id_range_on_the_fly
            )
            self.assertEqual(len(response_clicks_on_the_fly.to_dict()), 10)
            for i in range(10):
                record = response_clicks_on_the_fly.get_data_windows()[0].records[i]
                self.assertEqual(record.get("clicks"), i + 27)

            statement_indexed_select_two_dimensions_range = (
                bios.isql()
                .select("productId", "eventCategory", "clicks")
                .from_signal(signal_name)
                .where("productId >= 26 and productId < 36 and eventCategory = 'GROUP_C'")
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            response_clicks_2d = session.execute(statement_indexed_select_two_dimensions_range)
            self.assertEqual(len(response_clicks_2d.to_dict()), 1)
            record = response_clicks_2d.get_data_windows()[0].records[0]
            self.assertEqual(record.get("clicks"), 28)

            # select aggregates
            time.sleep(1)
            query_time2 = bios.time.now()
            print(" ... Selecting indexed aggregates (first)")
            statement_indexed_aggregates = (
                bios.isql()
                .select("min(productId)", "max(productId)", "count()")
                .from_signal(signal_name)
                .where("userId = 'user_b'")
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            response_indexed_aggregates_first = session.execute(statement_indexed_aggregates)
            self.assertEqual(len(response_indexed_aggregates_first.to_dict()), 1)
            self.assertEqual(response_indexed_aggregates_first.to_dict()[0].get("count()"), 10)
            self.assertEqual(
                response_indexed_aggregates_first.to_dict()[0].get("min(productId)"), 2
            )
            self.assertEqual(
                response_indexed_aggregates_first.to_dict()[0].get("max(productId)"), 29
            )

            inst_statement = (
                bios.isql()
                .select()
                .from_signal("_query")
                .where("tenant = 'biosPythonQA'")
                .time_range(query_time2 - 10, bios.time.minutes(10))
                .build()
            )
            time.sleep(2)
            inst_result = sadmin.execute(inst_statement).to_dict()
            self.assertEqual(len(inst_result), 1)
            self.assertEqual(inst_result[0].get("numDbQueries"), 1)
            self.assertEqual(inst_result[0].get("numDbRowsRead"), 10)
            self.assertGreater(inst_result[0].get("dbTotalLatency"), 0)

            # But on-the-fly extraction should pick up all
            print(" ... Selecting on-the-fly attributes")
            time.sleep(1)
            query_time2 = bios.time.now()
            statement_on_the_fly_product_id = (
                bios.isql()
                .select("productId")
                .from_signal(signal_name)
                .where("userId = 'user_a'")
                .time_range(insert_time, query_time - insert_time)
                .on_the_fly()
                .build()
            )
            response_on_the_fly = session.execute(statement_on_the_fly_product_id)
            self.assertEqual(len(response_on_the_fly.to_dict()), 20)
            for i in range(20):
                record = response_on_the_fly.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), i * 3 + 1)

            inst_statement = (
                bios.isql()
                .select()
                .from_signal("_query")
                .where("tenant = 'biosPythonQA'")
                .time_range(query_time2 - 10, bios.time.minutes(10))
                .build()
            )
            time.sleep(2)
            inst_result = sadmin.execute(inst_statement).to_dict()
            self.assertEqual(len(inst_result), 1)
            self.assertEqual(inst_result[0].get("numDbQueries"), 2)
            self.assertEqual(inst_result[0].get("numDbRowsRead"), 20)
            self.assertGreater(inst_result[0].get("dbTotalLatency"), 0)

            print(" ... Selecting on-the-fly attributes reversely sorted by timestamp")
            statement_on_the_fly_product_id_sorted = (
                bios.isql()
                .select("productId")
                .from_signal(signal_name)
                .where("userId = 'user_a'")
                .order_by(":timestamp", reverse=True)
                .time_range(insert_time, query_time - insert_time)
                .on_the_fly()
                .build()
            )
            response_on_the_fly_sorted = session.execute(statement_on_the_fly_product_id_sorted)
            self.assertEqual(len(response_on_the_fly_sorted.to_dict()), 20)
            for i in range(20):
                record = response_on_the_fly_sorted.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), (20 - i - 1) * 3 + 1)

            print(" ... Selecting on-the-fly aggregates")
            statement_indexed_aggregates = (
                bios.isql()
                .select("min(productId)", "max(productId)", "count()")
                .from_signal(signal_name)
                .where("userId = 'user_b'")
                .time_range(insert_time, query_time - insert_time)
                .on_the_fly()
                .build()
            )
            response_indexed_aggregates_first = session.execute(statement_indexed_aggregates)
            self.assertEqual(len(response_indexed_aggregates_first.to_dict()), 1)
            self.assertEqual(response_indexed_aggregates_first.to_dict()[0].get("count()"), 20)
            self.assertEqual(
                response_indexed_aggregates_first.to_dict()[0].get("min(productId)"), 2
            )
            self.assertEqual(
                response_indexed_aggregates_first.to_dict()[0].get("max(productId)"), 59
            )

            print(" ... Selecting non-indexed aggregates")
            statement_unindexed_aggregates = (
                bios.isql()
                .select("min(product_Id)", "max(product_Id)", "count()")
                .from_signal(signal_name)
                .where("userId = 'user_c'")
                .time_range(insert_time, query_time - insert_time)
                .on_the_fly()
                .build()
            )
            response_unindexed_aggregates_first = session.execute(statement_unindexed_aggregates)
            self.assertEqual(len(response_unindexed_aggregates_first.to_dict()), 1)
            self.assertEqual(response_unindexed_aggregates_first.to_dict()[0].get("count()"), 20)
            self.assertEqual(
                response_unindexed_aggregates_first.to_dict()[0].get("min(product_Id)"), 3
            )
            self.assertEqual(
                response_unindexed_aggregates_first.to_dict()[0].get("max(product_Id)"), 60
            )

            # Wait for the second rollup being completed
            estimated_second_rollup = estimated_first_rollup + 60
            now = time.time()
            if now < estimated_second_rollup:
                sleep_time = estimated_second_rollup - now
                until = strftime("%Y-%m-%dT%H:%M:%S", localtime(estimated_second_rollup))
                print(
                    f" ... Sleeping for {sleep_time:.1f} seconds"
                    f" until {until} to wait for the second rollup"
                )
                time.sleep(sleep_time)

            # Run the select with view table again, all records should be available this time
            print(" ... Selecting indexed attributes (second)")
            for i in range(num_trials):
                response_product_id_second = session.execute(statement_indexed_select_product_id)
                if i < num_trials - 1 and len(response_product_id_second.to_dict()) < 20:
                    print(
                        " ...   Response has not reached expected size, will retry after 5 seconds"
                    )
                    time.sleep(5)
                    continue
                break
            self.assertEqual(len(response_product_id_second.to_dict()), 20)
            for i in range(20):
                record = response_product_id_second.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), i * 3 + 1)

            print(" ... Selecting indexed attributes (2 dimensional, second)")
            response_index2_second = session.execute(statement_indexed2_select_product_id)
            self.assertEqual(len(response_index2_second.to_dict()), 5)
            for i in range(5):
                record = response_index2_second.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), (4 - i) * 12 + 1)
                self.assertEqual(record.get("eventCategory"), "GROUP_A")

            print(" ... Selecting indexed aggregates (second)")
            response_indexed_aggregates_second = session.execute(statement_indexed_aggregates)
            self.assertEqual(len(response_indexed_aggregates_second.to_dict()), 1)
            self.assertEqual(response_indexed_aggregates_second.to_dict()[0].get("count()"), 20)
            self.assertEqual(
                response_indexed_aggregates_second.to_dict()[0].get("min(productId)"), 2
            )
            self.assertEqual(
                response_indexed_aggregates_second.to_dict()[0].get("max(productId)"), 59
            )

            # select attribute (indexed, range query)
            response_clicks_second = session.execute(statement_indexed_select_product_id_range)
            self.assertEqual(len(response_clicks_second.to_dict()), 10)
            for i in range(10):
                record = response_clicks_second.get_data_windows()[0].records[i]
                self.assertEqual(record.get("clicks"), i + 27)

            statement_range_enabled_index_select_aggregates = (
                bios.isql()
                .select("productId", "max(clicks)")
                .from_signal(signal_name)
                .group_by("max(clicks)")
                .where("productId >= 26 AND productId < 36")
                .group_by("productId")
                .tumbling_window(bios.time.minutes(1))
                .time_range(insert_time, bios.time.minutes(2), bios.time.minutes(1))
                .build()
            )
            response_max_clicks = session.execute(statement_range_enabled_index_select_aggregates)
            data_windows = response_max_clicks.get_data_windows()
            self.assertEqual(len(data_windows), 2)
            self.assertEqual(len(data_windows[0].records), 5)
            self.assertEqual(len(data_windows[1].records), 5)

            dict_data = response_max_clicks.to_dict()
            self.assertIsInstance(dict_data, list)
            self.assertEqual(len(dict_data), 2)
            window0 = dict_data[0]
            self.assertEqual(window0.get("timestamp"), data_windows[0].window_begin_time)
            self.assertEqual(len(window0.get("records")), 5)
            for i, record in enumerate(window0.get("records")):
                record_in_resp = data_windows[0].records[i]
                self.assertEqual(
                    record.get("productId"),
                    record_in_resp.get("productId"),
                    f"window[0].record[{i}]['productId']",
                )
                self.assertEqual(
                    record.get("max(clicks)"),
                    record_in_resp.get("max(clicks)"),
                    f"window[0].record[{i}]['max(clicks)']",
                )
            window1 = dict_data[1]
            self.assertEqual(window1.get("timestamp"), data_windows[1].window_begin_time)
            self.assertEqual(len(window1.get("records")), 5)
            for i, record in enumerate(window1.get("records")):
                record_in_resp = data_windows[1].records[i]
                self.assertEqual(
                    record.get("productId"),
                    record_in_resp.get("productId"),
                    f"window[1].record[{i}]['productId']",
                )
                self.assertEqual(
                    record.get("max(clicks)"),
                    record_in_resp.get("max(clicks)"),
                    f"window[1].record[{i}]['max(clicks)']",
                )

    def test_index_adding_feature(self):
        """Test adding indexed feature"""
        signal_name = SIGNAL_NAME + "_add_indexed_feature"
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            # create the test signal without feature
            initial_signal = copy.deepcopy(self.SIGNAL_CONFIG)
            initial_signal["signalName"] = signal_name
            del initial_signal["postStorageStage"]
            try:
                session.delete_signal(signal_name)
            except ServiceError:
                pass
            print(f"Test tenant                     : {TEST_TENANT_NAME}")
            print(f"Creating signal without feature : {signal_name}")
            session.create_signal(initial_signal)

            # avoid the edge of the window
            if int(time.time()) % 60 > 50:
                print("Sleeping for 15 seconds to run the test after the top of the minute")
                time.sleep(15)

            # insert first-tier records
            user_names = ["user_a", "user_b", "user_c"]
            user_categories = ["GROUP_A", "GROUP_B", "GROUP_C", "GROUP_D"]
            inputs = []
            for i in range(30):
                inputs.append((i + 1, user_names[i % 3], user_categories[i % 4], i + 2))
            initial_insert_time = self.insert_records(session, signal_name, inputs)
            print(f"Initial insertion time: {initial_insert_time}")

            sleep_until = next_checkpoint(initial_insert_time / 1000, 60) + 15
            sleep_time = sleep_until - time.time()
            if sleep_time > 0:
                print(f"Sleeping for {sleep_time} seconds to wait for the next rollup time passed")
                time.sleep(sleep_time)

            # add the feature
            second_signal = copy.deepcopy(self.SIGNAL_CONFIG)
            second_signal["signalName"] = signal_name
            print("Modifying the signal to add the indexed feature")
            session.update_signal(signal_name, second_signal)

            # insert second-tier records
            inputs = []
            for i in range(60):
                inputs.append((i + 31, user_names[i % 3], user_categories[(i + 30) % 4], i + 32))
            second_insert_time = self.insert_records(session, signal_name, inputs)
            print(f"Second insertion time: {second_insert_time}")

            # wait for the first rollup being completed
            now = time.time()
            estimated_second_rollup = next_checkpoint(second_insert_time / 1000, 60) + 5
            if now < estimated_second_rollup:
                sleep_time = estimated_second_rollup - now
                print(f"Sleeping for {sleep_time} seconds until {estimated_second_rollup}")
                time.sleep(sleep_time)

            # Run select with view table
            query_time = bios.time.now()
            statement = (
                bios.isql()
                .select("productId")
                .from_signal(signal_name)
                .where("userId = 'user_a'")
                .time_range(initial_insert_time, query_time - initial_insert_time)
                .build()
            )
            for _ in range(30):  # try several times in case the rollup delays
                response = session.execute(statement)
                if len(response.to_dict()) == 30:
                    break
                time.sleep(1)

            self.assertEqual(len(response.to_dict()), 30, json.dumps(response.to_dict(), indent=2))
            for i in range(30):
                record = response.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), i * 3 + 1)

    def test_index_modify_feature(self):
        """Test select with index table with a signal that has a non-indexed feature, then
        modify it to be indexed"""
        signal_name = SIGNAL_NAME + "_modifying_feature"
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            signal = copy.deepcopy(self.SIGNAL_CONFIG)
            signal["signalName"] = signal_name
            del signal.get("postStorageStage").get("features")[0]["indexed"]
            try:
                session.delete_signal(signal_name)
            except ServiceError:
                pass
            print(f"Test tenant                              : {TEST_TENANT_NAME}")
            print(f"Creating signal with non-indexed feature : {signal_name}")
            session.create_signal(signal)

            # avoid the edge of the window
            if int(time.time()) % 60 > 50:
                print("Sleeping for 15 seconds")
                time.sleep(15)

            # insert first-tier records
            user_names = ["user_a", "user_b", "user_c"]
            user_categories = ["GROUP_A", "GROUP_B", "GROUP_C", "GROUP_D"]
            inputs = []
            for i in range(30):
                inputs.append((i + 1, user_names[i % 3], user_categories[i % 4], i + 2))
            insert_time = self.insert_records(session, signal_name, inputs)
            print(f"Insertion time: {insert_time}")

            print("Sleeping for 60 seconds")
            time.sleep(60)

            # insert second-tier records
            inputs = []
            for i in range(30):
                inputs.append((i + 31, user_names[i % 3], user_categories[(i + 30) % 4], i + 32))
            self.insert_records(session, signal_name, inputs)

            # since the signal does not have an indexed feature that covers the query,
            # the results are available immediately (but the execution is slow)
            query_time = bios.time.now()
            filter_statement = (
                bios.isql()
                .select("productId")
                .from_signal(signal_name)
                .where("userId = 'user_a'")
                .time_range(insert_time, query_time - insert_time)
                .build()
            )
            response_first_filter = session.execute(filter_statement)
            self.assertEqual(len(response_first_filter.to_dict()), 20)
            for i in range(20):
                record = response_first_filter.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), i * 3 + 1)

            # wait for the first rollup being completed
            now = time.time()
            estimated_first_rollup = int(((insert_time / 1000) + 60) / 60) * 60 + 15
            if now < estimated_first_rollup:
                sleep_time = estimated_first_rollup - now
                print(f"Sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)

            # summaries of the first tier are available
            summary_statement = (
                bios.isql()
                .select("count()")
                .from_signal(signal_name)
                .group_by(["userId"])
                .tumbling_window(bios.time.minutes(1))
                .time_range(insert_time, bios.time.minutes(1))
                .build()
            )
            response_first_summary = session.execute(summary_statement)
            self.assertEqual(len(response_first_summary.get_data_windows()), 1)
            self.assertEqual(len(response_first_summary.get_data_windows()[0].records), 3)

            # Modify the signal to index the feature
            second_signal = copy.deepcopy(self.SIGNAL_CONFIG)
            second_signal["signalName"] = signal_name
            print("Modifying the signal to change the feature to be indexed")
            session.update_signal(signal_name, second_signal)

            # Wait for the second rollup being completed
            estimated_second_rollup = estimated_first_rollup + 55
            now = time.time()
            if now < estimated_second_rollup:
                sleep_time = estimated_second_rollup - now
                print(f"Sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)

            # Run the select with view table again, all records should be available
            for _ in range(10):  # repeat in case the rollup delays
                response_second = session.execute(filter_statement)
                if len(response_second.to_dict()) == 20:
                    break
                time.sleep(5)
            self.assertEqual(len(response_second.to_dict()), 20)
            for i in range(20):
                record = response_second.get_data_windows()[0].records[i]
                self.assertEqual(record.get("productId"), i * 3 + 1)

            # Ensure that the summaries are still available
            summary_statement = (
                bios.isql()
                .select("count()")
                .from_signal(signal_name)
                .group_by(["userId"])
                .tumbling_window(bios.time.minutes(1))
                .time_range(insert_time, bios.time.minutes(2), bios.time.minutes(1))
                .build()
            )
            response_first_summary = session.execute(summary_statement)
            self.assertEqual(len(response_first_summary.get_data_windows()), 2)
            self.assertEqual(len(response_first_summary.get_data_windows()[0].records), 3)
            self.assertEqual(len(response_first_summary.get_data_windows()[1].records), 3)

    def test_index_adding_invalid_feature(self):
        """Test adding indexed feature"""
        signal_name = SIGNAL_NAME + "_invalid_feature"
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            # create the test signal without feature
            initial_signal = copy.deepcopy(self.SIGNAL_CONFIG)
            initial_signal["signalName"] = signal_name
            del initial_signal["postStorageStage"]
            try:
                session.delete_signal(signal_name)
            except ServiceError:
                pass
            print(f"Test tenant                     : {TEST_TENANT_NAME}")
            print(f"Creating signal without feature : {signal_name}")
            session.create_signal(initial_signal)

            # Modify the signal to index the feature
            second_signal = copy.deepcopy(self.SIGNAL_CONFIG)
            second_signal["signalName"] = signal_name
            print("Modifying the signal to change the feature to be indexed")
            second_signal["postStorageStage"]["features"][0]["timeIndexInterval"] = 150000
            with self.assertRaises(ServiceError) as ec:
                session.update_signal(signal_name, second_signal)
            self.assertEqual(ec.exception.error_code, ErrorCode.BAD_INPUT)


if __name__ == "__main__":
    pytest.main(sys.argv)
