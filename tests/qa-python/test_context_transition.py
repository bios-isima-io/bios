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
import pprint
import random
import sys
import time
import unittest
from collections import OrderedDict
from multiprocessing import Process

import bios
import pytest
from bios import ErrorCode, ServiceError
from tablename import make_table_name
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_context, setup_tenant_config

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

OP = "_operation"
PID = "productId"
PPID = "prevProductId"
PNAME = "productName"
PPNAME = "prevProductName"
CID = "categoryId"
PCID = "prevCategoryId"
PRICE = "price"
PPRICE = "prevPrice"
GTIN = "gtin"
PGTIN = "prevGtin"

PRODUCT_CONTEXT = {
    "contextName": "placeholder",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": PID, "type": "Integer"},
        {"attributeName": PNAME, "type": "String"},
        {"attributeName": CID, "type": "Integer"},
        {"attributeName": PRICE, "type": "String"},
    ],
    "primaryKey": [PID],
}


class ListItem:
    def __init__(self, value, prev_item=None, next_item=None):
        self.value = value
        self.prev_item = prev_item
        self.next_item = next_item


class ContextTransitionTest(unittest.TestCase):
    """Tests upserting context while updating to verify
    1. Operations do not fail
    2. Data does not break
    """

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        cls.ID_RANGE = 100
        cls.DATA_RANGE = 2

        cls.product_ids = []
        cls.product_names = []
        cls.category_ids = []
        cls.prices = []
        cls.gtins = []
        for i in range(cls.ID_RANGE):
            cls.product_ids.append(i + 1)
        for i in range(cls.DATA_RANGE):
            cls.product_names.append(str(i + 1))
            cls.category_ids.append((i + 1) * 10)
            cls.prices.append(str(10 + i / 10.0))
            cls.gtins.append(500 + i)

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    def tearDown(self):
        self.session.close()

    def test_context_transition(self):
        # Setup initial context (productId, productName, categoryId, price)
        context_name = "contextTransition"
        audit_signal_name = f"audit{context_name[0].upper()}{context_name[1:]}"
        context = copy.deepcopy(PRODUCT_CONTEXT)
        context["contextName"] = context_name
        context["auditEnabled"] = True

        # Create the initial context
        setup_context(self.session, context)

        # Second context: productId, productName, categoryId, price, gtin
        second_context = copy.deepcopy(context)
        second_context["attributes"].append(
            {"attributeName": GTIN, "type": "Integer", "default": -1}
        )

        # Third context: productId, productName, price, gtin
        third_context = copy.deepcopy(second_context)
        attrs = third_context.get("attributes")
        third_context["attributes"] = [attrs[0], attrs[1], attrs[3], attrs[4]]

        # The process modifies contexts from initial -> second -> third, then terminates
        process = Process(
            target=self.modify_context, args=(second_context, third_context, audit_signal_name)
        )
        process.start()

        # Required attributes are different for each context generation.
        # These filters are used to make a CSV record for each generation.
        filter_values = [
            lambda tup: [tup[0], tup[1], tup[2], tup[3], None],
            lambda tup: [tup[0], tup[1], tup[2], tup[3], tup[4]],
            lambda tup: [tup[0], tup[1], None, tup[3], tup[4]],
        ]

        # The main program keeps upserting records until the modify_context thread terminates
        generation = 0
        start = bios.time.now()
        upserted_records = []
        while process.is_alive():
            values = filter_values[generation](self.make_record())
            csv = ",".join([str(value) for value in values if value is not None])
            statement = bios.isql().upsert().into(context_name).csv(csv).build()
            try:
                self.session.execute(statement)
                upserted_records.append(values)
            except ServiceError as err:
                generation += 1
                if err.error_code not in [
                    ErrorCode.BAD_INPUT,
                    ErrorCode.SCHEMA_MISMATCHED,
                ] or generation >= len(filter_values):
                    raise
                time.sleep(3)
                print(values)
                print(err)

        time.sleep(5)

        delta = bios.time.now() - start

        # Retrieve the audit records
        resp_records = self.session.execute(
            bios.isql().select().from_signal(audit_signal_name).time_range(start, delta).build()
        ).to_dict()

        # Convert the audit records to a linked hash map
        audit_records = OrderedDict([(i, record) for i, record in enumerate(resp_records)])

        # pprint.pprint(audit_records)

        print(f"audit={len(audit_records)}, count={len(upserted_records)}")
        assert len(audit_records) <= len(upserted_records)

        # Generate expected context audit records locally, compare them with actual ones
        replayed_context = {}
        check_generation = [
            lambda ent: ent[4] is None,
            lambda ent: ent[2] is not None and ent[4] is not None,
            lambda ent: ent[2] is None,
        ]
        equals = [
            lambda e1, e2: e1[1] == e2[1] and e1[2] == e2[2] and e1[3] == e2[3],
            lambda e1, e2: e1[1] == e2[1] and e1[2] == e2[2] and e1[3] == e2[3] and e1[4] == e2[4],
            lambda e1, e2: e1[1] == e2[1] and e1[3] == e2[3] and e1[4] == e2[4],
        ]
        generation = 0
        history = []
        for i, upserted in enumerate(upserted_records):
            if not check_generation[generation](upserted):
                generation += 1
            product_id = upserted[0]
            existing = replayed_context.get(product_id)
            if not existing:
                expected_op = "Insert"
            else:
                if equals[generation](upserted, existing):
                    # no change
                    continue
                expected_op = "Update"

            # If we reach here, the corresponding audit record should exist
            try:
                assert len(audit_records) > 0, f"Upserted record index = {i}"
            except Exception:
                for item in history[-3:]:
                    gen, idx, ups, rec = item
                    print(f"[{idx}] gen={gen}, expected={ups}")
                    pprint.pprint(rec)

            for seek_index, (audit_index, audit_record) in enumerate(audit_records.items()):
                # verify the contents of the audit record
                message = f"[{audit_index}] {upserted} -- {audit_record}"
                try:
                    history.append((generation, audit_index, upserted, audit_record))
                    assert audit_record.get(OP) == expected_op, message
                    assert audit_record.get(PID) == product_id, message
                    assert audit_record.get(PNAME) == upserted[1], message
                    assert audit_record.get(PRICE) == upserted[3], message
                    assert audit_record.get(GTIN) == (upserted[4] or -1), message
                    if expected_op == "Update":
                        assert audit_record.get(PPNAME) == existing[1], message
                        assert audit_record.get(PPRICE) == existing[3], message
                        assert audit_record.get(PGTIN) == (existing[4] or -1), message

                    del audit_records[audit_index]
                    break

                except Exception:
                    for item in history[-3:]:
                        gen, idx, ups, rec = item
                        print(f"[{idx}] gen={gen}, expected={ups}")
                        pprint.pprint(rec)
                    print("-----")
                    for j in range(idx + 1, idx + 4):
                        pprint.pprint(audit_records.get(j))
                    print(f"\nfollowing upserted:")
                    pprint.pprint(upserted_records[i + 1 : i + 4])
                    if seek_index >= 3:
                        raise

            # Put the record to replayed local context
            replayed_context[product_id] = upserted

        # Verify that the context entries are as expected
        keys = [[key] for key in replayed_context]
        context_entries = self.session.execute(
            bios.isql().select().from_context(context_name).where(keys=keys).build()
        ).to_dict()

        assert len(context_entries) == len(keys)

        for context_entry in context_entries:
            product_id = context_entry.get(PID)
            expected_values = replayed_context.get(product_id)

            assert context_entry.get(PNAME) == expected_values[1]
            assert context_entry.get(PRICE) == expected_values[3]
            assert context_entry.get(GTIN) == (expected_values[4] or -1)

    @classmethod
    def modify_context(cls, second_context, third_context, audit_signal_name):
        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            tenant = session.get_tenant()
            print(make_table_name("tenant", tenant.get("tenantName"), tenant.get("version")))
            signal_1 = session.get_signal(audit_signal_name)
            print(
                make_table_name("signal", signal_1.get("signalName"), signal_1.get("biosVersion"))
            )
            time.sleep(3)
            session.update_context(second_context.get("contextName"), second_context)
            signal_2 = session.get_signal(audit_signal_name)
            print(
                make_table_name("signal", signal_2.get("signalName"), signal_2.get("biosVersion"))
            )
            time.sleep(10)
            session.update_context(third_context.get("contextName"), third_context)
            signal_3 = session.get_signal(audit_signal_name)
            print(
                make_table_name("signal", signal_3.get("signalName"), signal_3.get("biosVersion"))
            )
            time.sleep(10)

    def make_record(self):
        product_id = self.product_ids[random.randrange(self.ID_RANGE)]
        product_name = self.product_names[random.randrange(self.DATA_RANGE)]
        category_id = self.category_ids[random.randrange(self.DATA_RANGE)]
        price = self.prices[random.randrange(self.DATA_RANGE)]
        gtin = self.gtins[random.randrange(self.DATA_RANGE)]
        return product_id, product_name, category_id, price, gtin


if __name__ == "__main__":
    pytest.main(sys.argv)
