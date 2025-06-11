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
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import (
    setup_context,
    setup_signal,
    setup_tenant_config,
    try_delete_signal,
)

ADMIN_USER = f"{admin_user}@{TENANT_NAME}"

SIMPLE_CONTEXT = {
    "contextName": "context_example",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "userName", "type": "String"},
        {
            "attributeName": "name",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "n/a",
        },
    ],
    "primaryKey": ["userName"],
}

PRODUCT_CONTEXT = {
    "contextName": "placeholder",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "productId", "type": "integer"},
        {"attributeName": "productName", "type": "string"},
        {"attributeName": "price", "type": "decimal"},
    ],
    "primaryKey": ["productId"],
}

SIGNAL_ENRICHED_BY_SIMPLE_CONTEXT = {
    "signalName": "signal_to_enrich",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "user",
            "type": "String",
        },
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "bySimpleContext",
                "foreignKey": ["user"],
                "missingLookupPolicy": "Reject",
                "contextName": "context_example",
                "contextAttributes": [{"attributeName": "name", "as": "fullName"}],
            }
        ]
    },
}

SIGNAL_ENRICHED_BY_PRODUCT_CONTEXT = {
    "signalName": "signal_to_enrich",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "productId",
            "type": "Integer",
        },
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "byProductContext",
                "foreignKey": ["productId"],
                "missingLookupPolicy": "Reject",
                "contextName": "placeholder",
                "contextAttributes": [{"attributeName": "productName"}],
            }
        ]
    },
}

CONTEXT_HOST = {
    "contextName": "host",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "hostName", "type": "String"},
        {
            "attributeName": "hostFriendlyName",
            "type": "String",
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "None",
        },
        {"attributeName": "role", "type": "String"},
        {"attributeName": "subRole", "type": "String"},
        {"attributeName": "cloud", "type": "String"},
        {"attributeName": "ipAddress", "type": "String"},
        {"attributeName": "numCpus", "type": "Integer"},
        {"attributeName": "memoryGb", "type": "Integer"},
    ],
    "primaryKey": ["hostName"],
    "missingLookupPolicy": "FailParentLookup",
    "auditEnabled": False,
}

SIGNAL_EXCEPTION = {
    "signalName": "exception",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "hostName", "type": "String"},
        {"attributeName": "serviceType", "type": "String"},
        {"attributeName": "serviceName", "type": "String"},
        {
            "attributeName": "eventTimestamp",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Timestamp", "unit": "UnixMillisecond"},
        },
        {"attributeName": "origEventTimestamp", "type": "String"},
        {
            "attributeName": "severity",
            "type": "String",
            "allowedValues": ["Debug", "Info", "Warning", "Error"],
        },
        {"attributeName": "origSeverity", "type": "String"},
        {"attributeName": "tenant", "type": "String"},
        {"attributeName": "logLocation", "type": "String"},
        {"attributeName": "message", "type": "String"},
        {"attributeName": "messageDigest", "type": "String"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "host",
                "foreignKey": ["hostName"],
                "missingLookupPolicy": "StoreFillInValue",
                "contextName": "host",
                "contextAttributes": [
                    {"attributeName": "hostFriendlyName", "fillIn": "MISSING"},
                    {"attributeName": "role", "fillIn": "MISSING"},
                    {"attributeName": "subRole", "fillIn": "MISSING"},
                    {"attributeName": "cloud", "fillIn": "MISSING"},
                ],
            }
        ],
        "ingestTimeLag": [
            {
                "ingestTimeLagName": "reportDelay",
                "attribute": "eventTimeStamp",
                "as": "reportDelay",
                "tags": {"category": "Quantity", "kind": "Duration", "unit": "Millisecond"},
            }
        ],
    },
}


class UpdateContextTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    def tearDown(self):
        self.session.close()

    def test_adding_one_attribute(self):
        # setup
        context_name = "addingOneAttribute"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name

        # we'll also test enrichment
        signal_name = "addingOneAttributeSignal"
        signal = copy.deepcopy(SIGNAL_ENRICHED_BY_SIMPLE_CONTEXT)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["contextName"] = context_name

        # Create the initial context and signal
        try_delete_signal(self.session, signal_name)
        initial_context = setup_context(self.session, context)
        setup_signal(self.session, signal)

        # Upsert the initial context entries
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(["user1,User Name 1", "user2,User Name 2", "user3,User Name 3"])
            .build()
        )

        first_insert_time = (
            self.session.execute(bios.isql().insert().into(signal_name).csv("user1").build())
            .records[0]
            .timestamp
        )

        # Verify the initial context entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"]])
            .build()
        ).to_dict()
        assert len(entries) == 3
        assert entries[0].get("userName") == "user1"
        assert entries[0].get("name") == "User Name 1"
        assert entries[1].get("userName") == "user2"
        assert entries[1].get("name") == "User Name 2"
        assert entries[2].get("userName") == "user3"
        assert entries[2].get("name") == "User Name 3"

        # Add the context an attribute
        to_modify = copy.deepcopy(context)
        to_modify["attributes"].append(
            {"attributeName": "email", "type": "String", "default": "MISSING"}
        )
        updated_context = self.session.update_context(context_name, to_modify)
        assert updated_context.get("version") > initial_context.get("version")
        assert updated_context.get("biosVersion") == initial_context.get("biosVersion")

        # Verify that the initial data is still available
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"]])
            .build()
        ).to_dict()
        assert len(entries) == 3
        expected_values = [
            ("user1", "User Name 1", "MISSING"),
            ("user2", "User Name 2", "MISSING"),
            ("user3", "User Name 3", "MISSING"),
        ]
        for i, (user_name, name, email) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("userName") == user_name, message
            assert entries[i].get("name") == name, message
            assert entries[i].get("email") == email, message

        self.session.execute(bios.isql().insert().into(signal_name).csv("user2").build())

        # Upsert more entries
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(
                [
                    "user2,User Name 2,user2@example.com",
                    "user4,User Name 4,user4@example.com",
                    "user3,Three,user3@example.com",
                ]
            )
            .build()
        )

        # Verify the entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"], ["user4"]])
            .build()
        ).to_dict()
        assert len(entries) == 4
        expected_values = [
            ("user1", "User Name 1", "MISSING"),
            ("user2", "User Name 2", "user2@example.com"),
            ("user3", "Three", "user3@example.com"),
            ("user4", "User Name 4", "user4@example.com"),
        ]
        for i, (user_name, name, email) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("userName") == user_name, message
            assert entries[i].get("name") == name, message
            assert entries[i].get("email") == email, message

        for user in ["user3", "user4"]:  # we avoid using bulk to ensure the insertion order
            self.session.execute(bios.isql().insert().into(signal_name).csv(user).build())

        # Delete the entries
        self.session.execute(
            bios.isql()
            .delete()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"], ["user4"]])
            .build()
        )

        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"], ["user4"]])
            .build()
        ).to_dict()
        assert len(entries) == 0

        # check the enriched signal
        delta = bios.time.now() - first_insert_time
        events = self.session.execute(
            bios.isql()
            .select()
            .from_signal(signal_name)
            .time_range(first_insert_time, delta)
            .build()
        ).to_dict()

        expected_values = [
            ("user1", "User Name 1"),
            ("user2", "User Name 2"),
            ("user3", "Three"),
            ("user4", "User Name 4"),
        ]

        for i, (user, full_name) in enumerate(expected_values):
            message = f"index: {i}"
            assert events[i].get("user") == user, message
            assert events[i].get("fullName") == full_name, message

    def test_adding_multiple_attributes(self):
        # setup
        signal_name = "accessLog"
        try_delete_signal(self.session, signal_name)

        context_name = "addingMultipleAttributes"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name

        # Create the initial context
        initial_context = setup_context(self.session, context)

        # Upsert the initial context entries
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(["user1,User Name 1", "user2,User Name 2", "user3,User Name 3"])
            .build()
        )

        # Verify the initial context entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"]])
            .build()
        ).to_dict()
        assert len(entries) == 3
        assert entries[0].get("userName") == "user1"
        assert entries[0].get("name") == "User Name 1"
        assert entries[1].get("userName") == "user2"
        assert entries[1].get("name") == "User Name 2"
        assert entries[2].get("userName") == "user3"
        assert entries[2].get("name") == "User Name 3"

        # Add the context attributes
        to_modify = copy.deepcopy(context)
        to_modify["attributes"].extend(
            [
                {"attributeName": "email", "type": "String", "default": "MISSING"},
                {"attributeName": "zip", "type": "Integer", "default": -1},
                {"attributeName": "awakeness", "type": "Decimal", "default": 0.5},
                {"attributeName": "registered", "type": "Boolean", "default": False},
                {
                    "attributeName": "fingerprint",
                    "type": "Blob",
                    "default": "bm8gZmluZ2VycHJpbnQK",
                },
                {
                    "attributeName": "gender",
                    "type": "String",
                    "allowedValues": ["Male", "Female", "Unknown"],
                    "default": "Unknown",
                },
            ]
        )
        updated_context = self.session.update_context(context_name, to_modify)
        assert updated_context.get("version") > initial_context.get("version")
        assert updated_context.get("biosVersion") == initial_context.get("biosVersion")

        # Verify that the initial data is still available
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"]])
            .build()
        ).to_dict()
        assert len(entries) == 3
        expected_values = [
            ("user1", "User Name 1"),
            ("user2", "User Name 2"),
            ("user3", "User Name 3"),
        ]
        for i, (user_name, name) in enumerate(expected_values):
            message = f"index={i}"
            assert entries[i].get("userName") == user_name, message
            assert entries[i].get("name") == name, message
            assert entries[i].get("email") == "MISSING", message
            assert entries[i].get("zip") == -1, message
            assert pytest.approx(entries[i].get("awakeness"), 1.0e-5) == 0.5, message
            assert entries[i].get("registered") is False, message
            assert entries[i].get("fingerprint") == "bm8gZmluZ2VycHJpbnQK", message
            assert entries[i].get("gender") == "Unknown", message

        # Upsert more entries, one overwrites an existing entry, the other is new
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(
                [
                    "user2,User Name 2,user2@example.com,222,0.2,True,aGVsbG8K,Female",
                    "user4,User Name 4,user4@example.com,444,0.4,False,dXNlcjQK,Male",
                ]
            )
            .build()
        )

        # Verify the entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user1"], ["user2"], ["user3"], ["user4"]])
            .build()
        ).to_dict()
        assert len(entries) == 4
        # TODO(BIOS-4419): FIXME: Expected blob values should be of type byte array.
        # Strings are returned but it is not correct.
        expected_values = [
            ("user1", "User Name 1", "MISSING", -1, 0.5, False, "Unknown", "bm8gZmluZ2VycHJpbnQK"),
            ("user2", "User Name 2", "user2@example.com", 222, 0.2, True, "Female", "aGVsbG8K"),
            ("user3", "User Name 3", "MISSING", -1, 0.5, False, "Unknown", "bm8gZmluZ2VycHJpbnQK"),
            ("user4", "User Name 4", "user4@example.com", 444, 0.4, False, "Male", "dXNlcjQK"),
        ]
        for i, entry in enumerate(expected_values):
            (user, name, email, zipcode, awakeness, registered, gender, fingerprint) = entry
            message = f"index={i}"
            assert entries[i].get("userName") == user, message
            assert entries[i].get("name") == name, message
            assert entries[i].get("email") == email, message
            assert entries[i].get("zip") == zipcode, message
            assert pytest.approx(entries[i].get("awakeness"), 1.0e-5) == awakeness, message
            assert entries[i].get("registered") == registered, message
            assert entries[i].get("gender") == gender, message
            assert entries[i].get("fingerprint") == fingerprint, message

        # Try enrichments
        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "user", "type": "String"},
                {"attributeName": "operation", "type": "String"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "userInfo",
                        "foreignKey": ["user"],
                        "missingLookupPolicy": "Reject",
                        "contextName": context_name,
                        "contextAttributes": [
                            {"attributeName": "name"},
                            {"attributeName": "email"},
                            {"attributeName": "awakeness"},
                            {"attributeName": "registered"},
                            {"attributeName": "gender"},
                            # TODO(BIOS-4419): Also add blob attribute
                            # {"attributeName": "fingerprint"},
                        ],
                    }
                ]
            },
        }
        self.session.create_signal(signal)

        # Insert and check enrichments
        start = bios.time.now()
        time.sleep(1)
        self.session.execute(
            bios.isql()
            .insert()
            .into(signal_name)
            .csv_bulk(
                ["user1,Login", "user2,ChangePassword", "user3,UpdateSignal", "user4,Logout"]
            )
            .build()
        )
        time.sleep(1)

        records = self.session.execute(
            bios.isql()
            .select()
            .from_signal(signal_name)
            .time_range(start, bios.time.now() - start)
            .build()
        ).to_dict()

        pprint.pprint(records)
        assert len(records) == 4
        expected_values = [
            (
                "user1",
                "Login",
                "User Name 1",
                "MISSING",
                0.5,
                False,
                "Unknown",
                "bm8gZmluZ2VycHJpbnQK",
            ),
            (
                "user2",
                "ChangePassword",
                "User Name 2",
                "user2@example.com",
                0.2,
                True,
                "Female",
                "aGVsbG8K",
            ),
            (
                "user3",
                "UpdateSignal",
                "User Name 3",
                "MISSING",
                0.5,
                False,
                "Unknown",
                "bm8gZmluZ2VycHJpbnQK",
            ),
            (
                "user4",
                "Logout",
                "User Name 4",
                "user4@example.com",
                0.4,
                False,
                "Male",
                "dXNlcjQK",
            ),
        ]
        for i, entry in enumerate(expected_values):
            (user, operation, name, email, awakeness, registered, gender, fingerprint) = entry
            record = records[i]
            msg = f"index: {i}"
            assert record.get("user") == user, msg
            assert record.get("operation") == operation, msg
            assert record.get("name") == name, msg
            assert record.get("email") == email, msg
            assert pytest.approx(record.get("awakeness"), 1.0e-5) == awakeness, msg
            assert record.get("registered") == registered, msg
            assert record.get("gender") == gender, msg
            # TODO(BIOS-4419): FIXME: Blob attributes do not return
            # assert record.get("fingerprint"), fingerprint, msg)

    def test_adding_two_attributes_with_audit(self):
        # setup
        context_name = "addingTwoAttributes"
        context = copy.deepcopy(SIMPLE_CONTEXT)
        context["contextName"] = context_name
        context["auditEnabled"] = True

        audit_name = f"audit{context_name[0].upper()}{context_name[1:]}"

        # Create the initial context
        setup_context(self.session, context)

        # Upsert the initial context entries
        start = bios.time.now()
        time.sleep(0.5)  # Add some margin for clock skew
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(["user0,name0", "user1,name1", "user2,name2", "user3,name3", "user4,name4"])
            .build()
        )

        time.sleep(1)

        delta = bios.time.now() - start

        # Verify the initial audit signal
        entries = self.session.execute(
            bios.isql().select().from_signal(audit_name).time_range(start, delta).build()
        ).to_dict()
        assert len(entries) == 5
        expected_values = {
            ("Insert", "user0", "user0", "name0", "name0"),
            ("Insert", "user1", "user1", "name1", "name1"),
            ("Insert", "user2", "user2", "name2", "name2"),
            ("Insert", "user3", "user3", "name3", "name3"),
            ("Insert", "user4", "user4", "name4", "name4"),
        }
        for i in range(5):
            entry = entries[i]
            tuple = (
                entry.get("_operation"),
                entry.get("userName"),
                entry.get("prevUserName"),
                entry.get("name"),
                entry.get("prevName"),
            )
            message = f"index={i}: {entry}"
            assert tuple in expected_values, message
            expected_values.remove(tuple)

        # Add the context an attribute
        to_modify = copy.deepcopy(context)
        to_modify["attributes"].extend(
            [
                {"attributeName": "email", "type": "String", "default": ""},
                {
                    "attributeName": "gender",
                    "type": "String",
                    "allowedValues": ["Male", "Female", "Unknown"],
                    "default": "Unknown",
                },
            ]
        )
        self.session.update_context(context_name, to_modify)

        # Verify that the initial context data is still available
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user0"], ["user1"], ["user2"], ["user3"], ["user4"]])
            .build()
        ).to_dict()
        assert len(entries) == 5
        expected_values = [
            ("user0", "name0", "", "Unknown"),
            ("user1", "name1", "", "Unknown"),
            ("user2", "name2", "", "Unknown"),
            ("user3", "name3", "", "Unknown"),
            ("user4", "name4", "", "Unknown"),
        ]
        for i, (user_name, name, email, gender) in enumerate(expected_values):
            entry = entries[i]
            message = f"index={i}: {entry}"
            assert entry.get("userName") == user_name, message
            assert entry.get("name") == name, message
            assert entry.get("email") == email, message
            assert entry.get("gender") == gender, message

        # Verify the audit signal again
        entries = self.session.execute(
            bios.isql().select().from_signal(audit_name).time_range(start, delta).build()
        ).to_dict()
        assert len(entries) == 5
        expected_values = {
            ("Insert", "user0", "user0", "name0", "name0", "", "", "Unknown", "Unknown"),
            ("Insert", "user1", "user1", "name1", "name1", "", "", "Unknown", "Unknown"),
            ("Insert", "user2", "user2", "name2", "name2", "", "", "Unknown", "Unknown"),
            ("Insert", "user3", "user3", "name3", "name3", "", "", "Unknown", "Unknown"),
            ("Insert", "user4", "user4", "name4", "name4", "", "", "Unknown", "Unknown"),
        }
        for i in range(5):
            entry = entries[i]
            tuple = (
                entry.get("_operation"),
                entry.get("userName"),
                entry.get("prevUserName"),
                entry.get("name"),
                entry.get("prevName"),
                entry.get("email"),
                entry.get("prevEmail"),
                entry.get("gender"),
                entry.get("prevGender"),
            )
            message = f"index={i}: {entry}"
            assert tuple in expected_values, message
            expected_values.remove(tuple)

        # Upsert more entries
        start = bios.time.now()
        time.sleep(1)
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(
                [
                    "user1,Name 1,user1@e.c,Male",  # updates all
                    "user2,name2,user2@e.c,Female",  # updates new attrs
                    "user3,Name 3,,Unknown",  # updates name/old attr
                    "user4,name4,,Unknown",  # updates none
                    "user5,name5,user5@e.c,Female",  # new entry
                ]
            )
            .build()
        )

        # Verify the entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[["user0"], ["user1"], ["user2"], ["user3"], ["user4"], ["user5"]])
            .build()
        ).to_dict()
        assert len(entries) == 6
        expected_values = [
            ("user0", "name0", "", "Unknown"),
            ("user1", "Name 1", "user1@e.c", "Male"),
            ("user2", "name2", "user2@e.c", "Female"),
            ("user3", "Name 3", "", "Unknown"),
            ("user4", "name4", "", "Unknown"),
            ("user5", "name5", "user5@e.c", "Female"),
        ]
        for i, (user_name, name, email, gender) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("userName") == user_name, message
            assert entries[i].get("name") == name, message
            assert entries[i].get("email") == email, message
            assert entries[i].get("gender") == gender, message

        time.sleep(1)
        delta = bios.time.now() - start

        # Verify the audit signal
        entries = self.session.execute(
            bios.isql().select().from_signal(audit_name).time_range(start, delta).build()
        ).to_dict()
        assert len(entries) == 4, f'len({[entry.get("userName") for entry in entries]})'
        expected_values = {
            ("Update", "user1", "user1", "Name 1", "name1", "user1@e.c", "", "Male", "Unknown"),
            ("Update", "user2", "user2", "name2", "name2", "user2@e.c", "", "Female", "Unknown"),
            ("Update", "user3", "user3", "Name 3", "name3", "", "", "Unknown", "Unknown"),
            (
                "Insert",
                "user5",
                "user5",
                "name5",
                "name5",
                "user5@e.c",
                "user5@e.c",
                "Female",
                "Female",
            ),
        }
        for i in range(4):
            entry = entries[i]
            tuple = (
                entry.get("_operation"),
                entry.get("userName"),
                entry.get("prevUserName"),
                entry.get("name"),
                entry.get("prevName"),
                entry.get("email"),
                entry.get("prevEmail"),
                entry.get("gender"),
                entry.get("prevGender"),
            )
            message = f"index={i}: {entry}"
            assert tuple in expected_values, message
            expected_values.remove(tuple)

    def test_deleting_one_attribute(self):
        # setup
        context_name = "deletingOneAttribute"
        context = copy.deepcopy(PRODUCT_CONTEXT)
        context["contextName"] = context_name
        context["auditEnabled"] = True

        audit_name = f"audit{context_name[0].upper()}{context_name[1:]}"

        # Create the initial context
        initial_context = setup_context(self.session, context)

        # Upsert the initial context entries
        start = bios.time.now()
        time.sleep(0.5)  # Add some margin for clock skew
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(
                ["101,Fishing Net,10.99", "202,Swimming Goagle,39.99", "303,Running Shoes,100.0"]
            )
            .build()
        )

        # Remove an attribute from the context
        to_modify = copy.deepcopy(context)
        attributes = to_modify["attributes"]
        to_modify["attributes"] = [attributes[0], attributes[2]]
        updated_context = self.session.update_context(context_name, to_modify)
        assert updated_context.get("version") > initial_context.get("version")
        assert updated_context.get("biosVersion") == initial_context.get("biosVersion")

        updated_audit_signal = self.session.get_signal(audit_name)
        attributes = updated_audit_signal.get("attributes")
        assert len(attributes) == 5
        expected_audit_attribute_names = [
            "_operation",
            "productId",
            "prevProductId",
            "price",
            "prevPrice",
        ]
        assert [attr.get("attributeName") for attr in attributes] == expected_audit_attribute_names

        # Verify that the initial data is still available
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[[101], [202], [303]])
            .build()
        ).to_dict()
        assert len(entries) == 3
        expected_values = [(101, 10.99), (202, 39.99), (303, 100.0)]
        for i, (product_id, price) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("productId") == product_id, message
            assert pytest.approx(entries[i].get("price"), 1.0e-5) == price, message

        # Upsert more entries, one overwrites an existing entry, the other is new
        self.session.execute(
            bios.isql().upsert().into(context_name).csv_bulk(["202,35.49", "404,8.23"]).build()
        )

        # Verify the entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[[101], [202], [303], [404]])
            .build()
        ).to_dict()
        assert len(entries) == 4
        expected_values = [(101, 10.99), (202, 35.49), (303, 100.0), (404, 8.23)]
        for i, (product_id, price) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("productId") == product_id, message
            assert pytest.approx(entries[i].get("price"), 1.0e-5) == price, message
            assert entries[i].get("productName") is None

        # Verify the audit signal
        time.sleep(1)
        delta = bios.time.now() - start
        entries = self.session.execute(
            bios.isql().select().from_signal(audit_name).time_range(start, delta).build()
        ).to_dict()
        assert len(entries) == 5, f'len({[entry.get("productId") for entry in entries]})'
        expected_values = {
            ("Insert", 101, 101, 10.99, 10.99),
            ("Insert", 202, 202, 39.99, 39.99),
            ("Insert", 303, 303, 100.0, 100.0),
            ("Update", 202, 202, 35.49, 39.99),
            ("Insert", 404, 404, 8.23, 8.23),
        }
        for i in range(5):
            entry = entries[i]
            message = f"index={i}: {entry}"
            tuple = (
                entry.get("_operation"),
                entry.get("productId"),
                entry.get("prevProductId"),
                entry.get("price"),
                entry.get("prevPrice"),
            )
            assert tuple in expected_values, message
            expected_values.remove(tuple)

    def test_adding_and_deleting_attributes(self):
        # setup
        context_name = "replaceAttributes"
        context = copy.deepcopy(PRODUCT_CONTEXT)
        context["contextName"] = context_name

        # Create the initial context
        initial_context = setup_context(self.session, context)

        # Upsert the initial context entries
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(
                ["101,Fishing Reel,29.99", "202,Swimming Goagle,39.99", "303,Running Shoes,100.0"]
            )
            .build()
        )

        # Remove an attribute from the context
        to_modify = copy.deepcopy(context)
        to_modify["attributes"] = [
            to_modify["attributes"][0],
            {"attributeName": "categoryId", "type": "Integer", "default": -1},
            {"attributeName": "brand", "type": "String", "default": ""},
        ]
        updated_context = self.session.update_context(context_name, to_modify)
        assert updated_context.get("version") > initial_context.get("version")
        assert updated_context.get("biosVersion") == initial_context.get("biosVersion")

        # Verify that the initial data is still available
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[[101], [202], [303]])
            .build()
        ).to_dict()
        assert len(entries) == 3
        expected_values = [(101, -1, ""), (202, -1, ""), (303, -1, "")]
        for i, (product_id, category_id, brand) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("productId") == product_id, message
            assert entries[i].get("categoryId") == category_id, message
            assert entries[i].get("brand") == brand, message

        # Upsert more entries, one overwrites an existing entry, the other is new
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(["101,1000,Shimano", "404,4000,Campagnolo"])
            .build()
        )

        # Verify the entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(context_name)
            .where(keys=[[101], [202], [303], [404]])
            .build()
        ).to_dict()
        assert len(entries) == 4
        expected_values = [
            (101, 1000, "Shimano"),
            (202, -1, ""),
            (303, -1, ""),
            (404, 4000, "Campagnolo"),
        ]
        for i, (product_id, category_id, brand) in enumerate(expected_values):
            message = f"index: {i}"
            assert entries[i].get("productId") == product_id, message
            assert entries[i].get("categoryId") == category_id, message
            assert entries[i].get("brand") == brand, message
            assert entries[i].get("productName") is None
            assert entries[i].get("price") is None

    def test_negative_modify_attribute_type(self):
        """Changing attribute type in a context should be prohibited"""
        context_name = "productContextModifyAttributes"
        product_context = copy.deepcopy(PRODUCT_CONTEXT)
        product_context["contextName"] = context_name
        setup_context(self.session, product_context)

        updated_product_context = copy.deepcopy(product_context)
        updated_product_context["attributes"][2]["type"] = "String"
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_context(context_name, updated_product_context)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert "Constraint violation: Attribute type may not be changed;" in excinfo.value.message

    def test_negative_change_primary_key(self):
        """Changing the primary key should not be allowed"""
        context_name = "productContextDropPrimaryKey"
        product_context = copy.deepcopy(PRODUCT_CONTEXT)
        product_context["contextName"] = context_name
        setup_context(self.session, product_context)

        # Try to switch primary key to the second attribute
        updated_product_context = copy.deepcopy(product_context)
        updated_product_context["primaryKey"] = ["productName"]
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_context(context_name, updated_product_context)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert (
            "Constraint violation: Primary key attributes may not be modified;"
            in excinfo.value.message
        )

        # Try another pattern; drop the primary key and use the third attr as the primary key
        updated_product_context2 = copy.deepcopy(product_context)
        updated_product_context2["attributes"] = updated_product_context2["attributes"][1:]
        updated_product_context2["primaryKey"] = ["price"]
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_context(context_name, updated_product_context2)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert (
            "Constraint violation: Primary key attributes may not be modified;"
            in excinfo.value.message
        )

    def test_negative_delete_referred_attribute(self):
        # setup
        context_name = "deleteEnrichingAttribute"
        context = copy.deepcopy(PRODUCT_CONTEXT)
        context["contextName"] = context_name

        signal_name = "deleteEnrichingAttributeSignal"
        signal = copy.deepcopy(SIGNAL_ENRICHED_BY_PRODUCT_CONTEXT)
        signal["signalName"] = signal_name
        signal["enrich"]["enrichments"][0]["contextName"] = context_name

        # Create the initial context
        try_delete_signal(self.session, signal_name)
        setup_context(self.session, context)
        setup_signal(self.session, signal)

        # Remove the enriching attribute from the context
        to_modify = copy.deepcopy(context)
        to_modify["attributes"] = [to_modify["attributes"][0], to_modify["attributes"][2]]
        with pytest.raises(ServiceError) as excinfo:
            self.session.update_context(context_name, to_modify)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert (
            "Constraint violation: Enriching attribute name must exist in referring context;"
            in excinfo.value.message
        )

    # Regression BIOS-5722
    def test_update_enriching_context(self):
        # Setup
        context = copy.deepcopy(CONTEXT_HOST)
        context_name = context.get("contextName")

        signal = copy.deepcopy(SIGNAL_EXCEPTION)
        signal_name = signal.get("signalName")

        # Create the initial context and signal
        try_delete_signal(self.session, signal_name)
        setup_context(self.session, context)
        setup_signal(self.session, signal)

        # Populate the context
        self.session.execute(
            bios.isql()
            .upsert()
            .into(context_name)
            .csv_bulk(
                [
                    "aHost,ahost1,compute,integrations,aws,10.20.30.40,8,32",
                    "hostTwo,hostTwo1,storage,signal,aws,20.30.40.50,32,122",
                ]
            )
            .build()
        )

        time0 = bios.time.now()

        # Try inserting a signal
        timestamp = bios.time.now()
        statement = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv(
                f"aHost,realtime,test,{timestamp},{timestamp},"
                "Warning,warn,aTenant,/log/me,msg,msgDigest"
            )
            .build()
        )
        self.session.execute(statement)

        # Update the context - setting TTL
        context["ttl"] = 60 * 24 * 3600 * 1000
        self.session.update_context(context_name, context)

        # Try inserting a signal again
        timestamp = bios.time.now()
        statement = (
            bios.isql()
            .insert()
            .into(signal_name)
            .csv(
                f"hostTwo,batch,test,{timestamp},{timestamp},"
                "Error,erro,aTenant,/log/me,msg,msgDigest"
            )
            .build()
        )
        self.session.execute(statement)

        end = bios.time.now()
        result = self.session.execute(
            bios.isql().select().from_signal(signal_name).time_range(time0, end - time0).build()
        ).to_dict()

        pprint.pprint(result)
        assert len(result) == 2

        record0 = result[0]
        assert record0.get("hostName") == "aHost"
        assert record0.get("severity") == "Warning"
        assert record0.get("hostFriendlyName") == "ahost1"
        assert record0.get("reportDelay") >= 0

        record1 = result[1]
        assert record1.get("hostName") == "hostTwo"
        assert record1.get("severity") == "Error"
        assert record1.get("hostFriendlyName") == "hostTwo1"
        assert record1.get("reportDelay") >= 0


if __name__ == "__main__":
    pytest.main(sys.argv)
