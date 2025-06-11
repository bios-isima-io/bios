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
from bios import ErrorCode, ServiceError
from bios.models import ContextRecords
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

from setup_common import read_file, try_delete_stream, update_stream_expect_error

TEST_TENANT_NAME = "biosContextEnrichmentTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME

CONTEXT_TO_ENRICH_CONTEXT1_NAME = "resourceType"
CONTEXT_TO_ENRICH_CONTEXT2_NAME = "staff"
CONTEXT_TO_ENRICH_CONTEXT3_NAME = "doctor"
CONTEXT_TO_ENRICH_CONTEXT4_NAME = "venue"
CONTEXT_TO_ENRICH_CONTEXT5_NAME = "auxiliary"
CONTEXT_TO_ENRICH_CONTEXT6_NAME = "machine"
CONTEXT_TO_ENRICH_CONTEXT7_NAME = "workspace"
ENRICHED_CONTEXT_NAME = "enrichedContext"
SECOND_LEVEL_ENRICHED_CONTEXT_NAME = "secondLevelEnrichedContext"
SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME = "patientVisits"
SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_NAME = "secondLevelEnrichedSignal"

CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX = "../resources/bios-context-to-enrich-context"
CONTEXT_TO_BE_ENRICHED_BIOS = "../resources/bios-context-to-be-enriched.json"
ENRICHED_CONTEXT_BIOS = "../resources/bios-enriched-context.json"
SECOND_LEVEL_ENRICHED_CONTEXT_BIOS = "../resources/bios-second-level-enriched-context.json"
SIGNAL_FOR_CONTEXT_ENRICHMENT_BIOS = "../resources/bios-signal-for-context-enrichment.json"
SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_BIOS = (
    "../resources/" "bios-signal-for-second-context-enrichment.json"
)

ENRICHED_CONTEXT_MODIFY1_BIOS = "../resources/bios-enriched-context-modify1.json"

CONTEXT_TFOS_PREFIX = "../../server/src/test/resources/ContextToEnrichContext"
ENRICHED_CONTEXT_TFOS = "../../server/src/test/resources/" "EnrichedContextTfos.json"
SECOND_LEVEL_ENRICHED_CONTEXT_TFOS = (
    "../../server/src/test/resources/" "SecondLevelEnrichedContextTfos.json"
)
SIGNAL_FOR_CONTEXT_ENRICHMENT_TFOS = (
    "../../server/src/test/resources/" "SignalForContextEnrichmentTfos.json"
)
SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_TFOS = (
    "../../server/src/test/resources/" "SignalForSecondContextEnrichmentTfos.json"
)


class BiosContextEnrichmentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    def setUp(self):
        self.session = bios.login(ep_url(), ADMIN_USER, admin_pass)
        self.assertIsNotNone(self.session)

    def tearDown(self):
        # BIOS-2938 workaround to stop frequent test failure, but this may be hiding a bug
        time.sleep(5)
        try_delete_stream(self.session, SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_NAME, "signal")
        try_delete_stream(self.session, SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME, "signal")
        try_delete_stream(self.session, SECOND_LEVEL_ENRICHED_CONTEXT_NAME, "context")
        try_delete_stream(self.session, ENRICHED_CONTEXT_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT2_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT3_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT4_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT5_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT6_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT7_NAME, "context")
        try_delete_stream(self.session, CONTEXT_TO_ENRICH_CONTEXT1_NAME, "context")
        self.session.close()
        time.sleep(5)

    def verify_schema_common(self, context, context2):
        self.assertEqual(context["enrichments"][0]["enrichmentName"], "resourceType")
        self.assertEqual(context["enrichments"][0]["foreignKey"][0], "resourceTypeNum")
        self.assertEqual(
            context["enrichments"][0]["enrichedAttributes"][0]["value"],
            "resourceType.resourceTypeCode",
        )
        self.assertEqual(
            context["enrichments"][0]["enrichedAttributes"][1]["as"],
            "myResourceCategory",
        )
        self.assertEqual(context["enrichments"][1]["foreignKey"][0], "resourceId")
        # self.assertEqual(context['enrichments'][1]['missingLookupPolicy'], 'StoreFillInValue')
        self.assertEqual(
            context["enrichments"][1]["enrichedAttributes"][1]["valuePickFirst"][5],
            "workspace.workspaceId",
        )
        self.assertEqual(context["enrichments"][1]["enrichedAttributes"][1]["as"], "resourceValue")
        self.assertEqual(context2["enrichments"][0]["enrichmentName"], "resource")
        self.assertEqual(context2["enrichments"][1]["enrichmentName"], "resourceType")
        self.assertEqual(context2["enrichments"][0]["foreignKey"][0], "resourceId")
        self.assertEqual(
            context2["enrichments"][0]["enrichedAttributes"][0]["value"],
            "enrichedContext.resourceTypeNum",
        )
        self.assertEqual(
            context2["enrichments"][1]["enrichedAttributes"][0]["value"],
            "resourceType.resourceCategory",
        )

    def verify_schema_new_api(self):
        context = self.session.get_context(ENRICHED_CONTEXT_NAME)
        context2 = self.session.get_context(SECOND_LEVEL_ENRICHED_CONTEXT_NAME)
        signal1 = self.session.get_signal(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME)
        signal2 = self.session.get_signal(SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_NAME)
        self.verify_schema_common(context, context2)
        self.assertEqual(context["contextName"], ENRICHED_CONTEXT_NAME)
        self.assertGreater(len(context), 1)
        self.assertEqual(context["missingLookupPolicy"], "FailParentLookup")
        self.assertEqual(
            signal1["enrich"]["enrichments"][0]["contextAttributes"][2]["attributeName"],
            "resourceTypeCode",
        )
        self.assertEqual(
            signal1["enrich"]["enrichments"][0]["contextAttributes"][3]["attributeName"],
            "myResourceCategory",
        )
        self.assertEqual(
            signal2["enrich"]["enrichments"][0]["contextAttributes"][0]["attributeName"],
            "resourceId",
        )
        self.assertEqual(
            signal2["enrich"]["enrichments"][1]["contextAttributes"][3]["attributeName"],
            "myResourceCategory",
        )

    def create_streams_with_new_json(self):
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "1.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "2.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "3.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "4.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "5.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "6.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "7.json"))
        self.session.create_context(read_file(ENRICHED_CONTEXT_BIOS))
        self.session.create_context(read_file(SECOND_LEVEL_ENRICHED_CONTEXT_BIOS))
        self.session.create_signal(read_file(SIGNAL_FOR_CONTEXT_ENRICHMENT_BIOS))
        self.session.create_signal(read_file(SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_BIOS))

    def test_create_with_new_json(self):
        self.create_streams_with_new_json()
        self.verify_schema_new_api()

    def upsert_entries(self, context_name, entries):
        # print('Upsert into ' + context_name)
        st = bios.isql().upsert().into(context_name).csv_bulk(entries).build()
        return self.session.execute(st)

    def insert_event(self, signal_name, event):
        # print('Insert into ' + signal_name)
        st = bios.isql().insert().into(signal_name).csv(event).build()
        return self.session.execute(st)

    def insert_expect_error(self, signal_name, event, error):
        # print('Expecting exception for insert into ' + signal_name)
        exception_thrown = None
        try:
            st = bios.isql().insert().into(signal_name).csv(event).build()
            self.session.execute(st)
        except ServiceError as err:
            exception_thrown = err
        self.assertEqual(exception_thrown.error_code, ErrorCode.BAD_INPUT)
        self.assertEqual(exception_thrown.message[: len(error)], error)

    def extract_events(self, signal_name, from_time, to_time):
        st = bios.isql().select().from_signal(signal_name)
        st = st.time_range(from_time, to_time - from_time + 1).build()
        return self.session.execute(st)

    def upsert_context_entries(self):
        # All contexts have 1 and 6
        # Skip 2 in this context
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["1,staff,1000"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["3,venue,3000"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["4,auxiliary,4000"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["6,workspace,6000"])
        # Skip 3 in this set of contexts
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT2_NAME, ["resource1,1.1,staffAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT3_NAME, ["resource2,2.1,doctorAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT5_NAME, ["resource4,4.1,auxiliaryAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT7_NAME, ["resource6,6.1,workspaceAlias1"])
        # Skip 4 in this context
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource1,1,11111"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource2,2,22222"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource3,3,33333"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource6,6,66666"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["delete-test,-1,-1"])

        # fetch enriched entries
        entries = self.session.execute(
            bios.isql()
            .select()
            .from_context(ENRICHED_CONTEXT_NAME)
            .where(keys=[["resource2"], ["resource1"], ["resource3"], ["resource4"]])
            .build()
        ).to_dict()

        self.assertEqual(len(entries), 1)
        entry0 = entries[0]
        self.assertEqual(entry0.get("resourceId"), "resource1")
        self.assertEqual(entry0.get("resourceTypeNum"), 1)
        self.assertEqual(entry0.get("zipcode"), "11111")
        self.assertEqual(entry0.get("resourceTypeCode"), "staff")
        self.assertEqual(entry0.get("myResourceCategory"), 1000)
        self.assertAlmostEqual(entry0.get("cost"), 1.1)
        self.assertEqual(entry0.get("resourceValue"), "staffAlias1")

        # test entry deletion
        self.session.execute(
            bios.isql()
            .delete()
            .from_context(ENRICHED_CONTEXT_NAME)
            .where(keys=[["delete-test"]])
            .build()
        )

    def insert_events_and_verify(self):
        signal = SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME
        resp1 = self.insert_event(signal, "111,11,resource1")
        self.insert_expect_error(
            signal, "222,22,resource2", "Missed to lookup context with a foreign key:"
        )
        self.insert_expect_error(
            signal, "333,33,resource3", "Missed to lookup context with a foreign key:"
        )
        self.insert_expect_error(
            signal, "444,44,resource4", "Missed to lookup context with a foreign key:"
        )
        resp2 = self.insert_event(signal, "666,66,resource6")
        start = resp1.records[0].get_timestamp()
        end = resp2.records[0].get_timestamp()
        # print(f"signal={signal}, start={start}, end={end}")
        events = self.extract_events(signal, start, end)
        records = events.get_data_windows()[0].records
        self.assertEqual("staff", records[0].get("resourceTypeCode"))
        self.assertEqual(1000, records[0].get("myResourceCategory"))
        self.assertEqual(1.1, records[0].get("cost"))
        self.assertEqual("66666", records[1].get("zipcode"))
        self.assertEqual("workspace", records[1].get("resourceTypeCode"))
        self.assertEqual("workspaceAlias1", records[1].get("resourceValue"))

    def test_upsert_insert_select(self):
        self.create_streams_with_new_json()
        self.upsert_context_entries()
        self.insert_events_and_verify()

    def delete_expect_error(self, context_name, error):
        exception_thrown = None
        try:
            self.session.delete_context(context_name)
        except ServiceError as err:
            exception_thrown = err
        self.assertEqual(exception_thrown.error_code, ErrorCode.BAD_INPUT)
        self.assertEqual(exception_thrown.message[: len(error)], error)

    def test_enrich_old_context(self):
        self.session.create_context(read_file(CONTEXT_TO_BE_ENRICHED_BIOS))
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource1,1,11111"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource0,0,00000"])

        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "1.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "2.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "3.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "4.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "5.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "6.json"))
        self.session.create_context(read_file(CONTEXT_TO_ENRICH_CONTEXT_BIOS_PREFIX + "7.json"))

        self.session.update_context(ENRICHED_CONTEXT_NAME, read_file(ENRICHED_CONTEXT_BIOS))
        self.session.create_signal(read_file(SIGNAL_FOR_CONTEXT_ENRICHMENT_BIOS))

        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["1,staff,1000"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["6,workspace,6000"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT2_NAME, ["resource1,1.1,staffAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT7_NAME, ["resource6,6.1,workspaceAlias1"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource6,6,66666"])
        self.insert_events_and_verify()

    def modify1(self):
        self.session.update_context(
            ENRICHED_CONTEXT_NAME, read_file(ENRICHED_CONTEXT_MODIFY1_BIOS)
        )

    def modify2(self):
        context = self.session.get_context(CONTEXT_TO_ENRICH_CONTEXT1_NAME)
        context["attributes"].pop(2)
        update_stream_expect_error(
            self.session,
            "context",
            CONTEXT_TO_ENRICH_CONTEXT1_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Constraint violation: Enrichment attribute name must exist",
        )

    def modify3(self):
        context = self.session.get_context(ENRICHED_CONTEXT_NAME)
        update_stream_expect_error(
            self.session,
            "context",
            ENRICHED_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Requested config for change is identical to existing",
        )
        context["attributes"].pop(2)
        update_stream_expect_error(
            self.session,
            "context",
            ENRICHED_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Constraint violation: Enriching attribute name must exist in referring context;",
        )

    def modify4(self):
        context = self.session.get_context(CONTEXT_TO_ENRICH_CONTEXT1_NAME)
        a = context["attributes"]
        a.append({"attributeName": "resourceExtraAttr", "type": "Decimal", "default": "0"})
        self.session.update_context(CONTEXT_TO_ENRICH_CONTEXT1_NAME, context)

    def modify5(self):
        context = self.session.get_context(ENRICHED_CONTEXT_NAME)
        ea = context["enrichments"][0]["enrichedAttributes"]
        ea.append({"value": "resourceType.resourceExtraAttr"})
        self.session.update_context(ENRICHED_CONTEXT_NAME, context)

    def modify6(self):
        context = self.session.get_context(SECOND_LEVEL_ENRICHED_CONTEXT_NAME)
        update_stream_expect_error(
            self.session,
            "context",
            SECOND_LEVEL_ENRICHED_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Requested config for change is identical to existing",
        )
        ea = context["enrichments"][0]["enrichedAttributes"]
        ea.append({"value": "enrichedContext.resourceExtraAttr"})
        self.session.update_context(SECOND_LEVEL_ENRICHED_CONTEXT_NAME, context)

    def modify7(self):
        signal = self.session.get_signal(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME)
        ea = signal["enrich"]["enrichments"][0]["contextAttributes"]
        ea.append({"attributeName": "resourceExtraAttr", "fillIn": "0"})
        self.session.update_signal(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME, signal)

    def test_modify1(self):
        self.create_streams_with_new_json()
        self.modify1()
        self.modify2()
        self.modify3()

    def test_modify2(self):
        self.create_streams_with_new_json()
        self.modify4()
        self.modify5()
        self.modify6()
        self.modify7()

    def test_modify2_and_insert(self):
        self.create_streams_with_new_json()
        self.modify4()
        self.modify5()
        self.modify6()
        self.modify7()
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["1,staff,1000,1.11"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["6,workspace,6000,6.66"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT2_NAME, ["resource1,1.1,staffAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT7_NAME, ["resource6,6.1,workspaceAlias1"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource1,1,11111"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource6,6,66666"])
        resp1 = self.insert_event(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME, "111,11,resource1")
        resp2 = self.insert_event(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME, "666,66,resource6")
        events = self.extract_events(
            SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME,
            resp1.records[0].get_timestamp(),
            resp2.records[0].get_timestamp(),
        )
        records = events.get_data_windows()[0].records
        self.assertEqual(1000, records[0].get("myResourceCategory"))
        self.assertEqual(1.11, records[0].get("resourceExtraAttr"))
        self.assertEqual(6.66, records[1].get("resourceExtraAttr"))

    def test_modify3(self):
        self.create_streams_with_new_json()
        context = self.session.get_context(ENRICHED_CONTEXT_NAME)
        ea = context["enrichments"][0]["enrichedAttributes"]
        ea.append({"value": "resourceType.resourceExtraAttr"})
        update_stream_expect_error(
            self.session,
            "context",
            ENRICHED_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Constraint violation: Enrichment attribute name must exist",
        )

    def test_modify_and_delete1(self):
        self.create_streams_with_new_json()
        self.modify4()
        self.modify5()
        self.session.delete_signal(SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_NAME)
        self.modify6()
        self.modify7()
        self.session.delete_signal(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME)

    def test_modify_and_delete2(self):
        self.create_streams_with_new_json()
        self.modify4()
        self.modify5()
        self.delete_expect_error(
            SECOND_LEVEL_ENRICHED_CONTEXT_NAME,
            "Constraint violation: Context cannot be deleted because other",
        )
        self.session.delete_signal(SIGNAL_FOR_SECOND_CONTEXT_ENRICHMENT_NAME)
        self.session.delete_context(SECOND_LEVEL_ENRICHED_CONTEXT_NAME)
        self.modify7()

    def test_delete1(self):
        self.create_streams_with_new_json()
        self.delete_expect_error(
            CONTEXT_TO_ENRICH_CONTEXT2_NAME,
            "Constraint violation: Context cannot be deleted because other",
        )

    def test_modify_and_delete3(self):
        self.create_streams_with_new_json()
        self.delete_expect_error(
            CONTEXT_TO_ENRICH_CONTEXT4_NAME,
            "Constraint violation: Context cannot be deleted because other",
        )
        context = self.session.get_context(ENRICHED_CONTEXT_NAME)
        del context["enrichments"][1]["enrichedAttributes"][0]["valuePickFirst"][2]
        del context["enrichments"][1]["enrichedAttributes"][1]["valuePickFirst"][2]
        self.session.update_context(ENRICHED_CONTEXT_NAME, context)
        self.session.delete_context(CONTEXT_TO_ENRICH_CONTEXT4_NAME)

    @unittest.skip(reason="Enable after BIOS-1474 is fixed")
    def test_modify4(self):
        context = self.session.get_context(SECOND_LEVEL_ENRICHED_CONTEXT_NAME)
        context["enrichments"][0]["enrichedAttributes"].pop(1)
        update_stream_expect_error(
            self.session,
            "context",
            SECOND_LEVEL_ENRICHED_CONTEXT_NAME,
            context,
            ErrorCode.BAD_INPUT,
            "Constraint violation: Enrichment attribute name must exist",
        )

    def test_modify_after_inserts(self):
        self.test_upsert_insert_select()
        self.modify4()
        self.modify5()
        self.modify6()
        self.modify7()

    def test_modify_after_inserts2(self):
        self.create_streams_with_new_json()
        self.upsert_context_entries()
        self.insert_event(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME, "111,11,resource1")
        self.modify4()

        # This context does not contain any enrichments yet; add one now.
        context = self.session.get_context(CONTEXT_TO_ENRICH_CONTEXT2_NAME)
        a = context["attributes"]
        a.append({"attributeName": "newIntegerAttr", "type": "Integer", "default": "0"})
        enrichment = {}
        enrichment["enrichmentName"] = "freshEnrichment"
        enrichment["missingLookupPolicy"] = "FailParentLookup"
        enrichment["foreignKey"] = ["newIntegerAttr"]
        eas = [{"value": "resourceType.resourceExtraAttr"}]
        enrichment["enrichedAttributes"] = eas
        enrichments = [enrichment]
        context["enrichments"] = enrichments
        self.session.update_context(CONTEXT_TO_ENRICH_CONTEXT2_NAME, context)

        # Use the newly enriched context in a signal
        signal = self.session.get_signal(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME)
        enrichment = {}
        enrichment["enrichmentName"] = "freshEnrichmentSignal"
        enrichment["missingLookupPolicy"] = "Reject"
        enrichment["foreignKey"] = ["resourceId"]
        enrichment["contextName"] = CONTEXT_TO_ENRICH_CONTEXT2_NAME
        cas = [{"attributeName": "resourceExtraAttr", "fillIn": 0}]
        enrichment["contextAttributes"] = cas
        signal["enrich"]["enrichments"].append(enrichment)
        self.session.update_signal(SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME, signal)

        # self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["1,staff,1000,9.9"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["1,staff,1000,9.1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["3,venue,3000,9.3"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["4,auxiliary,4000,9.4"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["5,machine,5000,9.5"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT1_NAME, ["6,workspace,6000,9.6"])
        # Skip 3 in this set of contexts
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT2_NAME, ["resource1,1.1,staffAlias1,1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT2_NAME, ["resource6,6.1,workspaceAlias1,6"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT2_NAME, ["resource5,5.1,machineAlias1,5"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT6_NAME, ["resource5,5.1,machineAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT3_NAME, ["resource2,2.1,doctorAlias1"])
        self.upsert_entries(CONTEXT_TO_ENRICH_CONTEXT5_NAME, ["resource4,4.1,auxiliaryAlias1"])
        # Skip 4 in this context
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource1,1,11111"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource2,2,22222"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource3,3,33333"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource5,5,55555"])
        self.upsert_entries(ENRICHED_CONTEXT_NAME, ["resource6,6,66666"])
        self.insert_events_and_verify()
        signal = SIGNAL_FOR_CONTEXT_ENRICHMENT_NAME
        # TODO(BIOS-1902)
        # resp1 = self.insert_event(signal, "555,55,resource5")
        # events = self.extract_events(signal, resp1.records[0].get_timestamp(),
        #                              resp1.records[0].get_timestamp())
        # records = events.get_data_windows()[0].records
        # self.assertEqual('staff', records[0].get('resourceTypeCode'))
        # self.assertEqual(1000, records[0].get('myResourceCategory'))
        # self.assertEqual(9.9, records[0].get('resourceExtraAttr'))

    def test_shim_context(self):
        # Test a context that only has the key attribute, and all other atributes
        # come from enrichments.
        enriching_context1 = {
            "contextName": "enrichingContext1",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "the_key", "type": "Integer"},
                {"attributeName": "the_value_1", "type": "String"},
            ],
            "primaryKey": ["the_key"],
        }
        enriching_context2 = {
            "contextName": "enrichingContext2",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "the_key", "type": "Integer"},
                {"attributeName": "the_value_2", "type": "Decimal"},
            ],
            "primaryKey": ["the_key"],
        }
        shim_context = {
            "contextName": "shimContext",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "the_key", "type": "Integer"},
            ],
            "primaryKey": ["the_key"],
            "missingLookupPolicy": "StoreFillInValue",
            "enrichments": [
                {
                    "enrichmentName": "shimEnrichment1",
                    "foreignKey": ["the_key"],
                    "enrichedAttributes": [
                        {"value": "enrichingContext1.the_value_1", "fillIn": "Missing"},
                    ],
                },
                {
                    "enrichmentName": "shimEnrichment2",
                    "foreignKey": ["the_key"],
                    "enrichedAttributes": [
                        {"value": "enrichingContext2.the_value_2", "fillIn": 0.0},
                    ],
                },
            ],
        }
        self.session.create_context(enriching_context2)
        self.session.create_context(enriching_context1)
        self.session.create_context(shim_context)

        entries1 = ["1,one", "2,two", "3,three"]
        st = bios.isql().upsert().into("enrichingContext1").csv_bulk(entries1).build()
        self.session.execute(st)

        entries2 = ["1,100.25", "2,200.25", "3,300.25"]
        st = bios.isql().upsert().into("enrichingContext2").csv_bulk(entries2).build()
        self.session.execute(st)

        entries3 = ["1", "2", "3"]
        st = bios.isql().upsert().into("shimContext").csv_bulk(entries3).build()
        self.session.execute(st)

        keys = [["1"], ["2"], ["3"]]
        select_request = bios.isql().select().from_context("shimContext").where(keys=keys).build()
        reply = self.session.execute(select_request)
        self.assertIsInstance(reply, ContextRecords)
        print(reply.to_dict())
        records = reply.get_records()
        self.assertEqual(len(records), 3)
        record0 = records[0]
        self.assertEqual(record0.get_primary_key(), [1])
        self.assertEqual(record0.get("the_value_1"), "one")
        self.assertEqual(record0.get("the_value_2"), 100.25)


if __name__ == "__main__":
    pytest.main(sys.argv)
