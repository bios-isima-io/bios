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
import time
import unittest

import bios
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user, extract_pass, extract_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as ep
from tsetup import ingest_pass, ingest_user

from bios_tutils import get_all_types_signal, get_minimum_signal
from setup_common import BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME
from setup_common import setup_tenant_config


@unittest.skip("v1.1 has not supported this feature yet")
class DataSynthesisTest(unittest.TestCase):
    TENANT_POSTFIX = "@" + TENANT_NAME

    @classmethod
    def setUpClass(cls):
        setup_tenant_config()

        cls.SIGNAL_NAME_MINIMUM = "dataSynthSignalMinimum"
        cls.SIGNAL_MINIMUM = get_minimum_signal(cls.SIGNAL_NAME_MINIMUM)

        cls.SIGNAL_NAME_ALL_TYPES = "dataSynthSignalAllTypes"
        cls.SIGNAL_ALL_TYPES = get_all_types_signal(cls.SIGNAL_NAME_ALL_TYPES)

        cls.CONTEXT_NAME_ZIP = "dataSynthContextZipcodes"
        cls.CONTEXT_ZIP = {
            "contextName": cls.CONTEXT_NAME_ZIP,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "zipCode", "type": "Integer"},
                {"attributeName": "areaName", "type": "String"},
                {
                    "attributeName": "county",
                    "type": "String",
                    "allowedValues": ["San Francisco", "San Mateo", "Santa Clara"],
                },
            ],
            "primaryKey": ["zipCode"],
        }

        cls.CONTEXT_NAME_FIRE_DANGER_RATINGS = "dataSynthContextFireDangerRatings"
        cls.CONTEXT_FIRE_DANGER_RATINGS = {
            "contextName": cls.CONTEXT_NAME_FIRE_DANGER_RATINGS,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {
                    "attributeName": "areaCode",
                    "type": "String",
                    "allowedValues": ["CAL001", "CAL002", "CAL003", "CAL004"],
                },
                {
                    "attributeName": "fireDangerLevel",
                    "type": "String",
                    "allowedValues": [
                        "LOW",
                        "MODERATE",
                        "HIGH",
                        "VERY_HIGH",
                        "EXTREME",
                    ],
                },
            ],
            "primaryKey": ["areaCode"],
        }

        cls.SIGNAL_NAME_SIMPLE_ENRICH = "dataSynthSignalSimpleEnrich"
        cls.SIGNAL_SIMPLE_ENRICH = {
            "signalName": cls.SIGNAL_NAME_SIMPLE_ENRICH,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "incidentId", "type": "Integer"},
                {"attributeName": "zipCode", "type": "Integer"},
                {
                    "attributeName": "areaCode",
                    "type": "String",
                    "allowedValues": ["CAL001", "CAL002", "CAL003", "CAL004"],
                },
                {
                    "attributeName": "incidentType",
                    "type": "String",
                    "allowedValues": [
                        "CONSTRUCTION_REPORT",
                        "OBSTACLES",
                        "COLLISION",
                        "FIRE",
                    ],
                },
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "joinAreaName",
                        "foreignKey": ["zipcode"],  # BB-1342 change case on purpose
                        "missingLookupPolicy": "Reject",
                        "contextName": cls.CONTEXT_NAME_ZIP,
                        "contextAttributes": [
                            {"attributeName": "areaName"},
                            {"attributeName": "county"},
                        ],
                    },
                    {
                        "enrichmentName": "joinFireDangerRatings",
                        "foreignKey": ["areaCode"],  # BB-1342 change case on purpose
                        "missingLookupPolicy": "Reject",
                        "contextName": cls.CONTEXT_NAME_FIRE_DANGER_RATINGS,
                        "contextAttributes": [{"attributeName": "fireDangerLevel"}],
                    },
                ]
            },
        }

        cls.CONTEXT_NAME_ZIP2 = "dataSynthContextZipcodes2"
        cls.CONTEXT_ZIP2 = copy.deepcopy(cls.CONTEXT_ZIP)
        cls.CONTEXT_ZIP2["contextName"] = cls.CONTEXT_NAME_ZIP2

        cls.SIGNAL_NAME_ENRICH_WITH_FILLIN = "dataSynthSignalEnrichWithFillin"
        cls.SIGNAL_ENRICH_WITH_FILLIN = {
            "signalName": cls.SIGNAL_NAME_ENRICH_WITH_FILLIN,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "incidentId", "type": "Integer"},
                {"attributeName": "zipCode", "type": "Integer"},
            ],
            "enrich": {
                "enrichments": [
                    {
                        "enrichmentName": "joinAreaName",
                        "foreignKey": ["zipCode"],
                        "missingLookupPolicy": "StoreFillInValue",
                        "contextName": cls.CONTEXT_NAME_ZIP2,
                        "contextAttributes": [{"attributeName": "areaName", "fillIn": "n/a"}],
                    }
                ]
            },
        }

        cls.TFOS_SIGNAL_INCOMPATIBLE_NAME = "dataSynthesisSignalTfosIncompatible"
        cls.TFOS_SIGNAL_INCOMPATIBLE = {
            "name": cls.TFOS_SIGNAL_INCOMPATIBLE_NAME,
            "attributes": [{"name": "eventDate", "type": "date"}],
        }

        with bios.login(ep_url(), admin_user + cls.TENANT_POSTFIX, admin_pass) as admin:
            cls.add_signal(admin, cls.SIGNAL_MINIMUM)
            cls.add_signal(admin, cls.SIGNAL_ALL_TYPES)
            cls.add_signal_context(
                admin,
                cls.SIGNAL_SIMPLE_ENRICH,
                cls.CONTEXT_ZIP,
                cls.CONTEXT_FIRE_DANGER_RATINGS,
            )
            cls.add_signal_context(admin, cls.SIGNAL_ENRICH_WITH_FILLIN, cls.CONTEXT_ZIP2)
        # We keep this signal config until we remove the TFOS signals completely
        with Client(*ep()).admin_login(TENANT_NAME, admin_user, admin_pass) as temp_admin:
            try:
                temp_admin.delete_stream(TENANT_NAME, cls.TFOS_SIGNAL_INCOMPATIBLE_NAME)
            except ServiceError as err:
                if err.error_code != ErrorCode.NO_SUCH_STREAM:
                    raise
            temp_admin.add_stream(TENANT_NAME, cls.TFOS_SIGNAL_INCOMPATIBLE)

    @classmethod
    def add_signal(cls, session, signal_config):
        try:
            session.delete_signal(signal_config.get("signalName"))
        except ServiceError:
            pass
        session.create_signal(signal_config)

    @classmethod
    def add_signal_context(cls, session, signal_config, *context_configs):
        try:
            session.delete_signal(signal_config.get("signalName"))
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_STREAM:
                raise

        for context_config in context_configs:
            try:
                session.delete_context(context_config.get("contextName"))
            except ServiceError as err:
                if err.error_code != ErrorCode.NO_SUCH_STREAM:
                    raise
            session.create_context(context_config)

        session.create_signal(signal_config)

    def test_data_synthesis(self):
        with bios.login(ep_url(), ingest_user + self.TENANT_POSTFIX, ingest_pass) as ingest:
            # Start data synthesis
            start_time = bios.time.now()
            with self.assertRaises(ServiceError) as context:
                ingest.start_data_synthesis(self.SIGNAL_NAME_SIMPLE_ENRICH)
            self.assertEqual(context.exception.error_code, ErrorCode.OPERATION_UNEXECUTABLE)

            with Client(*ep()).admin_login(TENANT_NAME, admin_user, admin_pass) as temp_admin:
                temp_admin.put_context_entries(
                    TENANT_NAME,
                    self.CONTEXT_NAME_ZIP,
                    [
                        "94101,San Francisco,San Francisco",
                        "94002,Belmont,San Mateo",
                        "94005,Brisbane,San Mateo",
                        "94010,Hillsborough,San Mateo",
                        "94011,Burlingame,San Mateo",
                        "94014,Colma,San Mateo",
                        "94014,Daly City,San Mateo",
                        "94018,El Granada,San Mateo",
                        "94019,Half Moon Bay,San Mateo",
                        "94020,La Honda,San Mateo",
                        "94021,Loma Mar,San Mateo",
                        "94025,West Menlo Park,San Mateo",
                        "94025,Menlo Park,San Mateo",
                        "94027,Atherton,San Mateo",
                        "94030,Millbrae,San Mateo",
                        "94061,Redwood City,San Mateo",
                        "95050,Santa Clara,Santa Clara",
                    ],
                )

            updated0 = ingest.start_data_synthesis(self.CONTEXT_NAME_FIRE_DANGER_RATINGS)
            # data synthesis against a context is one shot
            self.assertEqual(updated0.get("dataSynthesisStatus"), "Disabled")

            updated11 = ingest.start_data_synthesis(self.SIGNAL_NAME_MINIMUM)
            self.assertEqual(updated11.get("dataSynthesisStatus"), "Enabled")
            updated21 = ingest.start_data_synthesis(self.SIGNAL_NAME_SIMPLE_ENRICH)
            self.assertEqual(updated21.get("dataSynthesisStatus"), "Enabled")
            updated31 = ingest.start_data_synthesis(self.SIGNAL_NAME_ALL_TYPES)
            self.assertEqual(updated31.get("dataSynthesisStatus"), "Enabled")
            updated41 = ingest.start_data_synthesis(self.SIGNAL_NAME_ENRICH_WITH_FILLIN)
            self.assertEqual(updated41.get("dataSynthesisStatus"), "Enabled")
            time.sleep(30)

            # Stop data synthesis
            updated12 = ingest.stop_data_synthesis(self.SIGNAL_NAME_MINIMUM)
            self.assertEqual(updated12.get("dataSynthesisStatus"), "Disabled")
            updated22 = ingest.stop_data_synthesis(self.SIGNAL_NAME_SIMPLE_ENRICH)
            self.assertEqual(updated22.get("dataSynthesisStatus"), "Disabled")
            updated32 = ingest.stop_data_synthesis(self.SIGNAL_NAME_ALL_TYPES)
            self.assertEqual(updated32.get("dataSynthesisStatus"), "Disabled")
            updated42 = ingest.stop_data_synthesis(self.SIGNAL_NAME_ENRICH_WITH_FILLIN)
            self.assertEqual(updated42.get("dataSynthesisStatus"), "Disabled")

            # Verify the data has been actually generated
            time.sleep(15)  # the synth has to stop at this time point ideally
            with bios.login(ep_url(), extract_user + self.TENANT_POSTFIX, extract_pass) as extract:
                now = bios.time.now()

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_MINIMUM)
                    .time_range(now, start_time - now)
                    .build()
                )
                response11 = extract.execute(statement)
                self.assertGreater(len(response11.data_windows[0].records), 10)

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_SIMPLE_ENRICH)
                    .time_range(now, start_time - now)
                    .build()
                )
                response21 = extract.execute(statement)
                self.assertGreater(len(response21.data_windows[0].records), 10)

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_ALL_TYPES)
                    .time_range(now, start_time - now)
                    .build()
                )
                response31 = extract.execute(statement)
                self.assertGreater(len(response31.data_windows[0].records), 10)

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_ENRICH_WITH_FILLIN)
                    .time_range(now, start_time - now)
                    .build()
                )
                response41 = extract.execute(statement)
                self.assertGreater(len(response41.data_windows[0].records), 10)

                # Retry the extraction a while after to make sure the synthesis has stopped
                time.sleep(10)
                now = bios.time.now()
                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_MINIMUM)
                    .time_range(now, start_time - now)
                    .build()
                )
                response12 = extract.execute(statement)
                self.assertEqual(
                    len(response12.data_windows[0].records),
                    len(response11.data_windows[0].records),
                )

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_SIMPLE_ENRICH)
                    .time_range(now, start_time - now)
                    .build()
                )
                response22 = extract.execute(statement)
                self.assertEqual(
                    len(response22.data_windows[0].records),
                    len(response21.data_windows[0].records),
                )

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_ALL_TYPES)
                    .time_range(now, start_time - now)
                    .build()
                )
                response32 = extract.execute(statement)
                self.assertEqual(
                    len(response32.data_windows[0].records),
                    len(response31.data_windows[0].records),
                )

                statement = (
                    bios.isql()
                    .select()
                    .from_signal(self.SIGNAL_NAME_ENRICH_WITH_FILLIN)
                    .time_range(now, start_time - now)
                    .build()
                )
                response42 = extract.execute(statement)
                self.assertEqual(
                    len(response42.data_windows[0].records),
                    len(response41.data_windows[0].records),
                )


if __name__ == "__main__":
    unittest.main()
