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
import unittest

from bios import ServiceError, ErrorCode
import bios
from bios_tutils import get_minimum_signal
from setup_common import (
    BIOS_QA_COMMON_TENANT_NAME as TENANT_NAME,
    setup_signal,
    setup_tenant_config,
)
from tsetup import (
    sadmin_pass,
    sadmin_user,
    system_ingest_user,
    system_ingest_pass,
    admin_user,
    admin_pass,
    report_user,
    report_pass,
    mlengineer_user,
    mlengineer_pass,
    extract_user,
    extract_pass,
    ingest_user,
    ingest_pass,
)
from tsetup import get_endpoint_url as ep_url


class PermissionsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_tenant_config()
        cls.SIGNAL_NAME = "permissionTestMinimum"
        cls.SIGNAL_MINIMUM = get_minimum_signal(cls.SIGNAL_NAME)
        cls.MLE_SIGNAL = "signalPermissionsMlengineer"
        with bios.login(ep_url(), f"{admin_user}@{TENANT_NAME}", admin_pass) as admin:
            try:
                admin.delete_signal(cls.MLE_SIGNAL)
            except ServiceError:
                pass

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), f"{admin_user}@{TENANT_NAME}", admin_pass) as admin:
            tenant_config = admin.get_tenant(detail=True)
            for import_source in tenant_config.get("importSources"):
                admin.delete_import_source(import_source.get("importSourceId"))
            for import_destination in tenant_config.get("importDestinations"):
                admin.delete_import_destination(import_destination.get("importDestinationId"))
            for import_flow_spec in tenant_config.get("importFlowSpecs"):
                admin.delete_import_flow_spec(import_flow_spec.get("importFlowId"))
            for import_data_processor in tenant_config.get("importDataProcessors"):
                admin.delete_import_data_processor(import_data_processor.get("processorName"))

    def test_crud_system_signal_by_sysadmin(self):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.create_signal(self.SIGNAL_MINIMUM)
                signal = sadmin.get_signal(self.SIGNAL_NAME)
                self.assertEqual(signal.get("signalName"), self.SIGNAL_NAME)
                to_modify = copy.deepcopy(self.SIGNAL_MINIMUM)
                to_modify["attributes"].append(
                    {"attributeName": "two", "type": "string", "default": ""}
                )
                sadmin.update_signal(self.SIGNAL_NAME, to_modify)
                signal2 = sadmin.get_signal(self.SIGNAL_NAME)
                self.assertEqual(len(signal2.get("attributes")), 2)
            finally:
                sadmin.delete_signal(self.SIGNAL_NAME)

    def test_signal_config_permissions(self):
        signal_name = self.MLE_SIGNAL
        signal = get_minimum_signal(signal_name)

        extract_client = bios.login(ep_url(), f"{extract_user}@{TENANT_NAME}", extract_pass)
        ingest_client = bios.login(ep_url(), f"{ingest_user}@{TENANT_NAME}", ingest_pass)
        report_client = bios.login(ep_url(), f"{report_user}@{TENANT_NAME}", report_pass)

        # ingest and extract users should not be able to create a signal
        with self.assertRaises(ServiceError) as ec:
            extract_client.create_signal(signal)
        self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
        with self.assertRaises(ServiceError) as ec:
            ingest_client.create_signal(signal)
        self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)

        with bios.login(
            ep_url(), f"{mlengineer_user}@{TENANT_NAME}", mlengineer_pass
        ) as mle_client:
            mle_client.create_signal(signal)

            # UPDATE
            modified = copy.deepcopy(signal)
            modified["attributes"].append(
                {"attributeName": "two", "type": "string", "default": ""}
            )
            # ingest and extract users should not be able to create a signal
            with self.assertRaises(ServiceError) as ec:
                extract_client.update_signal(signal_name, modified)
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            with self.assertRaises(ServiceError) as ec:
                ingest_client.update_signal(signal_name, modified)
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            mle_client.update_signal(signal_name, modified)

            # READ
            retrieved = mle_client.get_signal(signal_name)
            self.assertEqual(retrieved.get("signalName"), signal_name)
            self.assertEqual(retrieved.get("attributes")[1].get("attributeName"), "two")

            retrieved_e = extract_client.get_signal(signal_name)
            self.assertEqual(retrieved_e.get("signalName"), signal_name)
            retrieved_i = ingest_client.get_signal(signal_name)
            self.assertEqual(retrieved_i.get("signalName"), signal_name)
            retrieved_r = report_client.get_signal(signal_name)
            self.assertEqual(retrieved_r.get("signalName"), signal_name)

            # READ via get_tenant
            tenant_e = extract_client.get_tenant()
            self._assert_includes_signal(tenant_e, signal_name)
            tenant_i = ingest_client.get_tenant()
            self._assert_includes_signal(tenant_i, signal_name)
            tenant_r = report_client.get_tenant()
            self._assert_includes_signal(tenant_r, signal_name)

            # DELETE
            # ingest and extract users should not be able to create a signal
            with self.assertRaises(ServiceError) as ec:
                extract_client.delete_signal(signal_name)
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)
            with self.assertRaises(ServiceError) as ec:
                ingest_client.delete_signal(signal_name)
            self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)

            mle_client.delete_signal(signal_name)

        extract_client.close()
        ingest_client.close()
        report_client.close()

    def test_integrations(self):
        admin = bios.login(ep_url(), f"{admin_user}@{TENANT_NAME}", admin_pass)
        mle = bios.login(ep_url(), f"{mlengineer_user}@{TENANT_NAME}", mlengineer_pass)
        ingest = bios.login(ep_url(), f"{ingest_user}@{TENANT_NAME}", ingest_pass)
        extract = bios.login(ep_url(), f"{extract_user}@{TENANT_NAME}", extract_pass)
        report = bios.login(ep_url(), f"{report_user}@{TENANT_NAME}", report_pass)

        signal_name = "alertsSignalInPermissionsTest"

        import_source = {
            "importSourceName": "Webhook Alerts",
            "importSourceId": "205",
            "type": "Webhook",
            "webhookPath": "/system",
            "authentication": {"type": "InMessage"},
        }

        import_destination = {
            "importDestinationId": "101",
            "type": "Bios",
            "endpoint": "https://localhost",
            "authentication": {
                "type": "Login",
                "user": "admin@test",
                "password": "admin",
            },
        }

        signal = {
            "signalName": signal_name,
            "missingAttributePolicy": "Reject",
            "attributes": [{"attributeName": "hello", "type": "String"}],
        }

        import_flow_spec = {
            "importFlowName": "Event Received",
            "importFlowId": "301",
            "sourceDataSpec": {
                "importSourceId": "205",
                "webhookSubPath": "/alerts",
                "webhookFilter": "attribute: signalName = 'monitoredSignal'",
                "payloadType": "Json",
                "mapperName": "alertsMapping",
            },
            "destinationDataSpec": {
                "importDestinationId": "101",
                "type": "Signal",
                "name": signal_name,
                "destinationBatchSize": 1024,
            },
            "dataPickupSpec": {
                "attributeSearchPath": "",
                "attributes": [{"sourceAttributeName": "hello"}],
            },
            "checkpointingEnabled": True,
        }

        processor = """
           def process(record):
               return record
        """
        setup_signal(admin, signal)

        # Import source
        self._expect_denied(lambda: ingest.create_import_source(import_source))
        self._expect_denied(lambda: extract.create_import_source(import_source))
        self._expect_denied(lambda: mle.create_import_source(import_source))
        self._expect_denied(lambda: report.create_import_source(import_source))
        self._expect_allowed(lambda: admin.create_import_source(import_source))

        self._expect_allowed(lambda: ingest.get_import_source("205"))
        self._expect_allowed(lambda: extract.get_import_source("205"))
        self._expect_allowed(lambda: mle.get_import_source("205"))
        self._expect_allowed(lambda: report.get_import_source("205"))
        self._expect_allowed(
            lambda: self.assertEqual(
                report.get_tenant(detail=True).get("importSources")[0].get("importSourceId"),
                "205",
            )
        )
        self._expect_allowed(lambda: admin.get_import_source("205"))

        self._expect_denied(lambda: ingest.update_import_source("205", import_source))
        self._expect_denied(lambda: extract.update_import_source("205", import_source))
        self._expect_denied(lambda: mle.update_import_source("205", import_source))
        self._expect_denied(lambda: report.update_import_source("205", import_source))
        self._expect_allowed(lambda: admin.update_import_source("205", import_source))

        # Import destination
        self._expect_denied(lambda: ingest.create_import_destination(import_destination))
        self._expect_denied(lambda: extract.create_import_destination(import_destination))
        self._expect_denied(lambda: mle.create_import_destination(import_destination))
        self._expect_denied(lambda: report.create_import_destination(import_destination))
        self._expect_allowed(lambda: admin.create_import_destination(import_destination))

        self._expect_allowed(lambda: ingest.get_import_destination("101"))
        self._expect_allowed(lambda: extract.get_import_destination("101"))
        self._expect_allowed(lambda: mle.get_import_destination("101"))
        self._expect_allowed(lambda: report.get_import_destination("101"))
        self._expect_allowed(lambda: admin.get_import_destination("101"))
        self._expect_allowed(
            lambda: self.assertEqual(
                report.get_tenant(detail=True)
                .get("importDestinations")[0]
                .get("importDestinationId"),
                "101",
            )
        )

        self._expect_denied(lambda: ingest.update_import_destination("101", import_destination))
        self._expect_denied(lambda: extract.update_import_destination("101", import_destination))
        self._expect_denied(lambda: mle.update_import_destination("101", import_destination))
        self._expect_denied(lambda: report.update_import_destination("101", import_destination))
        self._expect_allowed(lambda: admin.update_import_destination("101", import_destination))

        # Import flow specs
        self._expect_denied(lambda: ingest.create_import_flow_spec(import_flow_spec))
        self._expect_denied(lambda: extract.create_import_flow_spec(import_flow_spec))
        self._expect_denied(lambda: report.create_import_flow_spec(import_flow_spec))
        self._expect_allowed(lambda: mle.create_import_flow_spec(import_flow_spec))
        import_flow_spec["importFlowId"] = "302"
        self._expect_allowed(lambda: admin.create_import_flow_spec(import_flow_spec))

        self._expect_allowed(lambda: ingest.get_import_flow_spec("301"))
        self._expect_allowed(lambda: extract.get_import_flow_spec("301"))
        self._expect_allowed(lambda: report.get_import_flow_spec("301"))
        self._expect_allowed(lambda: mle.get_import_flow_spec("301"))
        self._expect_allowed(lambda: admin.get_import_flow_spec("302"))
        self._expect_allowed(
            lambda: self.assertEqual(
                report.get_tenant(detail=True).get("importFlowSpecs")[0].get("importFlowId"),
                "301",
            )
        )

        self._expect_denied(lambda: ingest.update_import_flow_spec("301", import_flow_spec))
        self._expect_denied(lambda: extract.update_import_flow_spec("301", import_flow_spec))
        self._expect_denied(lambda: report.update_import_flow_spec("301", import_flow_spec))
        self._expect_allowed(lambda: mle.update_import_flow_spec("301", import_flow_spec))
        self._expect_allowed(lambda: admin.update_import_flow_spec("302", import_flow_spec))

        self._expect_denied(lambda: ingest.delete_import_flow_spec("301"))
        self._expect_denied(lambda: extract.delete_import_flow_spec("301"))
        self._expect_denied(lambda: report.delete_import_flow_spec("301"))
        self._expect_allowed(lambda: mle.delete_import_flow_spec("301"))
        self._expect_allowed(lambda: admin.delete_import_flow_spec("302"))

        # Import data processors
        processor_name = "processor"
        processor_name2 = "processor2"
        self._expect_denied(lambda: ingest.create_import_data_processor(processor_name, processor))
        self._expect_denied(
            lambda: extract.create_import_data_processor(processor_name, processor)
        )
        self._expect_denied(lambda: report.create_import_data_processor(processor_name, processor))
        self._expect_allowed(lambda: mle.create_import_data_processor(processor_name, processor))
        self._expect_allowed(
            lambda: admin.create_import_data_processor(processor_name2, processor)
        )

        self._expect_allowed(lambda: ingest.get_import_data_processor(processor_name))
        self._expect_allowed(lambda: extract.get_import_data_processor(processor_name))
        self._expect_allowed(lambda: report.get_import_data_processor(processor_name))
        self._expect_allowed(lambda: mle.get_import_data_processor(processor_name))
        self._expect_allowed(lambda: admin.get_import_data_processor(processor_name2))
        self._expect_allowed(
            lambda: self.assertEqual(
                report.get_tenant(detail=True).get("importDataProcessors")[0].get("processorName"),
                processor_name,
            )
        )

        self._expect_denied(lambda: ingest.delete_import_source("205"))
        self._expect_denied(lambda: extract.delete_import_source("205"))
        self._expect_denied(lambda: mle.delete_import_source("205"))
        self._expect_denied(lambda: report.delete_import_source("205"))
        self._expect_allowed(lambda: admin.delete_import_source("205"))

        self._expect_denied(lambda: ingest.delete_import_destination("101"))
        self._expect_denied(lambda: extract.delete_import_destination("101"))
        self._expect_denied(lambda: mle.delete_import_destination("101"))
        self._expect_denied(lambda: report.delete_import_destination("101"))
        self._expect_allowed(lambda: admin.delete_import_destination("101"))

        self._expect_denied(lambda: ingest.update_import_data_processor(processor_name, processor))
        self._expect_denied(
            lambda: extract.update_import_data_processor(processor_name, processor)
        )
        self._expect_denied(lambda: report.update_import_data_processor(processor_name, processor))
        self._expect_allowed(lambda: mle.update_import_data_processor(processor_name, processor))
        self._expect_allowed(
            lambda: admin.update_import_data_processor(processor_name2, processor)
        )

        self._expect_denied(lambda: ingest.delete_import_data_processor(processor_name))
        self._expect_denied(lambda: extract.delete_import_data_processor(processor_name))
        self._expect_denied(lambda: report.delete_import_data_processor(processor_name))
        self._expect_allowed(lambda: mle.delete_import_data_processor(processor_name))
        self._expect_allowed(lambda: admin.delete_import_data_processor(processor_name2))

        admin.close()
        mle.close()
        ingest.close()
        extract.close()
        report.close()

    def _expect_allowed(self, func):
        func()

    def _expect_denied(self, func):
        with self.assertRaises(ServiceError) as ec:
            func()
        self.assertEqual(ec.exception.error_code, ErrorCode.FORBIDDEN)

    def _assert_includes_signal(self, tenant_config, signal_name):
        names = [signal.get("signalName") for signal in tenant_config.get("signals")]
        self.assertIn(signal_name, names)


if __name__ == "__main__":
    unittest.main()
