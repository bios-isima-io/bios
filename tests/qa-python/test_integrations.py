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

import base64
import copy
import json
import logging
import os
import sys
import time
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios.models import ImportDataMappingType
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user


class IntegrationsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
        cls.TEST_TENANT_NAME = "integrationsTest"
        cls.ADMIN_USER = admin_user + "@" + cls.TEST_TENANT_NAME

        logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(cls.TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.create_tenant({"tenantName": cls.TEST_TENANT_NAME})
            sadmin.set_property("prop.apps.resourceReservationSeconds", "20")
            sadmin.set_property("prop.apps.hosts", "bios-apps-1, bios-apps-2, bios-apps-3")

    @classmethod
    def tearDownClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            sadmin.set_property("prop.apps.resourceReservationSeconds", "0")
            try:
                sadmin.delete_tenant(cls.TEST_TENANT_NAME)
            except ServiceError:
                pass
            sadmin.set_property("prop.apps.resourceReservationSeconds", "")
            sadmin.set_property("prop.apps.hosts", "")

    def tearDown(self):
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            tenant = admin.get_tenant(detail=True)
            for flow in tenant.get("importFlowSpecs"):
                admin.delete_import_flow_spec(flow.get("importFlowId"))
            for source in tenant.get("importSources"):
                admin.delete_import_source(source.get("importSourceId"))
            for destination in tenant.get("importDestinations"):
                admin.delete_import_destination(destination.get("importDestinationId"))
            for signal in tenant.get("signals"):
                if signal.get("signalName") in {"alertsSignal", "covidDataSignal", "invoices"}:
                    admin.delete_signal(signal.get("signalName"))

    def test_crud_import_sources(self):
        import_source_json = """{
          "importSourceName": "Webhook Alerts",
          "importSourceId": "203",
          "type": "Webhook",
          "webhookPath": "/system",
          "authentication": {
            "type": "InMessage"
          }
        }"""
        import_source = json.loads(import_source_json)
        source_without_id = copy.deepcopy(import_source)
        del source_without_id["importSourceId"]
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            # create
            created = admin.create_import_source(import_source_json)
            self.assertEqual(created, import_source)
            entry_id = created.get("importSourceId")
            self.assertTrue(isinstance(entry_id, str))
            self.assertEqual(entry_id, import_source.get("importSourceId"))

            # Adding the same entry would cause resource conflict
            with self.assertRaises(ServiceError) as error_context:
                admin.create_import_source(import_source)
            self.assertEqual(error_context.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)

            # Creating with the same content makes another entry
            entry_without_id = copy.deepcopy(created)
            del entry_without_id["importSourceId"]
            created2 = admin.create_import_source(entry_without_id)
            entry_id2 = created2.get("importSourceId")
            self.assertNotEqual(entry_id2, entry_id)
            created2_without_id = copy.deepcopy(created2)
            del created2_without_id["importSourceId"]
            self.assertEqual(created2_without_id, entry_without_id)

            # read
            self.assertEqual(admin.get_import_source(entry_id), created)
            self.assertEqual(admin.get_import_source(entry_id2), created2)

            # update
            to_update = copy.deepcopy(import_source)
            to_update["importSourceName"] = "Webhook system alerts"
            to_update["webhookPath"] = "/deli/system"
            updated = admin.update_import_source(entry_id, to_update)
            self.assertEqual(updated.get("importSourceId"), entry_id)
            to_update["importSourceId"] = entry_id
            self.assertEqual(updated, to_update)
            self.assertEqual(admin.get_import_source(entry_id), to_update)

            # get via get_tenant
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            self.assertEqual(len(tenant_config.get("importSources")), 2)
            self.assertTrue(updated in tenant_config.get("importSources"))
            self.assertTrue(created2 in tenant_config.get("importSources"))

            # delete
            admin.delete_import_source(entry_id)
            with self.assertRaises(ServiceError) as error_context:
                admin.get_import_source(entry_id)
            self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_FOUND)
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            self.assertEqual(len(tenant_config.get("importSources")), 1)

    def test_crud_import_destinations(self):
        import_destination_json = """{
          "importDestinationId": "101",
          "importDestinationName": "BIOS destination",
          "type": "Bios",
          "endpoint": "https://localhost",
          "sslCertFile": "cacerts.pem",
          "authentication": {
            "type": "Login",
            "user": "admin@test",
            "password": "admin"
          }
        }"""
        import_destination = json.loads(import_destination_json)
        destination_without_id = copy.deepcopy(import_destination)
        del destination_without_id["importDestinationId"]
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            # create
            created = admin.create_import_destination(import_destination_json)
            self.assertEqual(created, import_destination)
            entry_id = created.get("importDestinationId")
            self.assertTrue(isinstance(entry_id, str))
            self.assertEqual(entry_id, import_destination.get("importDestinationId"))

            # creating the same destination config with entry ID would cause conflict
            with self.assertRaises(ServiceError) as error_context:
                admin.create_import_destination(import_destination)
            self.assertEqual(error_context.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)

            # Creating with the same content makes dupliate entries
            created2 = admin.create_import_destination(destination_without_id)
            entry_id2 = created2.get("importDestinationId")
            self.assertNotEqual(entry_id2, entry_id)
            created2_without_id = copy.deepcopy(created2)
            del created2_without_id["importDestinationId"]
            self.assertEqual(created2_without_id, destination_without_id)

            # read
            self.assertEqual(admin.get_import_destination(entry_id), created)
            self.assertEqual(admin.get_import_destination(entry_id2), created2)

            # update
            to_update = copy.deepcopy(import_destination)
            to_update["importDestinationName"] = "Bios"
            to_update["endpoint"] = "https://172.18.0.11:8443"
            updated = admin.update_import_destination(entry_id, to_update)
            self.assertEqual(updated.get("importDestinationId"), entry_id)
            to_update["importDestinationId"] = entry_id
            self.assertEqual(updated, to_update)
            self.assertEqual(admin.get_import_destination(entry_id), to_update)

            # get via get_tenant
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            destinations = tenant_config.get("importDestinations")
            self.assertEqual(len(destinations), 2)
            self.assertTrue(updated in destinations)
            self.assertTrue(created2 in destinations)

            # delete
            admin.delete_import_destination(entry_id)
            with self.assertRaises(ServiceError) as error_context:
                admin.get_import_destination(entry_id)
            self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_FOUND)
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            destinations = tenant_config.get("importDestinations")
            self.assertEqual(len(destinations), 1)

    def test_crud_import_flow_specs(self):
        import_source_config = {
            "importSourceId": "215",
            "importSourceName": "Webhook Covid Data Source",
            "type": "Webhook",
            "webhookPath": "/ct",
            "authentication": {
                "type": "InMessage",
            },
        }

        import_destination_config = {
            "importDestinationId": "111",
            "type": "BIOS",
            "endpoint": "https://bios.isima.io",
            "authentication": {"type": "Login", "user": "ravid@isima.io", "password": "Test123!"},
        }

        signal_config = {
            "signalName": "alertsSignal",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "signalName", "type": "String"},
                {"attributeName": "featureName", "type": "String"},
                {"attributeName": "alertName", "type": "String"},
                {"attributeName": "condition", "type": "String"},
                {"attributeName": "event", "type": "String"},
                {"attributeName": "alertRaisedTimestamp", "type": "String"},
                {"attributeName": "year", "type": "Integer"},
                {"attributeName": "month", "type": "Integer"},
            ],
        }

        import_flow_spec_json = """{
          "importFlowName": "Event Received",
          "importFlowId": "305",
          "sourceDataSpec": {
            "importSourceId": "215",
            "webhookSubPath": "/alerts",
            "payloadType": "Csv",
            "headerPresent": true
          },
          "dataPickupSpec": {
            "filters": [
              {
                "sourceAttributeName": "signalName",
                "filter": "lambda value: value == 'monitoredSignal'"
              }
            ],
            "attributeSearchPath": "items/*/data",
            "attributes": [
              { "sourceAttributeName": "signalName" },
              { "sourceAttributeName": "featureName" },
              { "sourceAttributeName": "alertName" },
              { "sourceAttributeName": "condition" },
              { "sourceAttributeName": "event" },
              {
                "sourceAttributeName": "timestampMillisSinceEpoch",
                "as": "alertRaisedTimestamp"
              },
              {
                "sourceAttributeName": "windowStartTime",
                "processes": [
                  {
                    "processorName": "alertUtils",
                    "method": "parse_datetime"
                  }
                ],
                "transforms": [
                  {
                    "rule": "lambda parsed: parsed.year",
                    "as": "year"
                  },
                  {
                    "rule": "lambda parsed: parsed.month",
                    "as": "month"
                  }
                ]
              }
            ]
          },
          "destinationDataSpec": {
            "importDestinationId": "111",
            "type": "Signal",
            "name": "alertsSignal"
          },
          "checkpointingEnabled": true
        }"""
        import_flow_spec = json.loads(import_flow_spec_json)
        flow_spec_without_id = copy.deepcopy(import_flow_spec)
        del flow_spec_without_id["importFlowId"]
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            # prep
            signal_name = signal_config.get("signalName")
            admin.create_signal(signal_config)
            admin.create_import_destination(import_destination_config)
            admin.create_import_source(import_source_config)
            # create with importSourceId
            created = admin.create_import_flow_spec(import_flow_spec_json)
            self.assertEqual(created, import_flow_spec)
            entry_id = created.get("importFlowId")
            self.assertTrue(isinstance(entry_id, str))
            self.assertEqual(entry_id, import_flow_spec.get("importFlowId"))

            # create with ID again would cause conflict
            with self.assertRaises(ServiceError) as error_context:
                admin.create_import_flow_spec(import_flow_spec)
            self.assertEqual(error_context.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)

            # Creating with the same content but without ID makes another entry
            spec_without_id = copy.deepcopy(import_flow_spec)
            del spec_without_id["importFlowId"]
            created2 = admin.create_import_flow_spec(spec_without_id)
            entry_id2 = created2.get("importFlowId")
            self.assertNotEqual(entry_id2, entry_id)
            created2_without_id = copy.deepcopy(created2)
            del created2_without_id["importFlowId"]
            self.assertEqual(created2_without_id, spec_without_id)

            # read
            self.assertEqual(admin.get_import_flow_spec(entry_id), created)
            self.assertEqual(admin.get_import_flow_spec(entry_id2), created2)

            # update
            to_update = copy.deepcopy(import_flow_spec)
            to_update["importFlowName"] = "Abc Abc Abc"
            to_update["sourceDataSpec"]["webhookSubPath"] = "deli/alerts"
            updated = admin.update_import_flow_spec(entry_id, to_update)
            self.assertEqual(updated.get("importFlowId"), entry_id)
            to_update["importFlowId"] = entry_id
            self.assertEqual(updated, to_update)
            self.assertEqual(admin.get_import_flow_spec(entry_id), to_update)

            # get via get_tenant
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            flow_specs = tenant_config.get("importFlowSpecs")
            self.assertEqual(len(flow_specs), 2)
            self.assertTrue(updated in flow_specs)
            self.assertTrue(created2 in flow_specs)

            # deleting a signal that has an attached flow should be rejected
            with self.assertRaises(ServiceError) as error_context:
                admin.delete_signal(signal_name)
            assert error_context.exception.error_code == ErrorCode.BAD_INPUT
            assert error_context.exception.message.startswith(
                "Constraint violation: Import flows to the signal exist. Delete them first; flows="
            )
            assert "Abc Abc Abc (305)" in error_context.exception.message
            assert "Event Received" in error_context.exception.message

            # delete
            admin.delete_import_flow_spec(entry_id)
            with self.assertRaises(ServiceError) as error_context:
                admin.get_import_flow_spec(entry_id)
            self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_FOUND)
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            flow_specs = tenant_config.get("importFlowSpecs")
            self.assertEqual(len(flow_specs), 1)

            # still one more flow left
            with self.assertRaises(ServiceError) as error_context:
                admin.delete_signal(signal_name)
            assert error_context.exception.error_code == ErrorCode.BAD_INPUT
            assert error_context.exception.message.startswith(
                "Constraint violation: An import flow to the signal exists. Delete it first; flow="
            )
            assert "Event Received" in error_context.exception.message

            # the signal can be deleted when all flows are gone
            admin.delete_import_flow_spec(entry_id2)
            admin.delete_signal(signal_name)

    def test_import_flow_spec_to_context(self):
        import_source_config = {
            "importSourceId": "216",
            "importSourceName": "Webhook Covid Data Source",
            "type": "Webhook",
            "webhookPath": "/ct",
            "authentication": {
                "type": "InMessage",
            },
        }

        import_destination_config = {
            "importDestinationId": "112",
            "type": "BIOS",
            "endpoint": "https://bios.isima.io",
            "authentication": {"type": "Login", "user": "ravid@isima.io", "password": "Test123!"},
        }

        context_config = {
            "contextName": "productsByFlow",
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "productId", "type": "String"},
                {"attributeName": "productName", "type": "String"},
            ],
            "primaryKey": ["productId"],
        }

        import_flow_spec_json = """{
          "importFlowName": "Product Flow",
          "importFlowId": "306",
          "sourceDataSpec": {
            "importSourceId": "216",
            "webhookSubPath": "",
            "payloadType": "Json"
          },
          "dataPickupSpec": {
            "attributeSearchPath": "items/*/data",
            "attributes": [
              { "sourceAttributeName": "productId" },
              { "sourceAttributeName": "productName" }
            ]
          },
          "destinationDataSpec": {
            "importDestinationId": "112",
            "type": "Context",
            "name": "productsByFlow"
          },
          "checkpointingEnabled": true
        }"""
        import_flow_spec = json.loads(import_flow_spec_json)
        flow_spec_without_id = copy.deepcopy(import_flow_spec)
        del flow_spec_without_id["importFlowId"]
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            # prep
            context_name = context_config.get("contextName")
            admin.create_context(context_config)
            admin.create_import_destination(import_destination_config)
            admin.create_import_source(import_source_config)
            # create with importSourceId
            created = admin.create_import_flow_spec(import_flow_spec_json)
            self.assertEqual(created, import_flow_spec)
            entry_id = created.get("importFlowId")
            self.assertTrue(isinstance(entry_id, str))
            self.assertEqual(entry_id, import_flow_spec.get("importFlowId"))

            # Creating with the same content but without ID makes another entry
            spec_without_id = copy.deepcopy(import_flow_spec)
            del spec_without_id["importFlowId"]
            created2 = admin.create_import_flow_spec(spec_without_id)
            entry_id2 = created2.get("importFlowId")

            # Rename the first flow
            to_update = copy.deepcopy(import_flow_spec)
            to_update["importFlowName"] = "Abc Abc Abc"
            admin.update_import_flow_spec(entry_id, to_update)

            # deleting a context that has an attached flow should be rejected
            with self.assertRaises(ServiceError) as error_context:
                admin.delete_context(context_name)
            assert error_context.exception.error_code == ErrorCode.BAD_INPUT
            assert error_context.exception.message.startswith(
                "Constraint violation: Import flows to the context exist. Delete them first;"
                " flows="
            )
            assert "Abc Abc Abc (306)" in error_context.exception.message
            assert "Product Flow" in error_context.exception.message

            # delete
            admin.delete_import_flow_spec(entry_id)
            with self.assertRaises(ServiceError) as error_context:
                admin.get_import_flow_spec(entry_id)
            self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_FOUND)
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            flow_specs = tenant_config.get("importFlowSpecs")
            self.assertEqual(len(flow_specs), 1)

            # still one more flow left
            with self.assertRaises(ServiceError) as error_context:
                admin.delete_context(context_name)
            assert error_context.exception.error_code == ErrorCode.BAD_INPUT
            assert error_context.exception.message.startswith(
                "Constraint violation: An import flow to the context exists. Delete it first;"
                " flow="
            )
            assert "Product Flow" in error_context.exception.message

            # the signal can be deleted when all flows are gone
            admin.delete_import_flow_spec(entry_id2)
            admin.delete_context(context_name)

    def test_crud_import_data_processors(self):
        processor_name = "convertAlert"
        processor = """
           def process(record):
               return record
        """
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            # create
            admin.create_import_data_processor(processor_name, processor)

            # Creating with the same mapping name causes an exception
            with self.assertRaises(ServiceError) as error_context:
                admin.create_import_data_processor(processor_name, processor)
            self.assertEqual(error_context.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)

            # get
            retrieved = admin.get_import_data_processor(processor_name)
            self.assertEqual(retrieved, processor)

            # update
            to_update = """
            def process(record):
                record['merged'] = f"{record['a']}_{record['b']}"
                return record
            """
            admin.update_import_data_processor(processor_name, to_update)
            self.assertEqual(admin.get_import_data_processor(processor_name), to_update)

            # get via get_tenant
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            mappings = tenant_config.get("importDataProcessors")
            self.assertEqual(len(mappings), 1)
            self.assertEqual(
                mappings[0],
                {
                    "processorName": processor_name,
                    "encoding": "Base64",
                    "code": base64.b64encode(bytes(to_update, "utf-8")).decode("utf-8"),
                },
            )

            # delete
            admin.delete_import_data_processor(processor_name)
            with self.assertRaises(ServiceError) as error_context:
                admin.get_import_data_processor(processor_name)
            self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_FOUND)
            tenant_config = admin.get_tenant(self.TEST_TENANT_NAME, detail=True)
            mappings = tenant_config.get("importDataProcessors")
            self.assertEqual(len(mappings), 0)

    def test_apps_service_registration(self):
        """Test bios-apps service registration"""
        admin = bios.login(ep_url(), self.ADMIN_USER, admin_pass)
        sadmin = bios.login(ep_url(), sadmin_user, sadmin_pass)
        try:
            # test registration
            with self.assertRaises(ServiceError) as error_context:
                admin.register_apps_service(self.TEST_TENANT_NAME)
            self.assertEqual(error_context.exception.error_code, ErrorCode.FORBIDDEN)
            registered = sadmin.register_apps_service(self.TEST_TENANT_NAME)

            self.assertEqual(registered.get("tenantName"), self.TEST_TENANT_NAME)
            self.assertEqual(
                registered.get("hosts"), ["bios-apps-1", "bios-apps-2", "bios-apps-3"]
            )
            self.assertEqual(registered.get("controlPort"), 9001)
            self.assertEqual(registered.get("webhookPort"), 8081)

            with self.assertRaises(ServiceError) as error_context:
                sadmin.register_apps_service(self.TEST_TENANT_NAME)
            self.assertEqual(error_context.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)

            # test apps service info retrieval
            with self.assertRaises(ServiceError) as error_context:
                admin.get_apps_info(self.TEST_TENANT_NAME)
            self.assertEqual(error_context.exception.error_code, ErrorCode.FORBIDDEN)

            retrieved = sadmin.get_apps_info(self.TEST_TENANT_NAME)
            self.assertEqual(retrieved, registered)

            with self.assertRaises(ServiceError) as error_context:
                sadmin.get_apps_info("noSuchTenant")
            self.assertEqual(error_context.exception.error_code, ErrorCode.NO_SUCH_TENANT)

            # test deregistration
            with self.assertRaises(ServiceError) as error_context:
                admin.get_apps_info(self.TEST_TENANT_NAME)
            self.assertEqual(error_context.exception.error_code, ErrorCode.FORBIDDEN)

            sadmin.deregister_apps_service(self.TEST_TENANT_NAME)

            with self.assertRaises(ServiceError) as error_context:
                sadmin.register_apps_service(
                    self.TEST_TENANT_NAME, control_port=9001, webhook_port=8081
                )
            self.assertEqual(error_context.exception.error_code, ErrorCode.RESOURCE_ALREADY_EXISTS)

            registered = sadmin.register_apps_service(
                self.TEST_TENANT_NAME, control_port=9008, webhook_port=8088
            )
            self.assertEqual(registered.get("controlPort"), 9008)
            self.assertEqual(registered.get("webhookPort"), 8088)

            sadmin.deregister_apps_service(self.TEST_TENANT_NAME)
            registered = sadmin.register_apps_service(self.TEST_TENANT_NAME, hosts=["test-host"])
            self.assertEqual(registered.get("hosts"), ["test-host"])
            assert registered.get("controlPort") > 9008
            assert registered.get("webhookPort") > 8088
            sadmin.deregister_apps_service(self.TEST_TENANT_NAME)

            with self.assertRaises(ServiceError) as error_context:
                sadmin.deregister_apps_service(self.TEST_TENANT_NAME)
            self.assertEqual(error_context.exception.error_code, ErrorCode.NOT_FOUND)

            # wait for the reserved port numbers being expired
            time.sleep(25)

        finally:
            admin.close()
            sadmin.close()

    def test_s3_config(self):
        s3_conf = {
            "signals": [
                {
                    "signalName": "covidDataSignal",
                    "missingAttributePolicy": "Reject",
                    "attributes": [
                        {"attributeName": "geoId", "type": "Integer"},
                        {"attributeName": "cases", "type": "Integer"},
                        {"attributeName": "deaths", "type": "Integer"},
                        {"attributeName": "dataRep", "type": "String"},
                    ],
                }
            ],
            "importSources": [
                {
                    "importSourceId": "202",
                    "importSourceName": "S3 Covid Data Source",
                    "type": "S3",
                    "endpoint": "https://this.is.optional/src.goes.to.amazon.if.omitted",
                    "authentication": {
                        "type": "AccessKey",
                        "accessKey": "c29tZSBhY2Nlc3Mga2V5",
                        "secretKey": "c29tZSBzZWNyZXQga2V5",
                    },
                    "pollingInterval": 30,
                }
            ],
            "importDestinations": [
                {
                    "importDestinationId": "101",
                    "type": "bios",
                    "endpoint": "https://bios.isima.io",
                    "authentication": {
                        "type": "Login",
                        "user": "ravid@isima.io",
                        "password": "Test123!",
                    },
                }
            ],
            "importFlowSpecs": [
                {
                    "importFlowName": "Consume Covid Data",
                    "importFlowId": "302",
                    "sourceDataSpec": {
                        "importSourceId": "202",
                        "s3Bucket": "reports",
                        "payloadType": "Csv",
                        "headerPresent": False,
                        "sourceBatchSize": 1024,
                    },
                    "destinationDataSpec": {
                        "importDestinationId": "101",
                        "type": "Signal",
                        "name": "covidDataSignal",
                    },
                    "checkpointingEnabled": True,
                    "dataPickupSpec": {
                        "attributeSearchPath": "",
                        "attributes": [
                            {"sourceAttributeName": "0", "as": "geoId"},
                            {"sourceAttributeName": "1", "as": "cases"},
                            {"sourceAttributeName": "2", "as": "deaths"},
                            {"sourceAttributeName": "3", "as": "dataRep"},
                        ],
                    },
                }
            ],
        }
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            try:
                signal_config = s3_conf["signals"][0]
                import_destination = s3_conf["importDestinations"][0]
                import_source = s3_conf["importSources"][0]
                import_flow_spec = s3_conf["importFlowSpecs"][0]

                admin.create_signal(signal_config)
                admin.create_import_destination(import_destination)
                registered_source = admin.create_import_source(import_source)
                registered_flow = admin.create_import_flow_spec(import_flow_spec)

                self.assertEqual(registered_source, import_source)
                self.assertEqual(registered_flow, import_flow_spec)

                retrieved_source = admin.get_import_source(import_source["importSourceId"])
                self.assertEqual(retrieved_source, import_source)
                retrieved_flow = admin.get_import_flow_spec(import_flow_spec["importFlowId"])
                self.assertEqual(retrieved_flow, import_flow_spec)

            finally:
                admin.delete_import_flow_spec(registered_flow["importFlowId"])
                admin.delete_signal(signal_config["signalName"])

    def test_file_config(self):
        file_conf = {
            "signals": [
                {
                    "signalName": "invoices",
                    "missingAttributePolicy": "Reject",
                    "attributes": [
                        {"attributeName": "invoiceNo", "type": "Integer"},
                        {"attributeName": "stockCode", "type": "Integer"},
                        {"attributeName": "description", "type": "String"},
                        {"attributeName": "quantity", "type": "Integer"},
                        {"attributeName": "eventTimeBucketEpochMillis", "type": "Integer"},
                        {"attributeName": "eventTimeBucketString", "type": "String"},
                    ],
                }
            ],
            "importSources": [
                {
                    "importSourceId": "201",
                    "importSourceName": "Ecom example",
                    "type": "File",
                    "fileLocation": "/var/spool",
                    "pollingInterval": 30,
                }
            ],
            "importDestinations": [
                {
                    "importDestinationId": "101",
                    "type": "bios",
                    "endpoint": "https://bios.isima.io",
                    "authentication": {
                        "type": "Login",
                        "user": "ravid@isima.io",
                        "password": "Test123!",
                    },
                }
            ],
            "importFlowSpecs": [
                {
                    "importFlowName": "Consume Covid Data",
                    "importFlowId": "301",
                    "sourceDataSpec": {
                        "importSourceId": "201",
                        "fileNamePrefix": "reports-",
                        "payloadType": "Csv",
                        "headerPresent": True,
                        "sourceBatchSize": 1024,
                    },
                    "destinationDataSpec": {
                        "importDestinationId": "101",
                        "type": "Signal",
                        "name": "invoices",
                    },
                    "dataPickupSpec": {
                        "attributeSearchPath": "",
                        "attributes": [
                            {"sourceAttributeName": "InvoiceNo", "as": "invoiceNo"},
                            {"sourceAttributeName": "StockCode", "as": "stockCode"},
                            {"sourceAttributeName": "Description", "as": "description"},
                            {
                                "sourceAttributeName": "Quantity",
                                "transforms": [{"rule": "lambda x: x * x", "as": "quantity"}],
                            },
                            {
                                "sourceAttributeName": "InvoiceDate",
                                "transforms": [
                                    {
                                        "rule": "lambda return_value: return_value[0]",
                                        "as": "eventTimeBucketEpochMillis",
                                    },
                                    {
                                        "rule": "lambda return_value: return_value[1]",
                                        "as": "eventTimeBucketString",
                                    },
                                ],
                            },
                        ],
                    },
                    "checkpointingEnabled": True,
                }
            ],
        }
        with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
            try:
                signal_config = file_conf["signals"][0]
                import_destination = file_conf["importDestinations"][0]
                import_source = file_conf["importSources"][0]
                import_flow_spec = file_conf["importFlowSpecs"][0]

                admin.create_signal(signal_config)
                admin.create_import_destination(import_destination)
                registered_source = admin.create_import_source(import_source)
                registered_flow = admin.create_import_flow_spec(import_flow_spec)

                self.assertEqual(registered_source, import_source)
                self.assertEqual(registered_flow, import_flow_spec)

                retrieved_source = admin.get_import_source(import_source["importSourceId"])
                self.assertEqual(retrieved_source, import_source)
                retrieved_flow = admin.get_import_flow_spec(import_flow_spec["importFlowId"])
                self.assertEqual(retrieved_flow, import_flow_spec)

            finally:
                admin.delete_import_flow_spec(registered_flow["importFlowId"])
                admin.delete_signal(signal_config["signalName"])


if __name__ == "__main__":
    pytest.main(sys.argv)
