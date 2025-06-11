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


TEST_TENANT_NAME = "integrationsNegativeTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


@pytest.fixture(scope="module")
def set_up_class():
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        try:
            sadmin.delete_tenant(TEST_TENANT_NAME)
        except ServiceError:
            pass
        sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})


@pytest.fixture(autouse=True)
def admin(set_up_class):
    session = bios.login(ep_url(), ADMIN_USER, admin_pass)

    yield session

    tenant = session.get_tenant(detail=True)
    for flow in tenant.get("importFlowSpecs"):
        session.delete_import_flow_spec(flow.get("importFlowId"))
    for source in tenant.get("importSources"):
        session.delete_import_source(source.get("importSourceId"))
    for destination in tenant.get("importDestinations"):
        session.delete_import_destination(destination.get("importDestinationId"))
    for signal in tenant.get("signals"):
        if signal.get("signalName") in {"alertsSignal", "covidDataSignal", "invoices"}:
            session.delete_signal(signal.get("signalName"))

    session.close()


@pytest.mark.parametrize(
    "props,message",
    [
        (["importSourceName"], "Property 'content.importSourceName' must not be null"),
        (["type"], "Property 'content.type' must not be null"),
        (["webhookPath"], "Property 'webhookPath' is required for a Webhook data source"),
        (["authentication", "type"], "Property 'content.authentication.type' must not be null"),
    ],
)
def test_incomplete_import_sources(admin, props, message):
    source = {
        "importSourceName": "test incomplete import source",
        "type": "Webhook",
        "webhookPath": "/system",
        "authentication": {"type": "InMessage"},
    }

    import_source_id = None
    try:
        current = source
        for i, prop in enumerate(props):
            if i == len(props) - 1:
                del current[prop]
            else:
                current = current[prop]
        with pytest.raises(ServiceError) as excinfo:
            created = admin.create_import_source(source)
            import_source_id = created.get("importSourceId")
        print(excinfo.value)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert message in excinfo.value.message
    finally:
        if import_source_id:
            try:
                admin.delete_import_source(import_source_id)
            except ServiceError:
                pass


def test_webhook_import_source_without_auth(admin):
    source = {
        "importSourceName": "test webhok import source without auth",
        "type": "Webhook",
        "webhookPath": "/system",
    }

    import_source_id = None
    try:
        created = admin.create_import_source(source)
        import_source_id = created.get("importSourceId")
    finally:
        if import_source_id:
            try:
                admin.delete_import_source(import_source_id)
            except ServiceError:
                pass


@pytest.mark.parametrize(
    "props,message",
    [
        (["type"], "Property 'content.type' must not be null"),
        (["authentication", "type"], "Property 'content.authentication.type' must not be null"),
    ],
)
def test_incomplete_import_destination(admin, props, message):
    destination = {
        "importDestinationName": "test incomplete import destination",
        "type": "Bios",
        "authentication": {"type": "Login"},
    }

    import_destination_id = None
    try:
        current = destination
        for i, prop in enumerate(props):
            if i == len(props) - 1:
                del current[prop]
            else:
                current = current[prop]
        with pytest.raises(ServiceError) as excinfo:
            created = admin.create_import_destination(destination)
            import_source_id = created.get("importSourceId")
        print(excinfo.value)
        assert excinfo.value.error_code == ErrorCode.BAD_INPUT
        assert message in excinfo.value.message
    finally:
        if import_destination_id:
            try:
                admin.delete_import_destination(import_destination_id)
            except ServiceError:
                pass


if __name__ == "__main__":
    pytest.main(sys.argv)
