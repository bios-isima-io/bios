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
import json
import logging
import os
import sys
import unittest

import bios
import pytest
from bios import ErrorCode, ServiceError
from bios.models import AttributeType, ContextRecords
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import sadmin_pass, sadmin_user

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

TEST_TENANT_NAME = "integrationRegistrationTest"
ADMIN_USER = admin_user + "@" + TEST_TENANT_NAME


class IntegrationRegistrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
            try:
                sadmin.delete_tenant(TEST_TENANT_NAME)
            except ServiceError as err:
                if err.error_code != ErrorCode.NO_SUCH_TENANT:
                    raise
            sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    def test_bios_context_crud(self):
        with open("../resources/tenant_config.json", "r") as tenant_json:
            tenant_config = json.load(tenant_json)

        integrations_config_file_name = "../resources/integrations_config.json"
        with open(integrations_config_file_name, "r") as integrations_json:
            integrations_config_src = (
                integrations_json.read()
                .replace('"${MYSQL_PORT}"', "3306")
                .replace('"${POSTGRES_PORT}"', "5432")
            )
        integrations_config = json.loads(integrations_config_src)

        with bios.login(ep_url(), ADMIN_USER, admin_pass) as session:
            for context in tenant_config.get("contexts") or []:
                session.create_context(context)

            for signal in tenant_config.get("signals") or []:
                session.create_signal(signal)

            for import_destination in integrations_config.get("importDestinations"):
                session.create_import_destination(import_destination)

            for import_source in integrations_config.get("importSources"):
                session.create_import_source(import_source)

            import_data_processors = integrations_config.get("importDataProcessors") or []
            for import_data_processor in import_data_processors:
                if import_data_processor.get("encoding") == "source_file":
                    code_dir = os.path.dirname(integrations_config_file_name)
                    code_path = f"{code_dir}/{import_data_processor.get('code')}"
                    with open(code_path, "r", encoding="utf-8") as file:
                        code = file.read()
                else:
                    code = base64.b64decode(import_data_processor.get("code")).decode("utf-8")
                session.create_import_data_processor(
                    import_data_processor.get("processorName"),
                    code,
                )

            for flow_spec in integrations_config.get("importFlowSpecs"):
                session.create_import_flow_spec(flow_spec)


if __name__ == "__main__":
    pytest.main(sys.argv)
