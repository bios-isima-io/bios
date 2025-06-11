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
import configparser
import json
import logging
import os
import sys

import bios
from bios import ErrorCode, ServiceError


def setup(config_file_name: str, deli_config_json: str, stream_config_json: str):
    assert isinstance(config_file_name, str)
    assert isinstance(deli_config_json, str)
    if stream_config_json:
        assert isinstance(stream_config_json, str)

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    logging.info("Setting up Deli configuration for use case '%s'", deli_config_json)
    config = configparser.ConfigParser()
    config.read(config_file_name)
    endpoint_url = config.get("PullConfigLoader", "endpoint")
    cert_file = config.get("PullConfigLoader", "sslCertFile", fallback=None)

    # create tenant
    if stream_config_json:
        with open(stream_config_json, "r") as stream_config_file:
            stream_config = json.load(stream_config_file)
    else:
        stream_config = {"signals": [], "contexts": []}
    with open(deli_config_json, "r") as deli_config_file:
        appendix_config = json.load(deli_config_file)
    # with bios.login(endpoint_url, "superadmin", "superadmin", cafile=cert_file) as sadmin:
    #     logging.info("Setting up tenant %s", stream_config.get("tenantName"))
    #     try:
    #         sadmin.delete_tenant(stream_config.get("tenantName"))
    #     except ServiceError:
    #         pass
    #     sadmin.create_tenant({"tenantName": stream_config.get("tenantName")})

    # set up signals integrations config
    username = config.get("PullConfigLoader", "user")
    password = config.get("PullConfigLoader", "password")
    with bios.login(endpoint_url, username, password, cafile=cert_file) as admin:
        logging.info("Removing signals")
        for signal in stream_config.get("signals") or []:
            try:
                admin.delete_signal(signal.get("signalName"))
                logging.info("      (delted: %s)", signal.get("signalName"))
            except ServiceError as err:
                if err.error_code is not ErrorCode.NO_SUCH_STREAM:
                    raise
        logging.info("Removing contexts")
        for context in stream_config.get("contexts") or []:
            try:
                admin.delete_context(context.get("contextName"))
                logging.info("      (deleted: %s)", context.get("contextName"))
            except ServiceError as err:
                if err.error_code is not ErrorCode.NO_SUCH_STREAM:
                    raise

        logging.info("Creating contexts")
        for context in stream_config.get("contexts") or []:
            logging.info("  %s", context.get("contextName"))
            admin.create_context(context)

        logging.info("Creating signals")
        for signal in stream_config.get("signals") or []:
            logging.info("  %s", signal.get("signalName"))
            admin.create_signal(signal)

        logging.info("Creating import sources")
        sources = appendix_config.get("importSources") or []
        for source in sources:
            try:
                admin.delete_import_source(source.get("importSourceId"))
                logging.info(
                    "      (deleted: (%s) importSourceName = %s)",
                    idstring(source.get("importSourceId")),
                    source.get("importSourceName"),
                )
            except ServiceError:
                pass
        for source in sources:
            logging.info(
                "  (%s) importSourceName = %s",
                idstring(source.get("importSourceId")),
                source.get("importSourceName"),
            )
            admin.create_import_source(source)

        logging.info("Creating import destinations")
        destinations = appendix_config.get("importDestinations") or []
        for destination in destinations:
            try:
                admin.delete_import_destination(destination.get("importDestinationId"))
                logging.info(
                    "      (deleted: (%s) %s)",
                    idstring(destination.get("importDestinationId")),
                    destination.get("importDestinationName"),
                )
            except ServiceError:
                pass
        for destination in destinations:
            logging.info(
                "  (%s) %s",
                idstring(destination.get("importDestinationId")),
                destination.get("importDestinationName"),
            )
            admin.create_import_destination(destination)

        logging.info("Creating import flow specs")
        import_flow_specs = appendix_config.get("importFlowSpecs") or []
        for import_flow_spec in import_flow_specs:
            try:
                admin.delete_import_flow_spec(import_flow_spec.get("importFlowId"))
                logging.info(
                    "      (deleted: (%s) %s)",
                    idstring(import_flow_spec.get("importFlowId")),
                    import_flow_spec.get("importFlowName"),
                )
            except ServiceError:
                pass
        for import_flow_spec in import_flow_specs:
            logging.info(
                "  (%s) %s",
                idstring(import_flow_spec.get("importFlowId")),
                import_flow_spec.get("importFlowName"),
            )
            admin.create_import_flow_spec(import_flow_spec)

        logging.info("Creating processors")
        import_data_processors = appendix_config.get("importDataProcessors") or []
        for import_data_processor in import_data_processors:
            try:
                admin.delete_import_data_processor(import_data_processor.get("processorName"))
                logging.info("      (deleted: %s)", import_data_processor.get("processorName"))
            except ServiceError:
                pass
        for import_data_processor in import_data_processors:
            logging.info("  %s", import_data_processor.get("processorName"))
            code = base64.b64decode(import_data_processor.get("code")).decode("utf-8")
            admin.create_import_data_processor(
                import_data_processor.get("processorName"),
                code,
            )

        logging.info("DONE.")


def idstring(src: str) -> str:
    return f"{src[0:7]} ..." if len(src) > 7 else src


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: set_qa <config.ini> <deli_config.json> <stream_config.json>")
        sys.exit(1)
    setup(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
