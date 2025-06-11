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


def setup(
    config_file_name: str,
    deli_config_json: str,
    stream_config_json: str,
    apps_hosts: str,
    apps_control_port: int,
):
    assert isinstance(config_file_name, str)
    assert isinstance(deli_config_json, str)
    if stream_config_json:
        assert isinstance(stream_config_json, str)
    if apps_hosts:
        assert isinstance(apps_hosts, str)
    if apps_control_port is not None:
        assert isinstance(apps_control_port, int)

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    logging.info("Setting up Deli configuration for use case '%s'", deli_config_json)
    config = configparser.ConfigParser()
    config.read(config_file_name)
    listener_address = config.get("Webhook", "listenerAddress", fallback="0.0.0.0")
    listener_port = config.get("Webhook", "listenerPort", fallback=8081)
    webhook_url = f"http://{listener_address}:{listener_port}"
    endpoint_url = config.get("PullConfigLoader", "endpoint")
    cert_file = config.get("PullConfigLoader", "sslCertFile", fallback=None)
    mysql_host = config.get("MySQL", "databaseHost", fallback="172.17.0.1")
    mysql_port = config.get("MySQL", "databasePort", fallback="3306")
    postgres_host = config.get("Postgres", "databaseHost", fallback="172.17.0.1")
    postgres_port = config.get("Postgres", "databasePort", fallback="5432")

    # create tenant
    if stream_config_json:
        with open(stream_config_json, "r") as stream_config_file:
            src = (
                stream_config_file.read()
                .replace("${WEBHOOK_ENDPOINT}", webhook_url)
                .replace("${BIOS_ENDPOINT}", endpoint_url)
            )
            stream_config = json.loads(src)
    else:
        stream_config = {"signals": [], "contexts": []}

    # read deli config
    with open(deli_config_json, "r") as deli_config_file:
        deli_config_source = (
            deli_config_file.read()
            .replace("${WEBHOOK_ENDPOINT}", webhook_url)
            .replace("${BIOS_ENDPOINT}", endpoint_url)
            .replace("${MYSQL_HOST}", mysql_host)
            .replace('"${MYSQL_PORT}"', str(mysql_port))
            .replace("${POSTGRES_HOST}", postgres_host)
            .replace('"${POSTGRES_PORT}"', str(postgres_port))
        )
        appendix_config = json.loads(deli_config_source)

    tenant_name = stream_config.get("tenantName")
    if tenant_name:
        with bios.login(endpoint_url, "superadmin", "superadmin", cafile=cert_file) as sadmin:
            logging.info("Setting up tenant %s", tenant_name)
            try:
                sadmin.create_tenant({"tenantName": tenant_name})
            except ServiceError as err:
                if err.error_code is not ErrorCode.TENANT_ALREADY_EXISTS:
                    raise
            if apps_hosts and apps_control_port:
                hosts = [entry.strip() for entry in apps_hosts.split(",")]
                logging.info("Setting up apps service host=%s port=%s", hosts, apps_control_port)
                sadmin.register_apps_service(
                    tenant_name, hosts=hosts, control_port=apps_control_port
                )
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
            admin.create_import_destination(destination)

        for destination in destinations:
            logging.info(
                "  (%s) %s",
                idstring(destination.get("importDestinationId")),
                destination.get("importDestinationName"),
            )

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
            if import_data_processor.get("encoding") == "source_file":
                code_dir = os.path.dirname(deli_config_json)
                code_path = f"{code_dir}/{import_data_processor.get('code')}"
                with open(code_path, "r", encoding="utf-8") as file:
                    code = file.read()
            else:
                code = base64.b64decode(import_data_processor.get("code")).decode("utf-8")
            admin.create_import_data_processor(
                import_data_processor.get("processorName"),
                code,
            )

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

        logging.info("DONE.")


def idstring(src: str) -> str:
    return f"{src[0:7]} ..." if len(src) > 7 else src


def usage():
    print("Usage: set_qa [options] <config.ini> <deli_config.json> <stream_config.json>")
    print("options:")
    print("  --apps-host         : apps host")
    print("  --apps-control-port : apps control port")
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()

    APPS_HOSTS = None
    APPS_CONTROL_PORT = None
    INDEX = 1
    while sys.argv[INDEX].startswith("--"):
        if sys.argv[INDEX] == "--apps-hosts":
            INDEX += 1
            if INDEX >= len(sys.argv):
                usage()
            APPS_HOSTS = sys.argv[INDEX]
        elif sys.argv[INDEX] == "--apps-control-port":
            INDEX += 1
            if INDEX >= len(sys.argv):
                usage()
            APPS_CONTROL_PORT = int(sys.argv[INDEX])
        else:
            usage()
        INDEX += 1
    if INDEX + 1 >= len(sys.argv):
        usage()
    CONFIG_FILE_NAME = sys.argv[INDEX]
    INDEX += 1
    DELI_CONFIG_JSON = sys.argv[INDEX]
    INDEX += 1
    STREAM_CONFIG_JSON = sys.argv[INDEX] if INDEX < len(sys.argv) else None

    setup(CONFIG_FILE_NAME, DELI_CONFIG_JSON, STREAM_CONFIG_JSON, APPS_HOSTS, APPS_CONTROL_PORT)
