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
import logging
from configparser import ConfigParser
from datetime import datetime
from datetime import timezone

import mysql.connector

import bios

from deli.configuration import (
    ConfigUtils,
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)

from ..utils import bios_login


from .data_importer import DataImporter, EndpointHandler, SourceFilter


class SqlDataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.system_config = system_config
        self.host = self.config.get(Keyword.DATABASE_HOST)
        self.port = self.config.get(Keyword.DATABASE_PORT) or 3306
        self.db_name = self.config.get(Keyword.DATABASE_NAME)
        self.polling_interval = self.config.get_polling_interval()
        auth = self.config.get(Keyword.AUTHENTICATION)
        self.user = ConfigUtils.get_property(auth, Keyword.USER)
        self.password = ConfigUtils.get_property(auth, Keyword.PASSWORD)
        self.table_name = self.config.get(Keyword.DATABASE_TABLE)
        self.row_limit = 8196
        self.state_key = "timestamp"

        flow_config.get(Keyword.SOURCE_DATA_SPEC)
        self.ssl_disabled = True
        if self.config.get(Keyword.SSL_ENABLE):
            self.ssl_disabled = False
        self.ctx_last_known_state = flow_config.get_name() + "_state"

    def poll_timeout(self):
        return self.polling_interval

    def start_importer(self):
        try:
            conn = None
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.user,
                password=self.password,
                ssl_disabled=self.ssl_disabled,
            )
            for handler in self.endpoint_handlers.values():
                handler.handle(
                    conn,
                    self.db_name,
                    self.system_config,
                    self.ctx_last_known_state,
                    self.state_key,
                    self.row_limit,
                )
        except Exception:  # pylint: disable=broad-except
            self.logger.error(
                "Error happened while connecting to the data source;"
                " host=%s port=%s database=%s user=%s",
                self.host,
                self.port,
                self.db_name,
                self.user,
                exc_info=True,
            )
        if conn is not None:
            conn.close()

    def setup_importer(self):
        # define the campaign metrics to be checked against for changes
        context = {
            "contextName": self.ctx_last_known_state,
            "missingAttributePolicy": "Reject",
            "attributes": [
                {"attributeName": "key", "type": "String"},
                {"attributeName": "value", "type": "String"},
            ],
            "primaryKey": ["key"],
            "dataSynthesisStatus": "Disabled",
            "missingLookupPolicy": "FailParentLookup",
            "enrichments": [],
        }
        with bios_login(self.system_config) as session:
            try:
                session.create_context(context)
            except Exception:  # pylint: disable=broad-except
                self.logger.error(
                    "Context %s already exists", self.ctx_last_known_state, exc_info=True
                )
            finally:
                pass

    def shutdown_importer(self):
        pass

    def get_config_value(self, key: str, source_data_spec: SourceDataSpec) -> str:
        value = None
        try:
            value = source_data_spec.get_or_raise(key)
        except Exception:  # pylint: disable=broad-except
            self.logger.error(
                "Error happened while fetching config",
                exc_info=True,
            )
        return value

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        """Generates MySQL pull endpoint handler ID.
        The ID consists of <database_name>.<table_name>
        """
        table_name = self.get_config_value(Keyword.DATABASE_TABLE, source_data_spec)
        column_name = self.get_config_value(Keyword.COLUMN_NAME, source_data_spec)
        if table_name is None or column_name is None:
            return None
        handler_id = (
            self.db_name.lower()
            + "."
            + source_data_spec.get_or_raise(Keyword.DATABASE_TABLE).lower()
        )
        return handler_id

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec) -> EndpointHandler:
        # handler ID is the service path
        return SqlHandler(self.config, source_data_spec, handler_id)


class SqlHandler(EndpointHandler):
    def __init__(
        self,
        config: SourceConfiguration,
        source_data_spec: SourceDataSpec,
        handler_id: str,
    ):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id
        self.table = self.source_data_spec.get_or_raise(Keyword.DATABASE_TABLE)
        self.column_name = self.source_data_spec.get_or_raise(Keyword.COLUMN_NAME)

    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        pass

    def setup(self):
        pass

    def start(self):
        pass

    def shutdown(self):
        pass

    def handle(self, conn, db_name, system_config, ctx_last_known_state, ctx_key, num_rows):
        cursor = None
        with bios_login(system_config) as session:
            try:
                req = (
                    bios.isql()
                    .select()
                    .from_context(ctx_last_known_state)
                    .where(keys=[[ctx_key]])
                    .build()
                )
                res = session.execute(req)
                record = res.get_records()
            except Exception:  # pylint: disable=broad-except
                self.logger.error("Unknown context =%s", ctx_last_known_state)
                return
            finally:
                pass

            # initialize to current time if not available
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            next_timestamp = timestamp
            if len(record) > 0:
                timestamp = record[0].get("value")
                next_timestamp = timestamp

            try:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(f"USE {db_name}")
                query = f"select * from {self.table}" f" where {self.column_name} > '{timestamp}'"
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(num_rows)
                    if len(rows) == 0:
                        break
                    for row in rows:
                        if row.get(self.column_name):
                            next_timestamp = row.get(self.column_name).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            del row[self.column_name]
                        self.publish(row)

                req = (
                    bios.isql()
                    .upsert()
                    .into(ctx_last_known_state)
                    .csv(ctx_key + "," + next_timestamp)
                    .build()
                )
                session.execute(req)
            except Exception:  # pylint: disable=broad-except
                self.logger.error(
                    "Error happened while fetching data; database=%s, table=%s",
                    db_name,
                    self.table,
                    exc_info=True,
                )
            finally:
                if cursor:
                    cursor.close()


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.MYSQLPULL] = SqlDataImporter
