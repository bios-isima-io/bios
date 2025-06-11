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

#!/usr/bin/env python3
"""Program to delete old data from a signal table.

The program assumes that the target table is a signal."""

import os
import sys
from ssl import PROTOCOL_TLS, SSLContext
from typing import Tuple

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from .dozer_conf import DozerConf


class DbClient:
    """Cassandra client to be used for querying the DB"""

    def __init__(self, dozer_conf: DozerConf):
        if dozer_conf.db_client_use_ssl:
            ssl_context = SSLContext(PROTOCOL_TLS)
            ssl_context.load_cert_chain(
                certfile=dozer_conf.db_cert_file, keyfile=dozer_conf.db_key_file
            )
            ssl_context.load_verify_locations(cafile=dozer_conf.db_cert_file)
        else:
            ssl_context = None

        # Set authentication credentials if required
        auth_provider = PlainTextAuthProvider(
            username=dozer_conf.db_username, password=dozer_conf.db_password
        )

        # Create a Cluster instance with SSL options and authentication provider
        cluster = Cluster(
            dozer_conf.nodetool_initial_hosts,
            port=dozer_conf.db_port,
            ssl_context=ssl_context,
            auth_provider=auth_provider,
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4,
        )

        # Connect to the cluster
        self._session = cluster.connect()
        self._cluster = cluster

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._session.shutdown()
        self._cluster.shutdown()
        self._session = None
        self._cluster = None

    def get_table_metadata(self, keyspace_name: str, table_name: str):
        """Retrieves a table metadata. Returns None if not found."""
        keyspace = self._cluster.metadata.keyspaces.get(keyspace_name)
        return keyspace.tables.get(table_name) if keyspace else None

    def get_table_options(self, keyspace_name: str, table_name: str):
        """Retrieves options of a table. Returns None if not found."""
        table_metadata = self.get_table_metadata(keyspace_name, table_name)
        return table_metadata.options if table_metadata else None
