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

import heapq
import logging
import time

import bios
from bios import ServiceError

from .dbdozer_utils import (
    KEY_CONTEXT_NAME,
    KEY_KEYSPACE_NAME,
    KEY_LAST_FINISHED,
    KEY_TABLE_NAME,
    KEY_TENANT_NAME,
)


class ContextMaintenanceStatus:
    def __init__(self, status: dict, maintenance_interval_seconds: int):
        assert isinstance(status, dict)
        assert isinstance(maintenance_interval_seconds, int)
        self.tenant_name = status.get(KEY_TENANT_NAME)
        self.context_name = status.get(KEY_CONTEXT_NAME)
        self.last_finished = status.get(KEY_LAST_FINISHED) or 0
        self.next_maintenance_time = self.last_finished + maintenance_interval_seconds
        self.keyspace_name = status.get(KEY_KEYSPACE_NAME)
        self.table_name = status.get(KEY_TABLE_NAME)
        self._interval = maintenance_interval_seconds

    def __lt__(self, other) -> bool:
        return self.next_maintenance_time < other.next_maintenance_time

    def finish(self, finished_time: int):
        assert isinstance(finished_time, int)
        self.last_finished = finished_time
        self.next_maintenance_time = self.last_finished + self._interval

    def to_dict(self) -> dict:
        obj = self.__dict__.copy()
        del obj["next_maintenance_time"]
        del obj["_interval"]
        return obj


class ContextMaintainer:
    def __init__(
        self,
        statuses: dict,
        context_maintenance_interval: int,
        initial_endpoint: str,
        username: str,
        password: str,
    ):
        assert isinstance(statuses, dict)
        assert isinstance(context_maintenance_interval, int)
        assert isinstance(initial_endpoint, str)
        assert isinstance(username, str)
        assert isinstance(password, str)
        self._initial_endpoint = initial_endpoint
        self._username = username
        self._password = password
        self._contexts = []
        with bios.login(initial_endpoint, username, password) as session:
            for tenant_name in statuses:
                tenant = statuses.get(tenant_name)
                for context_name in tenant:
                    status = tenant.get(context_name)
                    item = ContextMaintenanceStatus(status, context_maintenance_interval)
                    try:
                        if not item.keyspace_name or not item.table_name:
                            self._fill_table_info(session, item)
                        logging.info(
                            "  context=%s.%s, table=%s.%s, last_finished=%s",
                            item.tenant_name,
                            item.context_name,
                            item.keyspace_name,
                            item.table_name,
                            item.last_finished,
                        )
                        heapq.heappush(self._contexts, item)
                    except ServiceError as err:
                        logging.warning(
                            "  Failed to check context %s.%s: %s",
                            item.tenant_name,
                            item.context_name,
                            err,
                        )

    def get_next_maintenance_time(self) -> int:
        return self._contexts[0].next_maintenance_time if len(self._contexts) > 0 else 0

    def try_maintain(self) -> dict:
        """Try maintaining the contexts
        Returns: dict: status of maintained context.
                        None is returned if no contexts were maintained
        """
        now = int(time.time())
        if len(self._contexts) == 0 or now < self._contexts[0].next_maintenance_time:
            return None

        item = heapq.heappop(self._contexts)
        logging.info("=====")
        logging.info(
            "--> Maintaining context %s.%s (table %s.%s)",
            item.tenant_name,
            item.context_name,
            item.keyspace_name,
            item.table_name,
        )
        with bios.login(self._initial_endpoint, self._username, self._password) as session:
            # Run cleanup only once. The rollup nodes would take care of the rest once you move the
            # context maintenance progress from the bottom.
            # We avoid running maintenance from here as much as possible since the python SDK
            # cannot route the maintenance requests to the rollup node.
            try:
                result = session.maintain_context(
                    "cleanup", item.tenant_name, item.context_name, batch_size=8192
                )
                item.keyspace_name = result.get("keyspaceName")
                item.table_name = result.get("tableName")
                logging.info(
                    "... done maintaining context, coverage=%s percent",
                    result.get("maintenanceProgress"),
                )
            except ServiceError as err:
                logging.warning("Error happened during context maintenance: %s", err)

        now = int(time.time())
        item.finish(now)
        heapq.heappush(self._contexts, item)
        return item.to_dict()

    def _fill_table_info(self, session, status: ContextMaintenanceStatus):
        context_info = session.maintain_context(
            "describe", status.tenant_name, status.context_name
        )
        status.keyspace_name = context_info.get("keyspaceName")
        status.table_name = context_info.get("tableName")
