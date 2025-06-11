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
import time
from typing import List, Tuple

import yaml

from .dbdozer_utils import (
    KEY_DB_RESTART,
    KEY_LAST_BACKUP_TIME,
    KEY_LAST_FINISHED,
    KEY_LAST_INCR_BACKUP_TIME,
    KEY_LAST_RESTARTED,
    KEY_LAST_STARTED,
    KEY_LAST_TABLE,
    KEY_STATUS,
    KEY_TABLE_INFO_COLLECTION,
    VALUE_DONE,
    VALUE_IN_PROGRESS,
    dump_dict_to_file,
    resolve_keyspace_category,
)
from .dozer_conf import DozerConf


class KeyspaceMaintenanceProgressItem:
    def __init__(self, item: dict):
        self.last_started = item.get(KEY_LAST_STARTED)
        self.last_finished = item.get(KEY_LAST_FINISHED)
        self.status = item.get(KEY_STATUS)
        self.last_table = item.get(KEY_LAST_TABLE) or ""
        self.last_backup_time = item.get(KEY_LAST_BACKUP_TIME)
        self.last_incr_backup_time = item.get(KEY_LAST_INCR_BACKUP_TIME)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


class AllowedTimeRange:
    """Class used for checking a timestamp is in configured time ranges.
    The time range is specified by a string with format
      hhmm-hhmm[,hhmm-hhmm[..]]
      e.g.
        0830-1130
        1900-2500
        0100-0300,1300-1500
    A time item can exceed 2400. In such a case. The overflew range is wrapped into the next day
    and the time of the day is used for the range specification.
    """

    ONE_DAY = 24 * 3600

    """Allowed time range checker"""

    def __init__(self, spec: str):
        if not spec:
            # all pass
            self.ranges = [0, self.ONE_DAY]
        else:
            self.ranges = []
            items = spec.split(",")
            for item in items:
                start_end = item.split("-")
                if len(start_end) != 2:
                    raise ValueError(f"allowed time range syntax error: {item}")
                start = self._to_second(int(start_end[0]))
                end = self._to_second(int(start_end[1]))
                if end < start:
                    raise ValueError(f"start time must be earlier than end time: {item}")

                offset = int(start / self.ONE_DAY) * self.ONE_DAY
                start -= offset
                end -= offset
                if end <= self.ONE_DAY:
                    self.ranges.append((start, end))
                else:
                    self.ranges.append((start, self.ONE_DAY))
                    self.ranges.append((0, end % self.ONE_DAY))

    @classmethod
    def _to_second(cls, hour_min: int):
        """Converts an integer of format hhmm to seconds"""
        hours = int(hour_min / 100)
        minutes = hour_min % 100
        if minutes >= 60:
            raise ValueError("invalid minute")
        return hours * 3600 + minutes * 60

    def test(self, timestamp: int):
        """Tests if the specified timestamp (in seconds) is in an allowed ranges"""
        second_of_day = timestamp % self.ONE_DAY
        for begin, end in self.ranges:
            if begin <= second_of_day < end:
                return True
        return False


class DozerStatusRecorder:
    """Records status of dbdozer"""

    def __init__(self, dozer_conf: DozerConf = None):
        # dict of keyspace name and MaintenanceProgressItem
        self.status = {}
        self.last_db_restarted = 0
        # dict of node and time
        self.last_table_info_collected = {}
        if dozer_conf:
            self.file_name = dozer_conf.progress_filename
            self.interval_by_keyspace_category = dozer_conf.intervals
            self.failure_backoff_seconds = dozer_conf.failure_backoff_seconds
            self.db_restart_enabled = dozer_conf.db_restart_enabled
            self.db_restart_interval_seconds = dozer_conf.db_restart_interval_hours * 3600
            self.db_restart_allowed_time_range = AllowedTimeRange(
                dozer_conf.db_restart_allowed_time_range
            )
            self.table_info_collection_interval = dozer_conf.table_info_collection_interval_seconds
        else:
            raise RuntimeError("dozer_conf must be set")

        if self.file_name:
            self.load_progress_file(self.file_name)

    def load_progress_file(self, file_name: str):
        """Loads initial progresses from a file. An empty progress is loaded
        when the file is missing."""
        try:
            with open(file_name, "r") as file:
                dozer_status = yaml.load(file, Loader=yaml.FullLoader)
                self.load_keyspace_statuses(dozer_status.get(KEY_STATUS))
                restart_status = dozer_status.get(KEY_DB_RESTART) or {}
                self.last_db_restarted = restart_status.get(KEY_LAST_RESTARTED) or 0
                self.last_table_info_collected = dozer_status.get(KEY_TABLE_INFO_COLLECTION) or {}
        except EnvironmentError:
            logging.info(
                "     ... Progress file %s not found, this is the first execution", file_name
            )
        return

    def load_keyspace_statuses(self, status_data: dict):
        """loads keyspace statuses
        example status item in yaml:
        tfos_d_0d9930f000d1347cbbbac64f3498fe70:
          last_started: 1654198095
          last_finished: 0
          status: done
          last_backup_time: 1654206290
          last_incr_backup_time: 1653759760
          last_table: evt_7d3774b9e5fb3c95a7e357bd380f483a
        """
        if status_data is None:
            return
        self.status = {
            key: KeyspaceMaintenanceProgressItem(item) for key, item in status_data.items()
        }

    def update_available_keyspaces(self, available_keyspaces: List[str]):
        """Updates available keyspace names"""
        new_status = {}
        for keyspace in available_keyspaces:
            item = self.get_by_keyspace(keyspace)
            if not item:
                item = KeyspaceMaintenanceProgressItem(
                    {
                        KEY_STATUS: VALUE_DONE,
                        KEY_LAST_STARTED: 0,
                        KEY_LAST_FINISHED: 0,
                    }
                )
            new_status[keyspace] = item
        # Replace the status map. Entries for non-existing keyspaces are flushed off.
        self.status = new_status
        self.dump()

    def set_failure_backoff_seconds(self, failure_backoff_seconds: int):
        """Sets failure backoff seconds"""
        self.failure_backoff_seconds = failure_backoff_seconds

    def get_failure_backoff_seconds(self) -> int:
        """Returns current failure backoff seconds"""
        return self.failure_backoff_seconds

    def get_next_keyspace(self) -> Tuple[str, str, int]:
        """Find the next keyspace to maintain"""
        keyspace_candidate = None
        last_table = ""
        earliest_maintenance_time = None
        for keyspace, item in self.status.items():
            category = resolve_keyspace_category(keyspace)
            maintenance_time = item.last_finished + self.interval_by_keyspace_category[category]
            if item.status == VALUE_IN_PROGRESS:
                # Resume the maintenance (with next to the last_table)
                return keyspace, item.last_table, item.last_started
            if not earliest_maintenance_time or maintenance_time < earliest_maintenance_time:
                keyspace_candidate = keyspace
                last_table = item.last_table
                earliest_maintenance_time = maintenance_time
        return keyspace_candidate, last_table, earliest_maintenance_time

    def is_restart_time(self) -> bool:
        """Returns if cassandra nodes should be restarted now"""
        if not self.db_restart_enabled:
            return False
        now = int(time.time())
        if not self.db_restart_allowed_time_range.test(now):
            return False
        next_restart_time = self.last_db_restarted + self.db_restart_interval_seconds
        return now >= next_restart_time

    def record_restart_time(self, restart_time: int):
        """Records the last DB restart time"""
        self.last_db_restarted = restart_time
        self.dump()

    def is_table_info_collection_time(self, node: str) -> bool:
        """Returns if table info should be collected for a node"""
        now = int(time.time())
        interval = self.table_info_collection_interval
        last_collection_time = self.last_table_info_collected.get(node) or 0
        last_checkpoint = int(last_collection_time / interval) * interval
        next_checkpoint = last_checkpoint + interval
        return now >= next_checkpoint

    def record_table_info_collection_time(self, nodes: List[str], collection_time: int):
        """Records the last DB restart time"""
        for node in nodes:
            self.last_table_info_collected[node] = collection_time
        self.dump()

    def record_maintenance_started(self, keyspace: str, start_time: int):
        """Records maintaining keyspace started"""
        item = self.get_by_keyspace(keyspace)
        item.last_started = start_time
        item.status = VALUE_IN_PROGRESS
        self.dump()

    def mark_last_table(self, keyspace: str, last_table: str):
        """Records maintaining a keyspace finished"""
        item = self.get_by_keyspace(keyspace)
        item.last_table = last_table
        self.dump()

    def record_maintenance_finished(self, keyspace: str, finish_time: int):
        """Records maintaining a keyspace finished"""
        item = self.get_by_keyspace(keyspace)
        item.last_finished = finish_time
        item.status = VALUE_DONE
        item.last_table = ""
        self.dump()

    def get_by_keyspace(self, keyspace: str) -> KeyspaceMaintenanceProgressItem:
        """Gets progress status by keyspace"""
        return self.status.get(keyspace)

    def to_dict(self):
        """Convert current status to a dict"""
        return {
            KEY_STATUS: {key: self.status[key].to_dict() for key in sorted(self.status.keys())},
            KEY_DB_RESTART: {KEY_LAST_RESTARTED: self.last_db_restarted},
            KEY_TABLE_INFO_COLLECTION: self.last_table_info_collected,
        }

    def dump(self):
        """Dump current status to the progress file"""
        if not self.file_name:
            return
        dump_dict_to_file(self.to_dict(), self.file_name)


# class ContextMaintenanceScheduler:
