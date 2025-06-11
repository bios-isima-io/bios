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

"""Collects DB statuses and send them to biOS"""

import copy
import logging
import random
import re
import threading
import time
from typing import Any, Dict, List

import bios
from bios import ServiceError

from .db_client import DbClient
from .dbdozer_utils import (
    collect_table_info,
    enumerate_keyspaces,
    make_generic_options,
    make_nodetool_options,
    run_command,
    run_nodetool,
    run_ssh,
)
from .dozer_conf import DozerConf
from .scheduler import DozerStatusRecorder


class DbStatusCollector:
    """Class that collects db statistics from all available nodes"""

    def __init__(self, dozer_conf: DozerConf):
        self._all_nodes = []
        self._node_to_hostname = {}
        self._logger = logging.getLogger(DbStatusCollector.__name__)
        self._dozer_conf = dozer_conf
        self._status_pattern = re.compile("([UD][NLJM])\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.*")
        self._thread = None
        self._shutting_down = False
        self._collecting = False
        self._paused_node = None
        self._dozer_status_recorder = None

    def attach_dozer_status_recorder(self, dozer_status_recorder: DozerStatusRecorder):
        self._dozer_status_recorder = dozer_status_recorder

    def set_nodes(self, all_nodes: List[str]):
        """Sets all available names"""
        self._all_nodes = all_nodes

    def update_dozer_conf(self, dozer_conf: DozerConf):
        """Replaces the configuration"""
        self._dozer_conf = dozer_conf

    def start(self):
        """Start running the collector"""
        if not self._dozer_conf.status_collection_interval:
            self._logger.warning("DB status collection is disabled, not started")
            return
        self._thread = threading.Thread(target=self._thread_main)
        self._thread.start()

    def is_started(self) -> bool:
        """Returns true if the collector has started"""
        return self._thread is not None

    def pause(self, pausing_node):
        """Pauses status updates of a node"""
        logging.info("Pausing DB status collection for node %s", pausing_node)
        self._paused_node = pausing_node
        # collection may have started already, wait until it's done in the case
        while self._collecting:
            time.sleep(1)
        logging.info("DB status collection paused for node %s", self._paused_node)

    def resume(self):
        """Resumes status updates of a paused node if any"""
        paused_node = self._paused_node
        self._paused_node = None
        if paused_node:
            logging.info("Resumed DB status collection for node %s", paused_node)

    def stop(self):
        """Stops the collector"""
        if not self._thread:
            return
        self._shutdown()
        self._thread.join()

    def restart_cassandra_nodes(self):
        """Rolling-restart Cassandra nodes"""
        logging.info("!!! Restarting cassandra nodes !!!")
        all_nodes = self._all_nodes.copy()
        gap_seconds = self._dozer_conf.db_rolling_restart_gap_seconds
        for node in all_nodes:
            if self._dozer_conf.db_restart_pause_status_collection:
                self.pause(node)
            try:
                restarted = self._restart_cassandra_node(node)
            except Exception:
                self._logger.error(
                    "An error encountered while restarting node %s", node, exc_info=True
                )
                restarted = False
            if not restarted:
                logging.info("!!! Aborting the rolling restart !!!")
                return
            logging.info(
                "!!!     ... node %s restarted, sleeping for %s seconds", node, gap_seconds
            )
            if self._dozer_conf.db_restart_pause_status_collection:
                self.resume()
            time.sleep(gap_seconds)
        logging.info("!!! Cassandra nodes restarted !!!")

    def _restart_cassandra_node(self, node: str) -> bool:
        options = make_generic_options(self._dozer_conf)
        db_container_name = self._dozer_conf.db_container_name
        command = f"docker restart --time 60 {db_container_name}"
        logging.info("!!!   restarting %s", node)
        is_remote = self._dozer_conf.status_collection_file_count_local_command is None
        success, _, stdout, stderr = (
            run_ssh(options, node, command) if is_remote else run_command(options, command)
        )
        if not success:
            logging.error(
                "Failed to restart cassandra node at %s; stdout=%s, stderr=%s",
                node,
                stdout,
                stderr,
            )
            return False

        sleep_time = self._dozer_conf.db_restart_wait_timeout_seconds
        if len(self._all_nodes) < 2:
            # Monitoring db status is not possible for a single node cluster
            logging.info(
                "!!!     ... done, waiting %s seconds for the node being settled",
                sleep_time,
            )
            time.sleep(sleep_time)
            return True

        logging.info(
            "!!!     ... done, waiting up to %s seconds for the node coming up",
            sleep_time,
        )
        # wait at least one minute
        time0 = time.time()
        time.sleep(50)
        until = time.time() + sleep_time
        while time.time() < until:
            time.sleep(10)
            all_statuses = self._collect_db_statuses(excluded_node=node)
            node_status = all_statuses.get(node) or {"isUp": False}
            if node_status.get("isUp"):
                return True
            time1 = time.time()
            if time1 - time0 > 60:
                logging.info("        ... still waiting")
                time0 += 60
        logging.error("!!!     ... node %s failed to restart", node)
        return False

    def _thread_main(self):
        self._logger.info("DB status collection started")
        now = int(time.time())
        interval = self._dozer_conf.status_collection_interval
        next_report_time = int((now + interval - 1) / interval) * interval
        while not self._shutting_down:
            time.sleep(10)
            now = int(time.time())
            if now < next_report_time:
                continue
            self._collecting = True
            next_report_time += interval
            try:
                self._collect()
            except Exception:
                self._logger.error("DB status collection error", exc_info=True)
            self._collecting = False

    def _shutdown(self):
        self._shutting_down = True

    def _collect(self):
        logging.debug("COLLECT")
        statuses = self._collect_db_statuses()
        self._may_collect_table_info(statuses)
        for node, node_status in statuses.items():
            if node not in self._node_to_hostname:
                self._node_to_hostname[node] = self._resolve_hostname(node)
            node_status["hostname"] = self._node_to_hostname[node]
            node_status["numDbFiles"] = self._get_num_db_files(node)
        # Suppress the status report of paused node if any
        if self._paused_node:
            logging.debug("Retrieved statuses (paused=%s): %s", self._paused_node, statuses)
        statuses.pop(self._paused_node, None)
        self._logger.debug("STATUSES: %s", statuses)
        self._report_statuses(statuses)

    def _may_collect_table_info(self, statuses: Dict[str, Dict[str, Any]]):
        """Collects table info when the time has come"""
        nodes_to_check = []
        for node in self._all_nodes:
            if node == self._paused_node:
                continue
            if not self._dozer_status_recorder.is_table_info_collection_time(node):
                continue
            nodes_to_check.append(node)

        if not nodes_to_check:
            return

        all_table_info = collect_table_info(self._dozer_conf, nodes_to_check)
        for node, node_table_info in all_table_info.items():
            node_info = statuses.setdefault(node, {})
            node_info["table_info"] = node_table_info

    def _collect_db_statuses(self, excluded_node=None) -> dict:
        """Fetches db status from any available DB node, returns a dict of ip and statuses.
        If failed to run nodetool for all nodes, the method returns an empty dict."""
        if excluded_node:
            all_nodes = list(filter(lambda node: node != excluded_node, self._all_nodes))
        else:
            all_nodes = self._all_nodes.copy()

        random.shuffle(all_nodes)
        statuses = {}
        for node in all_nodes:
            if node == self._paused_node:
                continue
            options = make_nodetool_options(node, self._dozer_conf)
            options.quiet = True
            options.command_timeout = 900
            result, _, stdout, stderr = run_nodetool(options, "status")
            if not result:
                self._logger.warning(
                    "Failed to run nodetool status, will try another node;"
                    " host=%s, stdout=%s, stderr=%s",
                    node,
                    stdout,
                    stderr,
                )
                continue
            for line in stdout.split("\n"):
                matched = self._status_pattern.match(line)
                if matched:
                    status = matched.group(1)
                    ip_address = matched.group(2)
                    try:
                        size = float(matched.group(3))
                    except ValueError as err:
                        logging.warn("Failed to parse db size: %s", err)
                        size = 0
                    unit = matched.group(4)
                    if unit == "TiB":
                        size_in_gib = size * 1024.0
                    elif unit == "GiB":
                        size_in_gib = size
                    elif unit == "MiB":
                        size_in_gib = size / 1024.0
                    elif unit == "KiB":
                        size_in_gib = size / 1024.0 / 1024.0
                    else:
                        # bytes?
                        size_in_gib = size / 1024.0 / 1024.0 / 1024.0
                    statuses[ip_address] = {
                        "isUp": status == "UN",
                        "dbSize": size_in_gib,
                    }
            return statuses

        # Failed to run nodetool for all nodes
        self._logger.error("Failed to run nodetool status for all nodes")
        return statuses

    def _resolve_hostname(self, node: str):
        """Resolves the host name of the specified node"""
        options = make_generic_options(self._dozer_conf)
        is_remote = self._dozer_conf.status_collection_file_count_local_command is None
        command = "hostname"
        result, executed, stdout, stderr = (
            run_ssh(options, node, command) if is_remote else run_command(options, command)
        )
        if result:
            tokens = stdout.strip().split(".")
            return tokens[0]

        self._logger.error("Command '%s' failed; stdout=%s, stderr=%s", executed, stdout, stderr)
        return None

    def _get_num_db_files(self, host: str) -> int:
        """Gets number of db files from specified remote host (or local)"""
        options = make_generic_options(self._dozer_conf)
        num_files = 0
        command_template = self._dozer_conf.status_collection_file_count_local_command
        is_remote = command_template is None
        if is_remote:
            command_template = "'find {dir} -type f | egrep -v \"/backups/|/snapshots/\" | wc -l'"
        directories = self._dozer_conf.host_data_file_directories[host]
        commands = [command_template.replace("{dir}", directory) for directory in directories]

        for command in commands:
            result, executed, stdout, stderr = (
                run_ssh(options, host, command) if is_remote else run_command(options, command)
            )
            if result:
                num_files += int(stdout.strip())
            else:
                self._logger.error(
                    "Command '%s' failed; stdout=%s, stderr=%s", executed, stdout, stderr
                )
        return num_files

    def _report_statuses(self, statuses: dict):
        url = self._dozer_conf.bios_initial_endpoint
        user = self._dozer_conf.bios_sysadmin_user.strip()
        password = self._dozer_conf.bios_sysadmin_password.strip()
        signal = self._dozer_conf.bios_db_status_signal
        events = [
            f'{str(status.get("isUp")).lower()},{status.get("dbSize")},'
            f'{status.get("numDbFiles")},{status.get("hostname")}'
            for status in statuses.values()
        ]
        try:
            with bios.login(
                url, user, password, app_name="dbdozer", app_type=bios.models.AppType.REALTIME
            ) as session:
                statement = bios.isql().insert().into(signal).csv_bulk(events).build()
                session.execute(statement)
                self._report_table_info(session, statuses)
        except ServiceError as err:
            self._logger.error(
                "Failed to insert db statuses; endpoint=%s, error_code=%s, message=%s, records=%s",
                err.endpoint,
                err.error_code,
                err.message,
                events,
            )

    def _report_table_info(self, session, statuses: dict):
        all_table_info = []
        nodes = []
        for ip, status in statuses.items():
            host_name = status.get("hostname")
            if not status.get("table_info"):
                continue
            nodes.append(ip)
            for keyspace_name, tables in status["table_info"].items():
                for table_name, table_info in tables.items():
                    if not table_info.get("tenant"):
                        continue
                    entry = copy.deepcopy(table_info)
                    entry["hostName"] = host_name
                    entry["ipAddress"] = ip
                    entry["keyspace"] = keyspace_name
                    entry["table"] = table_name
                    all_table_info.append(entry)

        if all_table_info:
            self._insert_table_info(session, all_table_info)
            self._dozer_status_recorder.record_table_info_collection_time(nodes, int(time.time()))

    def _insert_table_info(self, session, all_table_info):
        signal_name = self._dozer_conf.bios_db_tables_signal
        signal = session.get_signal(signal_name)
        events = []
        for table_info in all_table_info:
            values = []
            for attribute in signal["attributes"]:
                value = table_info.get(attribute.get("attributeName"))
                if value is None:
                    value = attribute.get("default")
                if value is None:
                    value = ""
                values.append(str(value))
            events.append(",".join(values))

        statement = bios.isql().insert().into(signal_name).csv_bulk(events).build()
        session.execute(statement)
