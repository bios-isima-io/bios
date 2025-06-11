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

"""dbdozer configuration manager"""

import os

import yaml

from .keyspace_category import KeyspaceCategory


class DozerConf:
    """dbdozer configuration manager"""

    REPAIR_INTERVALS = "repair_intervals"
    KEYSPACE_CATEGORY = "keyspace_category"
    INTERVAL_SECONDS = "interval_seconds"
    RUN_OPTIONS = "run_options"
    STEPS_PER_TOKEN = "steps_per_token"
    CONCURRENCY = "concurrency"
    NODETOOL_PATH = "nodetool_path"
    NODETOOL_INITIAL_HOSTS = "nodetool_initial_hosts"
    NODETOOL_INITIAL_PORT = "nodetool_initial_port"
    NODETOOL_USE_SSL = "nodetool_use_ssl"
    NODETOOL_CREDENTIALS_FILE = "nodetool_credentials_file"
    DB_PORT = "db_port"
    DB_CREDENTIALS_FILE = "db_credentials_file"
    DB_CLIENT_USE_SSL = "db_client_use_ssl"
    DB_CERT_FILE = "db_cert_file"
    DB_KEY_FILE = "db_key_file"
    LOG_FILENAME = "log_filename"
    LOG_ROTATE_SIZE = "log_rotate_size"
    LOG_BACKUP_COUNT = "log_backup_count"
    PROGRESS_FILENAME = "progress_filename"
    OUTPUT_FILE = "output_file"
    DRY_RUN = "dry_run"
    FAILURE_BACKOFF_SECONDS = "failure_backoff_seconds"
    STATUS_COLLECTION_INTERVAL = "status_collection_interval"
    STATUS_COLLECTION_FILE_COUNT_LOCAL_COMMAND = "status_collection_file_count_local_command"
    DEBUG = "debug"
    EXCLUDE_KEYSPACES = "exclude_keyspaces"
    COMMAND_TIMEOUT = "command_timeout"
    BACKUP_KEYSPACES = "backup_keyspaces"
    BACKUP_DAYS = "backup_days"
    BACKUP_COMMAND = "backup_command"
    CONTEXTS = "contexts"
    TENANT_NAME = "tenant_name"
    CONTEXT_NAME = "context_name"
    KEYPAIR_FILE = "keypair_file"
    SSH_USER = "ssh_user"
    BIOS_INITIAL_ENDPOINT = "bios_initial_endpoint"
    BIOS_SYSADMIN_USER = "bios_sysadmin_user"
    BIOS_SYSADMIN_PASSWORD = "bios_sysadmin_password"
    BIOS_DB_STATUS_SIGNAL = "bios_db_status_signal"
    BIOS_DB_TABLES_SIGNAL = "bios_db_tables_signal"
    GARBAGECOLLECT_ENABLED = "garbagecollect_enabled"
    HOST_DATA_FILE_DIRECTORIES = "host_data_file_directories"
    DB_RESTART_ENABLED = "db_restart_enabled"
    DB_CONTAINER_NAME = "db_container_name"
    DB_RESTART_WAIT_TIMEOUT_SECONDS = "db_restart_wait_timeout_seconds"
    DB_ROLLING_RESTART_GAP_SECONDS = "db_rolling_restart_gap_seconds"
    DB_RESTART_INTERVAL_HOURS = "db_restart_interval_hours"
    DB_RESTART_ALLOWED_TIME_RANGE = "db_restart_allowed_time_range"
    DB_RESTART_PAUSE_DB_COLLECTION = "db_restart_pause_status_collection"
    ONE_SHOT = "one_shot"
    KEYSPACE = "keyspace"
    TABLE = "table"
    TABLE_INFO_COLLECTION_INTERVAL_SECONDS = "table_info_collection_interval_seconds"

    """Carries dbdozer configuration."""

    def __init__(self, file_name):
        with open(file_name, "r", encoding="utf-8") as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
            self._original_conf = self._expand_env_vars(conf)
            # parse repair intervals settings
            intervals = self._original_conf.get(self.REPAIR_INTERVALS)
            if not isinstance(intervals, list):
                raise RuntimeError(
                    f"property {self.REPAIR_INTERVALS} is not set properly in the dbdozer config"
                )
            self.intervals = {}
            for i, entry in enumerate(intervals):
                try:
                    keyspace_category = KeyspaceCategory[entry.get(self.KEYSPACE_CATEGORY)]
                    interval_seconds = int(entry.get(self.INTERVAL_SECONDS))
                    self.intervals[keyspace_category] = interval_seconds
                except Exception as err:
                    raise RuntimeError(
                        f"Property {self.REPAIR_INTERVALS}[{i}] has misconfiguration"
                    ) from err
            # verify all necessary entity types are configured
            for keyspace_category in KeyspaceCategory:
                if self.intervals.get(keyspace_category) is None:
                    raise RuntimeError(
                        f"KeyspaceCategory {keyspace_category.name} is missing"
                        f" in {self.REPAIR_INTERVALS} configuration"
                    )
            if self.RUN_OPTIONS not in self._original_conf:
                raise RuntimeError(f"{self.RUN_OPTIONS} field must exist")

            run_options = self._original_conf[self.RUN_OPTIONS]
            self.steps_per_token = int(run_options.get(self.STEPS_PER_TOKEN) or 1)
            self.concurrency = int(run_options.get(self.CONCURRENCY) or 1)
            # nodetool setup
            self.nodetool_path = run_options.get(self.NODETOOL_PATH) or "/opt/db/bin/nodetool"
            self.nodetool_initial_hosts = run_options.get(self.NODETOOL_INITIAL_HOSTS)
            self.nodetool_initial_port = int(run_options.get(self.NODETOOL_INITIAL_PORT) or 10105)
            use_ssl_src = run_options.get(self.NODETOOL_USE_SSL)
            if use_ssl_src is None:
                use_ssl_src = False
            self.nodetool_use_ssl = use_ssl_src
            if run_options.get(self.NODETOOL_CREDENTIALS_FILE):
                with open(
                    run_options.get(self.NODETOOL_CREDENTIALS_FILE), "r", encoding="utf-8"
                ) as cred:
                    words = cred.readline().split()
                    self.username = words[0].strip()
                    self.password = words[1].strip() if len(words) > 1 else ""
            else:
                (self.username, self.password) = (None, None)
            # DB client setup
            self.db_port = int(run_options.get(self.DB_PORT)) or 10109
            db_cred_file_name = run_options.get(self.DB_CREDENTIALS_FILE)
            if not db_cred_file_name:
                raise RuntimeError(
                    f"Property {self.DB_CREDENTIALS_FILE} is missing in config file"
                )
            with open(db_cred_file_name, "r", encoding="utf-8") as cred_file:
                cred = yaml.load(cred_file, Loader=yaml.FullLoader)
                self.db_username = cred.get("username")
                self.db_password = cred.get("password")
                if not self.db_username or not self.db_password:
                    raise RuntimeError(
                        "Either username or password is not set in DB credentials file"
                        f" {db_cred_file_name}"
                    )
            self.db_client_use_ssl = run_options.get(self.DB_CLIENT_USE_SSL) or False
            self.db_cert_file = run_options.get(self.DB_CERT_FILE)
            self.db_key_file = run_options.get(self.DB_KEY_FILE)
            self.table_info_collection_interval_seconds = (
                run_options.get(self.TABLE_INFO_COLLECTION_INTERVAL_SECONDS) or 3600
            )
            # logs
            self.log_filename = run_options.get(self.LOG_FILENAME) or "dozer.log"
            self.log_rotate_size = run_options.get(self.LOG_ROTATE_SIZE) or 0
            self.log_backup_count = run_options.get(self.LOG_BACKUP_COUNT) or 0
            self.progress_filename = (
                run_options.get(self.PROGRESS_FILENAME) or "dozer_progress.yaml"
            )
            self.output_file = run_options.get(self.OUTPUT_FILE)
            self.dry_run = run_options.get(self.DRY_RUN)
            if self.dry_run is None:
                self.dry_run = False
            self.failure_backoff_seconds = run_options.get(self.FAILURE_BACKOFF_SECONDS)
            if self.failure_backoff_seconds is None:
                self.failure_backoff_seconds = 14400
            self.status_collection_interval = run_options.get(self.STATUS_COLLECTION_INTERVAL)
            if self.status_collection_interval is None:
                self.status_collection_interval = 120
            self.status_collection_file_count_local_command = run_options.get(
                self.STATUS_COLLECTION_FILE_COUNT_LOCAL_COMMAND
            )
            self.debug = run_options.get(self.DEBUG)
            if self.debug is None:
                self.debug = False
            exclude_keyspaces = run_options.get(self.EXCLUDE_KEYSPACES) or "system,system_schema"
            self.exclude_keyspaces = exclude_keyspaces.split(",")
            self.command_timeout = int(run_options.get(self.COMMAND_TIMEOUT) or "0") or None
            self.backup_keyspaces = set(run_options.get(self.BACKUP_KEYSPACES) or [])
            self.backup_days = set(run_options.get(self.BACKUP_DAYS) or [])
            self.backup_command = run_options.get(self.BACKUP_COMMAND)
            self.contexts = run_options.get(self.CONTEXTS) or []
            self.keypair_file = run_options.get(self.KEYPAIR_FILE)
            self.ssh_user = run_options.get(self.SSH_USER)
            self.bios_initial_endpoint = (
                run_options.get(self.BIOS_INITIAL_ENDPOINT) or "https://bios.isima.io"
            )
            self.bios_sysadmin_user = run_options.get(self.BIOS_SYSADMIN_USER) or ""
            self.bios_sysadmin_password = run_options.get(self.BIOS_SYSADMIN_PASSWORD) or ""
            self.bios_db_status_signal = run_options.get(self.BIOS_DB_STATUS_SIGNAL) or "dbStatus"
            self.bios_db_tables_signal = run_options.get(self.BIOS_DB_TABLES_SIGNAL) or "dbTables"
            self.garbagecollect_enabled = run_options.get(self.GARBAGECOLLECT_ENABLED) or False
            self.host_data_file_directories = (
                run_options.get(self.HOST_DATA_FILE_DIRECTORIES) or False
            )
            self.db_restart_enabled = run_options.get(self.DB_RESTART_ENABLED) or False
            self.db_container_name = run_options.get(self.DB_CONTAINER_NAME)
            self.db_restart_wait_timeout_seconds = run_options.get(
                self.DB_RESTART_WAIT_TIMEOUT_SECONDS
            )
            self.db_rolling_restart_gap_seconds = run_options.get(
                self.DB_ROLLING_RESTART_GAP_SECONDS
            )
            self.db_restart_interval_hours = run_options.get(self.DB_RESTART_INTERVAL_HOURS)
            self.db_restart_allowed_time_range = run_options.get(
                self.DB_RESTART_ALLOWED_TIME_RANGE
            )
            self.db_restart_pause_status_collection = (
                run_options.get(self.DB_RESTART_PAUSE_DB_COLLECTION) or False
            )
            # One-shot execution config if set
            self.one_shot = self._original_conf.get(self.ONE_SHOT)

    @classmethod
    def check_property(cls, conf, property_name):
        value = conf.get(property_name)
        if value is None:
            raise Exception(f"property {property_name} is missing")
        return value

    @classmethod
    def _expand_env_vars(cls, conf):
        """Read all properties recursively and expand environment variables in strings"""
        if isinstance(conf, str):
            return os.path.expandvars(conf)

        if isinstance(conf, dict):
            converted = {}
            for key, value in conf.items():
                converted[key] = cls._expand_env_vars(value)
            return converted

        if isinstance(conf, list):
            return [cls._expand_env_vars(value) for value in conf]

        return conf

    def __repr__(self):
        return yaml.dump(self._original_conf, Dumper=yaml.Dumper)
