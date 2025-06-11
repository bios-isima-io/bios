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
"""config_loader: Deli configuration loaders"""

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from configparser import ConfigParser
from multiprocessing import Pipe, Process
from multiprocessing.connection import Connection

import bios

from .configuration import Configuration


class ConfigLoader(ABC):
    """Abstract class that loads deli configuration"""

    @abstractmethod
    def load(self) -> Configuration:
        """Loads a deli configuration

        Returns: Configuration: Deli configuration
        """

    @classmethod
    def provide_loader(cls, system_config: ConfigParser) -> "ConfigLoader":
        """Class method to provide a config loader according to the specified system configuration.

        The method reads property 'configLoader' in section 'Common' of the system configuration
        and builds config loader as follows:
          - pull : PullConfigLoader
          - file : FileConfigLoader

        Args:
            - system_config (ConfigParser): System configuration

        Returns: ConfigLoader: The configuration loader
        """
        loader_type = system_config.get("Common", "configLoader", fallback="file").lower()
        if loader_type == "pull":
            return PullConfigLoader(system_config)
        elif loader_type == "file":
            return FileConfigLoader(system_config.get("Common", "configFileName"))
        raise Exception(f"Config loader type '{loader_type} is unsupported")


class FileConfigLoader(ConfigLoader):
    """Loads Deli configuration from a JSON file."""

    def __init__(self, file_name: str):
        assert isinstance(file_name, str)
        self.file_name = file_name

    def load(self) -> Configuration:
        with open(self.file_name, "r") as file:
            return json.load(file, object_pairs_hook=OrderedDict)


class PullConfigLoader(ConfigLoader):
    """Pulls Deli configuration from a BIOS server."""

    CONFIG_SECTION = "PullConfigLoader"

    def __init__(self, system_config: ConfigParser):
        assert isinstance(system_config, ConfigParser)
        self.logger = logging.getLogger(type(self).__name__)
        self.endpoint = system_config.get(self.CONFIG_SECTION, "endpoint")
        self.cafile = system_config.get(self.CONFIG_SECTION, "sslCertFile", fallback=None)
        self.user = system_config.get(self.CONFIG_SECTION, "user", fallback=None)
        self.password = system_config.get(self.CONFIG_SECTION, "password", fallback=None)
        self.tenant = system_config.get(self.CONFIG_SECTION, "tenant", fallback=None)
        self.app_name = system_config.get("Common", "appName", fallback=None)
        self.app_type = system_config.get("Common", "appType", fallback=None)
        # If only tenant is specified, we use the tenant's support account
        if not self.user and self.tenant:
            self.user = f"support+{self.tenant}@isima.io"
            self.password = os.getenv("BIOS_PASSWORD", "Test123!")
            # safety
            if len(self.password) == 0:
                self.password = "Test123!"
        if not self.endpoint:
            raise Exception(
                "PullConfigLoader: Option 'endpoint' is not set in the configuration file"
            )
        if not self.user:
            raise Exception("PullConfigLoader: Option 'user' is not set in the configuration file")
        if not self.user:
            raise Exception(
                "PullConfigLoader: Option 'password' is not set in the configuration file"
            )

    def load(self) -> Configuration:
        self.logger.info("Start loading tenant config...")
        parent_conn, child_conn = Pipe()
        process = Process(target=self._load, args=(child_conn,))
        process.start()
        tenant_config = parent_conn.recv()
        process.join()
        return tenant_config

    def _load(self, child_conn: Connection) -> Configuration:
        backoff_sleep = 1
        backoff_max_sleep = 32
        tenant_config = None
        while tenant_config is None:
            try:
                self.logger.info("Fetching configuration from BIOS server...")
                with bios.login(
                    self.endpoint,
                    self.user,
                    self.password,
                    cafile=self.cafile,
                    app_name=self.app_name,
                    app_type=self.app_type,
                ) as client:
                    tenant_config = client.get_tenant(detail=True)
            except Exception as err:  # pylint: disable=broad-except
                self.logger.error("Error happened while pulling configuration: %s", err)
                if tenant_config is None:
                    self.logger.info("Will retry in %d seconds...", backoff_sleep)
                time.sleep(backoff_sleep)
                if backoff_sleep < backoff_max_sleep:
                    backoff_sleep *= 2
        self.logger.info("done.")
        child_conn.send(tenant_config)
        self.logger.info("Tenant config obtained")
