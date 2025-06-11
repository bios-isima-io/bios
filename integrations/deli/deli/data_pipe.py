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
"""data_pipe: Per-flow data pipe"""
from multiprocessing.connection import Connection
from typing import List, Tuple

from .configuration import (
    Configuration,
    DataFlowConfiguration,
    SourceConfiguration,
    SourceDataSpec,
)
from .data_feeder import DataFeeder
from .event_spec import EventSpec


class DataPipe:
    """The data pipe class"""

    def __init__(
        self,
        flow_config: DataFlowConfiguration,
        configuration: Configuration,
        data_feeder: DataFeeder,
    ):
        assert isinstance(data_feeder, DataFeeder)
        self.flow_config = flow_config
        self.configuration = configuration
        self.flow_id = flow_config.get_id()
        self.data_feeder = data_feeder
        self.data_feeder.register_destination(configuration, flow_config)

    def get_flow_config(self) -> DataFlowConfiguration:
        """Returns data flow configuration."""
        return self.flow_config

    def get_source_data_spec(self) -> SourceDataSpec:
        """Returns source data specification."""
        return self.flow_config.get_source_spec()

    def get_source_config(self) -> SourceConfiguration:
        """Returns source configuration."""
        return self.configuration.get_source_config(self.get_source_data_spec().get_source_id())

    def get_configuration(self) -> Configuration:
        """Returns deli configuration."""
        return self.configuration

    def push(self, input_events: Tuple[List[EventSpec], str, str], conn_notify_back: Connection):
        """Push the data to the feeder

        Args:
            - input_events (Tuple[List[Event], str, str]): Input events, user name, and password
            - conn_notify_back (Connection): Connection as one end of a pipe that is used to notify
                  completion of delivery
        """
        assert isinstance(input_events, tuple)
        if conn_notify_back:
            assert isinstance(conn_notify_back, Connection)
        resp = self.data_feeder.feed(self.flow_id, input_events, conn_notify_back)
        return resp

    def start(self):
        """Start the pipe."""

    def shutdown(self):
        """Stop the pipe and release resources."""
