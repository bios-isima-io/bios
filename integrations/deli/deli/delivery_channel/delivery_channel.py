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
"""delivery_channel: Defines abstract classes used in delivery channels"""
import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from typing import List

from ..configuration import Configuration, DataFlowConfiguration, DestinationConfiguration
from ..event_spec import EventSpec


class Deliverer(ABC):
    """Abstract class that takes care of delivering messages.

    Instances are registered to DataFeeder that routs incoming messages to corresponding deliverers.
    Each instance is navigable to corresponding DeliveryChannel that takes care of the physical
    connection.
    """

    @abstractmethod
    def deliver(self, events: List[EventSpec], in_message_user: str, in_message_password: str):
        """Delivers a message.

        Args:
            - events (List[Event]): The incoming message to deliver
            - in_message_user (str): In-message user name or None
            - in_message_password (str): In-message password or NOne
        """


class DeliveryChannel(ABC):
    """Abstract class that takes care of physical connection to an importing data destination.

    This class maintains the physical connection to the target, but does not run actions to deliver.
    Deliverers deliver.
    """

    CHANNEL_BUILDERS = {}
    STUB_CHANNEL = None

    @classmethod
    def build(
        cls, destination_config: DestinationConfiguration, system_config: ConfigParser
    ) -> "DeliveryChannel":
        """Class method to build a delivery channel instance.

        The method creates a delivery channel from specified destination configuration.

        Args:
            destination_config (DestinationConfiguration): Destination configuration. The value may
                be None, in which case a stub channel is created.
        """

        # The stub data forwarder is used for testing
        if destination_config is None:
            return cls.STUB_CHANNEL
        assert isinstance(destination_config, DestinationConfiguration)
        if system_config:
            assert isinstance(system_config, ConfigParser)
        channel_type = destination_config.get_type()
        builder = cls.CHANNEL_BUILDERS.get(channel_type)
        if not builder:
            raise Exception(f"Unsupported channel type: {channel_type}")
        return builder(destination_config, system_config)

    def __init__(self):
        pass

    @abstractmethod
    def setup(self):
        """Sets up the channel.

        Build internal objects and establish necessary connections here.
        """

    @abstractmethod
    def make_deliverer(
        self, configuration: Configuration, flow_config: DataFlowConfiguration
    ) -> Deliverer:
        """Creates a deliverer from specified flow_config and global config.

        Args:
            configuration (Configuration): Deli global config
            flow_config (DataFlowConfiguration): Data flow configuration for the data pipe
        """

    @abstractmethod
    def start(self):
        """Activate delivery actions."""

    @abstractmethod
    def stop(self):
        """Deactivate delivery actions."""

    @abstractmethod
    def shutdown(self):
        """Shuts down the deliverer.

        Terminate resources in the class in this method.
        """


class StubDeliverer(Deliverer):
    """Stub deliverer."""

    def __init__(self):
        self.received_messages = []

    def deliver(self, events: List[EventSpec], in_message_user: str, in_message_password: str):
        logging.info("MESSAGE: %s", events)
        self.received_messages.append(events)


class StubDeliveryChannel(DeliveryChannel):
    """Stub delivery channel."""

    def __init__(self):
        super().__init__()
        self._stub_deliverer = StubDeliverer()

    def setup(self):
        pass

    def make_deliverer(
        self, configuration: Configuration, flow_config: DataFlowConfiguration
    ) -> Deliverer:
        return self._stub_deliverer

    def start(self):
        pass

    def stop(self):
        pass

    def shutdown(self):
        pass

    def get_received_messages(self):
        """Returns received message from deliverer... used for testing."""
        return self._stub_deliverer.received_messages


DeliveryChannel.STUB_CHANNEL = StubDeliveryChannel()
