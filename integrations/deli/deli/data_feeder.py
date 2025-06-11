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
"""data_feeder: Data feeder module"""
import logging
from abc import ABC, abstractmethod
from multiprocessing import Pipe
from multiprocessing.connection import Connection
from typing import List, Tuple

from bios import ErrorCode, ServiceError

from .configuration import Configuration, DataFlowConfiguration
from .delivery_channel import DeliveryChannel
from .event_spec import EventSpec


class DataFeeder(ABC):
    """Deli data feeder.

    A deli framework has only one instance of DataFeeder. The instance takes care of receiving and
    forwarding incoming messages. All importers feed the messages to the feeder; the feeder routs
    the messages to proper deliverers.
    """

    def __init__(self):
        self._delivery_channels = {}
        # dict of flow ID and corresponding deliverer
        self._deliverers = {}

    @abstractmethod
    def setup(self):
        """Sets up the data pipe."""

    @abstractmethod
    def start(self):
        """Starts the feeder."""

    @abstractmethod
    def stop(self):
        """Stop the sink asynchronously. Use stop_sync to wait for the stop completion."""

    @abstractmethod
    def shutdown(self):
        """Shut down the feeder"""

    def get_delivery_channels(self) -> dict:
        """Gets delivery channels.

        Returns: dict: Delivery channels as dict of channel ID and channel
        """
        return self._delivery_channels

    def register_delivery_channel(self, destination_id: str, delivery_channel: DeliveryChannel):
        """Registers a delivery channel for a destination.

        Args:
            destination_id (str): Destination ID
            delivery_channel (DeliveryChannel): Delivery channel to register
        """
        assert isinstance(destination_id, str)
        assert isinstance(delivery_channel, DeliveryChannel)
        self._delivery_channels[destination_id] = delivery_channel

    def get_delivery_channel(self, destination_id: str) -> DeliveryChannel:
        """Provides a registered delivery channel.

        Args:
            destination_id (str): Destination ID
        Returns: DeliveryChannel: Specified delivery channel
            or None if the specified channel does not exist
        """
        assert isinstance(destination_id, str)
        return self._delivery_channels.get(destination_id)

    def register_destination(self, config: Configuration, flow_config: DataFlowConfiguration):
        """Registers a forwarding destination by generating a deliverer.

        Args:
            flow_id (str): Data flow ID
            src_spec (SourceDataSpec): Source data specification
            dst_spec (DestinationDataSpec): Destination data specification
        """
        assert isinstance(config, Configuration)
        assert isinstance(flow_config, DataFlowConfiguration)

        channel = self.get_delivery_channel(
            flow_config.get_destination_spec().get_destination_id()
        )
        assert channel is not None
        self._deliverers[flow_config.get_id()] = channel.make_deliverer(config, flow_config)

    @abstractmethod
    def feed(
        self, flow_id: str, message: Tuple[List[EventSpec], str, str], conn_notify_back: Connection
    ):
        """Feeds an incoming message.

        The method does not return value, but the completion is notified via specified
        pipe Connection conn_notify_back.

        Args:
            flow_id (str): Data flow ID
            message (Tuple[List[event], str, str]): Tuple of input records, in-message user name,
                                                   and password
            conn_notify_back (Connection): One end of a pipe connected to the importer used to
                acknowledge the delivery. The value may be None. In case of None, the method
                just delivers the message without sending back the acknowledgement.
        """


class PipeDataFeeder(DataFeeder):
    """Deli data feeder based on pipes.
    This class is useful when the source importer and deliverer stay on different subprocesses.
    """

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self._pipe_sink = None
        self._pipe_source = None
        self.logger.info("Feeder type: PipeDataFeeder")

    def setup(self):
        """Sets up the data pipe."""
        self._pipe_sink, self._pipe_source = Pipe()

    def start(self):
        """Starts the feeder."""
        for channel in self._delivery_channels.values():
            channel.start()
        while True:
            try:
                data = self._pipe_sink.recv()
                if data is None:
                    break
                flow_id, message, conn_notify_back = data
                deliverer = self._deliverers[flow_id]
                deliverer.deliver(*message)
                result = ErrorCode.OK
            except ServiceError as service_error:
                self.logger.info("Requested data = %s", message)
                self.logger.warning("Delivery error received: %s", service_error, exc_info=True)
                # we borrow BIOS SDK error codes for error handling
                result = service_error.error_code
            except Exception:  # pylint: disable=broad-except
                self.logger.error("Exception received during delivery", exc_info=True)
                result = ServiceError(
                    ErrorCode.GENERIC_CLIENT_ERROR, "Unexpected error happened during delivery"
                )
            if conn_notify_back:
                conn_notify_back.send(result)
        for channel_id in self._delivery_channels:
            channel = self._delivery_channels[channel_id]
            channel.stop()
            self._pipe_sink.send(channel)

    def stop(self):
        """Stop the sink asynchronously. Use stop_sync to wait for the stop completion."""
        if self._pipe_source:
            self._pipe_source.send(None)

    def shutdown(self):
        """Shut down the feeder"""
        if self._pipe_sink:
            self._pipe_sink.close()
            self._pipe_source.close()
        for channel in self._delivery_channels.values():
            channel.shutdown()

    def feed(
        self, flow_id: str, message: Tuple[List[EventSpec], str, str], conn_notify_back: Connection
    ):
        """Feeds an incoming message.

        The method does not return value, but the completion is notified via specified
        pipe Connection conn_notify_back.

        Args:
            flow_id (str): Data flow ID
            message (Tuple[List[Event], str, str]): Tuple of input records, in-message user name,
                                                   and password
            conn_notify_back (Connection): One end of a pipe connected to the importer used to
                acknowledge the delivery. The value may be None. In case of None, the method
                just delivers the message without sending back the acknowledgement.
        """
        assert isinstance(flow_id, str)
        assert isinstance(message, tuple)
        if conn_notify_back is not None:
            assert isinstance(conn_notify_back, Connection)
        self._pipe_source.send((flow_id, message, conn_notify_back))

    def peek_returned_message(self):
        """Peeks returned message from thedeliverer. Used for testing."""
        hasany = self._pipe_source.poll()
        return self._pipe_source.recv() if hasany else None


class DirectDataFeeder(DataFeeder):
    """Deli data feeder that delivers records directly."""

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.info("Feeder type: DirectDataFeeder")

    def setup(self):
        """Sets up the data pipe."""

    def start(self):
        """Starts the feeder."""
        for channel in self._delivery_channels.values():
            channel.start()

    def stop(self):
        """Stop the feeder"""
        for channel_id in self._delivery_channels:
            channel = self._delivery_channels[channel_id]
            channel.stop()

    def shutdown(self):
        """Shut down the feeder"""
        for channel in self._delivery_channels.values():
            channel.shutdown()

    def feed(
        self, flow_id: str, message: Tuple[List[EventSpec], str, str], conn_notify_back: Connection
    ):
        """Feeds an incoming message.

        The method does not return value, but the completion is notified via specified
        pipe Connection conn_notify_back.

        Args:
            flow_id (str): Data flow ID
            message (Tuple[List[Event], str, str]): Tuple of input records, in-message user name,
                                                   and password
            conn_notify_back (Connection): One end of a pipe connected to the importer used to
                acknowledge the delivery. The value may be None. In case of None, the method
                just delivers the message without sending back the acknowledgement.
        """
        assert isinstance(flow_id, str)
        assert isinstance(message, tuple)
        if conn_notify_back is not None:
            assert isinstance(conn_notify_back, Connection)
        try:
            deliverer = self._deliverers[flow_id]
            deliverer.deliver(*message)
            result = ErrorCode.OK
        except ServiceError as service_error:
            self.logger.info("Requested data = %s", message)
            self.logger.warning("Delivery error received: %s", service_error, exc_info=True)
            # we borrow BIOS SDK error codes for error handling
            result = service_error.error_code
        except Exception:  # pylint: disable=broad-except
            self.logger.warning("Exception received during delivery", exc_info=True)
            result = ErrorCode.GENERIC_CLIENT_ERROR
        if conn_notify_back:
            conn_notify_back.send(result)
        return result
