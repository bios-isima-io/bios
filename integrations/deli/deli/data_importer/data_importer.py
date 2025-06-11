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
"""data_importer: Module that defines the base classes of importers"""

import copy
import csv
import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from multiprocessing import Pipe
from typing import Any, List, Tuple

from bios import ErrorCode, ServiceError

from ..configuration import (
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)
from ..data_pipe import DataPipe
from ..data_processor import DataProcessor, ImportedModules


def publishing(func):
    """Decorator to handle exceptions thrown while publishing imported data"""

    def execute(*args, **kwargs):
        # assuming this is used by an instance method
        self = args[0]
        try:
            return func(*args, **kwargs)
        except ServiceError as service_error:
            resp = service_error.error_code
            logging_function = self.logger.error
            if resp in {ErrorCode.FORBIDDEN, ErrorCode.UNAUTHORIZED}:
                logging_function = self.logger.warning
            logging_function("Failed to handle incoming data: %s", service_error, exc_info=True)
        except Exception as error:  # pylint: disable=broad-except
            self.logger.error("Failed to handle incoming data: %s", error, exc_info=True)
            resp = ErrorCode.GENERIC_CLIENT_ERROR
        return self.RESPONSE_CODE_TABLE.get(resp) or 500

    return execute


class SourceFilter(ABC):
    """Abstract class that is used for filtering source messages."""

    @classmethod
    def get_all_pass_filter(cls):
        """Provides all-pass filter."""
        return cls.ALL_PASS_FILTER

    @abstractmethod
    def apply(self, data: Any) -> bool:
        """Applies the data filter for a pipe.

        Args:
            data (Any): Input data of type defined by the data source

        Returns: bool: True if the data should be forwarded to the pipe
        """


class AllPassFilter(SourceFilter):
    """All pass filter."""

    def apply(self, data: Any) -> bool:
        return True


SourceFilter.ALL_PASS_FILTER = AllPassFilter()


class SourceTranslator(ABC):
    """Abstract class of source message translator.

    This class is instantiated for each import data pipe and takes care of converting incoming
    data to messages. A message consists of a dict of key value pairs.
    """

    @classmethod
    def get_transparent_translator(cls):
        """Returns transparent translator."""
        return cls.TRANSPARENT_TRANSLATOR

    @abstractmethod
    def translate(self, data: Any) -> dict:
        """Converts a source data to input message

        Args:
            data (Any): Input data of type defined by the data source

        Returns: Tuple[Any, bool]: Tuple of message and flag that indicates whether the data is bulk
        """


class CsvTranslator(SourceTranslator):
    """Translator for CSV input source.

    Args:
        source_data_spec (SourceDataSpec): Source data specification
    """

    def __init__(self, source_data_spec: SourceDataSpec):
        assert isinstance(source_data_spec, SourceDataSpec)
        self.header_present = source_data_spec.get_or_default(Keyword.HEADER_PRESENT, False)
        if self.header_present:
            self._translate = self._translate_with_header
        else:
            self._translate = self._translate_without_header

    def translate(self, data: Any) -> Tuple[dict, bool]:
        return self._translate(data)

    def _translate_without_header(self, data):
        messages = []
        rows = csv.reader(data)
        for row in rows:
            entry = {}
            for i_row, attribute in enumerate(row):
                entry[str(i_row)] = attribute
            messages.append(entry)
        return messages, True

    def _translate_with_header(self, data):
        messages = [row for row in csv.DictReader(data)]
        return messages, True


class TransparentTranslator(SourceTranslator):
    """Transparent translator."""

    def translate(self, data: dict) -> dict:
        assert isinstance(data, dict)
        return copy.deepcopy(data), False


SourceTranslator.TRANSPARENT_TRANSLATOR = TransparentTranslator()


class EndpointHandler(ABC):
    """Abstract class that takes care of import source endpoint.

    The class has responsibility of feeding incoming messages to the DataFeeder.
    How to deploy handlers depends on the type of Importer.
    """

    RESPONSE_CODE_TABLE = {
        ErrorCode.OK: 200,
        ErrorCode.BAD_INPUT: 400,
        ErrorCode.UNAUTHORIZED: 401,
        ErrorCode.FORBIDDEN: 403,
        ErrorCode.SERVER_CHANNEL_ERROR: 503,
        ErrorCode.SERVER_CONNECTION_FAILURE: 503,
        ErrorCode.BAD_GATEWAY: 503,
        ErrorCode.SERVICE_UNAVAILABLE: 503,
        ErrorCode.SERVICE_UNDEPLOYED: 503,
    }

    def __init__(self):
        self.logger = logging.getLogger(type(self).__name__)
        # List of tuples of DataPipe, corresponding SourceFilter, and SourceTranslator
        self.registered_pipes = []

    def register(self, data_pipe: DataPipe, imported_modules: ImportedModules = None):
        """Registers a data pipe.

        Args:
            data_pipe (DataPipe): Data pipe to register
        """
        assert isinstance(data_pipe, DataPipe)
        source_data_spec = data_pipe.get_source_data_spec()
        translator = self.create_source_translator(source_data_spec)
        flow_config = data_pipe.get_flow_config()
        data_processor = DataProcessor(
            flow_config.get_name(), flow_config.get_pickup_spec(), imported_modules
        )
        source_config = data_pipe.get_source_config()
        auth = source_config.get(Keyword.AUTHENTICATION)
        if auth and auth.get(Keyword.TYPE) and auth.get(Keyword.TYPE).upper() == "INMESSAGE":
            user_attr = auth.get(Keyword.IN_MESSAGE_USER_ATTRIBUTE)
            password_attr = auth.get(Keyword.IN_MESSAGE_PASSWORD_ATTRIBUTE)
            if not user_attr or not password_attr:
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    f"Flow={flow_config.get_name()}:"
                    + " user and password must be set for InMessage source authentication",
                )
            data_processor.set_in_message_auth_attributes(user_attr, password_attr)
        if (
            auth
            and auth.get(Keyword.TYPE)
            and auth.get(Keyword.TYPE).upper() == "HTTPHEADERSPLAIN"
        ):
            user_attr = auth.get(Keyword.USER_HEADER)
            password_attr = auth.get(Keyword.PASSWORD_HEADER)
            if not user_attr or not password_attr:
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    f"Flow={flow_config.get_name()}:"
                    + " user and password must be set for HttpHeadersPlain source authentication",
                )
            data_processor.set_in_http_headers_plain_auth_attributes(user_attr, password_attr)

        self.registered_pipes.append((data_pipe, translator, data_processor))

    def create_source_translator(self, source_data_spec: SourceDataSpec) -> SourceTranslator:
        """Creates a translator for the registering pipe

        Args:
            source_data_spec (dict): Source data specification of the pipe

        Returns: SourceTranslator: Created translator
        """
        assert isinstance(source_data_spec, SourceDataSpec)
        source_type = source_data_spec.get(Keyword.PAYLOAD_TYPE) if source_data_spec else None
        if not source_type or source_type.upper() == "JSON":
            return SourceTranslator.get_transparent_translator()
        elif source_type.upper() == "CSV":
            return CsvTranslator(source_data_spec)
        else:
            return None

    @publishing
    def publish(
        self, data: Any, user: str = None, password: str = None, attributes: dict = None
    ) -> int:
        """Method to publish an imported record.

        The handler should use this method to forward imported records to the later stages.

        Args:
            data (Any): The imported data. Type and format can be anything but the source
                filter and the translator translator should be able to understand it.
            user (str): User name from the source (optional, default=None)
            password (str): User password from the source (optional, default=None)
            attributes (dict): Additional attributes from source as a flat dictionary
                of key-value pairs. The keys must be in format of '${metadata_key_name}'.
        Returns: int: 200 - data delivery has been done successfully
                      202 - data has been accepted and would be delivered
                      400 - user input error
                      401 - unauthorized
                      403 - forbidden
                      503 - service error
                      500 - Deli internal error
        """
        return self._publish(data, user, password, attributes)

    @publishing
    def bulk_publish(
        self,
        in_records: List[Any],
        user: str = None,
        password: str = None,
        attributes: dict = None,
    ) -> int:
        """Method to publish an imported records. old publish couldn't be overloaded with List, since it affects
           behaviour of CSV handler

        Args:
            data (Any): List of Records of Any Type. Type and format can be anything but the source
                filter and the translator translator should be able to understand it.
            user (str): User name from the source (optional, default=None)
            password (str): User password from the source (optional, default=None)
            attributes (dict): Additional attributes from source as a flat dictionary
                of key-value pairs. The keys must be in format of '${metadata_key_name}'.
        Returns: int: 200 - data delivery has been done successfully
                      202 - data has been accepted and would be delivered
                      400 - user input error
                      401 - unauthorized
                      403 - forbidden
                      503 - service error
                      500 - Deli internal error
        """
        return self._bulk_publish(in_records, user, password, attributes)

    def _bulk_publish(self, in_records, user, password, attributes):
        if user:
            assert isinstance(user, str)
        if password:
            assert isinstance(password, str)
        if attributes:
            assert isinstance(attributes, dict)
        result = 200
        num_messages = 0
        for data_pipe, translator, data_processor in self.registered_pipes:
            # collect all messages per translator/pipe set
            out_messages = []
            for record in in_records:
                translated, is_bulk = translator.translate(record)
                if is_bulk:
                    processed = data_processor.process_bulk(
                        translated, user=user, password=password, attributes=attributes
                    )
                else:
                    processed = data_processor.process(
                        translated, user=user, password=password, attributes=attributes
                    )

                if processed is None:
                    continue
                out_messages.extend(processed[0])

            if len(out_messages) == 0:
                continue
            num_messages += len(out_messages)
            resp = data_pipe.push((out_messages, user, password), None)
            if data_pipe.get_flow_config().is_acknowledgement_enabled():
                result = self.RESPONSE_CODE_TABLE.get(resp) or 500
            else:
                result = 202
            if int(result / 100) != 2:
                break
        if num_messages == 0:
            self.logger.warning("No flows picked up the input message")
        return result

    def _publish(self, data, user, password, attributes):
        if user:
            assert isinstance(user, str)
        if password:
            assert isinstance(password, str)
        if attributes:
            assert isinstance(attributes, dict)
        result = 200
        num_messages = 0
        for data_pipe, translator, data_processor in self.registered_pipes:
            translated, is_bulk = translator.translate(data)
            if is_bulk:
                messages = data_processor.process_bulk(
                    translated, user=user, password=password, attributes=attributes
                )
            else:
                messages = data_processor.process(
                    translated, user=user, password=password, attributes=attributes
                )

            if messages is None:
                continue

            num_messages += len(messages)
            resp = data_pipe.push(messages, None)
            if data_pipe.get_flow_config().is_acknowledgement_enabled():
                result = self.RESPONSE_CODE_TABLE.get(resp) or 500
            else:
                result = 202
            if int(result / 100) != 2:
                break
        if num_messages == 0:
            self.logger.warning("No flows picked up the input message")
        return result

    @abstractmethod
    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        """Creates a source data filter for the registering pipe

        Args:
            source_data_spec (dict): Source data specification of the pipe

        Returns: SourceFilter: Created filter. Return None if filtering is not necessary.
        """

    @abstractmethod
    def setup(self):
        """Sets up the actions."""

    @abstractmethod
    def start(self):
        """Activates importing actions."""

    @abstractmethod
    def shutdown(self):
        """Stops importing actions and release resources."""

    def shutdown_await(self):
        """(optional) Waits for asynchronous shutdown completion"""


class DataImporter(ABC):
    """Importer for a import source.

    This class receives registrations of data pipes and deploys necessary components for importing.
    """

    IMPORTER_BUILDERS = {}
    IMPORTER_INITIALIZERS = {}

    @classmethod
    def build(
        cls,
        source_config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ) -> "DataImporter":
        """Class method to build a Importer instance from specified source configuration.

        Args:
            source_config (SourceConfiguration): Source configuration
            flow_config (DataFlowConfiguration): flow configuration
            system_config (ConfigParser): System global configuration
        """
        source_type = source_config.get_type()
        builder = cls.IMPORTER_BUILDERS.get(source_type)
        if not builder:
            raise Exception(f"Unsupported source type: {source_type}")
        return builder(source_config, flow_config, system_config)

    @classmethod
    def global_init(cls, source_type: SourceType, system_config: ConfigParser):
        assert isinstance(source_type, SourceType)
        assert isinstance(system_config, ConfigParser)
        initializer = cls.IMPORTER_INITIALIZERS.get(source_type)
        if initializer:
            initializer(system_config)

    def __init__(self, source_configuration: SourceConfiguration):
        self.source_configuration = source_configuration
        self.importer_name = source_configuration.get_name()
        # Map of handler identifier and handler
        self.endpoint_handlers = {}

    def get_importer_name(self):
        """Returns the name of the importer."""
        return self.importer_name

    def register(self, data_pipe: DataPipe, imported_modules: ImportedModules = None):
        """Registers a data pipe.

        This method finds or creates an endpoint handler for the specified data pipe and deploys it.

        Args:
            data_pipe (DataPipe): The data pipe to register
        """
        assert isinstance(data_pipe, DataPipe)
        if imported_modules:
            assert isinstance(imported_modules, ImportedModules)
        handler_id = self.generate_handler_id(data_pipe.get_source_data_spec())
        if handler_id is None:
            return
        handler = self.endpoint_handlers.get(handler_id)
        if not handler:
            handler = self.create_handler(handler_id, data_pipe.get_source_data_spec())
            self.endpoint_handlers[handler_id] = handler

        handler.register(data_pipe, imported_modules)

    def setup(self):
        """Sets up the importing actions."""

        for endpoint_handler in self.endpoint_handlers.values():
            endpoint_handler.setup()
        self.setup_importer()

    def start(self):
        """Activates the importing actions."""
        for endpoint_handler in self.endpoint_handlers.values():
            endpoint_handler.start()
        self.start_importer()

    def poll_timeout(self) -> int:
        """return the timeout specific to importer"""
        return None

    def poll_and_return(self) -> int:
        """runs the importer and return the to sleep"""
        self.start_importer()
        return None

    def shutdown(self):
        """Deactivates the importing actions and releases resources."""
        self.shutdown_importer()
        for endpoint_handler in self.endpoint_handlers.values():
            endpoint_handler.shutdown()
        for endpoint_handler in self.endpoint_handlers.values():
            endpoint_handler.shutdown_await()

    def setup_importer(self):
        """Sets up the importer"""

    @abstractmethod
    def start_importer(self):
        """Starts the importer."""

    @abstractmethod
    def shutdown_importer(self):
        """Shuts down the importer."""

    @abstractmethod
    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> Any:
        """Generates an endpoint handler ID for a pipe.

        This method must generate an identifier that identifies its data entry point.
        A webhook data entry point, for example, is identified by webhook path in the
        source configuration and webhook subpath in the source data spec
        (path + subpath makes the webhook's listening path). Multiple source data
        specs in different data flow specs may generate the same handler_id. Such
        flows would share an endpoint handler.

        Relationships among ImportSource, EndpointHandler, and DataPipe are::

            ImportSource     "1" -> "1..m" EndpointHandler "1" -> "1..n" DataPipe
            (ImportSourceConfig)    (DataSourceSpec)              (ImportFlowSpec)
                                    same specs are deduped

        Args:
            - source_data_spec (SourceDataSpec): Source data specification
        Returns: Any: Generated endpoint handler ID.
        """

    @abstractmethod
    def create_handler(self, handler_id: Any, source_data_spec: SourceDataSpec) -> EndpointHandler:
        """Creates an endpoint handler.

        Args:
            - handler_id (Any): Endpoint handler ID.
            - source_data_spec (SourceDataSpec): Source data specification for the handler

        Return: EndpointHandler: Created endpoint handler
        """
