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
"""Module bios_delivery_channel: DeliveryChannel and Deliverer for BIOS server"""

import logging
import time
from abc import abstractmethod
from configparser import ConfigParser
from typing import Any, Dict, List, Tuple

import bios
from bios import ErrorCode, ServiceError

from ..configuration import (
    AuthType,
    BiosStreamType,
    Configuration,
    DataFlowConfiguration,
    DestinationConfiguration,
    DestinationType,
    Keyword,
)
from ..event_spec import EventSpec
from .delivery_channel import Deliverer, DeliveryChannel


class CsvEncoder:
    """Class that encodes an input record into a CSV string"""

    @classmethod
    def encode(
        cls,
        attributes: Dict[str, Any],
        record: dict,
        logger: logging = None,
        flow_id: str = None,
        flow_name: str = None,
        stream_type: str = None,
        stream_name: str = None,
    ) -> str:
        assert isinstance(attributes, dict)
        assert isinstance(record, dict)

        values = []
        record_attributes = set(record)
        for attribute_name, default_value in attributes.items():
            value = record.get(attribute_name)
            if value is None:
                value = default_value
            values.append(cls._encode_csv_field(value))
            if attribute_name in record_attributes:
                record_attributes.remove(attribute_name)

        text = ",".join(values)

        if len(record_attributes) > 0:
            the_logger = logger or logging
            the_logger.warning(
                "Processed record has extra attribute(s) that are not in the target signal/context,"
                " Check dataPickupSpec configuration; flow=%s (%s), extra_attributes=%s, %s=%s",
                flow_id or "??",
                flow_name or "??",
                list(record_attributes),
                stream_type or "signal/context?",
                stream_name or "??",
            )

        return text

    @classmethod
    def _encode_csv_field(cls, value: any) -> str:
        if value is None:
            return ""
        str_value = str(value) if not isinstance(value, bool) else str(value).lower()
        encoded = "".join([element if element != '"' else '""' for element in str_value])
        return (
            f'"{encoded}"'
            if any([char in {'"', ",", "\r", "\n"} for char in encoded]) or not encoded.strip()
            else encoded
        )


class SessionCacheItem:
    """Session cache."""

    def __init__(self, username, password, session):
        assert isinstance(username, str)
        assert isinstance(password, str)
        assert session is not None
        self.username = username
        self.password = password
        self.session = session

    def close(self):
        """Closes the session in the cache."""
        if self.session:
            self.session.close()


def execute_bios(func):
    """Decorator to execute bios"""

    def execute(*args, **kwargs):
        self = args[0]
        username = args[2]

        max_trials = 3
        trials_credit = max_trials
        while trials_credit > 0:
            trials_credit -= 1
            try:
                if trials_credit < max_trials - 1:
                    self.logger.info("retrying...")
                func(*args, **kwargs)
                break
            except ServiceError as error:
                error_code = error.error_code
                what = None
                if error_code == ErrorCode.SESSION_EXPIRED:
                    what = "Session expired"
                    self._channel.delete_session(username)
                elif error_code == ErrorCode.OPERATION_CANCELLED:
                    what = "Operation cancelled"
                    time.sleep(1)
                elif error_code == ErrorCode.SCHEMA_MISMATCHED:
                    what = "Schema mismatched, the schema would be reloaded"
                    self.clear_schema()

                if what:
                    self.logger.warning("%s, retrying to deliver", what)
                else:
                    # rethrow
                    raise

    return execute


class BiosDeliverer(Deliverer):
    """Delivery handler for a BIOS signal.

    Args:
        stream_name (str): stream name
        channel (BiosDeliveryChannel): BIOS delivery channel that maintains the connection to the
            BIOS server
        configuration (Configuration): Deli configuration object
        flow_config (DataFlowConfiguration): Deli data flow configuration to be used for this
            deliverer
    """

    def __init__(
        self,
        stream_name: str,
        channel: "BiosDeliveryChannel",
        configuration: Configuration,
        flow_config: DataFlowConfiguration,
        record_dump_enabled: bool,
    ):
        assert isinstance(stream_name, str)
        assert isinstance(channel, BiosDeliveryChannel)
        assert isinstance(configuration, Configuration)
        assert isinstance(flow_config, DataFlowConfiguration)
        assert isinstance(record_dump_enabled, bool)

        self.logger = logging.getLogger(type(self).__name__)
        self._channel = channel

        self._stream_name = stream_name
        self._attributes = None
        self._primary_key = None  # valid only for contexts
        self._flow_config = flow_config
        src_spec = flow_config.get_source_spec()
        auth = configuration.get_source_config(src_spec.get_source_id()).get(
            Keyword.AUTHENTICATION
        )
        self._credentials_from_source = False
        if auth and auth.get(Keyword.TYPE):
            auth_type = auth.get(Keyword.TYPE).upper()
            if (
                auth_type == "INMESSAGE"
                or auth_type == "HTTPAUTHORIZATIONHEADER"
                or auth_type == "HTTPHEADERSPLAIN"
            ):
                self._credentials_from_source = True
        self._auth = auth
        self._session_cache = {}
        self._record_dump_enabled = record_dump_enabled

    @abstractmethod
    def get_type(self) -> str:
        """Returns type of the target (signal/context)"""

    @abstractmethod
    def get_stream_info(self, session) -> Tuple[str, Dict[str, Any]]:
        """Fetches stream information.

        Args:
            - session: BIOS session

        Returns: Tuple[str, Dict[str, Any]]: Tuple of stream name and map of attribute name to
            default value
        """

    @abstractmethod
    def build_statement(self, csv_texts: List[str]) -> "ISqlStatement":
        """Builds statement to insert/upsert data into the target stream.

        Args:
            - events (List[Event]): delivering records as list of strings

        Returns: ISqlStatement: Built statement
        """

    @abstractmethod
    def build_delete_statement(self, keys: List[List[Any]]) -> "ISqlStatement":
        """Builds statement to delete data into the target stream

        Args:
            - keys: List of primary keys

        Returns: ISqlStatement: Built statement
        """

    def clear_schema(self):
        """Clears current signal/context schema.

        The method is meant to be used for schema mismatch error recovery"""
        self._attributes = None
        self._primary_key = None

    def deliver(self, events: List[EventSpec], in_message_user: str, in_message_password: str):
        assert isinstance(events, list)

        if len(events) == 0:
            self.logger.warning(
                'No records were generated from the request; flow="%s", %s=%s',
                self._flow_config.get_name(),
                self.get_type(),
                self._stream_name,
            )
            return

        if self._credentials_from_source:
            username, password = (in_message_user, in_message_password)
        else:
            username, password = self._channel.get_default_user_pass()

        # Sort the incoming records into bins for each operation
        records_to_insert = []
        records_to_delete = []
        delegate_for = None
        for event in events:
            delegate_for = (event.attributes or {}).pop(Keyword.FOR_TENANT, None)
            if event.operation == EventSpec.Operation.DELETE:
                records_to_delete.append(event.attributes)
            else:
                records_to_insert.append(event.attributes)

        if records_to_insert:
            self._insert(records_to_insert, username, password, delegate_for)

        if records_to_delete:
            self._delete(records_to_delete, username, password, delegate_for)

    @execute_bios
    def _insert(self, records, username, password, delegate_for):
        session = self._channel.get_session(username, password)

        if self._attributes is None:
            self._setup_stream_config(session)

        csv_texts = [
            CsvEncoder.encode(
                self._attributes,
                record,
                logger=self.logger,
                flow_id=self._flow_config.get_id(),
                flow_name=self._flow_config.get_name(),
                stream_type=self.get_type(),
                stream_name=self._stream_name,
            )
            for record in records
        ]

        statement = self.build_statement(csv_texts)

        time0 = time.time()
        if delegate_for is not None:
            if delegate_for == "":
                raise ServiceError(ErrorCode.BAD_INPUT, "Target tenant not specified")
            try:
                session.for_tenant(delegate_for).execute(statement)
                delegate_message = f" for tenant/domain {delegate_for}"
            except ServiceError as err:
                if err.error_code == ErrorCode.NO_SUCH_TENANT:
                    raise ServiceError(ErrorCode.FORBIDDEN, "Invalid tenant") from err
                raise
        else:
            session.execute(statement)
            delegate_message = ""
        elapsed_time = int((time.time() - time0) * 1000)
        if self._record_dump_enabled:
            self.logger.info(
                'inserted%s; flow="%s", %s=%s, elapsed_ms=%d, records[%d]=%s',
                delegate_message,
                self._flow_config.get_name(),
                self.get_type(),
                self._stream_name,
                elapsed_time,
                len(csv_texts),
                csv_texts,
            )
        else:
            self.logger.info(
                'inserted%s; flow="%s", %s=%s, elapsed_ms=%d, num_records=%d',
                delegate_message,
                self._flow_config.get_name(),
                self.get_type(),
                self._stream_name,
                elapsed_time,
                len(csv_texts),
            )

    @execute_bios
    def _delete(self, records, username, password, delegate_for):
        session = self._channel.get_session(username, password)

        if self._attributes is None:
            self._setup_stream_config(session)

        # Build keys for entries to delete
        keys = []
        for record in records:
            key = [record.get(key_name) for key_name in self._primary_key]
            is_key_usable = True
            for key_element in key:
                if key_element is None or (isinstance(key_element, str) and len(key_element) == 0):
                    # the entry wouldn't be identified, skip the record
                    is_key_usable = False
                    break
            if is_key_usable:
                keys.append(key)

        if not keys:
            return

        time0 = time.time()
        if delegate_for is not None:
            if delegate_for == "":
                raise ServiceError(ErrorCode.BAD_INPUT, "Target tenant not specified")
            this_session = session.for_tenant(delegate_for)
            delegate_message = f" for tenant/domain {delegate_for}"
        else:
            this_session = session
            delegate_message = ""
        try:
            deleted_keys = self._execute_deletion(this_session, keys)
        except ServiceError as err:
            if err.error_code == ErrorCode.NO_SUCH_TENANT:
                raise ServiceError(ErrorCode.FORBIDDEN, "Invalid tenant") from err
            raise
        elapsed_time = int((time.time() - time0) * 1000)
        already_deleted = len(keys) - len(deleted_keys)
        if self._record_dump_enabled:
            self.logger.info(
                'deleted%s; flow="%s", %s=%s, elapsed_ms=%d, num_records=%d, already_deleted=%d,'
                " keys=%s",
                delegate_message,
                self._flow_config.get_name(),
                self.get_type(),
                self._stream_name,
                elapsed_time,
                len(deleted_keys),
                already_deleted,
                deleted_keys,
            )
        else:
            self.logger.info(
                'deleted; flow="%s", %s=%s, elapsed_ms=%d, num_records=%d, already_deleted=%d',
                self._flow_config.get_name(),
                self.get_type(),
                self._stream_name,
                elapsed_time,
                len(deleted_keys),
                already_deleted,
            )

    def _execute_deletion(self, session, keys: List[List[Any]]):
        statement = self.build_delete_statement(keys)
        try:
            session.execute(statement)
            return keys
        except ServiceError as err:
            if err.error_code != ErrorCode.NOT_FOUND:
                raise
            if len(keys) > 1:
                # Split keys and retry
                deleted_keys = []
                for key in keys:
                    deleted_keys.extend(self._execute_deletion(session, [key]))
                return deleted_keys
            else:
                # The key cannot be split, giving up silently
                return []

    def _encode(self, value: str) -> str:
        if value is None:
            return ""
        return '"' + value.replace('"', '""') + '"'

    def _setup_stream_config(self, session):
        self._stream_name, self._attributes, self._primary_key = self.get_stream_info(session)


class SignalDeliverer(BiosDeliverer):
    """Bios Signal Deliverer."""

    def get_type(self):
        return "signal"

    def get_stream_info(self, session):
        signal_config = session.get_signal(self._stream_name)
        signal_name = signal_config[Keyword.SIGNAL_NAME]
        attributes = {
            attribute[Keyword.ATTRIBUTE_NAME].lower(): attribute.get(Keyword.DEFAULT)
            for attribute in signal_config.get(Keyword.ATTRIBUTES)
        }
        return signal_name, attributes, None

    def build_statement(self, csv_texts):
        statement = bios.isql().insert().into(self._stream_name)
        if len(csv_texts) > 1:
            statement = statement.csv_bulk(csv_texts).build()
        else:
            statement = statement.csv(csv_texts[0]).build()
        return statement

    def build_delete_statement(self, keys: List[List[Any]]) -> "ISqlStatement":
        return None


class ContextDeliverer(BiosDeliverer):
    """Bios Context Deliverer."""

    def get_type(self):
        return "context"

    def get_stream_info(self, session):
        context_config = session.get_context(self._stream_name)
        context_name = context_config[Keyword.CONTEXT_NAME]
        attribute_names = {
            attribute[Keyword.ATTRIBUTE_NAME].lower(): attribute.get(Keyword.DEFAULT)
            for attribute in context_config.get(Keyword.ATTRIBUTES)
        }
        primary_key = [key.lower() for key in context_config.get(Keyword.PRIMARY_KEY)]
        return context_name, attribute_names, primary_key

    def build_statement(self, csv_texts):
        statement = bios.isql().upsert().into(self._stream_name)
        if len(csv_texts) > 1:
            statement = statement.csv_bulk(csv_texts).build()
        else:
            statement = statement.csv(csv_texts[0]).build()
        return statement

    def build_delete_statement(self, keys: List[List[Any]]) -> "ISqlStatement":
        return bios.isql().delete().from_context(self._stream_name).where(keys=keys).build()


class BiosDeliveryChannel(DeliveryChannel):
    """Delivery channel for BIOS server"""

    def __init__(self, destination_config: DestinationConfiguration, system_config: ConfigParser):
        super().__init__()
        assert isinstance(destination_config, DestinationConfiguration)
        assert isinstance(system_config, ConfigParser)
        self.system_config = system_config
        self._bios_client = None
        self._destination_config = destination_config
        self._endpoint = destination_config.get_endpoint()
        self._app_name = system_config.get(Keyword.COMMON, "appName", fallback=None)
        self._app_type = system_config.get(Keyword.COMMON, "appType", fallback=None)
        auth = destination_config.get_authentication()
        if auth:
            if auth.auth_type is not AuthType.Login:
                raise Exception(
                    "Bios Forwarder supports only Login type of authentication;"
                    + f" conf={destination_config}"
                )
            self._user_name = auth.config.get("user")
            self._password = auth.config.get("password")
        else:
            self._user_name = None
            self._password = None
        self._session_cache = {}
        self._record_dump_enabled = (
            system_config.get(Keyword.BIOS, Keyword.RECORD_DUMP_ENABLED, fallback="false")
            == "true"
        )

    def setup(self):
        threads_enabled = self.system_config.get(Keyword.BIOS, Keyword.THREADS, fallback="false")
        if threads_enabled == "true":
            bios.enable_threads()

    def make_deliverer(
        self, configuration: Configuration, flow_config: DataFlowConfiguration
    ) -> Deliverer:
        assert isinstance(configuration, Configuration)
        assert isinstance(flow_config, DataFlowConfiguration)
        dst_spec = flow_config.get_destination_spec()
        stream_type = dst_spec.get_stream_type()
        if stream_type is BiosStreamType.SIGNAL:
            deliverer = SignalDeliverer
        elif stream_type is BiosStreamType.CONTEXT:
            deliverer = ContextDeliverer
        else:
            raise Exception(f"BiosDeliveryChannel does not support BIOS stream type {stream_type}")
        return deliverer(
            dst_spec.get_stream_name(),
            self,
            configuration,
            flow_config,
            self._record_dump_enabled,
        )

    def start(self):
        pass

    def stop(self):
        if self._bios_client:
            self._bios_client.close()
            self._bios_client = None

    def shutdown(self):
        self.stop()

    def get_bios_client(self):
        """Returns bios client."""
        return self._bios_client

    def get_default_user_pass(self) -> Tuple[str, str]:
        """Returns default user name and password."""
        return self._user_name, self._password

    def get_endpoint(self) -> str:
        """Returns BIOS endpoint."""
        return self._endpoint

    def get_session(self, username, password) -> "BiosClient":
        """Gets a session for specified user and password.

        Args:
            - username (str): User name
            - password (str): Password
        Returns: BiosClient: Session fo the user and the password
        """
        assert isinstance(username, str)
        assert isinstance(password, str)
        item = self._session_cache.get(username)
        if item:
            if item.session.is_active and item.password == password:
                return item.session
            item.session.close()
        session = bios.login(
            self._endpoint,
            username,
            password,
            app_name=self._app_name,
            app_type=self._app_type,
        )
        self._session_cache[username] = SessionCacheItem(username, password, session)
        return session

    def delete_session(self, username):
        """Deletes the session for specified username."""
        if username in self._session_cache:
            self._session_cache[username].close()
            del self._session_cache[username]


# Register the forwarder to the factory method in the base class
DeliveryChannel.CHANNEL_BUILDERS[DestinationType.BIOS] = BiosDeliveryChannel
