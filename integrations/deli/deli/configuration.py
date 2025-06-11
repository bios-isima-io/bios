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
import base64
import logging
import urllib.parse
from configparser import ConfigParser
from enum import Enum, auto
from typing import List, Union


class Keyword:
    """Configuration property names"""

    ACCESS_KEY = "accessKey"
    ACCESS_TOKEN = "accessToken"
    ACKNOWLEDGEMENT_ENABLED = "acknowledgementEnabled"
    API_VERSION = "apiVersion"
    AS = "as"
    ATTRIBUTE_SEARCH_PATH = "attributeSearchPath"
    ATTRIBUTE_MAPPING = "attributeMapping"
    ATTRIBUTE_NAME = "attributeName"
    ATTRIBUTES = "attributes"
    AUTHENTICATION = "authentication"
    BIOS = "Bios"
    BODY_PARAMS = "bodyParams"
    BOOTSTRAP_SERVERS = "bootstrapServers"
    CDC_OPERATION_TYPES = "cdcOperationTypes"
    CHECKPOINTING_ENABLED = "checkpointingEnabled"
    CLIENT_ID = "clientId"
    CLIENT_SECRET = "clientSecret"
    CLIENT_TYPE = "clientType"
    CODE = "code"
    COLUMN_NAME = "columnName"
    COMMON = "Common"
    COMMON_KEYS = "commonKeys"
    CONFIG_SECTION = "PullConfigLoader"
    CONDITION = "condition"
    CONTEXT_NAME = "contextName"
    CSV = "csv"
    CUSTOMER_ID = "customerID"
    DATABASE_HOST = "databaseHost"
    DATABASE_PORT = "databasePort"
    DATABASE_NAME = "databaseName"
    DATABASE_TABLE = "tableName"
    DATA_PICKUP_SPEC = "dataPickupSpec"
    DEFAULT = "default"
    TENANT_FORWARDING = "tenantForwarding"
    DELETION_SPEC = "deletionSpec"
    DESTINATION_DATA_SPEC = "destinationDataSpec"
    DEVELOPER_TOKEN = "developerToken"
    DIGEST_BASE64_ENCODED = "digestBase64Encoded"
    ENDPOINT = "endpoint"
    FILE_LOCATION = "fileLocation"
    FILE_NAME_PREFIX = "fileNamePrefix"
    FILTER = "filter"
    FILTERS = "filters"
    FOR_TENANT = "__for_tenant"
    GROUP_ID = "groupId"
    GRANT_TYPE = "grantType"
    HEADERS = "headers"
    HEADER_PRESENT = "headerPresent"
    IMPORT_DATA_MAPPINGS = "importDataMappings"
    IMPORT_DATA_PROCESSORS = "importDataProcessors"
    IMPORT_DESTINATION_ID = "importDestinationId"
    IMPORT_DESTINATION_NAME = "importDestinationName"
    IMPORT_FLOW_NAME = "importFlowName"
    IMPORT_FLOW_ID = "importFlowId"
    IMPORT_FLOW_SPECS = "importFlowSpecs"
    IMPORT_DESTINATIONS = "importDestinations"
    IMPORT_SOURCE_ID = "importSourceId"
    IMPORT_SOURCE_NAME = "importSourceName"
    IMPORT_SOURCES = "importSources"
    IN_MESSAGE_USER_ATTRIBUTE = "inMessageUserAttribute"
    IN_MESSAGE_PASSWORD_ATTRIBUTE = "inMessagePasswordAttribute"
    HMAC_HEADER = "hmacHeader"
    SHARED_SECRET = "sharedSecret"
    USER_HEADER = "userHeader"
    PASSWORD_HEADER = "passwordHeader"
    JSON = "json"
    KW_ARGS = "kwArgs"
    MAPPER_NAME = "mapperName"
    METHOD = "method"
    NAME = "name"
    PASSWORD = "password"
    PASSWORD_HEADER = "passwordHeader"
    PAYLOAD_TYPE = "payloadType"
    PAYLOAD_VALIDATION = "payloadValidation"
    PLAIN = "PLAIN"
    POLLING_INTERVAL = "pollingInterval"
    PRIMARY_KEY = "primaryKey"
    PROCESSES = "processes"
    PROCESSOR_NAME = "processorName"
    PROCESSORS = "processors"
    QUERY_PARAMS = "queryParams"
    RECORD_DUMP_ENABLED = "recordDumpEnabled"
    REFRESH_TOKEN = "refreshToken"
    RULE = "rule"
    S3_BUCKET = "s3Bucket"
    SASL_MECHANISM = "sasl_mechanism"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_PLAIN_USERNAME = "sasl_plain_username"
    SASL_PLAIN_PASSWORD = "sasl_plain_password"
    SECRET_KEY = "secretKey"
    SECURITY_PROTOCOL = "security_protocol"
    SIGNAL_NAME = "signalName"
    SOURCE_ATTRIBUTE_NAME = "sourceAttributeName"
    SOURCE_ATTRIBUTE_NAMES = "sourceAttributeNames"
    SOURCE_BATCH_SIZE = "sourceBatchSize"
    SOURCE_DATA_SPEC = "sourceDataSpec"
    SUB_PATH = "subPath"
    SSL_ENABLE = "sslEnable"
    TABLE_NAME = "tableName"
    TENANT_NAME = "tenantName"
    THREADS = "threads"
    TRANSFORMS = "transforms"
    TOPIC = "topic"
    TYPE = "type"
    USER = "user"
    USER_HEADER = "userHeader"
    VALUE_DESERIALIZER = "value_deserializer"
    WEBHOOK_FILTER = "webhookFilter"
    WEBHOOK_PATH = "webhookPath"
    WEBHOOK_SUB_PATH = "webhookSubPath"


# Enum types #################################################################3


class AuthType(Enum):
    """Enum that represent authentication types."""

    Login = auto()
    InMessage = auto()
    SaslPlaintext = auto()
    HttpAuthorizationHeader = auto()
    HttpHeadersPlain = auto()
    FACEBOOKAD = auto()


class PayloadValidationType(Enum):
    """Enum that represent payload  validation types."""

    HmacSignature = auto()


class BiosStreamType(Enum):
    """Enum that represent BIOS stream types."""

    SIGNAL = auto()
    CONTEXT = auto()


class SourceType(Enum):
    """Enum that represent Deli source types."""

    _TEST = auto()
    FILE = auto()
    FACEBOOKAD = auto()
    S3 = auto()
    KAFKA = auto()
    WEBHOOK = auto()
    MAXWELL = auto()
    HIBERNATE = auto()
    FILETAILER = auto()
    RESTCLIENT = auto()
    MYSQLPULL = auto()
    GOOGLEAD = auto()


class CdcSourceType(Enum):
    """Enum that represent Deli CDC source types."""

    MYSQL = auto()
    MONGODB = auto()
    POSTGRES = auto()
    ORACLE = auto()
    SQLSERVER = auto()


class DestinationType(Enum):
    """Enum that represent Deli destination types."""

    _TEST = auto()
    BIOS = auto()


class PayloadType(Enum):
    """Enum that represent import payload type"""

    JSON = auto()
    CSV = auto()


class ConfigUtils:
    """Configuration utilities class."""

    @staticmethod
    def get_property(config: dict, property_name: str) -> Union[str, list, int, bool, dict]:
        """Get a property from a dictionary. The method validates whether the value exists
           and raises an Exception when the property is missing.

        Args:
            - config (dict): The configuration dictionary
            - property_name (str): The name of the property to fetch
        Returns: any: Property value
        Raises: KeyError: Thrown to indicate that the property does not exist.
        """
        assert isinstance(config, dict)
        assert isinstance(property_name, str)
        value = config.get(property_name)
        if value is None:
            raise KeyError(f"No such property: {property_name}, config={config}")
        return value

    @staticmethod
    def find_enum(enum_class: type, name: str) -> Enum:
        """Finds an enum entry by case insensitive match.

        Args:
            enum_class (type): Enum class to find the entry
            name (str): Entry name

        Returns: Enum: Found enum entry.

        Raises: Exception: thrown to indicate that no matching entry was not found
        """
        assert isinstance(enum_class, type)
        assert isinstance(name, str)
        upper_name = name.upper()
        for entry in enum_class:
            if entry.name.upper() == upper_name:
                return entry
        raise Exception(f"No matching entry in {enum_class.__name__} for name {name}")


class AuthConfiguration:
    """Configuration for authentication config entry."""

    def __init__(self, config: dict):
        assert isinstance(config, dict)
        self.config = config
        self.auth_type = ConfigUtils.find_enum(
            AuthType, ConfigUtils.get_property(config, Keyword.TYPE)
        )
        if self.auth_type is AuthType.InMessage or self.auth_type is AuthType.Login:
            self.user_name = ConfigUtils.get_property(config, Keyword.USER)
            self.password = ConfigUtils.get_property(config, Keyword.PASSWORD)
        if (
            self.auth_type is AuthType.HttpHeadersPlain
            or self.auth_type is AuthType.HttpHeadersPlain
        ):
            self.user_name = ConfigUtils.get_property(config, Keyword.USER)
            self.password = ConfigUtils.get_property(config, Keyword.PASSWORD)
        else:
            self.user_name = None
            self.password = None


class PayloadValidationConfiguration:
    """Configuration for payload validation config entry."""

    def __init__(self, config: dict):
        assert isinstance(config, dict)
        self.config = config
        self.payload_validation_type = ConfigUtils.find_enum(
            PayloadValidationType, ConfigUtils.get_property(config, Keyword.TYPE)
        )
        if self.payload_validation_type is PayloadValidationType.HmacSignature:
            self.hmac_header = ConfigUtils.get_property(config, Keyword.HMAC_HEADER)
            self.shared_secret = ConfigUtils.get_property(config, Keyword.SHARED_SECRET)
            self.digest_base64_encoded = ConfigUtils.get_property(
                config, Keyword.DIGEST_BASE64_ENCODED
            )


class EndPointConfiguration:
    """Base class of Source and Destination configurations."""

    def __init__(self, config: dict, endpoint_type: Enum, endpoint_name: str):
        assert isinstance(config, dict)
        assert isinstance(endpoint_type, Enum)
        if endpoint_name:
            assert isinstance(endpoint_name, str)
        self.config = config
        self.endpoint_type = endpoint_type
        self.endpoint_name = endpoint_name

    def get(self, name: str):
        """Gets a configuration parameter.

        The method returns None when the specified property does not exist.

        Args:
            - name (str): Property name
        Returns: any: Configuration value or None
        """
        return self.config.get(name)

    def get_or_raise(self, name: str):
        """Gets a configuration parameter.

        The method raises an exception when the target property does not exist.
        """
        assert isinstance(name, str)
        return ConfigUtils.get_property(self.config, name)

    def get_type(self):
        """Returns endpoint (source or destination) type."""
        return self.endpoint_type

    def get_name(self):
        """Returns the configured name of the endpoint."""
        return self.endpoint_name or ""


class SourceConfiguration(EndPointConfiguration):
    """Import source endpoint configuration."""

    def __init__(self, source_type: SourceType, source_config: dict, is_cdc_sink: bool = False):
        super().__init__(
            source_config,
            source_type,
            source_config.get(Keyword.IMPORT_SOURCE_NAME),
        )
        self._is_cdc_sink = is_cdc_sink

    def get_polling_interval(self) -> int:
        """Returns source polling interval. The default value is 60."""
        return self.config.get(Keyword.POLLING_INTERVAL) or 60

    def get_method(self) -> str:
        """Returns source retrieval method. The default value is 'GET'."""
        return self.config.get(Keyword.METHOD) or "GET"

    @property
    def is_cdc_sink(self) -> bool:
        """Returns if the source is a sink of a CDC source proxy"""
        return self._is_cdc_sink


class DestinationConfiguration(EndPointConfiguration):
    """Import destination endpoint configuration."""

    def __init__(self, dest_config: dict):
        super().__init__(
            dest_config,
            DestinationType[dest_config.get(Keyword.TYPE).upper()],
            dest_config.get(Keyword.IMPORT_DESTINATION_NAME),
        )

    def get_endpoint(self):
        """Returns the destination endpoint."""
        return ConfigUtils.get_property(self.config, Keyword.ENDPOINT)

    def get_authentication(self) -> AuthConfiguration:
        """Returns authentication configuration for the destination."""
        auth_config = self.config.get(Keyword.AUTHENTICATION)
        return AuthConfiguration(auth_config) if auth_config else None


class SourceDataSpec:
    """Source data specification of a data flow config."""

    def __init__(self, source_spec: dict):
        assert isinstance(source_spec, dict)
        self.config = source_spec
        self.payload_type = ConfigUtils.find_enum(
            PayloadType, self.get_or_default(Keyword.PAYLOAD_TYPE)
        )
        self.header_present = self.get_or_default(Keyword.HEADER_PRESENT, False)

    def get(self, name: str):
        """Generic method to return a property.
        The method raises an exception when the target property does not exist.
        """
        assert isinstance(name, str)
        return self.config.get(name)

    def get_or_default(self, name: str, default=None):
        """Generic method to return a property."""
        assert isinstance(name, str)
        value = self.config.get(name)
        return value if value is not None else default

    def get_or_raise(self, name: str):
        return ConfigUtils.get_property(self.config, name)

    def get_source_id(self):
        """Returns import source ID."""
        return self.get(Keyword.IMPORT_SOURCE_ID)

    def get_mapper_name(self):
        """deprecated"""
        return self.get(Keyword.MAPPER_NAME)

    def get_payload_type(self) -> PayloadType:
        """Returns payload type in upper case"""
        return self.payload_type

    def get_header_present(self) -> bool:
        """Returns whether the source CSV has header"""
        return self.header_present


class DestinationDataSpec:
    """Destination data specification of a data flow config."""

    def __init__(self, config: dict):
        """Constructor

        Args:
            config (dict): Original raw destinationDataSpec config
        """
        assert isinstance(config, dict)
        self.config = config
        self.destination_id = ConfigUtils.get_property(config, Keyword.IMPORT_DESTINATION_ID)
        self.stream_type = ConfigUtils.find_enum(
            BiosStreamType, ConfigUtils.get_property(config, Keyword.TYPE)
        )
        self.stream_name = ConfigUtils.get_property(config, Keyword.NAME)

    def get(self, name: str):
        return self.config.get(name)

    def get_destination_id(self) -> int:
        """Returns destination ID for the flow."""
        return self.destination_id

    def get_stream_type(self) -> BiosStreamType:
        """Get BIOS stream type (i.e., signal or context) of the destination."""
        return self.stream_type

    def get_stream_name(self) -> str:
        """Get BIOS stream (signal or context) name."""
        return self.stream_name


class DataPickupSpec:
    """Class to configure a data pickup specification.
    This class does only high-level config verification. Detail verification and
    interpretation would be done by DataProcessor class.
    """

    def __init__(self, pickup_config: dict):
        assert isinstance(pickup_config, dict)
        self.config = pickup_config
        self.attribute_search_path = pickup_config.get(Keyword.ATTRIBUTE_SEARCH_PATH) or ""
        self.attributes = pickup_config.get(Keyword.ATTRIBUTES)
        assert isinstance(self.attributes, list)
        self.filters = pickup_config.get(Keyword.FILTERS) or []
        self.deletion_spec = pickup_config.get(Keyword.DELETION_SPEC)
        self.tenant_forwarding = pickup_config.get(Keyword.TENANT_FORWARDING)

    def get_attribute_search_path(self) -> str:
        """Returns attribute search path."""
        return self.attribute_search_path

    def get_attributes(self):
        """Returns attribute configs."""
        return self.attributes

    def get_filters(self):
        """Returns filter configs."""
        return self.filters

    def get_deletion_spec(self):
        """Returns deletion spec."""
        return self.deletion_spec


class DataFlowConfiguration:
    """Configures a data flow specification."""

    def __init__(self, flow_config):
        # For now, I'll just keep the original config
        self.config = flow_config
        self.source_spec = SourceDataSpec(
            ConfigUtils.get_property(self.config, Keyword.SOURCE_DATA_SPEC)
        )
        self.destination_spec = DestinationDataSpec(
            ConfigUtils.get_property(self.config, Keyword.DESTINATION_DATA_SPEC)
        )
        self.pickup_spec = DataPickupSpec(flow_config.get(Keyword.DATA_PICKUP_SPEC))

    def get(self, name: str):
        """Generic method to get a property."""
        return self.config.get(name)

    def get_name(self) -> str:
        """Returns data flow name."""
        return ConfigUtils.get_property(self.config, Keyword.IMPORT_FLOW_NAME)

    def get_id(self) -> int:
        """Returns data flow ID."""
        return ConfigUtils.get_property(self.config, Keyword.IMPORT_FLOW_ID)

    def get_source_spec(self) -> SourceDataSpec:
        """Returns data source specification."""
        return self.source_spec

    def get_source_id(self) -> int:
        """Returns data source ID."""
        return self.source_spec.get_source_id()

    def get_destination_spec(self) -> DestinationDataSpec:
        """Returns data destination specification."""
        return self.destination_spec

    def get_destination_id(self) -> int:
        """Returns data destination ID."""
        return self.destination_spec.get_destination_id()

    def get_pickup_spec(self) -> DataPickupSpec:
        """Returns data pickup specification of the flow."""
        return self.pickup_spec

    def is_checkpointing_enabled(self) -> bool:
        """Returns True if checkpointing is enabled with the flow."""
        return self.get(Keyword.CHECKPOINTING_ENABLED) is True

    def is_acknowledgement_enabled(self) -> bool:
        """Returns True if acknowledgement is enabled with the flow."""
        value = self.get(Keyword.CHECKPOINTING_ENABLED)
        return value is None or value is True

    def transform_to_cdc_sink_flow(self):
        """ "Modify the object to be a CDC sink flow"""
        self.config[Keyword.IMPORT_FLOW_NAME] = f"{self.get_name()} (modified for CDC)"

        source_spec = self.source_spec.config
        source_spec[Keyword.PAYLOAD_TYPE] = Keyword.JSON
        table_name = self.get_source_spec().get_or_raise(Keyword.TABLE_NAME)
        source_spec[Keyword.WEBHOOK_SUB_PATH] = urllib.parse.quote_plus(table_name)

        pickup_spec = self.pickup_spec
        # pickup_spec.attribute_search_path = "records/*"
        pickup_spec.config[Keyword.ATTRIBUTE_SEARCH_PATH] = pickup_spec.attribute_search_path
        op_types = source_spec.get(Keyword.CDC_OPERATION_TYPES)
        if op_types:
            op_types_str = ", ".join([f"'{op.upper()}'" for op in op_types])
            filters = [
                {
                    Keyword.SOURCE_ATTRIBUTE_NAME: "${operationType}",
                    Keyword.FILTER: f"lambda op: op and op.upper() in [{op_types_str}]",
                }
            ]
            filters.extend(pickup_spec.get_filters())
            pickup_spec.config[Keyword.FILTERS] = filters
            pickup_spec.filters = filters

        if self.destination_spec.get(Keyword.TYPE) == "Context":
            pickup_spec.deletion_spec = {
                Keyword.SOURCE_ATTRIBUTE_NAME: "${operationType}",
                Keyword.CONDITION: "lambda type: type == 'Delete'",
            }
            pickup_spec.config[Keyword.DELETION_SPEC] = pickup_spec.deletion_spec


class Configuration:
    """Tenant configuration; only necessary parts are picked up"""

    def __init__(self, tenant_config: dict, system_config: ConfigParser):
        assert isinstance(tenant_config, dict), "'tenant_config' must be a dict"
        self._logger = logging.getLogger(type(self).__name__)
        self._load_config(tenant_config, system_config)

    def _load_config(self, tenant_config: dict, system_config: ConfigParser):
        self.tenant_name = tenant_config.get(Keyword.TENANT_NAME) or "n/a"

        self.sources = self._load_sources(tenant_config.get(Keyword.IMPORT_SOURCES), system_config)

        self.destinations = self._load_destinations(tenant_config.get(Keyword.IMPORT_DESTINATIONS))

        self.flow_configs = self._load_flow_configs(tenant_config.get(Keyword.IMPORT_FLOW_SPECS))

        self.processor_configs = self._load_processors(
            tenant_config.get(Keyword.IMPORT_DATA_PROCESSORS)
        )

    def _load_sources(self, sources_orig: list, system_config: ConfigParser):
        if not sources_orig:
            return {}
        sources = {}
        for source in sources_orig:
            source_id = source.get(Keyword.IMPORT_SOURCE_ID)
            type_name = source.get(Keyword.TYPE).upper()
            try:
                source_type = SourceType[type_name]
                sources[source_id] = SourceConfiguration(source_type, source)
            except KeyError:
                if (
                    system_config.get("Webhook", "cdcProxyEnabled", fallback="false").lower()
                    == "true"
                ):
                    try:
                        _ = CdcSourceType[type_name]
                        source_config = self._translate_cdc_source(
                            source_id, source, system_config
                        )
                        sources[source_id] = source_config
                        continue
                    except KeyError:
                        pass
                self._logger.info(
                    "Skipping unsupported source; sourceName=%s, type=%s",
                    source[Keyword.IMPORT_SOURCE_NAME],
                    source.get(Keyword.TYPE),
                )
        return sources

    def _translate_cdc_source(
        self, import_source_id: str, source: dict, system_config: ConfigParser
    ) -> SourceConfiguration:
        """Translates a CDC import source to a webhook source"""
        source_name = (
            f"{source.get(Keyword.IMPORT_SOURCE_NAME)} ({source.get(Keyword.TYPE)} -> Webhook)"
        )
        encoded_source_id = urllib.parse.quote_plus(import_source_id)
        user_header = system_config.get(
            "Webhook", "cdcProxyUserHeaderName", fallback="x-bios-username"
        )
        pass_header = system_config.get(
            "Webhook", "cdcProxyPasswordHeaderName", fallback="x-bios-password"
        )
        translated = {
            Keyword.IMPORT_SOURCE_ID: import_source_id,
            Keyword.IMPORT_SOURCE_NAME: source_name,
            Keyword.TYPE: SourceType.WEBHOOK.name,
            Keyword.WEBHOOK_PATH: f"__cdc/{encoded_source_id}",
            Keyword.AUTHENTICATION: {
                Keyword.TYPE: AuthType.HttpHeadersPlain.name,
                # TODO(Naoki): Make these values configurable
                Keyword.USER_HEADER: user_header,
                Keyword.PASSWORD_HEADER: pass_header,
            },
        }
        return SourceConfiguration(SourceType.WEBHOOK, translated, is_cdc_sink=True)

    def _load_destinations(self, destinations_orig: list):
        if not destinations_orig:
            return {}
        destinations = {}
        for destination in destinations_orig:
            destination_id = destination.get(Keyword.IMPORT_DESTINATION_ID)
            try:
                destinations[destination_id] = DestinationConfiguration(destination)
            except KeyError:
                # this happens for unknown/unsupported destination type
                self._logger.warning(
                    "Unsupported destination, skipping; name=%s, id=%s, type=%s",
                    destination.get(Keyword.IMPORT_DESTINATION_NAME),
                    destination_id,
                    destination.get(Keyword.TYPE),
                )
        return destinations

    def _load_mapping_configs(self, raw_configs: List[dict]) -> dict:
        if not raw_configs:
            return {}
        mapping_configs = {}
        for conf in raw_configs:
            name = ConfigUtils.get_property(conf, Keyword.MAPPER_NAME)
            mapping_configs[name] = conf
        return mapping_configs

    def _load_flow_configs(self, flow_specs_orig: list) -> List[DataFlowConfiguration]:
        if not flow_specs_orig:
            return []
        return [DataFlowConfiguration(flow) for flow in flow_specs_orig]

    def _load_processors(self, processors_config: List[dict]) -> dict:
        if not processors_config:
            return {}
        processors = {}
        for conf in processors_config:
            name = ConfigUtils.get_property(conf, Keyword.PROCESSOR_NAME)
            value = base64.b64decode(ConfigUtils.get_property(conf, Keyword.CODE)).decode("utf-8")
            processors[name] = value
        return processors

    def get_source_config(self, source_id: str):
        assert isinstance(source_id, str), "Parameter 'id' must be a string"
        return self.sources.get(source_id)

    def get_destination_config(self, destination_id: str):
        assert isinstance(destination_id, str), "Parameter 'id' must be a string"
        return self.destinations.get(destination_id)

    def get_flow_configs(self):
        return self.flow_configs

    def get_processor_configs(self):
        return self.processor_configs


class BiosStreamConfig:
    def __init__(self, stream_type: BiosStreamType, attribute_names: list):
        assert isinstance(type, BiosStreamType)
        assert isinstance(attribute_names, list)
        self.stream_type = stream_type
        # TODO(Naoki): Is it better to keep entire attribute config instead of just names?
        self.attributes = attribute_names

    def get_type(self):
        return self.stream_type

    def get_attribute_names(self):
        return self.attributes
