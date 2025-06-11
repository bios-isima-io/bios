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
"""data_processor: Module that provides class(es) to process importing data."""

import copy
import importlib.util
import logging
import tempfile
from typing import Any, Dict, List, Tuple, Union

import dpath
from bios import ErrorCode, ServiceError

from .configuration import DataPickupSpec, Keyword
from .event_spec import EventSpec
from .utils import parse_attribute_path


class AttributeFinder:
    """Finds an attribute"""

    def __init__(self, search_path: str):
        assert isinstance(search_path, str)
        self.search_path = search_path
        self.logger = logging.getLogger(type(self).__name__)
        if not search_path:
            self.all_pass = True
            return
        self.all_pass = False
        last_char = search_path[-1]
        if last_char == "*":
            if len(search_path) > 1 and search_path[-2] != "/":
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    "The last element of the path must be an explicit name or single asterisk",
                )
        elif last_char == "/":
            raise ServiceError(
                ErrorCode.BAD_INPUT,
                "Search path must not end with a slash",
            )

    def search_records(self, incoming_message: object) -> List[dict]:
        """Search for source records by the attribute search path.

        Args:
            - incoming_message (object): Incoming message
        Returns: List[dict]: Picked source records
        """
        if incoming_message is None:
            raise ServiceError(ErrorCode.BAD_INPUT, "Incoming message must not be null")
        if self.all_pass:
            return [incoming_message]
        return dpath.values(incoming_message, self.search_path)

    def find(
        self,
        record: dict,
        attribute_path: str,
        unflattened: dict = None,
        additional: dict = None,
        current_output: dict = None,
    ) -> Any:
        """Finds an attribute by the specified attribute path.

        Args:
            - record (dict): Source record
            - attribute_path (str): Attribute path
        Returns: Any: Found attribute or None
        """
        if isinstance(attribute_path, list):
            return self._find_by_path_list(
                record, attribute_path, unflattened, additional, current_output
            )
        if additional is not None and attribute_path.startswith("$"):
            assert isinstance(additional, dict)
            return additional.get(attribute_path.lower())
        if unflattened is not None and attribute_path.startswith("/"):
            if not isinstance(unflattened, dict):
                self.logger.info("Requested data = %s", unflattened)
                raise ServiceError(
                    ErrorCode.BAD_INPUT, "The unflattened record has unexpected data structure"
                )
            source = unflattened
        elif attribute_path == "":
            if type(record) not in {str, int, float, bool}:
                self.logger.info("Requested data = %s", record)
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    "The incoming record must be a scalar for attribute path ''",
                )
            return record
        else:
            if not isinstance(record, dict):
                self.logger.info("Requested data = %s", record)
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    f"The incoming record for source path {attribute_path} must be dict",
                )
            source = record
        # Try current output first
        found = (
            dpath.get(current_output, attribute_path.lower(), default=None)
            if current_output
            else None
        )
        if found is None:
            found = dpath.get(source, attribute_path, default=None)
        return found

    @classmethod
    def _find_by_path_list(cls, record, attribute_path, unflattened, additional, current_output):
        if attribute_path[0].startswith("$"):
            return cls._resolve(additional, attribute_path)
        if attribute_path[0] == "":  # full path
            src = unflattened if unflattened is not None else record
            return cls._resolve(src, attribute_path[1:])
        found = None
        if current_output is not None:
            found = cls._resolve(current_output, [element.lower() for element in attribute_path])
        if found is None:
            found = cls._resolve(record, attribute_path)
        return found

    @classmethod
    def _resolve(cls, source: dict, path: List[str]):
        element = path[0]
        if len(path) > 1:
            return (
                cls._resolve(source.get(element), path[1:]) if isinstance(source, dict) else None
            )
        if element == "":
            return source
        if element == "*":
            if not isinstance(source, list) or len(source) == 0:
                return None
            return source[0] if len(source) == 1 else source
        return source.get(element) if isinstance(source, dict) else None


class ImportedModules:
    """Class that loads external processors"""

    def __init__(self, processor_configs: dict):
        assert isinstance(processor_configs, dict)
        self.processor_configs = processor_configs
        self.modules = {}

    def load(self) -> "ImportedModules":
        """Loads configured processor modules"""
        for name, code in self.processor_configs.items():
            self.modules[name] = self._load_processor_module(name, code)
        return self

    def get_module(self, name: str) -> object:
        """Returns a loaded module."""
        assert isinstance(name, str)
        return self.modules.get(name)

    def _load_processor_module(self, name: str, code: str) -> object:
        assert isinstance(name, str)
        assert isinstance(code, str)
        return self._import_from_string(code, name)

    def _import_from_string(self, content, module_name):
        with tempfile.NamedTemporaryFile(
            mode="w", prefix=module_name, suffix=".py", delete=False
        ) as file:
            file.write(content)
            file_name = file.name

        spec = importlib.util.spec_from_file_location(module_name, file_name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


class ProcessingUtils:
    """Collection of utilities for processing incoming data"""

    @staticmethod
    def parse_source_attributes(
        source_attributes: List[str], source_attribute: str, flow_name: str, spec_name: str
    ) -> List[Union[List[str], str]]:
        """Parses import source attribute name specifications"""
        if not source_attribute and not source_attributes:
            raise ValueError(
                f"Configuration error: flow={flow_name}, {spec_name}:"
                " Either of property 'sourceAttributeName' or 'sourceAttributeNames'"
                " must be specified"
            )
        return [parse_attribute_path(path) for path in source_attributes or [source_attribute]]

    @staticmethod
    def parse_data_processing(
        proc_specs: List[Dict[str, Any]],
        imported_modules: ImportedModules,
        flow_name: str,
        spec_name: str,
    ) -> List[object]:
        """Parses data processing spec"""
        assert len(proc_specs) > 0
        assert imported_modules
        processors = []
        for i, proc_spec in enumerate(proc_specs):
            name = proc_spec.get(Keyword.PROCESSOR_NAME)
            method = proc_spec.get(Keyword.METHOD)
            if not name or not method:
                raise ValueError(
                    f"Configuration error: flow={flow_name}, {spec_name}.processes[{i}]:"
                    " Properties 'processorName' and 'method' must be specified"
                )
            module = imported_modules.get_module(name)
            if not module:
                raise ValueError(
                    f"flow={flow_name}, {spec_name}.processes[{i}]:"
                    f" Module not found; module_name={name}"
                )
            processors.append(eval(f"module.{method}"))  # pylint: disable=eval-used
        return processors

    @staticmethod
    def parse_transforms(transform_specs: str, flow_name: str, spec_name: str):
        """Parse transforms spec.

        Args:
            transform_specs (str): Transforms spec
            flow_name (str): Flow configuration name
            spec_name (str): Specification property name
        Returns: List[Tuple[object, str]]: Parsed tuples of transformer and output name
        """
        assert len(transform_specs) > 0
        transformers = []
        for transform in transform_specs:
            rule_src = transform.get(Keyword.RULE)
            dest_attribute = transform.get(Keyword.AS)
            if rule_src is None or dest_attribute is None:
                raise ValueError(
                    f"Configuration error: flow={flow_name}: {spec_name}:"
                    + " Both of properties 'rule' and 'as' must be set for a mapping entry"
                )
            rule = eval(rule_src)  # pylint: disable=eval-used
            transformers.append((rule, dest_attribute.lower()))
        return transformers


class ProcessingUnit:
    """A unit of incoming data processing

    Args:
        - flow_name (str): Data flow spec name
        - unit_index (int): Index of the data picker unit
        - unit_config (dict): Configuration of the unit
        - finder (AttributeFinder): Attribute finder to be used for retrieving the source attribute
        - imported_modules (ImportedModules): Imported external modules
    """

    def _transform_identically(self, value):
        return value

    def __init__(
        self,
        flow_name: str,
        unit_index: int,
        unit_config: dict,
        finder: AttributeFinder,
        imported_modules: ImportedModules,
        spec_name: str = None,
    ):
        assert isinstance(flow_name, str)
        assert isinstance(unit_index, int)
        assert isinstance(unit_config, dict)
        assert isinstance(finder, AttributeFinder)
        if imported_modules:
            assert isinstance(imported_modules, ImportedModules)
        self.flow_name = flow_name
        self.unit_index = unit_index
        self.logger = logging.getLogger(type(self).__name__)
        self.finder = finder
        source_attribute = unit_config.get(Keyword.SOURCE_ATTRIBUTE_NAME)
        source_attributes = unit_config.get(Keyword.SOURCE_ATTRIBUTE_NAMES)
        # data picker
        spec_name = spec_name if spec_name else f"dataPickupSpec[{unit_index}]"
        self.data_sources = ProcessingUtils.parse_source_attributes(
            source_attributes, source_attribute, flow_name, spec_name
        )

        # data source attributes
        self.processors = []
        proc_specs = unit_config.get(Keyword.PROCESSES)
        if proc_specs:
            self.processors = ProcessingUtils.parse_data_processing(
                proc_specs, imported_modules, flow_name, spec_name
            )

        # data mapper; tuple of mapping function and destination attribute name
        transform_specs = unit_config.get(Keyword.TRANSFORMS)
        if transform_specs:
            self.transformers = ProcessingUtils.parse_transforms(
                transform_specs, flow_name, spec_name
            )
        else:
            func = self._transform_identically
            dest_attribute = unit_config.get(Keyword.AS) or source_attribute
            self.transformers = [(func, dest_attribute.lower())]

    def process(
        self,
        incoming_message: dict,
        flattened_record: dict,
        additional_attributes: dict,
        out_record: dict,
    ):
        """Process an incoming message.

        Args:
            - incoming_message (dict): Incoming raw message
            - flattened_record (dict): Flattened source record
            - additional_attributes (dict): Request-common additional attributes
            - out_messages (dict): Importing record
        """
        # pick data
        data = tuple(
            [
                self.finder.find(
                    flattened_record,
                    attribute,
                    unflattened=incoming_message,
                    additional=additional_attributes,
                    current_output=out_record,
                )
                for attribute in self.data_sources
            ]
        )
        # process data
        for i, processor in enumerate(self.processors):
            try:
                out = processor(*data)
            except Exception as err:  # pylint: disable=broad-except
                message = (
                    f"Importing processing error; flow={self.flow_name},"
                    + f" source_attributes={self.data_sources}, process_index={i}"
                )
                self.logger.warning(message, exc_info=True)
                # giving up processing
                raise ServiceError(ErrorCode.BAD_INPUT, message) from err
            # Make the output a tuple
            data = out if isinstance(out, tuple) else tuple([out])

        # transform data
        for func, dest_attribute in self.transformers:
            try:
                out_record[dest_attribute] = func(*data)
            except Exception as err:  # pylint: disable=broad-except
                message = (
                    f"Transforming the output failed; flow={self.flow_name},"
                    + f" source_attributes={self.data_sources}, dest_attribute={dest_attribute}"
                )
                self.logger.warning(message, exc_info=True)
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    f"{message}, error={type(err).__name__}, message={str(err)}",
                ) from err


class DataProcessor:
    """Class that takes care of processing import data."""

    def __init__(
        self,
        flow_name: str,
        pickup_spec: DataPickupSpec,
        imported_modules: "ImportedModules" = None,
    ):
        assert isinstance(flow_name, str)
        assert isinstance(pickup_spec, DataPickupSpec)
        if imported_modules:
            assert isinstance(imported_modules, ImportedModules)

        self.flow_name = flow_name
        self.attribute_finder = AttributeFinder(pickup_spec.get_attribute_search_path())
        self.units = []
        for index, unit_config in enumerate(pickup_spec.get_attributes()):
            unit_config = pickup_spec.get_attributes()[index]
            self.units.append(
                ProcessingUnit(
                    flow_name, index, unit_config, self.attribute_finder, imported_modules
                )
            )
        self.filters = self._parse_filters(flow_name, pickup_spec)
        self.deletion_spec = self._parse_deletion_spec(flow_name, pickup_spec)
        self.forwarding_unit = self._parse_tenant_forwarding(
            flow_name, pickup_spec, self.attribute_finder, imported_modules
        )
        self.in_message_auth_user = None
        self.in_message_auth_password = None
        self.http_header_plain_auth_user = None
        self.http_header_plain_auth_password = None

    def _parse_filters(
        self, flow_name: str, pickup_spec: DataPickupSpec
    ) -> List[Tuple[str, object]]:
        filters = []
        for i, filter_entry in enumerate(pickup_spec.get_filters()):
            ctx = f"flow={flow_name}, filter_index={i}"
            if not isinstance(filter_entry, dict):
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    f"Invalid filter entry; {ctx}",
                )
            attr = filter_entry.get(Keyword.SOURCE_ATTRIBUTE_NAME)
            if not isinstance(attr, str):
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    "Invalid filter entry: property 'sourceAttributeName' is not set"
                    f" or not a string; {ctx}",
                )
            func_src = filter_entry.get(Keyword.FILTER)
            if not isinstance(attr, str):
                raise ServiceError(
                    ErrorCode.BAD_INPUT,
                    f"Invalid filter entry: property 'filter' must be set; {ctx}",
                )
            filters.append(
                (parse_attribute_path(attr), eval(func_src))  # pylint: disable=eval-used
            )
        return filters

    def _parse_deletion_spec(self, flow_name: str, pickup_spec: DataPickupSpec):
        deletion_spec = pickup_spec.get_deletion_spec()
        if not deletion_spec:
            return None, None
        ctx = f"flow={flow_name}"
        if not isinstance(deletion_spec, dict):
            raise ServiceError(
                ErrorCode.BAD_INPUT,
                f"Invalid deletion spec; {ctx}",
            )
        attr = deletion_spec.get(Keyword.SOURCE_ATTRIBUTE_NAME)
        if not isinstance(attr, str):
            raise ServiceError(
                ErrorCode.BAD_INPUT,
                "Invalid deletion spec: property 'sourceAttributeName' is not set"
                f" or not a string; {ctx}",
            )
        condition_lambda = deletion_spec.get(Keyword.CONDITION)
        if not isinstance(attr, str):
            raise ServiceError(
                ErrorCode.BAD_INPUT,
                f"Invalid filter entry: property 'filter' must be set; {ctx}",
            )
        deletion_condition = (
            parse_attribute_path(attr),
            eval(condition_lambda),  # pylint: disable=eval-used
        )
        return deletion_condition

    def _parse_tenant_forwarding(
        self,
        flow_name: str,
        pickup_spec: DataPickupSpec,
        finder: AttributeFinder,
        imported_modules: ImportedModules,
    ):
        assert pickup_spec is not None
        tenant_forwarding_orig = pickup_spec.tenant_forwarding
        if not tenant_forwarding_orig:
            return None
        tenant_forwarding = copy.deepcopy(tenant_forwarding_orig)
        tenant_forwarding[Keyword.AS] = Keyword.FOR_TENANT
        transform_spec = tenant_forwarding.get(Keyword.TRANSFORMS)
        if transform_spec:
            last_entry = transform_spec[-1]
            last_entry[Keyword.AS] = Keyword.FOR_TENANT
        return ProcessingUnit(
            flow_name,
            0,
            tenant_forwarding,
            finder,
            imported_modules,
            spec_name=Keyword.TENANT_FORWARDING,
        )

    def set_in_message_auth_attributes(self, user_attr: str, password_attr: str):
        """Sets in-message authentication attribute names."""
        assert isinstance(user_attr, str)
        assert isinstance(password_attr, str)
        self.in_message_auth_user = parse_attribute_path(user_attr)
        self.in_message_auth_password = parse_attribute_path(password_attr)

    # we are not using http_header_plain_auth_user and http_header_plain_auth_password
    # since for header auth we are directly propagating credential to next the function
    def set_in_http_headers_plain_auth_attributes(self, user_attr: str, password_attr: str):
        """Sets http-headers-plain authentication attribute names."""
        assert isinstance(user_attr, str)
        assert isinstance(password_attr, str)
        self.http_header_plain_auth_user = user_attr
        self.http_header_plain_auth_password = password_attr

    def process_bulk(
        self, in_messages: list, user: str = None, password: str = None, attributes: dict = None
    ) -> Tuple[dict, str, str, bool]:
        """Process incoming data.

        This method will be called for bulk processing payloads. (ex:
        CSV payload, where auth credentials are not part of incoming
        message)

        Args:
            - in_messages (List): List of Incoming messages
            - user (str): username for  http auth (optional)
            - password (str): password for http auth (optional)
        Returns: Tuple: containing List of processed records
        """
        assert isinstance(in_messages, list)
        events = []
        # default the user and password
        updated_user = user
        updated_password = password
        for in_message in in_messages:
            events_inner, updated_user, updated_password = self.process(
                in_message, user=user, password=password, attributes=attributes
            )
            if len(events_inner) > 0:
                events.extend(events_inner)
        return (events, updated_user, updated_password)

    def process(
        self,
        incoming_message: dict,
        user: str = None,
        password: str = None,
        attributes: dict = None,
    ) -> List[dict]:
        """Process incoming data.

        Args:
            - incoming_message (dict): Incoming message
            - user (str): Username
            - password (str): Password
        Returns: List[dict]: List of importing records
        """
        assert isinstance(incoming_message, dict)
        if user:
            assert isinstance(user, str)
        if password:
            assert isinstance(password, str)
        if attributes:
            assert isinstance(attributes, dict)
        else:
            attributes = {}

        # filter the message first
        for filter_attr, filter_function in self.filters:
            value = self.attribute_finder.find(
                incoming_message, filter_attr, additional=attributes
            )
            if not filter_function(value):
                return None

        # fetch credentials if necessary
        if self.in_message_auth_user:
            user = self.attribute_finder.find(incoming_message, self.in_message_auth_user)
            password = self.attribute_finder.find(incoming_message, self.in_message_auth_password)
            if user is None or password is None:
                raise ServiceError(
                    ErrorCode.UNAUTHORIZED, "In-message credentials are not available"
                )

        records = self.attribute_finder.search_records(incoming_message)

        out_events = []
        for record in records:
            event_attributes = {}
            for unit in self.units:
                unit.process(incoming_message, record, attributes, event_attributes)
            if self.forwarding_unit:
                self.forwarding_unit.process(
                    incoming_message, record, attributes, event_attributes
                )
                if event_attributes.get(Keyword.FOR_TENANT) is None:
                    event_attributes[Keyword.FOR_TENANT] = ""
            operation = self._determine_operation_type(incoming_message, record, attributes)
            out_events.append(EventSpec(event_attributes, operation))
        return (out_events, user, password)

    def _determine_operation_type(
        self, incoming_message: dict, flattened_record: dict, additional_attributes: dict
    ):
        operation = EventSpec.Operation.INSERT
        deletion_spec_attr, deletion_condition = self.deletion_spec
        if not deletion_spec_attr:
            return operation
        value = self.attribute_finder.find(
            flattened_record,
            deletion_spec_attr,
            unflattened=incoming_message,
            additional=additional_attributes,
        )
        if deletion_condition(value):
            operation = EventSpec.Operation.DELETE
        return operation
