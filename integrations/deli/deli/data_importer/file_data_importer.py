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
import glob
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from configparser import ConfigParser

from ..configuration import (
    DataFlowConfiguration,
    Keyword,
    PayloadType,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)
from .data_importer import DataImporter, EndpointHandler, SourceTranslator


class PayloadReader(ABC):
    """Abstract class used for reading various types of payload from file"""

    @abstractmethod
    def read(self, file, batch_size):
        """Reads fixed number of records from file and returns it

        Args:
            file: file handle
            header_present: boolean (identifies if header is present)
            batch_size: number of records to read

        Returns: Tuple[List[dict], bool]: list of records, EOF
        """


class CsvPayloadReader(PayloadReader):
    def __init__(self, header_present: bool):
        self.header_present = header_present
        self.header = None

    def read(self, file, batch_size):
        # TODO Python has a CSV library, replace this impl
        data = list()
        if not self.header and self.header_present:
            self.header = file.readline().strip()
        if self.header:
            data.append(self.header)
        count = 0
        while True:
            line = file.readline().strip()
            if not line:
                return data, True
            count += 1
            data.append(line)
            if count == batch_size:
                return data, False


class JsonPayloadReader(PayloadReader):
    def read(self, file, batch_size):
        return json.load(file), True


class FileDataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.config = config
        self.system_config = system_config
        self.polling_interval = self.config.get_polling_interval()
        self.file_location = config.get_or_raise(Keyword.FILE_LOCATION)

    def start_importer(self):
        while True:
            for handler in self.endpoint_handlers.values():
                handler.handle()
            time.sleep(self.polling_interval)

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        """Generates File pull endpoint handler ID.
        The ID consists of <spec.payloadType>:<conf.file_location>/<spec.filenamePrefix>
        """
        payload_type = source_data_spec.get_payload_type()
        if payload_type is PayloadType.CSV:
            scheme = f"{payload_type.name}.{source_data_spec.get_header_present()}"
        else:
            scheme = payload_type.name
        prefix = source_data_spec.get_or_raise(Keyword.FILE_NAME_PREFIX)
        return f"{scheme}:{self.file_location}/{prefix}"

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec):
        # handler ID is the service path
        return FileHandler(self.config, source_data_spec, handler_id)


class FileHandler(EndpointHandler):
    def __init__(
        self, config: SourceConfiguration, source_data_spec: SourceDataSpec, handler_id: str
    ):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id
        payload_type = source_data_spec.get_payload_type()
        if payload_type is PayloadType.CSV:
            self._reader = CsvPayloadReader(source_data_spec.get_header_present())
        elif payload_type is PayloadType.JSON:
            self._reader = JsonPayloadReader()
        else:
            raise Exception(f"Unsupported payload type: {payload_type}")

    def setup(self):
        pass

    def start(self):
        pass

    def handle(self):
        # start processing file data
        source_type = str(self.config.get(Keyword.TYPE)).upper()
        if not source_type == SourceType.FILE.name:
            return
        file_path = str(self.config.get_or_raise(Keyword.FILE_LOCATION))
        file_name_prefix = self.source_data_spec.get_or_raise(Keyword.FILE_NAME_PREFIX)
        file_path = file_path + "/" + file_name_prefix + "*"
        self.logger.info("file_path: %s", file_path)
        file_list = sorted(glob.glob(file_path), key=os.path.getmtime)
        self.logger.info("file_list: %s", file_list)
        source_batch_size = 0
        if self.source_data_spec.get(Keyword.SOURCE_BATCH_SIZE):
            source_batch_size = int(self.source_data_spec.get(Keyword.SOURCE_BATCH_SIZE))
        try:
            for file_name in file_list:
                if file_name.endswith(".processed"):
                    self.logger.info("Skipping processed file %s", file_name)
                    continue
                self.logger.info("Processing FILE: %s", file_name)
                with open(file_name, "r", encoding="utf-8") as file:
                    eof = False
                    while not eof:
                        try:
                            data, eof = self._reader.read(file, source_batch_size)
                            self.publish(data)
                        except Exception:
                            self.logger.error(
                                "Error occurred during message processing", exc_info=True
                            )
                # rename processed file
                os.rename(file_name, file_name + ".processed")
        except Exception:
            self.logger.error("Error occurred during message processing", exc_info=True)

    def shutdown(self):
        pass

    def create_source_filter(self, source_data_spec: dict) -> SourceTranslator:
        return None


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.FILE] = FileDataImporter
