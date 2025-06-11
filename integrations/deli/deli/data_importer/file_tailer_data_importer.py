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
import time
import json
import csv
import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser

from pygtail import Pygtail
from deli.configuration import (
    ConfigUtils,
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)

from .data_importer import DataImporter, EndpointHandler, SourceFilter


class PayloadReader(ABC):
    """Abstract class used for reading various types of payload from file"""

    @abstractmethod
    def read(self, fh, header_present, batch_size):
        """Reads fixed number of records from file and returns it

        Args:
            fh: file handle
            header_present: boolean (identifies if header is present)
            batch_size: number of records to read

        Returns: list of records, EOF
        """


class CsvPayloadReader(PayloadReader):

    header = None

    def read(self, fh, header_present, batch_size):
        data = list()
        if not self.header and header_present:
            self.header = fh.readline().strip()
        if self.header:
            data.append(self.header)
        count = 0
        while True:
            line = fh.readline().strip()
            if not line:
                return data, True
            count += 1
            data.append(line)
            if count == batch_size:
                return data, False


class JsonPayloadReader(PayloadReader):
    def read(self, fh, header_present, batch_size):
        data = json.loads(fh.read())
        return data, True


class FileTailerDataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.config = config
        self.system_config = system_config
        self.source_data_spec = None

    def start_importer(self):
        pass

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: dict) -> str:
        return (
            self.config.get(Keyword.FILE_LOCATION)
            + "_"
            + source_data_spec.get_or_raise(Keyword.IMPORT_SOURCE_ID)
            + "_"
        )

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec):
        # handler ID is the service path
        return TailerFileHandler(self.config, source_data_spec, handler_id)


class TailerFileHandler(EndpointHandler):
    def __init__(self, config: SourceConfiguration, source_data_spec: dict, handler_id: str):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id
        self._reader = None
        payload_type = str(self.source_data_spec.get_or_raise(Keyword.PAYLOAD_TYPE)).upper()
        if payload_type == "CSV":
            self._reader = CsvPayloadReader()
        if payload_type == "JSON":
            self._reader = JsonPayloadReader()

    def setup(self):
        pass

    def start(self):
        # start processing file data
        source_type = str(self.config.get(Keyword.TYPE)).upper()
        if source_type != SourceType.FILETAILER.name:
            return
        file_path = str(self.config.get(Keyword.FILE_LOCATION))
        self.logger.info("file_path: %s", file_path)

        try:
            while True:
                for line in Pygtail(file_path):
                    try:
                        if not line:
                            time.sleep(1)
                            continue
                        self.publish([line.strip() + ",0"])
                    except Exception as err:
                        self.logger.error(
                            "Error occurred during processing line: %s, Exception: %s",
                            line,
                            err,
                            exc_info=True,
                        )
        except Exception as error:
            self.logger.error(
                f"Error occurred during message processing: %s", error, exc_info=True
            )

    def shutdown(self):
        pass

    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        pass


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.FILETAILER] = FileTailerDataImporter
