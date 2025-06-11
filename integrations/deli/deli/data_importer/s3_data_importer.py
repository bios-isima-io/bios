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
import codecs
import json
import logging
import time
from configparser import ConfigParser

import boto3
import botocore

from ..configuration import (
    ConfigUtils,
    DataFlowConfiguration,
    Keyword,
    PayloadType,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)
from .data_importer import DataImporter, EndpointHandler, SourceFilter


class S3DataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.system_config = system_config
        self.polling_interval = self.config.get_polling_interval()
        self.checkpoint_file = system_config.get(
            "S3",
            "checkpointFile",
            fallback="/opt/bios/configuration/integrations-s3-checkpoints.json",
        )
        self.checkpoints = {}

    def start_importer(self):
        try:
            with open(self.checkpoint_file, "r") as file:
                checkpoints = json.load(file)
                for handler in self.endpoint_handlers.values():
                    handler_id = handler.handler_id
                    handler.current_key = checkpoints.get(handler_id) or ""
                    self.checkpoints[handler_id] = handler.current_key
        except (OSError, json.decoder.JSONDecodeError):
            for handler in self.endpoint_handlers.values():
                handler.current_key = ""
        while True:
            for handler in self.endpoint_handlers.values():
                handler.handle()
            time.sleep(self.polling_interval)

    def checkpoint(self, handler_id: str, position: str):
        assert isinstance(handler_id, str)
        assert isinstance(position, str)
        self.checkpoints[handler_id] = position
        try:
            with open(self.checkpoint_file, "w") as file:
                json.dump(self.checkpoints, file)
        except OSError:
            self.logger.error("Failed to save checkpoint", exc_info=True)

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        """Generates S3 pull endpoint handler ID.
        The ID consists of <source.bucket>/<spec.fileNamePrefix>
        """
        bucket = source_data_spec.get_or_raise(Keyword.S3_BUCKET)
        prefix = source_data_spec.get_or_default(Keyword.FILE_NAME_PREFIX, "")
        return f"{bucket}/{prefix}"

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec):
        # handler ID is the service path
        return S3Handler(handler_id, self.config, source_data_spec, self)


class S3Handler(EndpointHandler):
    def __init__(
        self,
        handler_id: str,
        config: SourceConfiguration,
        source_data_spec: SourceDataSpec,
        data_importer: S3DataImporter,
    ):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.handler_id = handler_id
        self.config = config
        self.source_data_spec = source_data_spec
        self.data_importer = data_importer

        self.endpoint_url = config.get(Keyword.ENDPOINT)
        if source_data_spec.get_payload_type() == PayloadType.CSV:
            self._provide_source = self._provide_csv_source
        else:
            self._provide_source = self._provide_json_source
        self.batch_size = source_data_spec.get_or_default(Keyword.SOURCE_BATCH_SIZE, 0)
        self.current_key = ""

    def setup(self):
        pass

    def start(self):
        pass

    def handle(self):
        # start processing file data
        auth = self.config.get(Keyword.AUTHENTICATION)
        auth_type = ConfigUtils.get_property(auth, Keyword.TYPE)
        if str(auth_type).upper() == "ACCESSKEY":
            access_key = ConfigUtils.get_property(auth, Keyword.ACCESS_KEY)
            secret_key = ConfigUtils.get_property(auth, Keyword.SECRET_KEY)
        else:
            self.logger.error("No access keys provided. Exiting")
            return
        s3_bucket = self.source_data_spec.get_or_raise(Keyword.S3_BUCKET)
        self.logger.debug("s3_bucket: %s", s3_bucket)
        file_prefix = self.source_data_spec.get_or_default(Keyword.FILE_NAME_PREFIX, "")
        self.logger.debug("file_prefix: %s", file_prefix)
        try:
            client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=self.endpoint_url,
            )
            objects = client.list_objects_v2(
                Bucket=s3_bucket, Prefix=file_prefix, StartAfter=self.current_key
            )
        except botocore.exceptions.ClientError:
            self.logger.error(
                "Failed to list S3 objects; handler_id=%s", self.handler_id, exc_info=True
            )
        source_count = 0
        for content in objects.get("Contents") or []:
            current_key = content.get("Key")
            if content.get("Size") == 0:
                continue
            source_count += 1
            if self.batch_size and source_count > self.batch_size:
                break
            try:
                self.logger.info("Pulling S3 data: bucket=%s, key=%s", s3_bucket, current_key)
                obj = client.get_object(Bucket=s3_bucket, Key=current_key)
                body = obj.get("Body")
                data_source = self._provide_source(body)
                self.publish(data_source)
            except Exception as err:
                self.logger.error(
                    "Error occurred during message processing; handler_id=%s",
                    self.handler_id,
                    exc_info=True,
                )
            finally:
                self.current_key = current_key
                self.data_importer.checkpoint(self.handler_id, self.current_key)

    def _provide_csv_source(self, body):
        reader = codecs.getreader("utf-8")(body)
        return iter(reader.readline, "")

    def _provide_json_source(self, body):
        reader = codecs.getreader("utf-8")(body)
        return json.load(reader)

    def shutdown(self):
        pass

    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        pass


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.S3] = S3DataImporter
