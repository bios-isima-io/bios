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
import logging
import time
from configparser import ConfigParser
import requests

from ..configuration import (
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)
from .data_importer import DataImporter, EndpointHandler, SourceFilter


class RestClientDataImporter(DataImporter):
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

    def start_importer(self):
        while True:
            for handler in self.endpoint_handlers.values():
                handler.handle()
            time.sleep(self.polling_interval)

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        """Generates REST client endpoint handler ID.
        The ID consists of <src_conf.endpoint>/<src_spec.subpath>
        """
        endpoint = self.config.get(Keyword.ENDPOINT)  # we disallow missing endpoint
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
        subpath = source_data_spec.get_or_default(Keyword.SUB_PATH, "").strip("/")
        handler_id = endpoint + "/" + subpath
        return handler_id

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec):
        # handler ID is the service path
        return RestClientHandler(self.config, self.system_config, source_data_spec, handler_id)


class RestClientHandler(EndpointHandler):
    def __init__(
        self,
        config: SourceConfiguration,
        system_config: ConfigParser,
        source_data_spec: SourceDataSpec,
        handler_id: str,
    ):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.system_config = system_config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id

        self.method = config.get_method().upper()
        self.body = self.source_data_spec.get_or_default(Keyword.BODY_PARAMS, {})
        self.query_params = self.source_data_spec.get_or_default(Keyword.QUERY_PARAMS, {})
        self.headers = self.source_data_spec.get_or_default(Keyword.HEADERS, {})
        sub_path = self.source_data_spec.get_or_default(Keyword.SUB_PATH, None)
        self.endpoint = self._construct_endpoint(config.get(Keyword.ENDPOINT), sub_path)
        auth = config.get(Keyword.AUTHENTICATION)
        if auth:
            auth_type = auth.get("type")
            if auth_type.upper() != "APIACCESSTOKEN":
                self.logger.error("unsupported authentication type %s. Exiting", auth_type)
                return

    def setup(self):
        pass

    def start(self):
        pass

    def handle(self):
        # start processing rest call
        try:
            response = None
            if self.method == "GET":
                response = requests.get(
                    self.endpoint, params=self.query_params, headers=self.headers
                )
            elif self.method == "POST":
                response = requests.post(self.endpoint, data=self.body, headers=self.headers)
            if response.status_code < 400:
                data = response.json()
                self.publish(data)
                return
            if response.status_code == 401:
                self.logger.info(
                    "fail to authenticate endpoint=%s status=%s message=%s",
                    self.endpoint,
                    response.status_code,
                    response.text,
                )
            else:
                self.logger.error(
                    "Failed to fetch data from endpoint; flow=%s, endpoint=%s status=%s message=%s",
                    self._handler_id,
                    self.endpoint,
                    response.status_code,
                    response.text,
                )
                return
        except Exception:
            self.logger.error(
                "Exception happened during fetching REST data; flow=%s",
                self._handler_id,
                exc_info=True,
            )

    def shutdown(self):
        pass

    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        pass

    def _construct_endpoint(self, host, sub_path=None):
        if sub_path is None:
            return host
        if host[-1] == "/":
            host = host.rstrip(host[-1])
        if sub_path[0] != "/":
            sub_path = "/" + sub_path
        return host + sub_path


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.RESTCLIENT] = RestClientDataImporter
