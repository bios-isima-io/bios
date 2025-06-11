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
"""webhook_data_importer: Module that provides importer classes for WebHook data source"""

import base64
import binascii
import csv
import hashlib
import hmac
import logging
import multiprocessing
from configparser import ConfigParser

from bios import ErrorCode, ServiceError
from flask import Flask, Response, request

from .._version import __version__ as deli_version
from ..configuration import (
    DataFlowConfiguration,
    Keyword,
    PayloadValidationConfiguration,
    PayloadValidationType,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)
from .data_importer import DataImporter, EndpointHandler, SourceFilter

# Load the flask app only once
APP = Flask("deli")


@APP.route("/")
def hello():
    """Returns hello message to a query for the root"""
    return f"biOS Integrations Webhook version {deli_version}\n"


class AttributeFilter(SourceFilter):
    """Webhook data source filter based on attribute value

    Args:
        - spec_content (str): Filter string
        - property_name (str): Property name to check
        - original_spec (str): Original filter specification string
    """

    def __init__(self, spec_content: str, property_name: str, original_spec: str):
        super().__init__()
        tokens = spec_content.split("=", 2)
        if len(tokens) < 2:
            raise Exception(
                f"Syntax error in source data spec; property={property_name}, value={original_spec}"
            )
        self.attribute_names = tokens[0].strip().split(".")
        filter_value = tokens[1].strip()
        if filter_value.startswith("'"):
            if not filter_value.endswith("'"):
                raise Exception(
                    "Syntax error in source data spec;"
                    + f" property={property_name}, value={original_spec}"
                )
            # type string
            self.filter_target = filter_value.strip("'")
        elif filter_value.lower() in ["true", "false"]:
            self.filter_target = filter_value.lower() == "true"
        elif "." in filter_value:
            self.filter_target = float(filter_value)
        else:
            self.filter_target = int(filter_value)

    def apply(self, data: dict) -> bool:
        value = data
        for attribute_name in self.attribute_names:
            if not isinstance(value, dict):
                return False
            value = value.get(attribute_name)
        return value == self.filter_target


class WebhookServiceHandler(EndpointHandler):
    """A webhook importer may have multiple listening URLs depending on subpaths in the import flow
    specs. This class takes care of each URL point

    Args:
        - server (WebhookServer): The webhook server object
        - path (str): Listener URL path for the importing endpoint
    """

    # Publishing response code and text message
    RESPONSE_TABLE = {
        200: "OK",
        202: "Accepted",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        500: "Internal Server Error",
        503: "Service Unavailable",
    }

    def __init__(self, server, path, source_config):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self._server = server
        self._path = path
        self.source_config = source_config

    def setup(self):
        self._server.add_handler(self)

    def start(self):
        pass

    def shutdown(self):
        pass

    def create_source_filter(self, source_data_spec: SourceDataSpec) -> SourceFilter:
        assert isinstance(source_data_spec, SourceDataSpec)
        filter_spec = source_data_spec.get_or_raise(Keyword.WEBHOOK_FILTER)
        if not filter_spec:
            return None
        parts = filter_spec.split(":", 2)
        if len(parts) < 2:
            raise Exception(
                f"Syntax error in source data spec {Keyword.WEBHOOK_FILTER}: {filter_spec}"
            )
        category = parts[0].strip().lower()
        spec_content = parts[1].strip()
        if category == "attribute":
            return AttributeFilter(spec_content, Keyword.WEBHOOK_FILTER, filter_spec)
        raise Exception(
            "Unknown filter category in source spec;"
            + f" property={Keyword.WEBHOOK_FILTER}, category={category}, spec={filter_spec}"
        )

    # Webhook specific methods ##########################################

    def get_path(self):
        """Returns listener URL path."""
        return self._path

    def get_data(self):
        """Returns data based on the Mime type"""
        content_type = request.headers.get("Content-Type")
        # be strict about csv for CSV and loose about json for backward compatibility
        if content_type == "text/csv":
            raw_data = request.get_data(as_text=True)
            # this is needed for parsing the http body with newlines properly
            lines = [l for l in raw_data.split("\n") if len(l) > 0]
            data = list()
            data.extend(lines)
        else:
            data = request.get_json()
        return data

    def handle_webhook_request(self):
        """View function of to be added to the server to handle the requests to the webhook
        endpoint.
        """
        self.logger.info(
            "request received at %s with size %d", request.path, request.content_length
        )
        data = self.get_data()

        # collect user credentials if authorization header is set
        auth = self.source_config.config.get("authentication", {})
        auth_type = auth.get("type")
        user, password = None, None
        http_header = request.headers
        if auth_type == "HttpAuthorizationHeader":
            if http_header.get("authorization"):
                authorization_header = http_header.get("authorization")
                header_token = authorization_header.split(" ")
                if header_token[0].upper() != "BASIC":
                    return Response(
                        f"Only Basic authentication type is supported ({header_token[0]} requested).\n",
                        status=401,
                        mimetype="text/plain",
                    )
                if len(header_token) < 2:
                    return Response(
                        "Syntax error in Authorization header.\n",
                        status=400,
                        mimetype="text/plain",
                    )

                try:
                    credentials = base64.b64decode(header_token[1]).decode().split(":")
                except binascii.Error as err:
                    self.logger.warning(
                        "Authorization header base64 decoding error: %s", err, exc_info=True
                    )
                    credentials = None

                if not credentials or len(credentials) < 2:
                    self.logger.warning(
                        "Failed to read authorization header: %s", authorization_header
                    )
                    return Response(
                        "Syntax error in Authorization header.\n",
                        status=400,
                        mimetype="text/plain",
                    )
                user, password = credentials[0], credentials[1]
            else:
                return Response(
                    "Authorization field is missing in the request headers.\n",
                    status=401,
                    mimetype="text/plain",
                )
        elif auth_type == "HttpHeadersPlain":
            # TODO(Naoki): Bring AuthConfiguration instance here and use it
            user = http_header.get(auth.get(Keyword.USER_HEADER))
            password = http_header.get(auth.get(Keyword.PASSWORD_HEADER))
            if user and password:
                user = user.strip()
                password = password.strip()
            else:
                return Response(
                    "Credentials are missing in the headers.\n", status=401, mimetype="text/plain"
                )

        payload_validation = self.source_config.config.get(Keyword.PAYLOAD_VALIDATION, None)
        if payload_validation:
            payload_validation_config = PayloadValidationConfiguration(payload_validation)
            if (
                payload_validation_config.payload_validation_type
                is PayloadValidationType.HmacSignature
            ):
                secret = payload_validation_config.shared_secret
                if secret is None:
                    raise ServiceError(
                        ErrorCode.INTERNAL_CLIENT_ERROR,
                        "missing shared secret for HMAC auth in importSourceConfig;"
                        f" id={self.source_config.get_id()}",
                    )
                header_name = payload_validation_config.hmac_header
                hmac_header = http_header.get(header_name)
                if hmac_header is None:
                    return Response(
                        f"HMAC header {header_name} is missing in the request.\n",
                        status=401,
                        mimetype="text/plain",
                    )
                req_data = request.get_data()
                if payload_validation_config.digest_base64_encoded:
                    digest = hmac.new(secret.encode("utf-8"), req_data, hashlib.sha256).digest()
                    decoded_hmac = base64.b64decode(hmac_header)
                    verified = hmac.compare_digest(digest, decoded_hmac)
                else:
                    digest = hmac.new(secret.encode("utf-8"), req_data, hashlib.sha256).hexdigest()
                    verified = hmac.compare_digest(digest, hmac_header)

                if not verified:
                    return Response(
                        status=401,
                        mimetype="text/plain",
                    )

        if not data:
            return Response("No Payload\n", status=400, mimetype="text/plain")

        # collect x- headers as additional attributes
        additional_attributes = None
        for name, value in request.headers.items(lower=True):
            if additional_attributes is None:
                additional_attributes = {}
            key = f"${{{name}}}"
            additional_attributes[key] = value

        # In case of CDC sink, put metadata to additional attributes
        if self.source_config.is_cdc_sink:
            additional_attributes = additional_attributes or {}
            for name, value in (data.get("metadata") or {}).items():
                additional_attributes[f"${{{name}}}"] = value
            data = data.get("record")

        response = self.publish(
            data, user=user, password=password, attributes=additional_attributes
        )

        message = self.RESPONSE_TABLE.get(response) or ""
        return Response(f"{message}\n", status=response, mimetype="text/plain")


class WebhookServer:
    """The webhook server class."""

    CONFIG_SECTION = "Webhook"
    PLUGIN_MODE = False

    def __init__(self, system_config: ConfigParser):
        assert isinstance(system_config, ConfigParser)
        self.logger = logging.getLogger(type(self).__name__)
        self._process = None
        self._app = APP
        self._started = False
        self.listener_address = system_config.get(
            self.CONFIG_SECTION, "listenerAddress", fallback="0.0.0.0"
        )
        self.listener_port = system_config.getint(
            self.CONFIG_SECTION, "listenerPort", fallback=8088
        )

    def get_server_app(self):
        return self._app

    def launch(self):
        """Method to launch the server.

        The method invokes a fork to run the server application and returns immediately.
        """
        if self._started or self.PLUGIN_MODE:
            return
        self._process = multiprocessing.Process(target=self._launch)
        self._process.start()
        self._started = True

    def _launch(self):
        self.logger.info("Starting Webhook server")
        self._app.run(host=self.listener_address, port=self.listener_port)

    def add_handler(self, service_handler: WebhookServiceHandler):
        """Method to add a view function to the server.

        Args:
            - service_handler (WebhookServiceHandler): The service handler that provides the view
                function
        """
        assert isinstance(service_handler, WebhookServiceHandler)
        view_function = service_handler.handle_webhook_request
        path = service_handler.get_path()
        self._app.add_url_rule(path, path, view_function, methods=["POST"])


class WebhookDataImporter(DataImporter):
    """Webhook importer class."""

    SERVER_INSTANCE = None

    @classmethod
    def global_init(cls, system_config: ConfigParser):
        assert isinstance(system_config, ConfigParser)
        WebhookDataImporter.SERVER_INSTANCE = WebhookServer(system_config)

    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.server = WebhookDataImporter.SERVER_INSTANCE
        if not self.server:
            self.server = WebhookServer(system_config)
            WebhookDataImporter.SERVER_INSTANCE = self.server
        root_path = config.get("webhookPath")  # TODO use Keyword
        if not root_path:
            root_path = "/"
        elif not root_path.startswith("/"):
            root_path = "/" + root_path
        while root_path.endswith("/"):
            root_path = root_path[:-1]
        self.root_path = root_path

    def set_server_app(self, server_app: Flask):
        assert isinstance(server_app, Flask)
        self.server.replace_app(server_app)

    def start_importer(self):
        self.server.launch()

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: dict) -> str:
        """Generate the handler ID. This class uses the service path as the ID."""
        subpath = source_data_spec.get_or_default("webhookSubPath", None)
        if subpath:
            if not subpath.startswith("/"):
                subpath = "/" + subpath
            path = self.root_path + subpath
        else:
            path = self.root_path
        return path

    def create_handler(self, handler_id: str, _: SourceDataSpec) -> WebhookServiceHandler:
        # handler ID is the service path
        return WebhookServiceHandler(self.server, handler_id, self.source_configuration)


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.WEBHOOK] = WebhookDataImporter
DataImporter.IMPORTER_INITIALIZERS[SourceType.WEBHOOK] = WebhookDataImporter.global_init
