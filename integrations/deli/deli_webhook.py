#!/usr/bin/env python3
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
"""deli_webhook: The deli framework for webhook."""

import logging

from flask import request

from deli import Deli, run_framework
from deli.configuration import SourceType
from deli.data_importer.webhook_data_importer import WebhookDataImporter, WebhookServer

# Header fields to obscure values when dumping
OBSCURING_HEADERS = {"authorization", "cookie"}

WebhookServer.PLUGIN_MODE = True
run_framework(source_type=SourceType.WEBHOOK, is_plugin=True)
app = (
    WebhookDataImporter.SERVER_INSTANCE.get_server_app()
)  # pylint: disable=invalid-name


@app.before_request
def dump_request():
    conf = Deli.INSTANCE.system_config
    max_bodysize = 64000
    content_length = request.content_length if request.content_length is not None else 0
    logging.info(
        "request received at %s with size %d", request.path, content_length
    )
    if (conf.get("Webhook", "requestHeaderDumpEnabled", fallback="false").lower() == "true"):
        logging.info(
            "Request at %s: headers=%s",
            request.path,
            [
                f"{name}: {value if name not in OBSCURING_HEADERS else '***'}"
                for name, value in request.headers.items(lower=True)
            ],
        )

    if (conf.get("Webhook", "requestDumpEnabled", fallback="false").lower() == "true" and content_length <= max_bodysize):
        logging.info(
            "Request at %s: body=%s", request.path, request.get_data(as_text=True)
        )


if __name__ == "__main__":
    app.run(
        host=WebhookDataImporter.SERVER_INSTANCE.listener_address,
        port=WebhookDataImporter.SERVER_INSTANCE.listener_port,
    )
