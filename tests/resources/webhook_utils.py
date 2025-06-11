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
import datetime
import json
import logging
import random
import time

import requests


def parse_timestamp(src):
    if not src:
        return -1
    try:
        return int(
            datetime.datetime.fromisoformat(src[:23].replace("Z", "+0000")).timestamp() * 1000
        )
    except Exception:
        return -1


def create_timestamp(_):
    return int(time.time() * 1000)


def encode_payload(src):
    if not src:
        return "{}"
    try:
        return json.dumps(src)
    except Exception:
        return "{}"


def _set_description(source_dict, title, prop_name, description):
    try:
        description[title] = str(source_dict[prop_name])
    except KeyError:
        pass


def send_slack_notification(webhook_id, domain, topic, timestamp, payload):
    logger = logging.getLogger("ShopifyWebhookHandler")
    slack_url = "${WEBHOOK_ENDPOINT}/slack-notification"

    subject = f"Request received: {topic} for domain {domain}"

    description = {
        "Request": topic,
        "Domain": domain,
        "Requested At": timestamp,
        "Webhook ID": webhook_id,
    }
    customer = payload.get("customer")
    if customer:
        _set_description(customer, "Customer ID", "id", description)
        _set_description(customer, "Customer Email", "email", description)
        _set_description(customer, "Customer Phone", "phone", description)
    _set_description(payload, "Orders Requested", "orders_requested", description)
    _set_description(payload, "Orders to Redact", "orders_to_redact", description)
    _set_description(payload, "Data Request", "data_request", description)

    details = "\n".join([f"*{key}:* {value}" for key, value in description.items()])

    message = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": subject}},
        ],
        "attachments": [
            {
                "color": "#ff5500",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": details,
                        },
                    }
                ],
            }
        ],
    }

    out = {"details": str(message)}

    response = requests.post(
        slack_url, json=message, headers={"content-type": "application/json"}, timeout=120
    )
    if response.status_code == 200:
        out["status"] = "OK"
        out["response"] = ""
    else:
        logger.error(
            "Sending message to Slack failed. Error: %s, response: %s, message: %s",
            response.status_code,
            response.text,
            message,
        )
        out["status"] = "ERROR"
        out["response"] = (
            "{"
            '"error": "UnableToSend",'
            f' "statusCode": {response.status_code},'
            f' "detail": "{response.text}"'
            "}"
        )
    return out


def json_encode(payload):
    try:
        return json.dumps(payload)
    except Exception:
        return ""
