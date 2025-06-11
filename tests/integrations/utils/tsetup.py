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
"""Module to provide test configuration."""

import json
import logging
import os
import traceback
from urllib.parse import urlparse

# user names and passwords
admin_user = "admin"
admin_pass = "admin"
sadmin_user = "superadmin"
sadmin_pass = "superadmin"
ingest_user = "ingest"
ingest_pass = "ingest"
extract_user = "extract"
extract_pass = "extract"
mlengineer_user = "mlengineer"
mlengineer_pass = "mlengineer"
report_user = "report"
report_pass = "report"
system_ingest_user = "ingest@isima.io"
system_ingest_pass = "ingest"
support_user = "support"
support_pass = "support"
tenant_usage_signal = "_usage"
system_operations_signal = "_operations"

opm_count_attr = "operations"
opm_count_metric = "sum(operations)"
opm_operation_attr = "request"
opm_db_attr = "dbAccesses"
opm_db_metric = "sum(dbAccesses)"
opm_pre_attr = "preProcessSuccessCount"
opm_pre_metric = "sum(preProcessSuccessCount)"


_LOG_LEVEL_SRC = os.environ.get("TFOS_LOG_LEVEL") or "INFO"
if _LOG_LEVEL_SRC == "DEBUG":
    _LOG_LEVEL = logging.DEBUG
elif _LOG_LEVEL_SRC == "INFO":
    _LOG_LEVEL = logging.INFO
else:
    _LOG_LEVEL = logging.WARN
logging.basicConfig(level=_LOG_LEVEL)


def get_single_endpoint():
    """Method to get a single-node TFOS service endpoint as a tuple of tfos.Client parameters

    The method returns endpoint 'https://172.18.0.11:8443' in default.

    The value can be changed by environment variable TFOS_ENDPOINT.
    """
    endpoint_url = get_endpoint_url()

    return _url_to_endpoint_tuples(endpoint_url)


def get_endpoint_url():
    """Method to get a single-node TFOS service endpoint as a URL.

    The method returns endpoint 'https://172.18.0.11:8443' in default.

    The value can be changed by environment variable TFOS_ENDPOINT.
    """
    return os.environ.get("TFOS_ENDPOINT") or "https://172.18.0.11:8443"


def get_multinode_endpoint(index):
    """Method to get a multi-node TFOS service endpoint.

    In default, three endpoints can be returned as follows:

    ::

        index=0: http://172.18.0.11:8080
        index=1: http://172.18.0.12:8080
        index=2: http://172.18.0.13:8080

    The list can be overriden by environment variable MULTINODE_ENDPOINTS. The values are specified
    by comma-separated URLs. This is useful for test case development. For example, when following
    value is set to MULTINODE_ENDPOINTS, all available three services with indexes 0, 1, 2 point
    to localhost:8080. Then, you may write test cases for multi-node when multi-node capability
    is not ready.

    ::

        http://localhost:8080, http://localhost:8080, http://localhost:8080

    Args:
        index (int): Node index.

    Returns:
        str: endpoint URL

    Raises:
        Exception: when index is negative or exceeds available nodes.
    """
    ev_endpoints = os.environ.get("MULTINODE_ENDPOINTS")
    if not ev_endpoints:
        ev_endpoints = "http://172.18.0.11:8080,http://172.18.0.12:8080,http://172.18.0.13:8080"

    endpoint_urls = ev_endpoints.split(",")
    endpoints = []
    for url in endpoint_urls:
        endpoints.append(_url_to_endpoint_tuples(url))
    if index < 0 or index >= len(endpoints):
        raise Exception("given index {} is out of range {}".format(index, len(endpoints)))
    return endpoints[index]


def get_rollup_interval_seconds():
    interval_src = os.environ.get("ROLLUP_INTERVAL") or "300"
    return int(interval_src)


def get_rollup_intervals():
    """Get 1x and 3x rollup intervals as TimeSpan objects.

    The method reads environment variable ROLLUP_INTERVAL that
    specifies rollup interval in seconds (default: 300). Then
    resolves 1x and 3x intervals as TimeSpan objects.

    Returns:
        (TimeSpan, TimeSpan): 1x and 3x rollup intervals
    """
    return get_rollup_interval_seconds() * 1000


def _url_to_endpoint_tuples(url):
    _url = urlparse(url)
    secure = _url.scheme == "https"

    if secure:
        port = 443
    else:
        port = 80

    elements = _url.netloc.split(":")
    host = elements[0]
    if len(elements) > 1:
        port = int(elements[1])

    cafile = os.environ.get("SSL_CERT_FILE") or "../../test_data/cert.pem"

    return (host, port, secure, "journal", cafile)


def get_webhook_url():
    """Method to get the webhook URL

    The method returns the value of environment variable WEBHOOK_ENDPOINT or http://localhost:8081
    """
    return os.environ.get("WEBHOOK_ENDPOINT") or "http://localhost:8081"


def get_webhook2_url():
    """Method to get the secondary webhook URL

    The method returns the value of environment variable WEBHOOK2_ENDPOINT or http://localhost:8081
    """
    return os.environ.get("WEBHOOK2_ENDPOINT") or "http://localhost:8081"


def get_operation_metrics_signal(tenant_name):
    return system_operations_signal if tenant_name == "_system" else tenant_usage_signal
