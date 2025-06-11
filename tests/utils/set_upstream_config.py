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

import json
import pprint
import sys

import bios


def build_upstream_config(ep_signal, ep_analysis, ep_rollup):
    return {
        "upstreams": [
            {
                "hostSet": [ep_signal],
                "operationSet": {
                    "operation": [
                        {"operationPolicy": "ALWAYS", "operationType": "INGEST"},
                        {"operationPolicy": "ALWAYS", "operationType": "EXTRACT"},
                        {"operationPolicy": "ALWAYS", "operationType": "CONTEXT_WRITE"},
                        {"operationPolicy": "ALWAYS", "operationType": "ADMIN_READ"},
                        {"operationPolicy": "ALWAYS", "operationType": "ADMIN_WRITE"},
                    ]
                },
            },
            {
                "hostSet": [ep_analysis, ep_rollup],
                "operationSet": {
                    "operation": [
                        {"operationPolicy": "ALWAYS", "operationType": "SUMMARIZE"},
                        {"operationPolicy": "ALWAYS", "operationType": "ADMIN_READ"},
                        {"operationPolicy": "ALWAYS", "operationType": "ADMIN_WRITE"},
                    ]
                },
            },
        ]
    }


def set_upstream_config(host_signal, host_analysis, host_rollup, port):
    ep_signal = f"https://{host_signal}:{port}"
    ep_analysis = f"https://{host_analysis}:{port}"
    ep_rollup = f"https://{host_rollup}:{port}"

    with bios.login(f"https://{host_signal}:{port}", "superadmin", "superadmin") as session:

        session.delete_endpoint(ep_signal)
        session.delete_endpoint(ep_analysis)
        session.delete_endpoint(ep_rollup)
        session.add_endpoint(ep_signal, nodetype=bios.NodeType.SIGNAL)
        session.add_endpoint(ep_analysis, nodetype=bios.NodeType.ANALYSIS)
        session.add_endpoint(ep_rollup, nodetype=bios.NodeType.ROLLUP)
        print("Endpoints are set:")
        pprint.pprint(session.list_endpoints())
        print("")

        # Set upstream config
        upstream_conf = build_upstream_config(ep_signal, ep_analysis, ep_rollup)
        session.set_property("upstream", json.dumps(upstream_conf))
        print("Upstream config has been set:")
        pprint.pprint(upstream_conf)
        print("")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "Usage: set_upstream_config.py <host_signal> <host_analysis> <host_rollup> [port:=443]"
        )
        sys.exit(1)
    HOST_SIGNAL = sys.argv[1]
    HOST_ANALYSIS = sys.argv[2]
    HOST_ROLLUP = sys.argv[3]
    PORT = int(sys.argv[4]) if len(sys.argv) > 4 else 443
    set_upstream_config(HOST_SIGNAL, HOST_ANALYSIS, HOST_ROLLUP, PORT)
