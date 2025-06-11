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

"""
bi(OS) utility to set/unset TTL for contexts.
"""

import argparse
import sys

import bios

# Initialize parser.
parser = argparse.ArgumentParser(description="bi(OS) utility to set/unset TTL for contexts")

required_group = parser.add_argument_group("Required Arguments")
required_group.add_argument(
    "-e",
    "--endpoint",
    help="bi(OS) endpoint, e.g. https://bios.isima.io",
    required=True,
)
required_group.add_argument("-u", "--email", help="The user email address", required=True)
required_group.add_argument("-p", "--password", help="The user's password", required=True)
required_group.add_argument(
    "-c", "--context", help="Name of the context to change TTL for", required=True
)
required_group.add_argument(
    "-t", "--ttl", help="TTL value in milliseconds; 0 to remove TTL", required=True
)

# Read arguments from command line.
try:
    args = parser.parse_args()
except Exception:
    parser.print_help()
    sys.exit(1)

if len(sys.argv) < 2:
    parser.print_help()
    sys.exit(2)

# Create a session.
session = bios.login(args.endpoint, args.email, args.password, "TtlUtility")
print("INFO: Logged into bi(OS)")

# Get the context config.
context = session.get_context(args.context)
if not context:
    print(f"Context {args.context} not found.")
    sys.exit(3)

if "ttl" in context:
    print(f"Current TTL: {context['ttl']} milliseconds.")
else:
    print(f"No TTL set currently.")

if int(args.ttl) == 0:
    if "ttl" in context:
        del context["ttl"]
else:
    context["ttl"] = int(args.ttl)

print()
print("Updated context config:")
print(context)

print()
print(f"Completed setting TTL {int(args.ttl)} milliseconds on context {args.context}.")
