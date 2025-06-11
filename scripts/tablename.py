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

#
# tablename.py
# A script to generate a table or keyspace name from type of the entity, name and version.
# The script is meant to be used for troubleshooting, so is written using only standard
# library.  You can bring and execute this script whereever python3 is available.

import hashlib
from os.path import basename
import sys


def make_table_name(tname, name, version):
    """Generate a table or keyspace name from entity type, name, and version

    Args:
        tname (str): Entity type name, signal|tenant|context|index|view|rollup|metrics
        name (str): Entity name
        version (str|int): Version number
    Returns: str: Generated table or keyspace name
    """
    if isinstance(version, int):
        version = str(version)

    if tname == "signal" or tname == "evt":
        prefix = "evt"
    elif tname == "tenant":
        prefix = "tfos_d"
    elif tname == "context" or tname == "ctx":
        prefix = "cx2"
    elif tname == "index" or tname == "idx":
        prefix = "idx"
    elif tname == "view" or tname == "ivw":
        prefix = "ivw"
    elif tname == "rollup" or tname == "rlp":
        prefix = "rlp"
    elif tname == "metrics" or tname == "met":
        prefix = "met"
    else:
        raise Exception(tname + ": unknown type")

    src = name.lower() + "." + version
    m = hashlib.md5()
    m.update(bytearray(src, "utf-8"))
    out = prefix + "_"
    digest = m.digest()
    index = 0
    for b in digest:
        if index == 6:
            b &= 0x0F  # clear version
            b |= 0x30  # set to version 3
        elif index == 8:
            b &= 0x3F  # clear variant
            b |= 0x80  # set to IETF variant
        out += "{:02x}".format(b)
        index += 1
    return out


def usage():
    print("Usage: {} [options] <name> <version>".format(basename(sys.argv[0])))
    print("options:")
    print(
        "    -t -- table type"
        " (signal|tenant|context|index|view|rollup|metrics) (default: signal)"
    )
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()

    tname = "signal"
    prefix = "evt"
    i = 1
    while i < len(sys.argv) and sys.argv[i].startswith("-"):
        if sys.argv[i] == "-t":
            i += 1
            if i == len(sys.argv):
                usage()
            tname = sys.argv[i]
        i += 1
    name = sys.argv[i]
    i += 1
    version = sys.argv[i]

    try:
        out = make_table_name(tname, name, version)
        print(out)
    except Exception as error:
        print(error.args[0])
        usage()
