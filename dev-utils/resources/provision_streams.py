#!/usr/bin/env python3
# Copyright (C) 2018 Isima, Inc. Confidential.
# All rights reserved. Do not distribute without consent.

import json
import sys

from bios import login, ErrorCode, ServiceError

def usage():
    print("Usage: provision_streams.py [--overwrite|--skip] <bios_url> <email> <password> <schema_config_file>")
    print("Options:")
    print("  --overwrite : Overwrite signal or context if exists")
    print("  --skip      : Skip creating signal or context if exists")
    sys.exit(1)

def abort():
    print("Failed to provision streams.")
    print('-------------------------------------------------------')
    sys.exit(-1)


if len(sys.argv) < 4:
    usage()

OVERWRITE_MODE = False
SKIP_MODE = False
iarg = 1
while sys.argv[iarg].startswith("--"):
    arg = sys.argv[iarg]
    if arg == "--overwrite":
        if SKIP_MODE:
            print("ERROR: Only one of --overwrite or --skip option can be specified")
            usage()
        OVERWRITE_MODE = True
    elif arg == "--skip":
        if OVERWRITE_MODE:
            print("ERROR: Only one of --overwrite or --skip option can be specified")
            usage()
        SKIP_MODE = True
    else:
        print(f"ERROR: Unknown option {arg}")
        usage()
    iarg += 1

url = sys.argv[iarg]
iarg += 1
email = sys.argv[iarg]
iarg += 1
if iarg >= len(sys.argv):
    usage()
password = sys.argv[iarg]
iarg += 1
if iarg >= len(sys.argv):
    usage()
stream_config_file = sys.argv[iarg]

with open(stream_config_file, 'r') as streams:
    stream_config = json.load(streams)

adminSession = login(url, email, password)

print('-------------------------------------------------------')
signals = []
contexts = []
for stream in stream_config:
    signalName = None
    contextName = None
    signalName = stream.get('signalName')
    contextName = stream.get('contextName')
    if signalName is not None:
        signals.append(stream)
    else:
        contexts.append(stream)

if OVERWRITE_MODE:
    print("Running in overwriting mode. Cleaning up existing signals and contexts")
    for signal in reversed(signals):
        try:
            signalName = signal.get("signalName")
            adminSession.delete_signal(signalName)
            print(f"deleted signal {signalName}")
        except ServiceError as err:
            if err.error_code is not ErrorCode.NO_SUCH_STREAM:
                print(f"ERROR: {str(err)}")
                abort()

    for context in reversed(contexts):
        try:
            contextName = context.get("contextName")
            adminSession.delete_context(contextName)
            print(f"deleted context {contextName}")
        except ServiceError as err:
            if err.error_code is not ErrorCode.NO_SUCH_STREAM:
                print(f"ERROR: {str(err)}")
                abort()

    print("")

for stream in contexts:
    contextName = stream.get("contextName")
    try:
        print(f"creating context {contextName} ... ", end="")
        sys.stdout.flush()
        adminSession.create_context(stream)
        print("ok")
    except ServiceError as err:
        if err.error_code is not ErrorCode.STREAM_ALREADY_EXISTS:
            print(f"ERROR: {str(err)}")
            abort()
        if not SKIP_MODE:
            print(
                f"ERROR: The context already exists."
                " Delete it manually and retry, or set --overwrite or --skip option"
            )
            abort()
        print("existing, skipped")

for stream in signals:
    signalName = stream.get("signalName")
    print(f"creating signal {signalName} ... ", end="")
    sys.stdout.flush()
    try:
        adminSession.create_signal(stream)
        print("ok")
    except ServiceError as err:
        if err.error_code is not ErrorCode.STREAM_ALREADY_EXISTS:
            raise
        if not SKIP_MODE:
            print(
                f"Already exists."
                " Delete it manually and retry, or set --overwrite or --skip option"
            )
            abort()
        print("existing, skipped")

print('')
print("Streams have been provisioned successfully.")
print('-------------------------------------------------------')
