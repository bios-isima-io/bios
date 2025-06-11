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

import math
import os
import sys
import time
import random

import bios
from bios import ServiceError
from tsetup import get_single_endpoint as endpoint
from tsetup import (
    get_endpoint_url as ep_url,
    sadmin_user,
    sadmin_pass,
    admin_user,
    admin_pass,
    ingest_user,
    ingest_pass,
)

TEST_TENANT_NAME = "onTheFlySelectTest"

SIGNAL_ON_THE_FLY_TEST = {
    "signalName": "onTheFlyTestSignal",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "country", "type": "string"},
        {"attributeName": "city", "type": "string"},
        {"attributeName": "score", "type": "integer"},
        {"attributeName": "duration", "type": "decimal"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byCountryCity",
                "dimensions": ["country", "city"],
                "attributes": ["score", "duration"],
                "featureInterval": 60000,
            }
        ]
    },
}

SIGNAL_ON_THE_FLY_NO_FEATURES = {
    "signalName": "onTheFlyNoFeaturesSignal",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "country", "type": "string"},
        {"attributeName": "city", "type": "string"},
        {"attributeName": "score", "type": "integer"},
        {"attributeName": "duration", "type": "decimal"},
    ],
}


def sleep_until_time(sleep_until):
    sleep_time = sleep_until - time.time()
    if sleep_time > 0:
        print(f"sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)


def setup():
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        sadmin.set_property("prop.rollup.suspend", "false")
        try:
            sadmin.delete_tenant(TEST_TENANT_NAME)
        except ServiceError:
            pass

        sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    with bios.login(ep_url(), f"{admin_user}@{TEST_TENANT_NAME}", admin_pass) as admin:
        admin.create_signal(SIGNAL_ON_THE_FLY_TEST)
        admin.create_signal(SIGNAL_ON_THE_FLY_NO_FEATURES)

    with bios.login(ep_url(), f"{ingest_user}@{TEST_TENANT_NAME}", ingest_pass) as ingest:
        start_time = math.ceil(time.time() / 60) * 60 + 1.0
        print("--> Adjust the start time")
        sleep_until_time(start_time)

        statement1 = (
            bios.isql()
            .insert()
            .into(SIGNAL_ON_THE_FLY_TEST.get("signalName"))
            .csv_bulk(
                [
                    "US,Redwood City,3,52.4",
                    "Japan,Tokyo,7,12.6",
                    "US,San Mateo,33,71.1",
                    "Japan,Tokyo,11,8.4",
                    "US,Campbell,22,31.6",
                    "US,Redwood City,5,83.3",
                    "US,Portland,91,12.4",
                    "Australia,Campbell,10,4.2",
                    "India,Bangalore,17,1.2",
                    "US,Portland,1,111.2",
                ]
            )
            .build()
        )
        statement1b = (
            bios.isql()
            .insert()
            .into(SIGNAL_ON_THE_FLY_NO_FEATURES.get("signalName"))
            .csv_bulk(
                [
                    "US,Redwood City,3,52.4",
                    "Japan,Tokyo,7,12.6",
                    "US,San Mateo,33,71.1",
                    "Japan,Tokyo,11,8.4",
                    "US,Tokyo,22,31.6",
                    "US,Redwood City,5,83.3",
                    "US,Portland,91,12.4",
                    "Australia,Sydney,10,4.2",
                    "India,Bangalore,17,1.2",
                    "US,Portland,1,111.2",
                ]
            )
            .build()
        )
        time0 = bios.time.now()
        resp = ingest.execute(statement1).records[0]
        resp_no_features = ingest.execute(statement1b).records[0]
        print("timestamp(onTheFlyTest):", resp.timestamp)
        print("timestamp(onTheFlyNoFeatures):", resp_no_features.timestamp)

        print("--> Sleep until the time to run the second insertions")
        sleep_until_time(start_time + 60)

        statement2 = (
            bios.isql()
            .insert()
            .into(SIGNAL_ON_THE_FLY_TEST.get("signalName"))
            .csv_bulk(
                [
                    "Australia,Campbell,51,38.2",
                    "Japan,Tokyo,73,12.6",
                    "Australia,Sydney,11,18.1",
                    "US,Campbell,95,88.0",
                    "Japan,Kobe,5,191.4",
                    "Australia,Sydney,14,11.2",
                    "US,Tokyo,10,70.9",
                    "US,Redwood City,10,11.3",
                    "Australia,Melbourne,17,15.4",
                ]
            )
            .build()
        )
        ingest.execute(statement2)

        statement2b = (
            bios.isql()
            .insert()
            .into(SIGNAL_ON_THE_FLY_NO_FEATURES.get("signalName"))
            .csv_bulk(
                [
                    "Australia,Campbell,51,38.2",
                    "Japan,Tokyo,73,12.6",
                    "Australia,Sydney,11,18.1",
                    "US,Campbell,95,88.0",
                    "Japan,Kobe,5,191.4",
                    "Australia,Sydney,14,11.2",
                    "US,Tokyo,10,70.9",
                    "US,Redwood City,10,11.3",
                    "Australia,Melbourne,17,15.4",
                ]
            )
            .build()
        )
        ingest.execute(statement2b)

        print("--> Sleep until the time to run the third insertions")
        sleep_until_time(start_time + 120)

        statement3 = (
            bios.isql()
            .insert()
            .into(SIGNAL_ON_THE_FLY_TEST.get("signalName"))
            .csv_bulk(
                [
                    "France,Nice,23,19.5",
                    "US,Redwood City,51,10.6",
                    "Australia,Melbourne,13,4.5",
                    "India,Bangalore,78,77.9",
                    "France,Paris,3,223.1",
                    "Japan,Yokohama,88,3.8",
                    "US,New York,11,69.4",
                    "India,Bangalore,9,42.1",
                ]
            )
            .build()
        )
        ingest.execute(statement3)

        statement3b = (
            bios.isql()
            .insert()
            .into(SIGNAL_ON_THE_FLY_NO_FEATURES.get("signalName"))
            .csv_bulk(
                [
                    "France,Nice,23,19.5",
                    "US,Redwood City,51,10.6",
                    "Australia,Melbourne,13,4.5",
                    "India,Bangalore,78,77.9",
                    "France,Paris,3,223.1",
                    "Japan,Yokohama,88,3.8",
                    "US,New York,11,69.4",
                    "India,Bangalore,9,42.1",
                ]
            )
            .build()
        )
        ingest.execute(statement3b)

    # Wait until the first insertion is rolled up and suspend the rollup
    print("--> Wait until the second rollup is processed")
    sleep_until_time(start_time + 115)
    with bios.login(ep_url(), f"{admin_user}@{TEST_TENANT_NAME}", admin_pass) as admin:
        for _ in range(120):
            windows = admin.execute(
                bios.isql()
                .select("count()")
                .from_signal(SIGNAL_ON_THE_FLY_TEST.get("signalName"))
                .where("country = 'Australia'")
                .tumbling_window(bios.time.minutes(2))
                .snapped_time_range(time0, bios.time.minutes(3), bios.time.minutes(1))
                .build()
            ).get_data_windows()
            if windows[0].records[0].get("count()") == 5:
                break
            time.sleep(1)

    print("--> Suspend rollup")
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        sadmin.set_property("prop.rollup.suspend", "true")

    # Generate test-env.sh
    print("--> Generate test-env.sh")
    env_file_path = "/tmp/on-the-fly-test-env.sh"
    with open(env_file_path, "w") as f:
        f.write("export TIMESTAMP_ON_THE_FLY_TEST={}\n".format(resp.timestamp))
        f.write("export TIMESTAMP_ON_THE_FLY_NO_FEATURES={}\n".format(resp_no_features.timestamp))
    print(f"Environment file created at {env_file_path}")
    print(f'Run "source {env_file_path}" to set the test environment variables')


def teardown():
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        sadmin.set_property("prop.rollup.suspend", "false")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "teardown":
        teardown()
    else:
        setup()
