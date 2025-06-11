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
import random
import sys
import time

from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint as endpoint
from tsetup import ingest_pass, ingest_user, sadmin_pass, sadmin_user

import bios
from bios import ServiceError

TEST_TENANT_NAME = "jsSimpleTest"
TEST_TENANT_NAME2 = "jsSimpleTest2"

SIGNAL_MINIMUM = {
    "signalName": "minimum",
    "missingAttributePolicy": "Reject",
    "attributes": [{"attributeName": "one", "type": "string"}],
    "postStorageStage": {
        "features": [
            {
                "featureName": "NonQuantitativeSketch",
                "attributes": ["one"],
                "dataSketches": ["DistinctCount"],
                "featureInterval": 5000,
            }
        ]
    },
}

SIGNAL_SIMPLE_ROLLUP = {
    "signalName": "simpleRollup",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "country", "type": "string"},
        {"attributeName": "state", "type": "string"},
        {"attributeName": "value", "type": "integer"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byCountryCity",
                "dimensions": ["country", "state"],
                "attributes": ["value"],
                "featureInterval": 60000,
            }
        ]
    },
}

SIGNAL_ROLLUP_TWO_VALUES = {
    "signalName": "rollupTwoValues",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "country", "type": "string"},
        {"attributeName": "state", "type": "string"},
        {"attributeName": "score", "type": "integer"},
        {"attributeName": "duration", "type": "decimal"},
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "byCountryCity",
                "dimensions": ["country", "state"],
                "attributes": ["score", "duration"],
                "featureInterval": 60000,
            }
        ]
    },
}

CONTEXT_INFER_TAGS = {
    "contextName": "inferTagsContext",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "zipcode", "type": "string"},
        {"attributeName": "country", "type": "string"},
        {"attributeName": "latitude", "type": "decimal"},
        {"attributeName": "longitude", "type": "decimal"},
        {"attributeName": "altitude", "type": "decimal"},
        {"attributeName": "flagged", "type": "boolean"},
        {"attributeName": "zipDescription", "type": "string", "tags": {"category": "Description"}},
        {
            "attributeName": "additionalDeliveryCharge",
            "type": "decimal",
            "tags": {
                "category": "Quantity",
                "kind": "Money",
                "unit": "USD",
                "positiveIndicator": "Low",
            },
        },
    ],
    "primaryKey": ["zipcode"],
    "auditEnabled": True,
}

SIGNAL_SKETCH = {
    "signalName": "dataSketch",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "orderId", "type": "Integer"},
        {
            "attributeName": "orderAmount",
            "type": "Decimal",
            "tags": {
                "category": "Quantity",
                "kind": "Money",
                "unit": "USD",
                "positiveIndicator": "High",
                "firstSummary": "AVG",
                "secondSummary": "SUM",
            },
        },
        {
            "attributeName": "deliveryTime",
            "type": "Integer",
            "tags": {
                "category": "Quantity",
                "kind": "Duration",
                "unit": "Minute",
                "positiveIndicator": "Low",
                "firstSummary": "AVG",
            },
        },
        {"attributeName": "zipcode", "type": "String", "tags": {"category": "Dimension"}},
        {
            "attributeName": "gender",
            "type": "String",
            "allowedValues": ["Male", "Female", "Unknown"],
        },
        {
            "attributeName": "deliveryTimestamp",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Timestamp", "unit": "unixSecond"},
        },
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "location",
                "foreignKey": ["zipcode"],
                "contextName": "inferTagsContext",
                "missingLookupPolicy": "StoreFillInValue",
                "contextAttributes": [
                    {"attributeName": "country", "fillIn": "None"},
                    {"attributeName": "latitude", "fillIn": "0"},
                    {"attributeName": "longitude", "fillIn": "0"},
                    {"attributeName": "altitude", "fillIn": "0"},
                    {"attributeName": "flagged", "fillIn": "false"},
                    {"attributeName": "zipDescription", "fillIn": "empty"},
                    {"attributeName": "additionalDeliveryCharge", "fillIn": "1"},
                ],
            }
        ]
    },
    "postStorageStage": {
        "features": [
            {
                "featureName": "QuantitativeSketchFor2Attributes",
                "attributes": [
                    "deliveryTime",
                    "orderAmount",
                    "deliveryTimestamp",
                    "latitude",
                    "longitude",
                    "altitude",
                    "additionalDeliveryCharge",
                ],
                "dataSketches": ["Moments", "Quantiles", "DistinctCount", "SampleCounts"],
                "featureInterval": 5000,
            },
            {
                "featureName": "NonQuantitativeSketches",
                "attributes": ["zipcode", "orderId", "gender", "country", "zipDescription"],
                "dataSketches": ["DistinctCount", "SampleCounts"],
                "featureInterval": 5000,
            },
        ]
    },
}

CONTEXT_LEAST = {
    "contextName": "contextLeast",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "ip", "type": "string", "tags": {"category": "Dimension"}},
        {"attributeName": "site", "type": "string", "tags": {"category": "Description"}},
    ],
    "primaryKey": ["ip"],
}

CONTEXT_ALL_TYPES = {
    "contextName": "contextAllTypes",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "areaCode", "type": "integer"},
        {"attributeName": "country", "type": "string"},
        {"attributeName": "altitude", "type": "decimal"},
        {"attributeName": "longitude", "type": "decimal"},
        {"attributeName": "flagged", "type": "boolean"},
        {"attributeName": "photo", "type": "blob"},
    ],
    "primaryKey": ["areaCode"],
}

SIGNAL_INFER_TAGS = {
    "signalName": "inferTags",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "orderId", "type": "Integer"},
        {"attributeName": "orderAmount", "type": "Decimal"},
        {"attributeName": "orderReceivedTime", "type": "Integer"},
        {"attributeName": "zipcode", "type": "String"},
        {"attributeName": "state", "type": "String"},
        {"attributeName": "itemId", "type": "Integer"},
        {"attributeName": "item", "type": "String"},
    ],
    "enrich": {
        "enrichments": [
            {
                "enrichmentName": "location",
                "foreignKey": ["zipcode"],
                "contextName": "inferTagsContext",
                "missingLookupPolicy": "Reject",
                "contextAttributes": [
                    {"attributeName": "country"},
                    {"attributeName": "latitude"},
                    {"attributeName": "longitude"},
                    {"attributeName": "altitude"},
                    {"attributeName": "flagged"},
                ],
            }
        ]
    },
}

CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE = {
    "contextName": "contextWithMultiDimensionalFeature",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "storeId", "type": "Integer"},
        {"attributeName": "city", "type": "String"},
        {"attributeName": "storeType", "type": "Integer"},
        {"attributeName": "numProducts", "type": "Integer"},
        {"attributeName": "quantity", "type": "Integer"},
    ],
    "primaryKey": ["storeId"],
    "auditEnabled": True,
    "features": [
        {
            "featureName": "byCityStoreType",
            "dimensions": ["city", "storeType"],
            "attributes": ["numProducts", "quantity"],
            "featureInterval": 15000,
            "indexed": True,
            "indexType": "ExactMatch",
        }
    ],
}

with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
    try:
        sadmin.delete_tenant(TEST_TENANT_NAME)
    except ServiceError:
        pass
    sadmin.create_tenant({"tenantName": TEST_TENANT_NAME})

    try:
        sadmin.delete_tenant(TEST_TENANT_NAME2)
    except ServiceError:
        pass
    sadmin.create_tenant({"tenantName": TEST_TENANT_NAME2})

with bios.login(ep_url(), f"{admin_user}@{TEST_TENANT_NAME}", admin_pass) as admin:
    admin.create_signal(SIGNAL_MINIMUM)
    admin.create_signal(SIGNAL_SIMPLE_ROLLUP)
    admin.create_signal(SIGNAL_ROLLUP_TWO_VALUES)
    admin.create_context(CONTEXT_INFER_TAGS)
    admin.create_signal(SIGNAL_SKETCH)
    admin.create_context(CONTEXT_LEAST)
    admin.create_context(CONTEXT_ALL_TYPES)
    admin.create_signal(SIGNAL_INFER_TAGS)
    admin.create_context(CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE)


def sleep_until_time(sleep_until):
    sleep_time = sleep_until - time.time()
    # (naresh) need to investiage why would this happen
    sleep_time = max(sleep_time, 1)
    print(f"sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)


with bios.login(ep_url(), f"{ingest_user}@{TEST_TENANT_NAME}", ingest_pass) as ingest:

    ingest.execute(
        bios.isql()
        .upsert()
        .into(CONTEXT_INFER_TAGS.get("contextName"))
        .csv_bulk(
            [
                "zip0,USA,40.35,-150.42,1078,True,a,1",
                "zip1,USA,41.35,-151.42,1178,False,a,1",
                "zip2,USA,42.35,-152.42,1278,False,a,1",
                "zip3,USA,43.35,-153.42,1378,False,a,1",
                "zip4,USA,44.35,-154.42,1478,False,a,1",
                "zip5,India,45.35,-155.42,1578,False,a,1",
                "zip6,India,46.35,-156.42,1678,False,a,1",
                "zip7,India,47.35,-157.42,1778,False,a,1",
                "zip8,Canada,48.35,-158.42,1878,False,a,1",
                "zip9,Canada,49.35,-159.42,1978,False,a,1",
            ]
        )
        .build()
    )
    ingest.execute(
        bios.isql()
        .upsert()
        .into(CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE.get("contextName"))
        .csv_bulk(
            [
                "10001,Menlo Park,100,8,1946",
                "10002,Menlo Park,101,10,2230",
                "10003,Palo Alto,100,5,819",
                "10004,Menlo Park,102,19,3100",
                "10005,Santa Clara,101,8,1711",
                "10006,Menlo Park,101,9,1100",
                "10007,Santa Clara,102,12,9876",
            ]
        )
        .build()
    )
    time.sleep(1)
    status = ingest.feature_status(
        CONTEXT_WITH_MULTI_DIMENSIONAL_FEATURE.get("contextName"), feature="byCityStoreType"
    )

    # Entries to insert into SIGNAL_INFER_TAGS
    entries = [""] * 200
    for j in range(20):
        for i in range(10):
            entries[(j * 10) + i] = (
                str(3000000 + j * 10 + i)
                + ","
                + str(random.random() * 100)
                + ","
                + str(int(time.time() - i * j) - 800)
                + ",zip"
                + str(i)
                + ",S"
                + str(i)
                + ","
                + str(random.randint(1000, 9999))
                + ",Item"
                + str(random.randint(1000, 9999))
            )

    # Using 10 second intervals for some of the queries here. Also, 1 minute and 5 minute windows
    # in some other places. To ensure we get events ingested
    # in exactly successive time windows, sleep until 1 second after the next multiple of 1 minute,
    # and then do the ingestion.
    start_time = math.ceil(time.time() / 60) * 60 + 1.0
    sleep_until_time(start_time)

    statement = bios.isql().insert().into(SIGNAL_MINIMUM.get("signalName")).csv("hello").build()
    response = ingest.execute(statement)
    resp_simple = response.records[0]
    print("timestamp(simple):", resp_simple.timestamp)

    statement2 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SIMPLE_ROLLUP.get("signalName"))
        .csv_bulk(["us,ca,2", "us,or,5", "japan,東京,7"])
        .build()
    )
    resp_rollup = ingest.execute(statement2).records[0]
    print("timestamp(rollup):", resp_rollup.timestamp)

    statement3 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 900),
                "2,2.0002,20000,zip2,Male," + str(int(time.time()) - 900),
                "3,3.0003,30000,zip3,Male," + str(int(time.time()) - 900),
            ]
        )
        .build()
    )
    resp_sketch = ingest.execute(statement3).records[0]
    print("timestamp(sketch):", resp_sketch.timestamp)

    statement4 = (
        bios.isql()
        .insert()
        .into(SIGNAL_ROLLUP_TWO_VALUES.get("signalName"))
        .csv_bulk(
            [
                "US,California,3,52.4",
                "Japan,東京,7,12.6",
                "US,Utah,33,71.1",
                "Japan,大阪,11,8.4",
                "US,California,22,31.6",
                "US,Oregon,5,83.3",
                "US,Utah,91,12.4",
                "Australia,Queensland,10,4.2",
                "India,Goa,17,1.2",
                "US,California,1,111.2",
            ]
        )
        .build()
    )
    resp_rollup_two_values = ingest.execute(statement4).records[0]
    print("timestamp(rollup_two_values):", resp_rollup_two_values.timestamp)
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 5)
    ingest.execute(
        bios.isql().insert().into(SIGNAL_MINIMUM.get("signalName")).csv("world").build()
    )
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "4,4.0004,40000,zip4,Male," + str(int(time.time()) - 900),
                "5,5.0005,50000,zip5,Male," + str(int(time.time()) - 900),
                "6,6.0006,60000,zip6,Male," + str(int(time.time()) - 900),
                "7,7.0007,70000,zip7,Male," + str(int(time.time()) - 900),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 10)
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "8,8.0008,80000,zip8,Male," + str(int(time.time()) - 900),
                "9,9.0009,90000,zip9,Male," + str(int(time.time()) - 900),
                "10,10.0010,100000,zip10,Male," + str(int(time.time()) - 900),
                "11,11.0011,110000,zip11,Female," + str(int(time.time()) - 900),
                "12,12.0012,120000,zip12,Male," + str(int(time.time()) - 900),
                "13,13.0013,130000,zip13,Male," + str(int(time.time()) - 900),
                "14,14.0014,140000,zip14,Male," + str(int(time.time()) - 900),
                "15,15.0015,150000,zip15,Male," + str(int(time.time()) - 900),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 15)
    ingest.execute(bios.isql().insert().into(SIGNAL_MINIMUM.get("signalName")).csv("!").build())
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "16,16.0016,160000,zip16,Male," + str(int(time.time()) - 900),
                "17,17.0017,170000,zip17,Male," + str(int(time.time()) - 900),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 20)
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(["18,18.0018,180000,zip18,Unknown," + str(int(time.time()) - 900)])
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 25)
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "19,19.0019,190000,zip19,Male," + str(int(time.time()) - 900),
                "20,20.0020,200000,zip20,Male," + str(int(time.time()) - 900),
                "21,21.0021,210000,zip21,Female," + str(int(time.time()) - 900),
                "22,22.0022,220000,zip22,Male," + str(int(time.time()) - 900),
                "23,23.0023,230000,zip23,Male," + str(int(time.time()) - 900),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 30)
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 35)
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "2,2.0002,20000,zip2,Male," + str(int(time.time()) - 600),
                "2,2.0002,20000,zip2,Male," + str(int(time.time()) - 600),
                "3,3.0003,30000,zip3,Male," + str(int(time.time()) - 600),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 40)
    statement5 = (
        bios.isql()
        .insert()
        .into(SIGNAL_SKETCH.get("signalName"))
        .csv_bulk(
            [
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "1,1.0001,10000,zip1,Female," + str(int(time.time()) - 600),
                "4,4.0004,40000,zip4,Male," + str(int(time.time()) - 600),
            ]
        )
        .build()
    )
    ingest.execute(statement5).records[0]
    ingest.execute(
        bios.isql().insert().into(SIGNAL_INFER_TAGS.get("signalName")).csv_bulk(entries).build()
    )

    sleep_until_time(start_time + 60)

    statement6 = (
        bios.isql()
        .insert()
        .into(SIGNAL_ROLLUP_TWO_VALUES.get("signalName"))
        .csv_bulk(
            [
                "Australia,New South Wales,51,38.2",
                "Japan,Hok海do,73,12.6",
                "Australia,Queensland,11,18.1",
                "US,California,95,88.0",
                "Japan,Hok海do,5,191.4",
                "Australia,Queensland,14,11.2",
                "US,California,10,70.9",
                "US,Utah,10,11.3",
                "Australia,Queensland,17,15.4",
            ]
        )
        .build()
    )
    ingest.execute(statement6).records[0]

    # Wait for the next window of default sketches to be built and then trigger running inference
    # by changing signal schema.
    next_sketch_time = math.floor(start_time / 300) * 300 + 300
    sleep_until_time(next_sketch_time + 10)
    SIGNAL_INFER_TAGS.get("attributes").append(
        {"attributeName": "dummy", "type": "Integer", "default": "0"}
    )
    with bios.login(ep_url(), f"{admin_user}@{TEST_TENANT_NAME}", admin_pass) as admin:
        admin.update_signal(SIGNAL_INFER_TAGS.get("signalName"), SIGNAL_INFER_TAGS)
        admin.feature_refresh(CONTEXT_INFER_TAGS.get("contextName"))


# Generate test-env.sh
env_file_path = os.path.dirname(sys.argv[0]) + "/test-env.sh"
with open(env_file_path, "w") as f:
    f.write("export TIMESTAMP_MINIMUM={}\n".format(resp_simple.timestamp))
    f.write("export TIMESTAMP_SIMPLE_ROLLUP={}\n".format(resp_rollup.timestamp))
    f.write("export TIMESTAMP_SKETCH={}\n".format(resp_sketch.timestamp))
    f.write("export TIMESTAMP_ROLLUP_TWO_VALUES={}\n".format(resp_rollup_two_values.timestamp))
