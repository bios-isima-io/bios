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

import os
import time

import bios
from bios import ErrorCode, ServiceError
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_single_endpoint, sadmin_pass, sadmin_user

TENANT_NAME = "biosClientE2eTest"
SIGNAL_NAME = "basic"
TENANT_CONFIG = {"name": TENANT_NAME}

SIGNAL_CONFIG = {
    "signalName": SIGNAL_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "one", "type": "string"},
        {"attributeName": "two", "type": "string", "default": ""},
        {"attributeName": "three", "type": "integer"},
    ],
}
CONTEXT_TENANT_NAME = TENANT_NAME
CONTEXT_TENANT_CONFIG = TENANT_CONFIG
ADMIN_USER = admin_user + "@" + CONTEXT_TENANT_NAME
SIMPLE_CONTEXT_NAME = "simpleContext"
SIMPLE_CONTEXT = {
    "contextName": SIMPLE_CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "the_key", "type": "integer"},
        {"attributeName": "the_value", "type": "string"},
    ],
    "primaryKey": ["the_key"],
}
STORE_CONTEXT_NAME = "storeContext"
STORE_CONTEXT = {
    "contextName": STORE_CONTEXT_NAME,
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "storeId",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "zipCode", "type": "Integer", "default": -1},
        {"attributeName": "address", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["storeId"],
}

CONTEXT_WITH_FEATURE_NAME = "contextWithFeature"
CONTEXT_WITH_FEATURE = {
    "contextName": CONTEXT_WITH_FEATURE_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "storeId", "type": "Integer"},
        {"attributeName": "zipCode", "type": "Integer"},
        {"attributeName": "address", "type": "String"},
        {"attributeName": "numProducts", "type": "Integer"},
    ],
    "primaryKey": ["storeId"],
    "auditEnabled": True,
    "features": [
        {
            "featureName": "byZipCode",
            "dimensions": ["zipCode"],
            "attributes": ["numProducts"],
            "featureInterval": 15000,
            "aggregated": True,
            "indexed": True,
            "indexType": "RangeQuery",
        }
    ],
}

CUSTOMER_CONTEXT_NAME = "customer_context"
COUNTRY_CONTEXT_NAME = "country_context"
COUNTRY_CONTEXT_MD_NAME = "country_context_md"
SIGNAL_ENRICH_GLOBAL_MVP_NAME = "enrich_global_mvp_policy"
SIGNAL_ENRICH_LOCAL_MVP_NAME = "enrich_local_mvp_policy"
SIGNAL_ENRICH_MULTI_MVP_NAME = "enrich_multi_mvp_policy"

CUSTOMER_CONTEXT = {
    "contextName": CUSTOMER_CONTEXT_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "customer_id", "type": "integer"},
        {"attributeName": "name", "type": "string"},
        {
            "attributeName": "gender",
            "type": "String",
            "allowedValues": ["MALE", "FEMALE", "UNKNOWN"],
        },
    ],
    "primaryKey": ["customer_id"],
}

COUNTRY_CONTEXT = {
    "contextName": COUNTRY_CONTEXT_NAME,
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "country_id",
            "type": "integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "country", "type": "string", "default": "n/a"},
        {"attributeName": "state", "type": "string", "default": "n/a"},
        {"attributeName": "city", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["country_id"],
}

COUNTRY_CONTEXT_MD = {
    "contextName": COUNTRY_CONTEXT_MD_NAME,
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {"attributeName": "country", "type": "string", "default": "n/a"},
        {"attributeName": "state", "type": "string", "default": "n/a"},
        {"attributeName": "city", "type": "String", "default": "n/a"},
        {"attributeName": "value", "type": "Integer", "default": -1},
    ],
    "primaryKey": ["country", "state", "city"],
}

SIGNAL_ENRICH_GLOBAL_MVP = {
    "signalName": SIGNAL_ENRICH_GLOBAL_MVP_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "customer_id", "type": "integer"},
        {"attributeName": "travel_destination", "type": "string"},
    ],
    "enrich": {
        "missingLookupPolicy": "StoreFillInValue",
        "enrichments": [
            {
                "enrichmentName": "join_customer_info",
                "foreignKey": ["customer_id"],
                "contextName": CUSTOMER_CONTEXT_NAME,
                "contextAttributes": [
                    {"attributeName": "name", "as": "customer_name", "fillIn": "empty"}
                ],
            }
        ],
    },
}

SIGNAL_ENRICH_LOCAL_MVP = {
    "signalName": SIGNAL_ENRICH_LOCAL_MVP_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "customer_id", "type": "integer"},
        {"attributeName": "travel_destination", "type": "string"},
    ],
    "enrich": {
        "missingLookupPolicy": "StoreFillInValue",
        "enrichments": [
            {
                "enrichmentName": "join_customer_info",
                "foreignKey": ["customer_id"],
                "contextName": CUSTOMER_CONTEXT_NAME,
                "missingLookupPolicy": "Reject",
                "contextAttributes": [
                    {
                        "attributeName": "name",
                        "as": "customer_name",
                    }
                ],
            }
        ],
    },
}

SIGNAL_ENRICH_MULTI_MVP = {
    "signalName": SIGNAL_ENRICH_MULTI_MVP_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {"attributeName": "customer_id", "type": "Integer"},
        {"attributeName": "country_id", "type": "Integer"},
        {"attributeName": "product", "type": "String"},
    ],
    "enrich": {
        "missingLookupPolicy": "StoreFillInValue",
        "enrichments": [
            {
                "enrichmentName": "join_customer_info",
                "foreignKey": ["customer_id"],
                "missingLookupPolicy": "StoreFillInValue",
                "contextName": CUSTOMER_CONTEXT_NAME,
                "contextAttributes": [
                    {"attributeName": "name", "as": "customer_name", "fillIn": "empty"},
                    {"attributeName": "gender", "fillIn": "UNKNOWN"},
                ],
            },
            {
                "enrichmentName": "join_country_context",
                "foreignKey": ["country_id"],
                "missingLookupPolicy": "Reject",
                "contextName": COUNTRY_CONTEXT_NAME,
                "contextAttributes": [
                    {"attributeName": "country"},
                    {"attributeName": "state"},
                    {"attributeName": "city"},
                ],
            },
        ],
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

CONTEXT_COUNTER_SNAPSHOTS_NAME = "inventoryCounterSnapshots"
CONTEXT_COUNTER_SNAPSHOTS = {
    "contextName": CONTEXT_COUNTER_SNAPSHOTS_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "fcType",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "fcId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "itemId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "timestamp",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Timestamp", "unit": "UuidTime"},
        },
        {
            "attributeName": "onHandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
        {
            "attributeName": "demandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
    ],
    "primaryKey": ["fcType", "fcId", "itemId"],
}

SIGNAL_COUNTER_UPDATES_NAME = "inventoryCounterUpdates"
SIGNAL_COUNTER_UPDATES = {
    "signalName": SIGNAL_COUNTER_UPDATES_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "fcType",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "fcId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "itemId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "operation",
            "type": "String",
            "allowedValues": ["change", "set"],
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "onHandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
        {
            "attributeName": "demandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "compactor",
                "dimensions": ["fcType", "fcId", "itemId", "operation"],
                "attributes": ["onHandValue", "demandValue"],
                "featureInterval": 5000,
                "materializedAs": "AccumulatingCount",
                "indexOnInsert": True,
                "featureAsContextName": "inventoryCounterSnapshots",
            },
        ]
    },
}

COUNTER_MICROSERVICE_TENANT_NAME = "inventoryMicroservice"
CONTEXT_COUNTER_MICROSERVICE_SNAPSHOTS_NAME = "inventoryCounterSnapshots"
CONTEXT_COUNTER_MICROSERVICE_SNAPSHOTS = {
    "contextName": CONTEXT_COUNTER_SNAPSHOTS_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "itemNumber",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "fulfillmentCenter",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "timestamp",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Timestamp", "unit": "UuidTime"},
        },
        {
            "attributeName": "onHandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
        {
            "attributeName": "demandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
    ],
    "primaryKey": ["itemNumber", "fulfillmentCenter"],
}

SIGNAL_COUNTER_MICROSERVICE_UPDATES_NAME = "inventoryCounterUpdates"
SIGNAL_COUNTER_MICROSERVICE_UPDATES = {
    "signalName": SIGNAL_COUNTER_UPDATES_NAME,
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "itemNumber",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "fulfillmentCenter",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "operation",
            "type": "String",
            "allowedValues": ["change", "set"],
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
        },
        {
            "attributeName": "onHandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
        {
            "attributeName": "demandValue",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
        },
    ],
    "postStorageStage": {
        "features": [
            {
                "featureName": "compactor",
                "dimensions": ["itemNumber", "fulfillmentCenter", "operation"],
                "attributes": ["onHandValue", "demandValue"],
                "featureInterval": 5000,
                "materializedAs": "AccumulatingCount",
                "indexOnInsert": "True",
                "featureAsContextName": "inventoryCounterSnapshots",
            },
        ]
    },
}

CONTEXT_PRODUCTS = {
    "contextName": "products",
    "missingAttributePolicy": "Reject",
    "attributes": [
        {
            "attributeName": "productId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "siteCountry",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "productName",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "lowestPrice",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Money"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "priceRangeStart",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Money"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "priceRangeEnd",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Money"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "relativePriceRangeSize",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Ratio"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "currency",
            "type": "String",
            "tags": {"category": "Description"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "productUrl",
            "type": "String",
            "tags": {"category": "Description", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "imageUrl",
            "type": "String",
            "tags": {"category": "Description", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "rating",
            "type": "Decimal",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "PlainNumber"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "brand",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "inStock",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "PlainNumber"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "reviews",
            "type": "Integer",
            "tags": {"category": "Quantity", "kind": "Dimensionless", "unit": "Count"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "parentProductId",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "categoryId1",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "categoryName1",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "categoryId2",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "categoryName2",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
        {
            "attributeName": "categoryId3",
            "type": "Integer",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": 0,
        },
        {
            "attributeName": "categoryName3",
            "type": "String",
            "tags": {"category": "Dimension", "firstSummary": "WORD_CLOUD"},
            "missingAttributePolicy": "StoreDefaultValue",
            "default": "MISSING",
        },
    ],
    "primaryKey": ["productId"],
    "auditEnabled": True,
}


QOS_TENANT_NAME = "biosContextLookupQosJava"
QOS_STORE_CONTEXT_NAME = "lookupQosContext"
QOS_STORE_CONTEXT = {
    "contextName": QOS_STORE_CONTEXT_NAME,
    "missingAttributePolicy": "StoreDefaultValue",
    "attributes": [
        {
            "attributeName": "id",
            "type": "Integer",
            "missingAttributePolicy": "Reject",
        },
        {"attributeName": "zipCode", "type": "Integer", "default": -1},
        {"attributeName": "address", "type": "String", "default": "n/a"},
    ],
    "primaryKey": ["id"],
}

(HOST, PORT, _, _, _) = get_single_endpoint()

FAST_TRACK_SIGNALS = (
    f"{TENANT_NAME}.{SIGNAL_COUNTER_UPDATES_NAME},"
    f"{COUNTER_MICROSERVICE_TENANT_NAME}.{SIGNAL_COUNTER_UPDATES_NAME}"
)

SHARED_PROPERTIES = {
    "prop.maintenance.fastTrackWorkerInterval": "1000",
    "prop.maintenance.fastTrackSignals": FAST_TRACK_SIGNALS,
    "prop.maintenance.fastTrackMargin": "0",
    f"tenant.{QOS_TENANT_NAME}.prop.lookupQosThresholdMillis": "200",
    "prop.test.analysis.MultiGetDelayContextName": QOS_STORE_CONTEXT["contextName"],
    "prop.counters.host": HOST,
    "prop.counters.port": str(PORT),
    "prop.counters.email": f"admin@{COUNTER_MICROSERVICE_TENANT_NAME}",
    "prop.counters.password": "admin",
    "prop.products.host": HOST,
    "prop.products.port": str(PORT),
    "prop.products.email": f"admin@{TENANT_NAME}",
    "prop.products.password": "admin",
    "prop.sharedPropertiesCacheExpiryMillis": "1000",
}


def setup_tenant_config(tenant_name):
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        try:
            sadmin.delete_tenant(tenant_name)
        except ServiceError as err:
            if err.error_code != ErrorCode.NO_SUCH_TENANT:
                raise
        sadmin.create_tenant({"tenantName": tenant_name})
        print("created tenant :", tenant_name)


def setup_basic_test_config():
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin_session:
        create_signal(admin_session, SIGNAL_CONFIG)


def upsert_entries(session, context_name, records):
    if not isinstance(records, list):
        records = [records]
    request = bios.isql().upsert().into(context_name).csv_bulk(records).build()
    session.execute(request)


def setup_context_test_config():
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        create_context(admin, SIMPLE_CONTEXT)
        create_context(admin, STORE_CONTEXT)
        create_context(admin, CONTEXT_WITH_FEATURE)
        print(" ... upserting entries")
        upsert_entries(
            admin,
            CONTEXT_WITH_FEATURE_NAME,
            [
                "10001,94061,Roosevelt,20",
                "10002,94061,Woodside,31",
                "10003,94064,The Willows,17",
                "10004,94066,Menlo Park,19",
                "10005,94064,Santa Clara,8",
                "10006,94067,Palo Alto,9",
                "10007,94061,Roosevelt,12",
            ],
        )
        print(" ... sleeping for 35 seconds to ensure context features/indexes are refreshed.")
        time.sleep(35)


def setup_enrich_test_config():
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        create_context(admin, CUSTOMER_CONTEXT)
        create_context(admin, COUNTRY_CONTEXT)
        create_context(admin, COUNTRY_CONTEXT_MD)
        create_signal(admin, SIGNAL_ENRICH_LOCAL_MVP)
        create_signal(admin, SIGNAL_ENRICH_GLOBAL_MVP)
        create_signal(admin, SIGNAL_ENRICH_MULTI_MVP)
        create_signal(admin, SIGNAL_ROLLUP_TWO_VALUES)


def setup_counters_test_config():
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        create_context(admin, CONTEXT_COUNTER_SNAPSHOTS)
        create_signal(admin, SIGNAL_COUNTER_UPDATES)


def setup_counters_microservice_test_config():
    with bios.login(ep_url(), f"admin@{COUNTER_MICROSERVICE_TENANT_NAME}", admin_pass) as admin:
        create_context(admin, CONTEXT_COUNTER_MICROSERVICE_SNAPSHOTS)
        create_signal(admin, SIGNAL_COUNTER_MICROSERVICE_UPDATES)


def setup_products_test_config():
    with bios.login(ep_url(), ADMIN_USER, admin_pass) as admin:
        create_context(admin, CONTEXT_PRODUCTS)


def create_signal(session, signal_config):
    print(f"Creating signal {signal_config.get('signalName')} ...", end="")
    session.create_signal(signal_config)
    print("done")


def create_context(session, context_config):
    print(f"Creating context {context_config.get('contextName')} ...", end="")
    session.create_context(context_config)
    print("done")


def setup_qos_test_config():
    with bios.login(ep_url(), f"{admin_user}@{QOS_TENANT_NAME}", admin_pass) as admin:
        create_context(admin, QOS_STORE_CONTEXT)


def setup_shared_properties():
    with bios.login(ep_url(), sadmin_user, sadmin_pass) as sadmin:
        for name, value in SHARED_PROPERTIES.items():
            sadmin.set_property(name, value)


# create JavaSDK Test Config
if __name__ == "__main__":
    setup_tenant_config(TENANT_NAME)
    setup_basic_test_config()
    setup_counters_test_config()
    setup_products_test_config()
    setup_tenant_config(COUNTER_MICROSERVICE_TENANT_NAME)
    setup_counters_microservice_test_config()
    setup_context_test_config()
    setup_enrich_test_config()
    setup_tenant_config(QOS_TENANT_NAME)
    setup_qos_test_config()
    setup_shared_properties()
    print("done")
