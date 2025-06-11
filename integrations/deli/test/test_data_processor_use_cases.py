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
import sys
import time
import unittest
from datetime import datetime, timezone

import pytest

from deli.configuration import DataPickupSpec
from deli.data_processor import DataProcessor, ImportedModules
from deli.event_spec import EventSpec


class TestDataProcessorUseCases(unittest.TestCase):
    """Tests data processor"""

    def test_webhook_use_case(self):
        """Use configuration for webhook example"""
        pickup_config_dict = {
            "attributeSearchPath": "/profiles/*/event_properties",
            "filters": [
                {
                    "sourceAttributeName": "/key_values/signal",
                    "filter": "lambda value: value == 'impressionSignal'",
                }
            ],
            "attributes": [
                {"sourceAttributeName": "trigger", "as": "impressionTrigger"},
                {"sourceAttributeName": "destination"},
                {"sourceAttributeName": "destination_id", "as": "destinationId"},
                {"sourceAttributeName": "click_id", "as": "clickId"},
                {
                    "sourceAttributeNames": ["element_unique_name", "platform"],
                    "transforms": [
                        {
                            "rule": "lambda un, p: f'{un}_{p}'",
                            "as": "elementUniqueNameAndPlatform",
                        },
                    ],
                },
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)
        processor = DataProcessor("webhook_example", pickup_spec)
        processor.set_in_message_auth_attributes("key_values/user", "key_values/pass")

        with open("test/resources/impression_signal_message.json", "r", encoding="utf-8") as file:
            in_message = json.load(file)
        records, user, password = processor.process(in_message)
        self.assertEqual(len(records), 3)
        self.assertEqual(
            records[0].attributes,
            {
                "impressiontrigger": "Trigger_a",
                "destination": "destination_a",
                "destinationid": 6,
                "clickid": 606,
                "elementuniquenameandplatform": "element0001_platform001",
            },
        )
        assert user == "admin@webhookTest"
        assert password == "admin"

        # test filter
        assert (
            processor.process(
                {"key_values": {"user": "admin@webhookTest", "pass": "admin", "signal": "clicks"}}
            )
            is None
        )

    def test_array_search_path(self):
        """Use configuration for webhook example"""
        pickup_config_dict = {
            "filters": [
                {
                    "sourceAttributeName": "key_values/signal",
                    "filter": "lambda value:value == 'sessionStartSignal'",
                }
            ],
            "attributeSearchPath": "profiles/*",
            "attributes": [
                {"sourceAttributeName": "/targetId", "as": "targetId"},
                {"sourceAttributeName": "objectId"},
                {"sourceAttributeName": "event_properties/is_logged_in", "as": "isLoggedIn"},
                {
                    "sourceAttributeName": "event_properties/has_delivered_order",
                    "as": "hasDeliveredOrder",
                },
                {
                    "sourceAttributeName": "event_properties/client_event_timestamp",
                    "as": "clientEventTimestamp",
                },
                {
                    "sourceAttributeName": "event_properties/client_event_timestamp",
                    "processes": [
                        {"processorName": "get_time_lag_ms", "method": "get_time_lag_ms"}
                    ],
                    "as": "clientEventTimestampLag",
                },
                {
                    "sourceAttributeName": "event_properties/client_event_timestamp",
                    "processes": [
                        {
                            "processorName": "ingest_utils",
                            "method": "timestamp_window_fifteen_minutes_epoch",
                        }
                    ],
                    "as": "clientEventTimeWindow",
                },
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)

        with open("test/resources/get_time_lag_ms.py", "r", encoding="utf-8") as file:
            get_time_lag_ms = file.read()
        with open("test/resources/ingest_utils.py", "r", encoding="utf-8") as file:
            ingest_utils = file.read()
        processor_configs = {"get_time_lag_ms": get_time_lag_ms, "ingest_utils": ingest_utils}
        imported_modules = ImportedModules(processor_configs).load()

        processor = DataProcessor("test", pickup_spec, imported_modules)
        processor.set_in_message_auth_attributes("key_values/user", "key_values/pass")

        with open("test/resources/session_start_message.json", "r", encoding="utf-8") as file:
            in_message = json.load(file)

        now = int(time.time() * 1000)
        event_timestamp = now - 300000
        in_message["profiles"][0]["event_properties"]["client_event_timestamp"] = str(
            event_timestamp
        )

        timestamp_window = (event_timestamp % 86400000) - (event_timestamp % 900000)

        records, user, password = processor.process(in_message)

        self.assertEqual(user, "support+test@isima.io")
        self.assertEqual(password, "P@55W0RD")

        assert len(records) == 3

        attr0 = records[0].attributes
        assert attr0.get("clienteventtimestamplag") > 0
        del attr0["clienteventtimestamplag"]
        assert records[0].attributes == {
            "targetid": 1628862519,
            "objectid": "1eae7553e2d940aaacd654135756cc80",
            "isloggedin": False,
            "hasdeliveredorder": None,
            "clienteventtimestamp": str(event_timestamp),
            "clienteventtimewindow": timestamp_window,
        }

        assert records[1].attributes == {
            "targetid": 1628862519,
            "objectid": "937fc83018f549b5ba3ed12e10bdd70a",
            "isloggedin": True,
            "hasdeliveredorder": None,
            "clienteventtimestamp": None,
            "clienteventtimestamplag": 0,
            "clienteventtimewindow": 0,
        }

        assert records[2].attributes == {
            "targetid": 1628862519,
            "objectid": "f4558cd96b564fcfa613994f45c83f6f",
            "isloggedin": False,
            "hasdeliveredorder": False,
            "clienteventtimestamp": None,
            "clienteventtimestamplag": 0,
            "clienteventtimewindow": 0,
        }

        in_message["key_values"]["signal"] = "noSuchSignal"

        # filter do not hit
        assert processor.process(in_message) is None

    def test_array_attribute_names(self):
        """Data pickup spec for an array of source attribute names"""
        pickup_config_dict = {
            "filters": [
                {
                    "sourceAttributeName": "key_values/signal",
                    "filter": "lambda value:value == 'AddtoCart'",
                }
            ],
            "attributeSearchPath": "profiles/*",
            "attributes": [
                {"sourceAttributeName": "identity", "as": "identity"},
                {
                    "sourceAttributeName": "event_properties/client_event_timestamp",
                    "processes": [
                        {
                            "processorName": "ingest_utils",
                            "method": "to_millisecond_timestamp_epoch",
                        }
                    ],
                    "as": "clientEventTimestamp",
                },
                {
                    "sourceAttributeName": "event_properties/client_event_timestamp",
                    "processes": [{"processorName": "ingest_utils", "method": "get_today"}],
                    "as": "clientEventDate",
                },
                {
                    "sourceAttributeNames": [
                        "event_properties/order_type",
                        "event_properties/product_id",
                    ],
                    "processes": [{"processorName": "ingest_utils", "method": "atc_vertical"}],
                    "as": "productId",
                },
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)

        with open("test/resources/ingest_utils.py", "r", encoding="utf-8") as file:
            ingest_utils = file.read()
        processor_configs = {"ingest_utils": ingest_utils}
        imported_modules = ImportedModules(processor_configs).load()

        processor = DataProcessor("test", pickup_spec, imported_modules)
        processor.set_in_message_auth_attributes("key_values/user", "key_values/pass")

        with open("test/resources/add_to_cart_message.json", "r", encoding="utf-8") as file:
            in_message = json.load(file)

        records, user, password = processor.process(in_message)

        self.assertEqual(user, "insert+test@isima.io")
        self.assertEqual(password, "P@55W0RD")

        assert len(records) == 2

        assert records[0].attributes == {
            "identity": "87439215",
            "clienteventtimestamp": 1682510726483,
            "clienteventdate": 1682467200,
            "productid": "-1",
        }

        assert records[1].attributes == {
            "identity": "60958327",
            "clienteventtimestamp": 1682510729968,
            "clienteventdate": 1682467200,
            "productid": "98765",
        }

        in_message["key_values"]["signal"] = "noSuchSignal"

        # filter do not hit
        assert processor.process(in_message) is None

    def test_checkout_items_flow(self):
        """Test a complex pickup config example"""
        pickup_config_dict = {
            "filters": [
                {
                    "sourceAttributeName": "pageType",
                    "filter": "lambda value:value == 'checkout / thankyou'",
                }
            ],
            "attributeSearchPath": "transactionItems/*",
            "attributes": [
                {"sourceAttributeName": "/transactionId", "as": "orderId"},
                {"sourceAttributeName": "modification_id", "as": "orderItemId"},
                {
                    "sourceAttributeNames": ["product_id", "/pageHostName"],
                    "transforms": [
                        {
                            "rule": "lambda v, p: \".\".join(p.split('.')[-2:])+'_'+str(v)",
                            "as": "productId",
                        }
                    ],
                },
                {"sourceAttributeName": "amount", "as": "quantity"},
                {"sourceAttributeName": "name"},
                {"sourceAttributeName": "sell_price", "as": "sellPrice"},
                {
                    "sourceAttributeNames": ["category_id", "/pageHostName"],
                    "transforms": [
                        {
                            "rule": "lambda c, p: \".\".join(p.split('.')[-2:])+'_'+str(c)",
                            "as": "categoryId",
                        }
                    ],
                },
                {"sourceAttributeName": "category_path", "as": "categoryPath"},
                {
                    "sourceAttributeNames": ["amount", "sell_price"],
                    "transforms": [
                        {"rule": "lambda a, b: (float(a)* float(b))", "as": "saleTotal"}
                    ],
                },
                {
                    "sourceAttributeNames": ["/pageHostName"],
                    "transforms": [{"rule": "lambda p: p.split('.')[-1]", "as": "country"}],
                },
                {"sourceAttributeName": "abTest"},
                {
                    "sourceAttributeName": "${x-real-ip}",
                    "transforms": [{"rule": "lambda ip: ip.split(',')[0]", "as": "ipAddr"}],
                },
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)

        processor = DataProcessor("test", pickup_spec)

        with open("test/resources/checkout_thankyou_message.json", "r", encoding="utf-8") as file:
            in_message = json.load(file)

        headers = {f"${{{name}}}": value for name, value in in_message.get("headers").items()}
        body = in_message.get("body")
        records, _, _ = processor.process(body, attributes=headers)

        assert len(records) == 1

        assert records[0].attributes == {
            "orderid": "62587139",
            "orderitemid": "96852143",
            "productid": "pigu.lt_74589631",
            "quantity": "1",
            "name": "Exciting, Scrumptious, Zesty, Whimsical, Radiant",
            "sellprice": "16.99",
            "categoryid": "pigu.lt_None",
            "categorypath": None,
            "saletotal": 16.99,
            "country": "lt",
            "abtest": None,
            "ipaddr": " 10.20.30.40",
        }

        in_message["pageType"] = "none"

        # filter do not hit
        assert processor.process(in_message) is None

    def test_deletion_spec(self):
        """Test deletion spec"""
        pickup_config_dict = {
            "deletionSpec": {
                "sourceAttributeName": "operation",
                "condition": "lambda operation: operation == 'DELETE'",
            },
            "attributeSearchPath": "products/*",
            "attributes": [
                {
                    "sourceAttributeNames": ["product_id", "country"],
                    "transforms": [
                        {
                            "rule": "lambda pid, country: country + '_' + str(pid)",
                            "as": "productId",
                        }
                    ],
                },
                {"sourceAttributeName": "modification_id", "as": "modificationId"},
                {"sourceAttributeName": "product_title", "as": "productTitle"},
                {"sourceAttributeName": "product_title_ru", "as": "productTitleRu"},
                {"sourceAttributeName": "category3id", "as": "category3Id"},
                {"sourceAttributeName": "category2id", "as": "category2Id"},
                {"sourceAttributeName": "status", "as": "status"},
                {"sourceAttributeName": "category1id", "as": "category1Id"},
                {"sourceAttributeName": "operation", "as": "operationCode"},
                {"sourceAttributeName": "brand", "as": "brand"},
                {"sourceAttributeName": "score", "as": "score"},
                {"sourceAttributeName": "sample", "as": "sample"},
                {"sourceAttributeName": "sell_price", "as": "sellPrice"},
                {"sourceAttributeName": "normal_price", "as": "normalPrice"},
                {"sourceAttributeName": "cost_price", "as": "costPrice"},
                {"sourceAttributeName": "amount", "as": "inStockCount"},
                {"sourceAttributeName": "seller", "as": "seller"},
                {"sourceAttributeName": "delivery_hours", "as": "deliveryEstimate"},
                {"sourceAttributeName": "weight", "as": "weight"},
                {"sourceAttributeName": "client_recommendation", "as": "clientRecommendation"},
                {"sourceAttributeName": "release_date", "as": "releaseDate"},
                {"sourceAttributeName": "product_color", "as": "productColor"},
                {"sourceAttributeName": "category_atribute1_name", "as": "categoryAtribute1Name"},
                {
                    "sourceAttributeName": "category_atribute1_value",
                    "as": "categoryAtribute1Value",
                },
                {"sourceAttributeName": "category_atribute2_name", "as": "categoryAtribute2Name"},
                {
                    "sourceAttributeName": "category_atribute2_value",
                    "as": "categoryAtribute2Value",
                },
                {"sourceAttributeName": "category_atribute3_name", "as": "categoryAtribute3Name"},
                {
                    "sourceAttributeName": "category_atribute3_value",
                    "as": "categoryAtribute3Value",
                },
                {"sourceAttributeName": "country", "as": "country"},
                {"sourceAttributeName": "image_url", "as": "imageUrl"},
                {"sourceAttributeName": "product_url", "as": "productUrl"},
                {"sourceAttributeName": "product_url_ru", "as": "productUrlRu"},
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)

        processor = DataProcessor("test", pickup_spec)

        with open("test/resources/product_catalog_message.json", "r", encoding="utf-8") as file:
            in_message = json.load(file)

        records, _, _ = processor.process(in_message.get("inserted"))

        assert len(records) == 1

        assert records[0].attributes == {
            "productid": "hansapost.ee_83971256",
            "modificationid": "42781593",
            "producttitle": "Agfa KM800, Grey",
            "producttitleru": "",
            "category3id": "Pardakaamerad ja auto videokaamerad",
            "category2id": "Autokaubad",
            "status": "active",
            "category1id": "Autokaubad",
            "operationcode": "INSERT",
            "brand": "AgfaPhoto",
            "score": "0",
            "sample": "0",
            "sellprice": "75.49",
            "normalprice": "75.49",
            "costprice": "61.86",
            "instockcount": "8",
            "seller": "PIGU",
            "deliveryestimate": "168",
            "weight": "0.5000",
            "clientrecommendation": "0",
            "releasedate": "2022-02-18",
            "productcolor": "",
            "categoryatribute1name": "Ekraani suurus",
            "categoryatribute1value": '290341"',
            "categoryatribute2name": "Salvestamise resolutsioon",
            "categoryatribute2value": "Tpsustamata",
            "categoryatribute3name": "",
            "categoryatribute3value": "",
            "country": "hansapost.ee",
            "imageurl": "https://www.example.com/images/example.jpg",
            "producturl": "https://www.example.com/products/abc?id=83971256mid=42781593",
            "producturlru": "https://www.example.com/products/abc?id=83971256mid=42781593",
        }

        records, _, _ = processor.process(in_message.get("deleted"))

        assert len(records) == 1

        assert records[0].attributes == {
            "productid": "hansapost.ee_3128690928",
            "modificationid": "91092834",
            "producttitle": "Agfa KM800, Grey",
            "producttitleru": "",
            "category3id": "Pardakaamerad ja auto videokaamerad",
            "category2id": "Autokaubad",
            "status": "inactive",
            "category1id": "Autokaubad",
            "operationcode": "DELETE",
            "brand": "AgfaPhoto",
            "score": "0",
            "sample": "0",
            "sellprice": "75.49",
            "normalprice": "75.49",
            "costprice": "61.86",
            "instockcount": "0",
            "seller": "PIGU",
            "deliveryestimate": "24",
            "weight": "0.5000",
            "clientrecommendation": "0",
            "releasedate": "2022-02-18",
            "productcolor": "",
            "categoryatribute1name": "Ekraani suurus",
            "categoryatribute1value": '290466"',
            "categoryatribute2name": "Salvestamise resolutsioon",
            "categoryatribute2value": "Tpsustamata",
            "categoryatribute3name": "",
            "categoryatribute3value": "",
            "country": "hansapost.ee",
            "imageurl": "https://www.example.com/images/example.jpg",
            "producturl": "https://www.example.com/products/abc?id=83971256mid=42781593",
            "producturlru": "https://www.example.com/products/abc?id=83971256mid=42781593",
        }

    def test_adobe_use_case(self):
        """Test a simple data processor."""
        with open("test/resources/adobe_pickup_spec_example.json", "r", encoding="utf-8") as file:
            pickup_config = json.load(file)
        pickup_spec = DataPickupSpec(pickup_config)

        with open("test/resources/adobe_process.py", "r", encoding="utf-8") as file:
            module_src = file.read()
        processor_config = {"ts_utils": module_src}

        processor = DataProcessor(
            "test_flow", pickup_spec, ImportedModules(processor_config).load()
        )

        with open("test/resources/adobe_event_example.json", "r", encoding="utf-8") as file:
            input_data = json.load(file)

        # source attributes are covered
        processed = processor.process(input_data)

        expected = {
            "eventid": "f408447c-e8fb-4b75-9556-e11f94ced521",
            "createtime": 1681930557165,
            "senttime": 1681930557065,
            "sourcefirsttimestamp": 1681930152000,
            "pagetype": "Product Detail",
            "pagename": "Product Detail | 233456787",
            "url": "/.product.3344567.html",
            "toplevelcategory": "Some Top Level Category",
            "fullcategory": "Some Top Level Category | Divide & Conquer",
            "navigationelement": "MISSING",
            "interfaceinteraction": "MISSING",
            "espotlinkname": "MISSING",
            "productfindingmethod": "MISSING",
            "activityid": "MISSING",
            "searchterm": "MISSING",
            "actualsearchterm": "MISSING",
            "productrecos": "MISSING",
            "productrecostype": "MISSING",
            "userinterfacemessage": "MISSING",
            "socialreferralcode": "MISSING",
            "emailtrackingcode": "MISSING",
            "adbutlerid": "MISSING",
            "costcoreferralcode": "MISSING",
            "clientip": "192.168.0.1",
        }

        assert processed[0][0].attributes == expected
        assert processed[0][0].operation == EventSpec.Operation.INSERT

    def test_system_alert(self):
        """Data pickup spec for alerts"""
        pickup_config_dict = {
            "attributeSearchPath": "",
            "attributes": [
                {"sourceAttributeName": "timestampMillisSinceEpoch", "as": "reportTime"},
                {"sourceAttributeName": "tenantName", "as": "tenant"},
                {"sourceAttributeName": "signalName", "as": "signal"},
                {"sourceAttributeName": "featureName", "as": "feature"},
                {"sourceAttributeName": "alertName", "as": "alert"},
                {"sourceAttributeName": "condition"},
                {
                    "sourceAttributeName": "windowStartTime",
                    "processes": [
                        {"processorName": "ObserveUtils", "method": "window_start_time_to_millis"}
                    ],
                    "as": "windowStartTime",
                },
                {"sourceAttributeName": "windowLength"},
                {
                    "sourceAttributeName": "event",
                    "processes": [{"processorName": "ObserveUtils", "method": "json_to_string"}],
                    "as": "event",
                },
                {
                    "sourceAttributeNames": [
                        "domainName",
                        "signalName",
                        "alertName",
                        "condition",
                        "windowStartTime",
                        "windowLength",
                        "event",
                    ],
                    "processes": [{"processorName": "ObserveUtils", "method": "process_alert"}],
                    "transforms": [
                        {"rule": 'lambda out: out["alertKey"]', "as": "alertKey"},
                        {"rule": 'lambda out: out["sentToSlack"]', "as": "sentToSlack"},
                        {"rule": 'lambda out: out["details"]', "as": "details"},
                    ],
                },
            ],
        }
        pickup_spec = DataPickupSpec(pickup_config_dict)

        with open("test/resources/observe_utils.py", "r", encoding="utf-8") as file:
            ingest_utils = file.read()
        processor_configs = {"ObserveUtils": ingest_utils}
        imported_modules = ImportedModules(processor_configs).load()

        processor = DataProcessor("test", pickup_spec, imported_modules)

        with open("test/resources/alert_message.json", "r", encoding="utf-8") as file:
            in_message = json.load(file)

        current_time = datetime.now(tz=timezone.utc)
        current_time_ms = int(current_time.timestamp() * 1000)
        window_start = int(current_time_ms / 300000) * 300000
        window_end = window_start + 300000
        in_message["windowStartTime"] = datetime.fromtimestamp(
            window_start / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S+0000")
        in_message["windowEndTime"] = datetime.fromtimestamp(
            window_end / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S+0000")
        in_message["timestamp"] = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        in_message["timestampMillisSinceEpoch"] = current_time_ms

        records, _, _ = processor.process(in_message)

        assert len(records) == 1

        expected = {
            "reporttime": current_time_ms,
            "tenant": "_system",
            "signal": "exception",
            "feature": "exception.rollup.alertFeature",
            "alert": "logErrorTurbine2",
            "condition": "(((serviceName == 'turbine') and (severity == 'Error'))"
            " and (count() >= 1))",
            "windowstarttime": window_start,
            "windowlength": "5 mins",
            "event": '{"hostFriendlyName": "deli-9", "severity": "Error", "serviceType": "apps",'
            ' "serviceName": "turbine", "count()": 2}',
            "alertkey": "logErrorTurbine2      deli-9    turbine",
            "senttoslack": "Silenced",
            "details": "{'blocks': [{'type': 'header', 'text': {'type': 'plain_text',"
            " 'text': 'logErrorTurbine2      deli-9    turbine    2 errors in 5 mins'}}],"
            " 'attachments': [{'color': '#D0E0EC'}]}",
        }

        assert records[0].attributes == expected


if __name__ == "__main__":
    pytest.main(sys.argv)
