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
"""googlead_data_importer: Module that provides importer classes for google ad data source"""

import logging
import sys
import time
from configparser import ConfigParser
from datetime import datetime

import bios
import grpc
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v19.enums.types.customer_status import CustomerStatusEnum
from google.api_core.retry import Retry

from deli.configuration import (
    ConfigUtils,
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)

from ..utils import bios_get_auth_params, bios_get_session, chunks, time_to_sleep
from .adctx_db import GOOGLE_METRICS_SCHEMA, AdDb, AdResult
from .data_importer import DataImporter, EndpointHandler, SourceFilter

CustomerStatus = CustomerStatusEnum.CustomerStatus


class GoogleAdDataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.system_config = system_config
        self.flow_config = flow_config
        self.polling_interval = self.config.get_polling_interval()
        self.customer_id = self.config.get(Keyword.CUSTOMER_ID)

    def start_importer(self):
        while True:
            start = time.time()
            self.logger.info("Start importing")
            for handler in self.endpoint_handlers.values():
                handler.handle()
            end = time.time()
            elapsed_time = end - start
            self.logger.info("Finished importing; elapsed_time=%s(sec)", elapsed_time)
            sleep_time = 0
            if elapsed_time < self.polling_interval:
                sleep_time = time_to_sleep(self.polling_interval)
            self.logger.info("sleeping for  %d seconds", sleep_time)
            time.sleep(sleep_time)

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        """Generates google ad handler ID.
        The ID consists of <customer_id>.<flow_id>
        """
        handler_id = str(self.customer_id) + "." + str(self.flow_config.get_id())
        return handler_id

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec) -> EndpointHandler:
        # handler ID is the service path
        return GoogleAdHandler(
            self.config,
            source_data_spec,
            handler_id,
            self.system_config,
        )


class GoogleAdHandler(EndpointHandler):
    def __init__(
        self,
        config: SourceConfiguration,
        source_data_spec: SourceDataSpec,
        handler_id: str,
        system_config: ConfigParser,
    ):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id

        self.bios_endpoint = system_config.get(Keyword.CONFIG_SECTION, "endpoint")
        self.bios_cafile = system_config.get(Keyword.CONFIG_SECTION, "sslCertFile", fallback=None)
        self.app_name = system_config.get("Common", "appName", fallback=None)
        self.app_type = system_config.get("Common", "appType", fallback=None)
        self.bios_user, self.bios_password = bios_get_auth_params(system_config)
        self.bios_extract_user, self.bios_extract_password = bios_get_auth_params(
            system_config, user_type="extract"
        )

        self.write_chunk_size = 5000
        self.read_chunk_size = 5000
        self.ctx_schema = GOOGLE_METRICS_SCHEMA
        self.ctx_name = "googleLastKnownMetrics"
        self.ctx_key = "actCampaignGroupAdId"
        self.precision = 11
        self.google_ad_handler = GoogleAdUtil(config, self.ctx_schema)

    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        pass

    def setup(self):
        pass

    def start(self):
        pass

    def shutdown(self):
        pass

    def save_context(self, upsert_session, context_data):
        # should calculate this dynamically later
        for chunk in chunks(context_data, self.write_chunk_size):
            try:
                if len(chunk) > 0:
                    self.logger.info("Inserting %d context records", len(chunk))
                    req = bios.isql().upsert().into(self.ctx_name).csv_bulk(chunk).build()
                    upsert_session.execute(req)
            except Exception as err:
                self.logger.error("ERROR encountered  -> %s", err)

    def handle(self):
        ad_list = self.google_ad_handler.run()
        context_data = []
        if len(ad_list) > 0:
            session_upsert = bios_get_session(
                self.bios_endpoint,
                self.bios_user,
                self.bios_password,
                self.bios_cafile,
                self.app_name,
                self.app_type,
            )
            if session_upsert is None:
                return
            session_select = bios_get_session(
                self.bios_endpoint,
                self.bios_user,
                self.bios_password,
                self.bios_cafile,
                self.app_name,
                self.app_type,
            )
            if session_select is None:
                session_select = session_upsert
            cache = {}
            for chunk in chunks(ad_list, self.read_chunk_size):
                keys = []
                for item in chunk:
                    keys.append([item["actCampaignGroupAdId"]])
                try:
                    req = bios.isql().select().from_context(self.ctx_name).where(keys=keys).build()
                    res = session_select.execute(req)
                    for record in res.get_records():
                        cache[record.get(self.ctx_key)] = record
                    for ad_record in chunk:
                        if ad_record[self.ctx_key] in cache:
                            # updates the ad data if it has changed
                            result = self.get_increments(cache[ad_record[self.ctx_key]], ad_record)
                            self.logger.debug(
                                "Data changed for ad -> %s %s",
                                ad_record["actCampaignGroupAdId"],
                                result.changed,
                            )
                            if result.changed:
                                # update the context
                                req = (
                                    bios.isql()
                                    .upsert()
                                    .into(self.ctx_name)
                                    .csv(self.get_context_data(result.base_ad))
                                    .build()
                                )
                                session_upsert.execute(req)
                                incr_ad = self.ctx_schema.get_signal_dict(result.incr_ad)
                                self.publish({"data": [incr_ad]})
                                self.logger.info(incr_ad)
                        else:
                            # slap in the derived fields for context
                            self.add_derived_values(ad_record)
                            context_data.append(self.ctx_schema.get_context_csv_record(ad_record))
                except Exception as err:
                    self.logger.error("ERROR encountered  -> %s", err)
            self.logger.info("Number of Context records -> %d", len(context_data))
            try:
                if len(context_data) > 0:
                    self.logger.info(
                        "No context available, Inserting %d records", len(context_data)
                    )
                    self.save_context(session_upsert, context_data)
            except Exception as err:
                self.logger.error("ERROR encountered  -> %s", err)
            session_upsert.close()
            session_select.close()
            ad_list = None
        else:
            self.logger.info("no data to publish")

    # this adds extra fields to the current record
    def add_derived_values(self, current):

        micros_per_unit = 1000000
        clicks = self.ctx_schema.get_value(current, "numClicks")
        avg_cpc = self.ctx_schema.get_value(current, "avgCPC")
        spend = round(clicks * avg_cpc, self.precision)
        current["spend"] = spend / micros_per_unit

    def get_increments(self, ctx, current):
        base_record = {}
        incr_record = {}
        changed = False
        for _, field in self.ctx_schema.get_fields().items():
            base_record[field.dest_name] = field.datatype(current[field.dest_name])
            incr_record[field.dest_name] = field.datatype(current[field.dest_name])

            # convert to the right type
            if field.in_ctx and field.delta:
                p_val = field.datatype(ctx.get(field.dest_name))
                c_val = field.datatype(current[field.dest_name])
                incr = c_val - p_val
                # change the base ad and increments if something changed
                incr_record[field.dest_name] = field.datatype(0)
                base_record[field.dest_name] = p_val
                if incr > 0:
                    changed = True
                    incr_record[field.dest_name] = incr
                    base_record[field.dest_name] = c_val

        if changed:
            for _, field in self.ctx_schema.get_fields().items():
                if field.delta:
                    continue
                c_val = field.datatype(current[field.dest_name])
                incr_record[field.dest_name] = c_val
                base_record[field.dest_name] = c_val

        self.add_derived_values(incr_record)
        self.add_derived_values(base_record)
        result = AdResult(changed, base_record, incr_record)
        return result

    def get_context_data(self, ad_record):
        return self.ctx_schema.get_context_csv_record(ad_record)


CLIENT_TIMEOUT_SECONDS = 10 * 60


class GoogleAdUtil:
    def __init__(self, config: SourceConfiguration, schema: AdDb):
        self.logger = logging.getLogger(type(self).__name__)
        self.customer_id = config.get(Keyword.CUSTOMER_ID)
        auth = config.get(Keyword.AUTHENTICATION)
        self.client_id = ConfigUtils.get_property(auth, Keyword.CLIENT_ID)
        self.client_secret = ConfigUtils.get_property(auth, Keyword.CLIENT_SECRET)
        self.developer_token = ConfigUtils.get_property(auth, Keyword.DEVELOPER_TOKEN)
        self.refresh_token = ConfigUtils.get_property(auth, Keyword.REFRESH_TOKEN)
        self.test_data = {}
        self.record_limit = sys.maxsize
        self.ctx_schema = schema
        # for debugging
        # self.record_limit = 100
        self._retry = Retry(
            # Sets the maximum accumulative timeout of the call; it
            # includes all tries.
            deadline=CLIENT_TIMEOUT_SECONDS,
            # Sets the timeout that is used for the first try to one tenth
            # of the maximum accumulative timeout of the call.
            initial=CLIENT_TIMEOUT_SECONDS / 10,
            # Sets the maximum timeout that can be used for any given try
            # to one fifth of the maximum accumulative timeout of the call
            # (two times greater than the timeout that is needed for the
            # first try).
            maximum=CLIENT_TIMEOUT_SECONDS / 5,
        )

    def run(self, test_data=False):
        if test_data:
            return self.get_test_data()
        return self.get_ad_data()

    def get_test_data(self):
        return self.test_data

    def get_ad_data(self):
        config = {
            "developer_token": self.developer_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "login_customer_id": self.customer_id,
            "use_proto_plus": True,
        }
        client = GoogleAdsClient.load_from_dict(config_dict=config, version="v19")
        # FIXME: get specific type of account
        account_hierarchy = self.get_account_hierarchy(client)
        ga_service = client.get_service("GoogleAdsService")
        today = datetime.now().strftime("%Y-%m-%d")
        stats_query = (
            """
            SELECT
                campaign.id,
                campaign.status,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group_ad.ad.resource_name,
                metrics.ctr,
                metrics.average_cost,
                metrics.average_cpc,
                metrics.average_cpe,
                metrics.average_cpm,
                metrics.average_page_views,
                metrics.average_time_on_site,
                metrics.average_cpv,
                metrics.impressions,
                metrics.interactions,
                metrics.clicks
            FROM ad_group_ad
            WHERE  metrics.impressions > 0  AND segments.date >= '2010-01-01' AND segments.date  <= '"""
            + today
            + """'"""
        )
        ad_records = []
        for account_id, account_name in account_hierarchy.items():
            try:
                response = ga_service.search_stream(
                    customer_id=str(account_id), query=stats_query, retry=self._retry
                )
                count = 0
                for batch in response:
                    for row in batch.results:
                        _id = (
                            str(account_id)
                            + "_"
                            + str(row.campaign.id)
                            + "_"
                            + str(row.ad_group.id)
                            + "_"
                            + str(row.ad_group_ad.ad.id)
                        )
                        insight = {
                            "actCampaignGroupAdId": _id,
                            "account_id": account_id,
                            "account_name": account_name,
                            "campaign_name": row.campaign.name,
                            "campaign_id": row.campaign.id,
                            "ad_group_name": row.ad_group.name,
                            "ad_group_id": row.ad_group.id,
                            "ad_group_ad_id": row.ad_group_ad.ad.id,
                            "ad_group_ad_name": row.ad_group_ad.ad.name,
                            "clicks": row.metrics.clicks,
                            "ctr": row.metrics.ctr,
                            "average_cost": row.metrics.average_cost,
                            "average_cpc": row.metrics.average_cpc,
                            "average_cpm": row.metrics.average_cpm,
                            "interactions": row.metrics.interactions,
                            "impressions": row.metrics.impressions,
                        }
                        ad_record = {}
                        for _, f_descriptor in self.ctx_schema.get_fields().items():
                            f_src_name = f_descriptor.src_name
                            f_dest_name = f_descriptor.dest_name
                            f_type = f_descriptor.datatype
                            f_defval = f_descriptor.default
                            if f_src_name in insight:
                                ad_record[f_dest_name] = f_type(insight[f_src_name])
                            else:
                                ad_record[f_dest_name] = f_type(f_defval)
                        count += 1
                        if count < self.record_limit:
                            ad_records.append(ad_record)
                self.logger.info("The account %s has %d records", account_id, count)
            except GoogleAdsException as err:
                code = err.error.code()
                if code == grpc.StatusCode.PERMISSION_DENIED:
                    self.logger.warning("Account %s (%s) is not enabled", account_id, account_name)
                    continue
                self.logger.error(
                    "A GoogleAdsException encountered while calling search_stream; error_code=%s, error=%s",
                    code,
                    err.error,
                )
                break
            except Exception as err:
                self.logger.error(
                    "A generic error encountered while calling search_stream: (type: %s) %s",
                    type(err),
                    err,
                    exc_info=True,
                )
                break
        return ad_records

    def get_account_hierarchy(self, client):
        googleads_service = client.get_service("GoogleAdsService")
        customer_service = client.get_service("CustomerService")
        seed_customer_ids = []
        query = """
            SELECT
                customer_client.status,
                customer_client.client_customer,
                customer_client.level,
                customer_client.manager,
                customer_client.descriptive_name,
                customer_client.currency_code,
                customer_client.time_zone,
                customer_client.id
            FROM customer_client
            WHERE customer_client.level <= 1"""

        if self.customer_id is not None:
            seed_customer_ids = [self.customer_id]
        else:
            customer_resource_names = []
            try:
                customer_service.list_accessible_customers().resource_names
            except Exception as err:
                self.logger.error(
                    "ERROR encountered while calling list_accessible_customers -> %s",
                    err,
                )
            for customer_resource_name in customer_resource_names:
                try:
                    customer = customer_service.get_customer(resource_name=customer_resource_name)
                    seed_customer_ids.append(customer.id)
                except Exception as err:
                    self.logger.error("ERROR encountered while calling get_customer -> %s", err)

        account_hierarchy = {}
        for seed_customer_id in seed_customer_ids:
            unprocessed_customer_ids = [seed_customer_id]
            customer_ids_to_child_accounts = dict()
            root_customer_client = None

            while unprocessed_customer_ids:
                customer_id = int(unprocessed_customer_ids.pop(0))
                response = []
                try:
                    response = googleads_service.search(
                        customer_id=str(customer_id), query=query, retry=self._retry
                    )
                except Exception as err:
                    self.logger.error(
                        "ERROR encountered while calling googleads_service.search -> %s",
                        err,
                    )

                for googleads_row in response:
                    customer_client = googleads_row.customer_client
                    if customer_client.level == 0:
                        if root_customer_client is None:
                            root_customer_client = customer_client
                        continue

                    if customer_client.status != CustomerStatus.ENABLED:
                        self.logger.info(
                            "Account is not enabled, ignoring; customer_id=%s account=%s (%s), status=%s",
                            customer_id,
                            customer_client.descriptive_name,
                            customer_client.id,
                            customer_client.status,
                        )
                        continue

                    if customer_id not in customer_ids_to_child_accounts:
                        customer_ids_to_child_accounts[customer_id] = []

                    customer_ids_to_child_accounts[customer_id].append(customer_client)

                    if customer_client.manager:
                        if (
                            customer_client.id not in customer_ids_to_child_accounts
                            and customer_client.level == 1
                        ):
                            unprocessed_customer_ids.append(customer_client.id)
            if root_customer_client is not None:
                self.logger.info(
                    "The hierarchy of customer ID %s is printed below:",
                    root_customer_client.id,
                )
                self._print_account_hierarchy(
                    root_customer_client,
                    customer_ids_to_child_accounts,
                    0,
                    account_hierarchy,
                )
            else:
                self.logger.info(
                    "Customer ID %s is likely a test account, so its customer client information"
                    "cannot be retrieved.",
                    self.customer_id,
                )
        return account_hierarchy

    def _print_account_hierarchy(
        self, customer_client, customer_ids_to_child_accounts, depth, account_hierarchy
    ):
        customer_id = customer_client.id
        self.logger.info(
            "%s, %s, %s, %s",
            customer_id,
            customer_client.descriptive_name,
            customer_client.currency_code,
            customer_client.time_zone,
        )
        # Recursively call this function for all child accounts of customer_client.
        if customer_id in customer_ids_to_child_accounts:
            for child_account in customer_ids_to_child_accounts[customer_id]:
                self._print_account_hierarchy(
                    child_account,
                    customer_ids_to_child_accounts,
                    depth + 1,
                    account_hierarchy,
                )
        else:
            account_hierarchy[customer_id] = customer_client.descriptive_name


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.GOOGLEAD] = GoogleAdDataImporter
