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
import logging
import random
import time
from configparser import ConfigParser

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccountuser import AdAccountUser as AdUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.adaccount import AdAccount

import bios

from ..utils import (
    bios_get_session,
    bios_get_auth_params,
    time_to_sleep,
)

from .adctx_db import AdDb, AdResult, FB_METRICS_SCHEMA


from ..configuration import (
    ConfigUtils,
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)

from .data_importer import DataImporter, EndpointHandler, SourceFilter


class FacebookAdDataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.config = config
        self.system_config = system_config
        self.polling_interval = self.config.get_polling_interval()
        self.logger = logging.getLogger(type(self).__name__)

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
            self.logger.info("Sleeping for  %d seconds", sleep_time)
            time.sleep(sleep_time)

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        """Generates REST client endpoint handler ID.
        The ID consists of <src_conf.endpoint>/<src_spec.subpath>
        """
        endpoint = "https://localhost"  # we disallow missing endpoint
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
        subpath = source_data_spec.get_or_default(Keyword.SUB_PATH, "").strip("/")
        handler_id = endpoint + "/" + subpath
        return handler_id

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec):
        # handler ID is the service path
        return FacebookHandler(self.config, self.system_config, source_data_spec, handler_id)


class FacebookHandler(EndpointHandler):
    def __init__(
        self,
        config: SourceConfiguration,
        system_config: ConfigParser,
        source_data_spec: SourceDataSpec,
        handler_id: str,
    ):
        super().__init__()
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id
        self.polling_interval = config.get_polling_interval()

        self.bios_endpoint = system_config.get(Keyword.CONFIG_SECTION, "endpoint")
        self.bios_cafile = system_config.get(Keyword.CONFIG_SECTION, "sslCertFile", fallback=None)
        self.app_name = system_config.get("Common", "appName", fallback=None)
        self.app_type = system_config.get("Common", "appType", fallback=None)
        self.bios_user, self.bios_password = bios_get_auth_params(system_config)
        self.bios_extract_user, self.bios_extract_password = bios_get_auth_params(
            system_config, user_type="extract"
        )
        self.ctx_schema = FB_METRICS_SCHEMA
        self.ctx_name = "fbLastKnownMetrics"
        self.ctx_key = "actCampaignGroupAdId"
        self.fb_handler = FacebookUtil(config, self.ctx_schema)
        self.precision = 11

    def setup(self):
        pass

    def start(self):
        pass

    # this adds extra fields to the current record
    def add_derived_values(self, current):

        clicks = self.ctx_schema.get_value(current, "numClicks")
        avg_cpc = self.ctx_schema.get_value(current, "avgCPC")
        spend = round(clicks * avg_cpc, self.precision)
        current["spend"] = spend

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

    def handle(self):
        # start processing rest call
        ad_records = self.fb_handler.run()
        if len(ad_records) > 0:
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
                self.bios_extract_user,
                self.bios_extract_password,
                self.bios_cafile,
                self.app_name,
                self.app_type,
            )
            if session_select is None:
                session_select = session_upsert

            for ad_record in ad_records:
                try:
                    req = (
                        bios.isql()
                        .select()
                        .from_context(self.ctx_name)
                        .where(keys=[[ad_record[self.ctx_key]]])
                        .build()
                    )
                    res = session_select.execute(req)
                    if len(res.get_records()) > 0:
                        # updates the ad data if it has changed
                        result = self.get_increments(res.get_records()[0], ad_record)
                        self.logger.debug(
                            "Data changed for ad -> %s %s",
                            ad_record[self.ctx_key],
                            result.changed,
                        )
                        # update the context
                        if result.changed:
                            req = (
                                bios.isql()
                                .upsert()
                                .into(self.ctx_name)
                                .csv(self.get_context_data(result.base_ad))
                                .build()
                            )
                            session_upsert.execute(req)
                            incr_ad = self.ctx_schema.get_signal_dict(result.incr_ad)
                            self.logger.info(incr_ad)
                            self.publish({"data": [incr_ad]})
                            self.logger.info(incr_ad)
                    else:
                        self.add_derived_values(ad_record)
                        ctx_ad = self.ctx_schema.get_context_csv_record(ad_record)
                        self.logger.info("No Context, Inserting base Record")
                        req = bios.isql().upsert().into(self.ctx_name).csv(ctx_ad).build()
                        session_upsert.execute(req)
                except Exception as err:
                    self.logger.error(
                        "An error encountered while updating ad records in biOS: (%s) %s",
                        type(err),
                        err,
                        exc_info=True,
                    )
            session_upsert.close()
            session_select.close()
        else:
            self.logger.info("no data to publish")

    def shutdown(self):
        pass

    def create_source_filter(self, source_data_spec: dict) -> SourceFilter:
        pass


class FacebookUtil:
    def __init__(self, config: SourceConfiguration, schema: AdDb):
        self.logger = logging.getLogger(type(self).__name__)
        auth = config.get(Keyword.AUTHENTICATION)
        auth_type = auth.get("type")
        if auth_type.upper() != "APPSECRET":
            self.logger.error("unsupported authentication type %s. Exiting", auth_type)
            return
        self.client_id = ConfigUtils.get_property(auth, Keyword.CLIENT_ID)
        self.client_secret = ConfigUtils.get_property(auth, Keyword.CLIENT_SECRET)
        self.access_token = ConfigUtils.get_property(auth, Keyword.ACCESS_TOKEN)
        self.loop_count = 0
        self.schema = schema
        self.test_data = [
            {
                "account_id": "564815087460433",
                "account_name": "zeroshopco",
                "campaign_id": "23847503749450092",
                "campaign_name": "Monetization - Remarketing - Special Ad Categories",
                "ad_id": "23847540790980092",
                "ad_name": "SF Alcohol - Main",
                "adset_id": "23847540790970092",
                "adset_name": "SF Alcohol_Dynamic_Main - Stories",
                "cpc": 0.0,
                "cpm": 95.360825,
                "cpp": "132.142857",
                "impressions": 97,
                "clicks": 0,
                "spend": 9.25,
            },
            {
                "account_id": "564815087460433",
                "account_name": "zeroshopco",
                "campaign_id": "23847503749450092",
                "campaign_name": "Monetization - Remarketing - Special Ad Categories",
                "ad_id": "23847540757510092",
                "ad_name": "SF Alcohol - Main",
                "adset_id": "23847540119040092",
                "adset_name": "SF Alcohol_Dynamic_Main - Copy 2",
                "cpc": 39.107,
                "cpm": "142.778386",
                "cpp": "469.471789",
                "impressions": 2739,
                "clicks": 10,
                "spend": 391.07,
            },
            {
                "account_id": "564815087460433",
                "account_name": "zeroshopco",
                "campaign_id": "23847503749450092",
                "campaign_name": "Monetization - Remarketing - Special Ad Categories",
                "ad_id": "23847503752080092",
                "ad_name": "Dynamic - 9OFF100",
                "adset_id": "23847503749480092",
                "adset_name": "Past Purchasers - 9OFF100",
                "cpc": "10.063333",
                "cpm": "84.094708",
                "cpp": "215.642857",
                "impressions": 359,
                "clicks": 3,
                "spend": 30.19,
            },
        ]

    def get_test_data(self):
        self.loop_count += 1
        if self.loop_count % 2 == 0:
            return self.test_data
        for index in range(0, len(self.test_data)):
            ad_record = self.test_data[index]
            ad_record["impressions"] = (
                ad_record["impressions"] + random.randint(5, 10) * self.loop_count
            )

        return self.test_data

    def run(self, test_data=False):
        if test_data:
            return self.get_test_data()
        return self.get_ad_data()

    def get_insights_async(self, account):
        fields = [
            "account_id",
            "account_name",
            "campaign_id",
            "campaign_name",
            "ad_id",
            "ad_name",
            "adset_id",
            "adset_name",
            "cpc",
            "cpm",
            "cpp",
            "impressions",
            "clicks",
            "spend",
            "date_start",
            "date_stop",
        ]
        params = {
            "level": "ad",
            "date_preset": "maximum",
        }

        async_job = account.get_insights(fields=fields, params=params, is_async=True)
        async_job.api_get()
        while (
            async_job[AdReportRun.Field.async_status] != "Job Completed"
            or int(async_job[AdReportRun.Field.async_percent_completion]) < 100
        ):
            time.sleep(1)
            async_job.api_get()
        return async_job.get_result()

    def get_ad_data(self):
        ad_records = []
        FacebookAdsApi.init(self.client_id, self.client_secret, self.access_token)
        user = AdUser(fbid="me")
        accounts = user.get_ad_accounts()
        for account in accounts:
            try:
                account_id = account.get(AdAccount.Field.account_id)
                insights = self.get_insights_async(account)
                for insight in insights:
                    ad_record = {}
                    for _, f_descriptor in self.schema.get_fields().items():
                        f_src_name = f_descriptor.src_name
                        f_dest_name = f_descriptor.dest_name
                        f_type = f_descriptor.datatype
                        f_defval = f_descriptor.default
                        if f_src_name in insight:
                            ad_record[f_dest_name] = f_type(insight[f_src_name])
                        else:
                            ad_record[f_dest_name] = f_type(f_defval)
                    id = (
                        insight[AdsInsights.Field.account_id]
                        + "_"
                        + insight[AdsInsights.Field.campaign_id]
                        + "_"
                        + insight[AdsInsights.Field.adset_id]
                        + "_"
                        + insight[AdsInsights.Field.ad_id]
                    )
                    ad_record["actCampaignGroupAdId"] = id
                    ad_records.append(ad_record)
                    self.logger.debug("ads_data -> %s", ad_record)
            except FacebookRequestError as err:
                error_details = err.body().get("error") if err.body() else None
                if error_details:
                    error_code = error_details.get("code")
                    error_subcode = error_details.get("error_subcode")
                else:
                    error_code, error_subcode = None, None
                if error_code == 80000 and error_subcode == 2446079:
                    self.logger.warning(
                        "An account received throttling request from facebook for get_insights, skipping; account_id=%d",
                        account_id,
                    )
                else:
                    self.logger.error(
                        "An error encountered while executing get_insights; account_id=%s, error=%s",
                        account_id,
                        json.dumps(error_details if error_details else err.body()),
                    )
                    return ad_records
            except Exception as err:
                self.logger.error(
                    "An error encountered while executing get_insights; account_id=%s, error=(%s) %s",
                    account_id,
                    type(err),
                    err,
                )
                return ad_records
        return ad_records


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.FACEBOOKAD] = FacebookAdDataImporter
