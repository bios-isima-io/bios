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
from collections import namedtuple, OrderedDict

FieldDesc = namedtuple(
    "FieldDesc", "src_name, dest_name, datatype, key,in_ctx,in_signal,default,delta"
)

AdResult = namedtuple("AdResult", "changed, base_ad, incr_ad")

fbMetrics_Fields = [
    FieldDesc(
        "actCampaignGroupAdId",
        "actCampaignGroupAdId",
        str,
        True,
        True,
        False,
        "MISSING",
        False,
    ),
    FieldDesc(
        "utmSource",
        "utmSource",
        str,
        True,
        False,
        True,
        "facebook",
        False,
    ),
    FieldDesc("account_id", "accountId", str, False, False, True, "", False),
    FieldDesc("account_name", "accountName", str, False, False, True, "", False),
    FieldDesc("campaign_id", "campaignId", str, False, False, True, "", False),
    FieldDesc("campaign_name", "campaignName", str, False, False, True, "", False),
    FieldDesc("adset_id", "adGroupId", str, False, False, True, "", False),
    FieldDesc("adset_name", "adGroupName", str, False, False, True, "", False),
    FieldDesc("ad_id", "adId", str, False, False, True, "", False),
    FieldDesc("ad_name", "adName", str, False, False, True, "", False),
    FieldDesc("clicks", "numClicks", int, False, True, True, 0, True),
    FieldDesc("impressions", "numImpressions", int, False, True, True, 0, True),
    FieldDesc("spend", "spend", float, False, True, True, 0.0, False),
    FieldDesc("cpc", "avgCPC", float, False, True, True, 0.0, False),
    FieldDesc("cpm", "avgCPM", float, False, True, True, 0.0, False),
    FieldDesc("cpp", "avgCPP", float, False, True, True, 0.0, False),
]

googleMetrics_Fields = [
    FieldDesc(
        "actCampaignGroupAdId",
        "actCampaignGroupAdId",
        str,
        True,
        True,
        False,
        "MISSING",
        False,
    ),
    FieldDesc(
        "utmSource",
        "utmSource",
        str,
        True,
        False,
        True,
        "google",
        False,
    ),
    FieldDesc("account_id", "accountId", str, False, False, True, "", False),
    FieldDesc("account_name", "accountName", str, False, False, True, "", False),
    FieldDesc("campaign_id", "campaignId", str, False, False, True, "", False),
    FieldDesc("campaign_name", "campaignName", str, False, False, True, "", False),
    FieldDesc("ad_group_id", "adGroupId", str, False, False, True, "", False),
    FieldDesc("ad_group_name", "adGroupName", str, False, False, True, "", False),
    FieldDesc("ad_group_ad_id", "adId", str, False, False, True, "", False),
    FieldDesc("ad_group_ad_name", "adName", str, False, False, True, "", False),
    FieldDesc("clicks", "numClicks", int, False, True, True, 0, True),
    FieldDesc("impressions", "numImpressions", int, False, True, True, 0, True),
    FieldDesc("interactions", "numInteractions", int, False, True, True, 0, True),
    FieldDesc("spend", "spend", float, False, True, True, 0.0, False),
    FieldDesc("average_cpc", "avgCPC", float, False, True, True, 0.0, False),
    FieldDesc("average_cpm", "avgCPM", float, False, True, True, 0.0, False),
    FieldDesc("average_cost", "avgCOST", float, False, True, True, 0.0, False),
]


class AdDb:
    def __init__(self, fields: dict, src_type: str):
        self.fields = fields
        self.src_type = src_type

    def get_fields(self):
        return self.fields

    def get_value(self, dictvalues, fld_name):
        f_desc = self.fields[fld_name]
        value = f_desc.datatype(dictvalues[fld_name])
        return value

    def get_context_csv_record(self, dictvalues):
        result = ""
        sep = ""
        for _, fld in self.fields.items():
            if fld.in_ctx:
                result += sep + str(dictvalues[fld.dest_name])
                sep = ","
        return result

    def get_signal_dict(self, dictvalues, src_type=None):
        result = {}
        for _, fld in self.fields.items():
            if fld.in_signal:
                result[fld.dest_name] = dictvalues[fld.dest_name]
        if src_type is not None:
            result["utmSource"] = src_type
        return result


FB_METRICS_SCHEMA = AdDb(
    OrderedDict([(f.dest_name, f) for f in fbMetrics_Fields]), "facebook"
)
GOOGLE_METRICS_SCHEMA = AdDb(
    OrderedDict([(f.dest_name, f) for f in googleMetrics_Fields]), "google"
)
