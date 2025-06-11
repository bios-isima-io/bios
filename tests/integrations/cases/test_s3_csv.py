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

import pprint
import time
import unittest

import bios
import boto3
from tsetup import admin_pass, admin_user
from tsetup import get_endpoint_url as ep_url
from tsetup import get_webhook_url


class TestS3Csv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TENANT_NAME = "test"
        cls.ADMIN_USER = f"{admin_user}@{cls.TENANT_NAME}"
        with bios.login(ep_url(), cls.ADMIN_USER, admin_pass) as admin:
            s3_source = admin.get_import_source("220")
            cls.S3 = boto3.client(
                "s3",
                aws_access_key_id=s3_source["authentication"]["accessKey"],
                aws_secret_access_key=s3_source["authentication"]["secretKey"],
            )
            cls.BUCKET = "deli-s3-integration"

    def test_s3_csv(self):
        signal_name = "covidDataSignal"
        prefix = "test/csvWithHeader"
        resources_dir = "../resources/files/csv"
        try:
            time0 = bios.time.now()
            self.S3.upload_file(
                f"{resources_dir}/file01.csv", self.BUCKET, prefix + "/2021/06/18/file01.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file02.csv", self.BUCKET, prefix + "/2021/06/18/file02.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file03.csv", self.BUCKET, prefix + "/2021/06/19/file03.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file04.csv", self.BUCKET, prefix + "/2021/06/20/file04.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file05.csv", self.BUCKET, prefix + "/2021/06/20/file05.csv"
            )
            delta = bios.time.seconds(43)
            sleep_ms = time0 + delta - bios.time.now()
            time.sleep(sleep_ms / 1000.0)
            with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
                statement = (
                    bios.isql()
                    .select()
                    .from_signal(signal_name)
                    .where("testName = 'csvWithHeader'")
                    .time_range(time0, delta)
                    .build()
                )
                resp = admin.execute(statement)
                records = resp.to_dict()
                self.assertEqual(
                    len(records), 7, f"signal={signal_name}, start={time0}, delta={delta}"
                )
                record0 = records[0]
                self.assertEqual(record0.get("geoId"), 94061)
                self.assertEqual(record0.get("cases"), 920)
                self.assertEqual(record0.get("deaths"), 4)
                self.assertEqual(record0.get("dataRep"), "file01 001")
                record6 = records[6]
                self.assertEqual(record6.get("geoId"), 94070)
                self.assertEqual(record6.get("cases"), 0)
                self.assertEqual(record6.get("deaths"), 0)
                self.assertEqual(record6.get("dataRep"), "file05 007")

            time0 = bios.time.now()
            self.S3.upload_file(
                f"{resources_dir}/file06.csv", self.BUCKET, prefix + "/2021/06/20/file06.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file07.csv", self.BUCKET, prefix + "/2021/07/01/file07.csv"
            )
            delta = bios.time.seconds(33)
            sleep_ms = time0 + delta - bios.time.now()
            time.sleep(sleep_ms / 1000.0)

            with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
                statement = (
                    bios.isql()
                    .select()
                    .from_signal(signal_name)
                    .where("testName = 'csvWithHeader'")
                    .time_range(time0, delta)
                    .build()
                )
                resp = admin.execute(statement)
                records = resp.to_dict()
                self.assertEqual(len(records), 6)

        finally:
            objects = self.S3.list_objects_v2(Bucket=self.BUCKET, Prefix=prefix)
            keys = [content.get("Key") for content in objects.get("Contents")]
            for key in keys:
                self.S3.delete_object(Bucket=self.BUCKET, Key=key)

    def test_s3_csv_without_header(self):
        signal_name = "covidDataSignal"
        prefix = "test/csvWithoutHeader"
        resources_dir = "../resources/files/csv_no_header"
        try:
            time0 = bios.time.now()
            self.S3.upload_file(
                f"{resources_dir}/file01.csv", self.BUCKET, prefix + "/2021/06/18/file01.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file02.csv", self.BUCKET, prefix + "/2021/06/18/file02.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file03.csv", self.BUCKET, prefix + "/2021/06/20/file03.csv"
            )

            sleep_ms = time0 + bios.time.seconds(10) - bios.time.now()
            time.sleep(sleep_ms / 1000.0)

            self.S3.upload_file(
                f"{resources_dir}/file04.csv", self.BUCKET, prefix + "/2021/06/20/file04.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file05.csv", self.BUCKET, prefix + "/2021/06/20/file05.csv"
            )

            sleep_ms = time0 + bios.time.seconds(20) - bios.time.now()
            time.sleep(sleep_ms / 1000.0)

            self.S3.upload_file(
                f"{resources_dir}/file06.csv", self.BUCKET, prefix + "/2021/06/20/file06.csv"
            )
            self.S3.upload_file(
                f"{resources_dir}/file07.csv", self.BUCKET, prefix + "/2021/07/01/file07.csv"
            )

            delta = bios.time.seconds(53)
            sleep_ms = time0 + delta - bios.time.now()
            time.sleep(sleep_ms / 1000.0)
            with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
                statement = (
                    bios.isql()
                    .select()
                    .from_signal(signal_name)
                    .where("testName = 'csv without header'")
                    .time_range(time0, delta)
                    .build()
                )
                resp = admin.execute(statement)
                records = resp.to_dict()
                self.assertEqual(
                    len(records), 13, f"signal={signal_name}, start={time0}, delta={delta}"
                )
                record0 = records[0]
                self.assertEqual(record0.get("geoId"), 94061)
                self.assertEqual(record0.get("cases"), 920)
                self.assertEqual(record0.get("deaths"), 4)
                self.assertEqual(record0.get("dataRep"), "file01 001")
                record12 = records[12]
                self.assertEqual(record12.get("geoId"), 94073)
                self.assertEqual(record12.get("cases"), -38)
                self.assertEqual(record12.get("deaths"), 0)
                self.assertEqual(record12.get("dataRep"), "file07 013")

        finally:
            objects = self.S3.list_objects_v2(Bucket=self.BUCKET, Prefix=prefix)
            keys = [content.get("Key") for content in objects.get("Contents")]
            for key in keys:
                self.S3.delete_object(Bucket=self.BUCKET, Key=key)

    def test_s3_json(self):
        signal_name = "impressionSignal"
        prefix = "test/jsonImpressionSignal"
        resources_dir = "../resources/files/json-impression"
        try:
            time0 = bios.time.now()
            self.S3.upload_file(
                f"{resources_dir}/impression_001.json",
                self.BUCKET,
                prefix + "/2021/06/18/file01.json",
            )
            self.S3.upload_file(
                f"{resources_dir}/impression_002.json",
                self.BUCKET,
                prefix + "/2021/06/18/file02.json",
            )
            self.S3.upload_file(
                f"{resources_dir}/impression_003.json",
                self.BUCKET,
                prefix + "/2021/06/20/file03.json",
            )

            delta = bios.time.seconds(33)
            sleep_ms = time0 + delta - bios.time.now()
            time.sleep(sleep_ms / 1000.0)
            with bios.login(ep_url(), self.ADMIN_USER, admin_pass) as admin:
                statement = (
                    bios.isql().select().from_signal(signal_name).time_range(time0, delta).build()
                )
                resp = admin.execute(statement)
                records = resp.to_dict()
                self.assertEqual(
                    len(records), 6, f"signal={signal_name}, start={time0}, delta={delta}"
                )
                pprint.pprint(records[0])
                pprint.pprint(records[5])

        finally:
            objects = self.S3.list_objects_v2(Bucket=self.BUCKET, Prefix=prefix)
            keys = [content.get("Key") for content in objects.get("Contents")]
            for key in keys:
                self.S3.delete_object(Bucket=self.BUCKET, Key=key)


if __name__ == "__main__":
    unittest.main()
