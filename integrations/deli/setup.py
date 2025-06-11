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

from setuptools import setup

setup(
    name="deli",
    description="biOS Integrations",
    setup_requires=[
        "boto3==1.28.57",
        "dpath==2.1.6",
        "facebook-business==15.0.0",
        "flask==3.0.0",
        "glob2==0.7",
        "google-ads==26.0.1",
        "kafka-python==2.0.2",
        "mysql-connector-python==9.2.0",
        "pygtail==0.14.0",
        "requests==2.31.0",
    ],
    install_requires=[
        "boto3==1.28.57",
        "cidr-trie==3.1.2",
        "cryptocode==0.1",
        "dpath==2.1.6",
        "facebook-business==15.0.0",
        "flask==3.0.0",
        "fuzzywuzzy==0.18.0",
        "glob2==0.7",
        "google-ads==26.0.1",
        "idna==3.4",
        "kafka-python==2.0.2",
        "mysql-connector-python==9.2.0",
        "pandas==1.3.5",
        "pygtail==0.14.0",
        "scipy==1.7.3",
        "sklearn==0.0",
        "requests==2.31.0",
        "ua-parser==0.18.0",
        "user-agents==2.2.0",
        "uwsgi==2.0.22",
        "six==1.16.0",
        "httpagentparser==1.9.5",
    ],
    package_dir={"deli": "deli"},
    packages=["deli", "deli.data_importer", "deli.delivery_channel"],
)
