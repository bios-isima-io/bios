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
import re

from setuptools import Extension, setup

# from package import Package
# Get the version
VERSION_REGEX = r'__version__ = ["\']([^"\']*)["\']'
with open("bios/_version.py", "r") as f:
    TEXT = f.read()
    MATCH = re.search(VERSION_REGEX, TEXT)

    if MATCH:
        VERSION = MATCH.group(1)
    else:
        raise RuntimeError("No version number found!")

PROTOBUF_VERSION = "5.26.1"

ROOT = os.environ.get("ROOT")
HOME = os.environ.get("HOME")
BUILD_TYPE = os.environ.get("BUILD_TYPE")

if not ROOT:
    raise RuntimeError("Environment variable ROOT is not set")

CSDK_DIR = f"{ROOT}/sdk/csdk"
THIRD_PARTY = f"{ROOT}/third-party/lib/"

COMPILE_ARGS = [
    f"-I{CSDK_DIR}/",
    f"-I{CSDK_DIR}/include",
    f"-I{CSDK_DIR}/target/include",
    f"-I{ROOT}/third-party/include",
    f"-I{ROOT}/sdk/sdk-python/_csdk_native",
    "-std=c++17",
    "-Wall",
    "-g",
]
if BUILD_TYPE in ("Debug", "DDebug"):
    COMPILE_ARGS.append("-O0")

UNDEF_MACROS = []
BUILD_TYPE = os.environ.get("BUILD_TYPE") or "RelWithDebInfo"
if BUILD_TYPE in ("Debug", "DDebug"):
    UNDEF_MACROS.append("NDEBUG")

CSDK_LIB = f"{CSDK_DIR}/target/libtfoscsdk.a"

extension = Extension(
    "_bios_csdk_native",
    sources=[
        "_csdk_native/py_csdk.cc",
        "_csdk_native/csdk_unit_manager.cc",
    ],
    extra_compile_args=COMPILE_ARGS,
    undef_macros=UNDEF_MACROS,
    extra_link_args=[
        CSDK_LIB,
        THIRD_PARTY + "libevent.a",
        THIRD_PARTY + "libevent_openssl.a",
        "-lssl",
        THIRD_PARTY + "libnghttp2.a",
        THIRD_PARTY + "libprotobuf.a",
        "-ldl",
    ],
)

setup(
    name="bios-sdk",
    version=VERSION,
    description="biOS Python SDK",
    author="Isima",
    author_email="info@isima.io",
    packages=[
        "bios",
        "bios.models",
        "bios._csdk",
        "bios._proto",
        "bios._proto._bios",
    ],
    setup_requires=["wheel"],
    install_requires=[f"protobuf=={PROTOBUF_VERSION}", "dpath"],
    include_package_data=True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
    ],
    ext_modules=[extension],
)
