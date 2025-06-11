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

"""
BIOS S3 export utility

Example usage:

python3 ./enable_export.py -e https://lcm-test.tieredfractals.com -u load@tenant1.com
-p strongPassword -b export-s3-bios-test -a xxxxx
-s xxxxx -r ap-south-1 -l productAddedToCartSignal patientVisits
productImpressionSignal

"""

import argparse
import sys

import bios

# Initialize parser
parser = argparse.ArgumentParser(description="BIOS S3 export Utility")

required_group = parser.add_argument_group("Required Arguments")
required_group.add_argument("-e", "--endpoint", help="The bios server endpoint")
required_group.add_argument("-u", "--email", help="The user email")
required_group.add_argument("-p", "--password", help="The user password")
required_group.add_argument("-b", "--bucket", help="The S3 bucket name")
required_group.add_argument("-a", "--accesskey", help="The S3 access key id")
required_group.add_argument("-s", "--secretkey", help="The S3 bucket secret key")
required_group.add_argument("-r", "--region", help="The S3 bucket region")
required_group.add_argument(
    "-l",
    "--listofsignals",
    nargs="*",
    type=str,
    default=[],
    help="The list of signals to export",
)

# Read arguments from command line
try:
    args = parser.parse_args()
except Exception:
    parser.print_help()
    sys.exit(1)

if len(sys.argv) < 2:
    parser.print_help()
    sys.exit(2)

# Create session / login
session = bios.login(args.endpoint, args.email, args.password, "S3ExportUtilityInstance")
print("INFO: Logged into bios")

# define export config
export_config = {}
export_config["storageType"] = "S3"
export_config["exportDestinationName"] = "S3"
export_config["storageConfig"] = {
    "s3BucketName": args.bucket,
    "s3AccessKeyId": args.accesskey,
    "s3SecretAccessKey": args.secretkey,
    "s3Region": args.region,
}
session.data_export(export_config)
session.get_data_export("S3")
print("INFO: S3 export config defined")

# update signals to use this export_config
for signal in args.listofsignals:
    print("INFO: Setting export config for signal: " + signal)
    signal_info = session.get_signal(signal)
    signal_info["exportDestinationName"] = "S3"
    session.update_signal(signal, signal_info)
    print("INFO: Export config set successfully for signal: " + signal)

# enable export
session.start_data_export("S3")
session.get_data_export("S3")
print("INFO: Export to S3 enabled!")
