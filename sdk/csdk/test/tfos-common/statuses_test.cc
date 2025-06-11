/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gtest/gtest.h"
#include "tfoscsdk/status.h"

using tfos::csdk::OpStatus;
using tfos::csdk::Statuses;

static void name_test(int *count, std::set<OpStatus> *tested, OpStatus error,
                      const std::string &name) {
  EXPECT_EQ(Statuses::Name(error), name);
  string_t cname = Statuses::CName(error);
  EXPECT_EQ(std::string(cname.text, cname.length), name);
  ++*count;
  EXPECT_TRUE(tested->find(error) == tested->end());
  tested->insert(error);
}

/**
 * Test Statuses::Name() behavior for all OpStatus entries.
 * Also checks number of tested entries to make sure all entries are tested.
 */
TEST(StatusesTest, StatusNames) {
  int count = 0;
  std::set<OpStatus> tested;

  name_test(&count, &tested, OpStatus::OK, "OK");
  name_test(&count, &tested, OpStatus::GENERIC_CLIENT_ERROR, "GENERIC_CLIENT_ERROR");
  name_test(&count, &tested, OpStatus::INVALID_ARGUMENT, "INVALID_ARGUMENT");
  name_test(&count, &tested, OpStatus::SESSION_INACTIVE, "SESSION_INACTIVE");
  name_test(&count, &tested, OpStatus::PARSER_ERROR, "PARSER_ERROR");
  name_test(&count, &tested, OpStatus::CLIENT_ALREADY_STARTED, "CLIENT_ALREADY_STARTED");
  name_test(&count, &tested, OpStatus::REQUEST_TOO_LARGE, "REQUEST_TOO_LARGE");
  name_test(&count, &tested, OpStatus::INVALID_STATE, "INVALID_STATE");
  name_test(&count, &tested, OpStatus::CLIENT_CHANNEL_ERROR, "CLIENT_CHANNEL_ERROR");
  name_test(&count, &tested, OpStatus::SERVER_CONNECTION_FAILURE, "SERVER_CONNECTION_FAILURE");
  name_test(&count, &tested, OpStatus::SERVER_CHANNEL_ERROR, "SERVER_CHANNEL_ERROR");
  name_test(&count, &tested, OpStatus::UNAUTHORIZED, "UNAUTHORIZED");
  name_test(&count, &tested, OpStatus::FORBIDDEN, "FORBIDDEN");
  name_test(&count, &tested, OpStatus::SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE");
  name_test(&count, &tested, OpStatus::GENERIC_SERVER_ERROR, "GENERIC_SERVER_ERROR");
  name_test(&count, &tested, OpStatus::TIMEOUT, "TIMEOUT");
  name_test(&count, &tested, OpStatus::BAD_GATEWAY, "BAD_GATEWAY");
  name_test(&count, &tested, OpStatus::SERVICE_UNDEPLOYED, "SERVICE_UNDEPLOYED");
  name_test(&count, &tested, OpStatus::OPERATION_CANCELLED, "OPERATION_CANCELLED");
  name_test(&count, &tested, OpStatus::OPERATION_UNEXECUTABLE, "OPERATION_UNEXECUTABLE");
  name_test(&count, &tested, OpStatus::SESSION_EXPIRED, "SESSION_EXPIRED");
  name_test(&count, &tested, OpStatus::NO_SUCH_TENANT, "NO_SUCH_TENANT");
  name_test(&count, &tested, OpStatus::NO_SUCH_STREAM, "NO_SUCH_STREAM");
  name_test(&count, &tested, OpStatus::NOT_FOUND, "NOT_FOUND");
  name_test(&count, &tested, OpStatus::TENANT_ALREADY_EXISTS, "TENANT_ALREADY_EXISTS");
  name_test(&count, &tested, OpStatus::STREAM_ALREADY_EXISTS, "STREAM_ALREADY_EXISTS");
  name_test(&count, &tested, OpStatus::RESOURCE_ALREADY_EXISTS, "RESOURCE_ALREADY_EXISTS");
  name_test(&count, &tested, OpStatus::BAD_INPUT, "BAD_INPUT");
  name_test(&count, &tested, OpStatus::NOT_IMPLEMENTED, "NOT_IMPLEMENTED");
  name_test(&count, &tested, OpStatus::CONSTRAINT_WARNING, "CONSTRAINT_WARNING");
  name_test(&count, &tested, OpStatus::SCHEMA_VERSION_CHANGED, "SCHEMA_VERSION_CHANGED");
  name_test(&count, &tested, OpStatus::INVALID_REQUEST, "INVALID_REQUEST");
  name_test(&count, &tested, OpStatus::BULK_INGEST_FAILED, "BULK_INGEST_FAILED");
  name_test(&count, &tested, OpStatus::SERVER_DATA_ERROR, "SERVER_DATA_ERROR");
  name_test(&count, &tested, OpStatus::SCHEMA_MISMATCHED, "SCHEMA_MISMATCHED");
  name_test(&count, &tested, OpStatus::USER_ID_NOT_VERIFIED, "USER_ID_NOT_VERIFIED");

  EXPECT_EQ(count, static_cast<unsigned int>(OpStatus::END));
}

/**
 * Test Statuses::get_code() by picking up a few entries and check their
 * status codes.
 */
TEST(StatusesTest, StatusCodes) {
  EXPECT_EQ(Statuses::StatusCode(OpStatus::OK), 0x000000);
  EXPECT_EQ(Statuses::StatusCode(OpStatus::NO_SUCH_TENANT), 0x210001);
  EXPECT_EQ(Statuses::StatusCode(OpStatus::BAD_INPUT), 0x210007);
}

/**
 * Test Statuses::GetDescription() by picking up a few entries and check their
 * descriptions.
 */
TEST(StatusesTest, Descriptions) {
  EXPECT_EQ(Statuses::Description(OpStatus::OK), "Successful");
  EXPECT_EQ(Statuses::Description(OpStatus::NO_SUCH_TENANT),
            "SDK tried an operation that requires a tenant,"
            " but the specified tenant does not exist");
  EXPECT_EQ(Statuses::Description(OpStatus::BAD_INPUT),
            "Server rejected input due to invalid data or format");
}
