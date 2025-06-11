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
#include "tfos-common/utils.h"

#include "gtest/gtest.h"

namespace tfos::csdk {

#define TEST_OP_NAME(name)                                     \
  do {                                                         \
    string_t opname = Utils::GetOperationName(CSDK_OP_##name); \
    EXPECT_EQ(opname.length, strlen(#name));                   \
    std::string std_returned(opname.text, opname.length);      \
    EXPECT_EQ(std_returned, #name);                            \
    ++count;                                                   \
  } while (false)

/**
 * Test Statuses::Name() behavior for all OpStatus entries.
 * Also checks number of tested entries to make sure all entries are tested.
 */
TEST(UtilsTest, GetOperationName) {
  int count = 0;
  TEST_OP_NAME(NOOP);
  TEST_OP_NAME(GET_VERSION);
  TEST_OP_NAME(START_SESSION);
  TEST_OP_NAME(RENEW_SESSION);
  TEST_OP_NAME(SELECT_CONTEXT_ENTRIES);
  TEST_OP_NAME(LIST_ENDPOINTS);
  TEST_OP_NAME(LIST_CONTEXT_ENDPOINTS);
  TEST_OP_NAME(UPDATE_ENDPOINTS);
  TEST_OP_NAME(GET_PROPERTY);
  TEST_OP_NAME(SET_PROPERTY);
  TEST_OP_NAME(GET_UPSTREAM_CONFIG);
  TEST_OP_NAME(SELECT_PROTO);
  TEST_OP_NAME(LOGIN_BIOS);
  TEST_OP_NAME(RESET_PASSWORD);
  TEST_OP_NAME(GET_SIGNALS);
  TEST_OP_NAME(CREATE_SIGNAL);
  TEST_OP_NAME(UPDATE_SIGNAL);
  TEST_OP_NAME(DELETE_SIGNAL);
  TEST_OP_NAME(GET_CONTEXTS);
  TEST_OP_NAME(GET_CONTEXT);
  TEST_OP_NAME(CREATE_CONTEXT);
  TEST_OP_NAME(UPDATE_CONTEXT);
  TEST_OP_NAME(DELETE_CONTEXT);
  TEST_OP_NAME(MULTI_GET_CONTEXT_ENTRIES_BIOS);
  TEST_OP_NAME(GET_CONTEXT_ENTRIES_BIOS);
  TEST_OP_NAME(CREATE_CONTEXT_ENTRY_BIOS);
  TEST_OP_NAME(UPDATE_CONTEXT_ENTRY_BIOS);
  TEST_OP_NAME(DELETE_CONTEXT_ENTRY_BIOS);
  TEST_OP_NAME(GET_TENANTS_BIOS);
  TEST_OP_NAME(GET_TENANT_BIOS);
  TEST_OP_NAME(CREATE_TENANT_BIOS);
  TEST_OP_NAME(UPDATE_TENANT_BIOS);
  TEST_OP_NAME(DELETE_TENANT_BIOS);
  TEST_OP_NAME(LIST_APP_TENANTS);
  TEST_OP_NAME(INSERT_PROTO);
  TEST_OP_NAME(INSERT_BULK_PROTO);
  TEST_OP_NAME(REPLACE_CONTEXT_ENTRY_BIOS);
  TEST_OP_NAME(CREATE_EXPORT_DESTINATION);
  TEST_OP_NAME(GET_EXPORT_DESTINATION);
  TEST_OP_NAME(UPDATE_EXPORT_DESTINATION);
  TEST_OP_NAME(DELETE_EXPORT_DESTINATION);
  TEST_OP_NAME(DATA_EXPORT_START);
  TEST_OP_NAME(DATA_EXPORT_STOP);
  TEST_OP_NAME(CREATE_TENANT_APPENDIX);
  TEST_OP_NAME(GET_TENANT_APPENDIX);
  TEST_OP_NAME(UPDATE_TENANT_APPENDIX);
  TEST_OP_NAME(DELETE_TENANT_APPENDIX);
  TEST_OP_NAME(DISCOVER_IMPORT_SUBJECTS);
  TEST_OP_NAME(CREATE_USER);
  TEST_OP_NAME(GET_USERS);
  TEST_OP_NAME(MODIFY_USER);
  TEST_OP_NAME(DELETE_USER);
  TEST_OP_NAME(REGISTER_APPS_SERVICE);
  TEST_OP_NAME(GET_APPS_INFO);
  TEST_OP_NAME(DEREGISTER_APPS_SERVICE);
  TEST_OP_NAME(MAINTAIN_KEYSPACES);
  TEST_OP_NAME(MAINTAIN_TABLES);
  TEST_OP_NAME(MAINTAIN_CONTEXT);
  TEST_OP_NAME(GET_REPORT_CONFIGS);
  TEST_OP_NAME(PUT_REPORT_CONFIG);
  TEST_OP_NAME(DELETE_REPORT);
  TEST_OP_NAME(GET_INSIGHT_CONFIGS);
  TEST_OP_NAME(PUT_INSIGHT_CONFIGS);
  TEST_OP_NAME(DELETE_INSIGHT_CONFIGS);
  TEST_OP_NAME(FEATURE_STATUS);
  TEST_OP_NAME(FEATURE_REFRESH);
  TEST_OP_NAME(GET_ALL_CONTEXT_SYNOPSES);
  TEST_OP_NAME(GET_CONTEXT_SYNOPSIS);
  TEST_OP_NAME(REGISTER_FOR_SERVICE);
  TEST_OP_NAME(STORES_QUERY);
  TEST_OP_NAME(TEST);
  TEST_OP_NAME(TEST_BIOS);
  TEST_OP_NAME(TEST_LOG);
  TEST_OP_NAME(END);
  EXPECT_EQ(count, CSDK_OP_END + 1);
}

/**
 * Test Actual operation name.
 */
TEST(UtilsTest, GetActualOperationName) {
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_LOGIN_BIOS), "LOGIN");

  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_INSERT_PROTO), "INSERT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_INSERT_BULK_PROTO), "INSERT_BULK");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_SELECT_PROTO), "SELECT");

  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS), "UPSERT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS),
      "MULTI_GET_CONTEXT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_GET_CONTEXT_ENTRIES_BIOS), "SELECT_CONTEXT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS), "UPDATE");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS), "DELETE");

  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_CREATE_SIGNAL), "CREATE_SIGNAL");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_GET_SIGNALS), "GET_SIGNALS");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_UPDATE_SIGNAL), "UPDATE_SIGNAL");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_DELETE_SIGNAL), "DELETE_SIGNAL");

  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_CREATE_CONTEXT), "CREATE_CONTEXT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_GET_CONTEXT), "GET_CONTEXT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_GET_CONTEXTS), "GET_CONTEXTS");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_UPDATE_CONTEXT), "UPDATE_CONTEXT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_DELETE_CONTEXT), "DELETE_CONTEXT");

  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_CREATE_TENANT_BIOS), "CREATE_TENANT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_GET_TENANT_BIOS), "GET_TENANT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_GET_TENANTS_BIOS), "GET_TENANTS");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_UPDATE_TENANT_BIOS), "UPDATE_TENANT");
  EXPECT_STREQ(Utils::GetActualOperationName(CSDK_OP_DELETE_TENANT_BIOS), "DELETE_TENANT");
}

const char *OpNameToActualName(const char *name) {
  return Utils::GetActualOperationName(Utils::OperationNameToId(std::string(name)));
}

TEST(UtilsTest, ConvertOperationName) {
  EXPECT_STREQ(OpNameToActualName("LOGIN_BIOS"), "LOGIN");

  EXPECT_STREQ(OpNameToActualName("INSERT_PROTO"), "INSERT");
  EXPECT_STREQ(OpNameToActualName("INSERT_BULK_PROTO"), "INSERT_BULK");
  EXPECT_STREQ(OpNameToActualName("SELECT_PROTO"), "SELECT");

  EXPECT_STREQ(OpNameToActualName("CREATE_CONTEXT_ENTRY_BIOS"), "UPSERT");
  EXPECT_STREQ(OpNameToActualName("GET_CONTEXT_ENTRIES_BIOS"), "SELECT_CONTEXT");
  EXPECT_STREQ(OpNameToActualName("UPDATE_CONTEXT_ENTRY_BIOS"), "UPDATE");
  EXPECT_STREQ(OpNameToActualName("DELETE_CONTEXT_ENTRY_BIOS"), "DELETE");

  EXPECT_STREQ(OpNameToActualName("CREATE_SIGNAL"), "CREATE_SIGNAL");
  EXPECT_STREQ(OpNameToActualName("GET_SIGNALS"), "GET_SIGNALS");
  EXPECT_STREQ(OpNameToActualName("UPDATE_SIGNAL"), "UPDATE_SIGNAL");
  EXPECT_STREQ(OpNameToActualName("DELETE_SIGNAL"), "DELETE_SIGNAL");

  EXPECT_STREQ(OpNameToActualName("CREATE_CONTEXT"), "CREATE_CONTEXT");
  EXPECT_STREQ(OpNameToActualName("GET_CONTEXT"), "GET_CONTEXT");
  EXPECT_STREQ(OpNameToActualName("GET_CONTEXTS"), "GET_CONTEXTS");
  EXPECT_STREQ(OpNameToActualName("UPDATE_CONTEXT"), "UPDATE_CONTEXT");
  EXPECT_STREQ(OpNameToActualName("DELETE_CONTEXT"), "DELETE_CONTEXT");

  EXPECT_STREQ(OpNameToActualName("CREATE_TENANT_BIOS"), "CREATE_TENANT");
  EXPECT_STREQ(OpNameToActualName("GET_TENANT_BIOS"), "GET_TENANT");
  EXPECT_STREQ(OpNameToActualName("GET_TENANTS_BIOS"), "GET_TENANTS");
  EXPECT_STREQ(OpNameToActualName("UPDATE_TENANT_BIOS"), "UPDATE_TENANT");
  EXPECT_STREQ(OpNameToActualName("DELETE_TENANT_BIOS"), "DELETE_TENANT");
}

struct reply {
  struct reply *self;
};

void ReplyErrorTestCallback(status_code_t status_code, completion_data_t *completion_data,
                            payload_t response_data, void *cb_args) {
  auto *state = reinterpret_cast<struct reply *>(cb_args);
  EXPECT_EQ(status_code, Statuses::StatusCode(OpStatus::GENERIC_CLIENT_ERROR));
  EXPECT_EQ(completion_data->endpoint.text, nullptr);
  EXPECT_EQ(completion_data->endpoint.length, 0);
  std::string returned((const char *)response_data.data, response_data.length);
  EXPECT_EQ(returned, "{\"message\":\"bad worse worst\"}");
  EXPECT_EQ(state, state->self);
  Utils::ReleasePayload(&response_data);
}

TEST(UtilsTest, ReplyErrorTest) {
  std::string message = "bad worse worst";
  struct reply reply;
  reply.self = &reply;
  std::string endpoint = "endpoint:443";
  Utils::ReplyError(OpStatus::GENERIC_CLIENT_ERROR, message, ReplyErrorTestCallback, &reply);
}

TEST(UtilsTest, UrlEncodeTest) {
  EXPECT_EQ(Utils::UrlEncode("hello_world"), "hello_world");
  EXPECT_EQ(Utils::UrlEncode("endpoint:https://172.18.0.11:8443"),
            "endpoint%3Ahttps%3A%2F%2F172.18.0.11%3A8443");
  EXPECT_EQ(Utils::UrlEncode("https://www.isima.io:8443/about?my_id=beef&favorite=chicken"),
            "https%3A%2F%2Fwww.isima.io%3A8443%2Fabout%3Fmy_id%3Dbeef%"
            "26favorite%3Dchicken");
  EXPECT_EQ(Utils::UrlEncode("bios+support@isima.io"), "bios%2Bsupport%40isima.io");
  EXPECT_EQ(Utils::UrlEncode("!*'();:@&=+$/?%#"),
            "%21%2A%27%28%29%3B%3A%40%26%3D%2B%24%2F%3F%25%23");
}

TEST(UtilsTest, EndpointDecodeTest) {
  EXPECT_EQ("localhost:443:1", Utils::UrlToEndpoint("https://localhost:443"));
  EXPECT_EQ("localhost:443:1", Utils::UrlToEndpoint("https://localhost"));
  EXPECT_EQ("localhost:8443:1", Utils::UrlToEndpoint("https://localhost:8443"));
  EXPECT_EQ("", Utils::UrlToEndpoint("http://localhost:443"));
  EXPECT_EQ("", Utils::UrlToEndpoint("ttps://localhost:443"));
}

#define TEST_OP_TYPE(name, type)                    \
  do {                                              \
    auto got_type = Utils::GetOperationType(name);  \
    EXPECT_EQ(got_type, type);                      \
    EXPECT_GE(got_type, OperationType::INGEST);     \
    EXPECT_LE(got_type, OperationType::SUMMARIZE);  \
    EXPECT_NE(got_type, OperationType::END);        \
    EXPECT_TRUE(got_type >= OperationType::INGEST); \
    ++count;                                        \
  } while (false)

/**
 * Test Statuses::Name() behavior for all OpStatus entries.
 * Also checks number of tested entries to make sure all entries are tested.
 */
TEST(UtilsTest, GetOperationType) {
  int count = 0;
  TEST_OP_TYPE(CSDK_OP_NOOP, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_VERSION, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_START_SESSION, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_RENEW_SESSION, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_SELECT_CONTEXT_ENTRIES, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_LIST_ENDPOINTS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_LIST_CONTEXT_ENDPOINTS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_UPDATE_ENDPOINTS, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_GET_PROPERTY, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_SET_PROPERTY, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_GET_UPSTREAM_CONFIG, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_SELECT_PROTO, OperationType::EXTRACT);
  TEST_OP_TYPE(CSDK_OP_LOGIN_BIOS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_RESET_PASSWORD, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_SIGNALS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_CREATE_SIGNAL, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_UPDATE_SIGNAL, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_SIGNAL, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_GET_CONTEXTS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_CONTEXT, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_CREATE_CONTEXT, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_UPDATE_CONTEXT, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_CONTEXT, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_CONTEXT_ENTRIES_BIOS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS, OperationType::CONTEXT_WRITE);
  TEST_OP_TYPE(CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS, OperationType::CONTEXT_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS, OperationType::CONTEXT_WRITE);
  TEST_OP_TYPE(CSDK_OP_TEST, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_TEST_BIOS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_TEST_LOG, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_TENANTS_BIOS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_TENANT_BIOS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_CREATE_TENANT_BIOS, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_UPDATE_TENANT_BIOS, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_TENANT_BIOS, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_LIST_APP_TENANTS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_INSERT_PROTO, OperationType::INGEST);
  TEST_OP_TYPE(CSDK_OP_INSERT_BULK_PROTO, OperationType::INGEST);
  TEST_OP_TYPE(CSDK_OP_REPLACE_CONTEXT_ENTRY_BIOS, OperationType::CONTEXT_WRITE);
  TEST_OP_TYPE(CSDK_OP_CREATE_EXPORT_DESTINATION, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_GET_EXPORT_DESTINATION, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_UPDATE_EXPORT_DESTINATION, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_EXPORT_DESTINATION, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DATA_EXPORT_START, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DATA_EXPORT_STOP, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_CREATE_TENANT_APPENDIX, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_TENANT_APPENDIX, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_UPDATE_TENANT_APPENDIX, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_DELETE_TENANT_APPENDIX, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_DISCOVER_IMPORT_SUBJECTS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_CREATE_USER, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_USERS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_MODIFY_USER, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_DELETE_USER, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_REGISTER_APPS_SERVICE, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_APPS_INFO, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_DEREGISTER_APPS_SERVICE, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_MAINTAIN_KEYSPACES, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_MAINTAIN_TABLES, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_MAINTAIN_CONTEXT, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_REPORT_CONFIGS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_PUT_REPORT_CONFIG, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_REPORT, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_GET_INSIGHT_CONFIGS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_PUT_INSIGHT_CONFIGS, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_DELETE_INSIGHT_CONFIGS, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_FEATURE_STATUS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_FEATURE_REFRESH, OperationType::ADMIN_WRITE);
  TEST_OP_TYPE(CSDK_OP_GET_ALL_CONTEXT_SYNOPSES, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_GET_CONTEXT_SYNOPSIS, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_REGISTER_FOR_SERVICE, OperationType::ADMIN_READ);
  TEST_OP_TYPE(CSDK_OP_STORES_QUERY, OperationType::ADMIN_READ);

  TEST_OP_TYPE(CSDK_OP_END, OperationType::ADMIN_READ);
  EXPECT_EQ(count, CSDK_OP_END + 1);
}

}  // namespace tfos::csdk
