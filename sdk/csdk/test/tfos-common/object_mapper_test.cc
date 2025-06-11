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
#include "tfos-common/object_mapper.h"

#include <boost/flyweight.hpp>
#include <memory>

#include "gtest/gtest.h"
#include "tfoscsdk/models.h"

namespace tfos::csdk {

static std::string MakeJson(const std::string &src) {
  std::string out;
  out.reserve(src.length() * 2);
  for (int i = 0; i < src.size(); ++i) {
    char ch = src[i];
    switch (ch) {
      case '\'':
        out += '"';
        break;
      default:
        out += ch;
    }
  }
  return out;
}

static std::string Payload2Str(const payload_t &src) {
  return std::string((const char *)src.data, static_cast<size_t>(src.length));
}

template <typename T>
std::unique_ptr<T> TestReadValue(const ObjectMapper &object_mapper, const std::string &src) {
  std::string json = MakeJson(src);
  return object_mapper.ReadValue<T>(reinterpret_cast<uint8_t *>(const_cast<char *>(json.c_str())),
                                    json.size());
}

TEST(ObjectMapperTest, StartSessionResponseWriting) {
  StartSessionResponse resp = {
      .sessionId = 314192,
  };
  ObjectMapper mapper;
  payload_t serialized = mapper.WriteValue(resp);
  std::string expected = "{'sessionId':314192}";
  EXPECT_EQ(Payload2Str(serialized), MakeJson(expected));
  delete[] serialized.data;
}

TEST(ObjectMapperTest, CredentialsParsing) {
  ObjectMapper mapper;
  std::string src =
      "{'username':'myuser','password':'mypassword','scope':'/"
      "tenants/mytenant'}";
  std::unique_ptr<Credentials> credentials = TestReadValue<Credentials>(mapper, src);
  ASSERT_TRUE(credentials);
  EXPECT_EQ(credentials->username, "myuser");
  EXPECT_EQ(credentials->password, "mypassword");
  EXPECT_EQ(credentials->scope, "/tenants/mytenant");
}

TEST(ObjectMapperTest, CredentialsParsingRandomPropertyOrder) {
  ObjectMapper mapper;
  std::string src =
      "{'password':'password can contain white spaces',"
      "'username':'myuser','scope':'/'}";
  std::unique_ptr<Credentials> credentials = TestReadValue<Credentials>(mapper, src);
  ASSERT_TRUE(credentials);
  EXPECT_EQ(credentials->username, "myuser");
  EXPECT_EQ(credentials->password, "password can contain white spaces");
  EXPECT_EQ(credentials->scope, "/");
}

TEST(ObjectMapperTest, BadCredentialsJson) {
  ObjectMapper mapper;
  std::string src =
      "{'username':'myuser','password','mypassword','scope':'/"
      "tenants/mytenant'}";
  std::unique_ptr<Credentials> credentials = TestReadValue<Credentials>(mapper, src);
  ASSERT_FALSE(credentials);
}

TEST(ObjectMapperTest, CredentialsWriting) {
  ObjectMapper mapper;
  Credentials credentials = {
      .username = "myuser",
      .password = "mypassword",
      .scope = "/tenants/mytenant",
  };
  payload_t serialized = mapper.WriteValue(credentials);
  std::string expected =
      "{'username':'myuser','password':'mypassword','scope':"
      "'/tenants/mytenant'}";
  EXPECT_EQ(Payload2Str(serialized), MakeJson(expected));
  delete[] serialized.data;
}

TEST(ObjectMapperTest, CredentialsBIOSParsing) {
  ObjectMapper mapper;
  std::string src = "{'email':'myuser','password':'mypassword'}";
  std::unique_ptr<CredentialsBIOS> credentials = TestReadValue<CredentialsBIOS>(mapper, src);
  ASSERT_TRUE(credentials);
  EXPECT_EQ(credentials->email, "myuser");
  EXPECT_EQ(credentials->password, "mypassword");
}

TEST(ObjectMapperTest, CredentialsBIOSParsingRandomPropertyOrder) {
  ObjectMapper mapper;
  std::string src =
      "{'password':'password can contain white spaces',"
      "'email':'myuser'}";
  std::unique_ptr<CredentialsBIOS> credentials = TestReadValue<CredentialsBIOS>(mapper, src);
  ASSERT_TRUE(credentials);
  EXPECT_EQ(credentials->email, "myuser");
  EXPECT_EQ(credentials->password, "password can contain white spaces");
}

TEST(ObjectMapperTest, BadCredentialsBIOSJson) {
  ObjectMapper mapper;
  std::string src = "{'email':'myuser','password','mypassword'}";
  std::unique_ptr<Credentials> credentials = TestReadValue<Credentials>(mapper, src);
  ASSERT_FALSE(credentials);
}

TEST(ObjectMapperTest, CredentialsBIOSWriting) {
  ObjectMapper mapper;
  CredentialsBIOS credentials = {
      .email = "myuser",
      .password = "mypassword",
  };
  payload_t serialized = mapper.WriteValue(credentials);
  std::string expected = "{'email':'myuser','password':'mypassword'}";
  EXPECT_EQ(Payload2Str(serialized), MakeJson(expected));
  delete[] serialized.data;
}

TEST(ObjectMapperTest, LoginResponseBiosWriting) {
  ObjectMapper mapper;
  LoginResponseBIOS login_response = {.tenant = "some_tenant",
                                      .upstreams = "upstreams",
                                      .dev_instance = "instance",
                                      .home_page_config = "example.com/home",
                                      .token = "some_token"};
  payload_t serialized = mapper.WriteValue(login_response);
  std::string expected =
      "{'tenant':'some_tenant','devInstance':'instance','"
      "homePageConfig':'example.com/home'}";
  EXPECT_EQ(Payload2Str(serialized), MakeJson(expected));
  delete[] serialized.data;
}

TEST(ObjectMapperTest, StartSessionResponseParsing) {
  ObjectMapper mapper;
  std::string src = "{'sessionId':8892}";
  auto start_session_response = TestReadValue<StartSessionResponse>(mapper, src);
  ASSERT_TRUE(start_session_response);
  EXPECT_EQ(start_session_response->sessionId, 8892);
}

TEST(ObjectMapperTest, LoginResponseParsing) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token',"
      "'expiry':1567629241000,"
      "'sessionTimeoutMillis':1567629241123,"
      "'permissions':['ADMIN']}";
  std::unique_ptr<LoginResponse> login_response = TestReadValue<LoginResponse>(mapper, src);
  ASSERT_TRUE(login_response);
  EXPECT_EQ(login_response->token, "the_token");
  EXPECT_EQ(login_response->expiry, 1567629241000);
  EXPECT_EQ(login_response->sessionTimeoutMillis, 1567629241123);
  ASSERT_EQ(login_response->permissions.size(), 1);
  EXPECT_EQ(login_response->permissions[0], Permission::ADMIN);
}

TEST(ObjectMapperTest, LoginResponseBIOSParsingInvalidResponse) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token'"
      "'expiry':1567629241000}";
  std::unique_ptr<LoginResponseBIOS> login_response = TestReadValue<LoginResponseBIOS>(mapper, src);
  ASSERT_FALSE(login_response);
}

TEST(ObjectMapperTest, LoginResponseBIOSParsingMissingField) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token',"
      "'tenant': 'the_tenant',"
      "'homePageConfig': 'www.example.com',"
      "'upstreams': 'foobar',"
      "'expiry':1567629241000}";
  std::unique_ptr<LoginResponseBIOS> login_response = TestReadValue<LoginResponseBIOS>(mapper, src);
  ASSERT_TRUE(login_response);
  EXPECT_EQ(login_response->token, "the_token");
  EXPECT_EQ(login_response->tenant, "the_tenant");
  EXPECT_EQ(login_response->dev_instance, "");
  EXPECT_EQ(login_response->home_page_config, "www.example.com");
  EXPECT_EQ(login_response->expiry, 1567629241000);
}

TEST(ObjectMapperTest, LoginResponseBIOSParsing) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token',"
      "'tenant': 'the_tenant',"
      "'devInstance': 'instance',"
      "'homePageConfig': 'www.example.com',"
      "'upstreams': 'foobar',"
      "'expiry':1567629241000}";
  std::unique_ptr<LoginResponseBIOS> login_response = TestReadValue<LoginResponseBIOS>(mapper, src);
  ASSERT_TRUE(login_response);
  EXPECT_EQ(login_response->token, "the_token");
  EXPECT_EQ(login_response->tenant, "the_tenant");
  EXPECT_EQ(login_response->dev_instance, "instance");
  EXPECT_EQ(login_response->home_page_config, "www.example.com");
  EXPECT_EQ(login_response->upstreams, "foobar");
  EXPECT_EQ(login_response->expiry, 1567629241000);
}

TEST(ObjectMapperTest, LoginResponseSuperAdminPermission) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token',"
      "'expiry':1567629241000,"
      "'sessionTimeoutMillis':1567629241123,"
      "'permissions':['SuperAdmin']}";
  std::unique_ptr<LoginResponse> login_response = TestReadValue<LoginResponse>(mapper, src);
  ASSERT_TRUE(login_response);
  EXPECT_EQ(login_response->token, "the_token");
  EXPECT_EQ(login_response->expiry, 1567629241000);
  EXPECT_EQ(login_response->sessionTimeoutMillis, 1567629241123);
  ASSERT_EQ(login_response->permissions.size(), 1);
  EXPECT_EQ(login_response->permissions[0], Permission::SUPERADMIN);
}

TEST(ObjectMapperTest, LoginResponseIngestPermission) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token',"
      "'expiry':1567629241000,"
      "'sessionTimeoutMillis':1567629241123,"
      "'permissions':['INGEST']}";
  std::unique_ptr<LoginResponse> login_response = TestReadValue<LoginResponse>(mapper, src);
  ASSERT_TRUE(login_response);
  EXPECT_EQ(login_response->token, "the_token");
  EXPECT_EQ(login_response->expiry, 1567629241000);
  EXPECT_EQ(login_response->sessionTimeoutMillis, 1567629241123);
  ASSERT_EQ(login_response->permissions.size(), 1);
  EXPECT_EQ(login_response->permissions[0], Permission::INGEST);
}

TEST(ObjectMapperTest, LoginResponseExtractPermission) {
  ObjectMapper mapper;
  std::string src =
      "{'token':'the_token',"
      "'expiry':1567629241000,"
      "'sessionTimeoutMillis':1567629241123,"
      "'permissions':['extract']}";
  std::unique_ptr<LoginResponse> login_response = TestReadValue<LoginResponse>(mapper, src);
  ASSERT_TRUE(login_response);
  EXPECT_EQ(login_response->token, "the_token");
  EXPECT_EQ(login_response->expiry, 1567629241000);
  EXPECT_EQ(login_response->sessionTimeoutMillis, 1567629241123);
  ASSERT_EQ(login_response->permissions.size(), 1);
  EXPECT_EQ(login_response->permissions[0], Permission::EXTRACT);
}

TEST(ObjectMapperTest, ErrorResponseWriterSimple) {
  ObjectMapper mapper;
  ErrorResponse response("connection failed");

  payload_t out = mapper.WriteValue(response);
  EXPECT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'message':'connection failed'}"));
  delete[] out.data;
}

TEST(ObjectMapperTest, ErrorResponseWriterFull) {
  ObjectMapper mapper;
  ErrorResponse response("connection failed", "0x010101");

  payload_t out = mapper.WriteValue(response);
  EXPECT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'message':'connection failed','errorCode':'0x010101'}"));
  delete[] out.data;
}

TEST(ObjectMapperTest, ErrorResponseReader) {
  ObjectMapper mapper;
  std::string src =
      "{'status': 'BAD_GATEWAY', 'message':'connection "
      "failed','errorCode':'0x010101'}";
  auto response = TestReadValue<ErrorResponse>(mapper, src);
  EXPECT_EQ(response->message, "connection failed");
  EXPECT_EQ(response->error_code, "0x010101");
}

TEST(ObjectMapperTest, IngestBulkErrorResponseReader) {
  ObjectMapper mapper;
  std::string src =
      "{'status': 'BAD_GATEWAY', 'message':'connection "
      "failed','errorCode':'0x010101'}";

  auto response = TestReadValue<IngestBulkErrorResponse>(mapper, src);
  EXPECT_EQ(response->message, "connection failed");
  EXPECT_EQ(response->error_code, "0x010101");
  EXPECT_EQ(response->results_with_error.size(), 0);
}

TEST(ObjectMapperTest, IngestBulkErrorResponseReaderWriterWithError) {
  ObjectMapper mapper;
  std::string src =
      "{'status': 'BAD_REQUEST', 'message': 'Ingest bulk failure',"
      "'resultsWithError':"
      "[{'timestamp':1585321762915,'eventId':'448f8a7f-b9ca-46ad-9836-"
      "f0f1135036d6'},"
      "{'statusCode':400,'errorMessage':'Test "
      "Error','serverErrorCode':'ADMIN03',"
      "'eventId':'2fa9afbe-0ea8-400e-b519-89ac0dcf842d'}]}";

  auto response = TestReadValue<IngestBulkErrorResponse>(mapper, src);
  EXPECT_EQ(response->message, "Ingest bulk failure");
  EXPECT_EQ(response->results_with_error.size(), 2);

  // change the status code and check as that is what we are going to do in CSDK
  response->results_with_error[1].status_code = 1000;
  payload_t out = mapper.WriteValue(*response);
  ASSERT_NE(out.data, nullptr);

  auto response1 = TestReadValue<IngestBulkErrorResponse>(mapper, Payload2Str(out));
  EXPECT_EQ(response1->results_with_error.size(), 2);
  EXPECT_EQ(response1->results_with_error[1].status_code, 1000);
  delete[] out.data;
}

TEST(ObjectMapperTest, IngestRequest) {
  ObjectMapper mapper;
  IngestRequest request = {
      .eventId = "77a5db7e-d5b8-11e9-bb65-2a2ae2dbcce4",
      .eventText = "hello,world",
      .streamVersion = 1568332462815,
  };
  payload_t out = mapper.WriteValue(request);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'eventId':'77a5db7e-d5b8-11e9-bb65-2a2ae2dbcce4',"
                                       "'eventText':'hello,world',"
                                       "'streamVersion':1568332462815}"));
  delete[] out.data;
  request = {
      .eventId = "77a5db7e-d5b8-11e9-bb65-2a2ae2dbcce4",
      .eventText = "hello,world",
      .streamVersion = 0,
  };
  out = mapper.WriteValue(request);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'eventId':'77a5db7e-d5b8-11e9-bb65-2a2ae2dbcce4',"
                                       "'eventText':'hello,world'}"));
  delete[] out.data;
}

TEST(ObjectMapperTest, IngestResponseParsing) {
  ObjectMapper mapper;
  std::string src =
      "{'eventId':'a6c7cf76-d5bc-11e9-bb65-2a2ae2dbcce4','"
      "ingestTimestamp':1567629241451}";
  std::unique_ptr<IngestResponse> ingest_response = TestReadValue<IngestResponse>(mapper, src);
  ASSERT_TRUE(ingest_response);
  EXPECT_EQ(ingest_response->eventId, "a6c7cf76-d5bc-11e9-bb65-2a2ae2dbcce4");
  EXPECT_EQ(ingest_response->ingestTimestamp, 1567629241451);
}

TEST(ObjectMapperTest, AdminOpType) {
  ObjectMapper mapper;
  AdminOpType add = AdminOpType::ADD;
  payload_t out_add = mapper.WriteValue(add);
  ASSERT_NE(out_add.data, nullptr);
  EXPECT_EQ(Payload2Str(out_add), MakeJson("'ADD'"));
  Utils::ReleasePayload(&out_add);

  AdminOpType remove = AdminOpType::REMOVE;
  payload_t out_remove = mapper.WriteValue(remove);
  ASSERT_NE(out_remove.data, nullptr);
  EXPECT_EQ(Payload2Str(out_remove), MakeJson("'REMOVE'"));
  Utils::ReleasePayload(&out_remove);

  AdminOpType update = AdminOpType::UPDATE;
  payload_t out_update = mapper.WriteValue(update);
  ASSERT_NE(out_update.data, nullptr);
  EXPECT_EQ(Payload2Str(out_update), MakeJson("'UPDATE'"));
  Utils::ReleasePayload(&out_update);
}

TEST(ObjectMapperTest, RequestPhase) {
  ObjectMapper mapper;
  RequestPhase initial = RequestPhase::INITIAL;
  payload_t out_initial = mapper.WriteValue(initial);
  ASSERT_NE(out_initial.data, nullptr);
  EXPECT_EQ(Payload2Str(out_initial), MakeJson("'INITIAL'"));
  Utils::ReleasePayload(&out_initial);

  RequestPhase final = RequestPhase::FINAL;
  payload_t out_final = mapper.WriteValue(final);
  ASSERT_NE(out_final.data, nullptr);
  EXPECT_EQ(Payload2Str(out_final), MakeJson("'FINAL'"));
  Utils::ReleasePayload(&out_final);
}

TEST(ObjectMapperTest, AdminWriteRequest) {
  ObjectMapper mapper;
  AdminWriteRequest request(AdminOpType::ADD, RequestPhase::INITIAL, 0, {0}, false);
  payload_t out = mapper.WriteValue(request);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'operation':'ADD','phase':'INITIAL','payload':{}}"));
  Utils::ReleasePayload(&out);

  request.phase = RequestPhase::FINAL;
  request.timestamp = 1570673838514;
  std::string payload = MakeJson("{'name':'hello','type':'signal'}");
  request.payload.data = reinterpret_cast<uint8_t *>(const_cast<char *>(payload.c_str()));
  request.payload.length = payload.size();
  request.force = true;
  out = mapper.WriteValue(request);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'operation':'ADD','phase':'FINAL',"
                                       "'timestamp':1570673838514,'force':true,"
                                       "'payload':{'name':'hello','type':'signal'}}"));
  Utils::ReleasePayload(&out);
}

TEST(ObjectMapperTest, AdminWriteResponse) {
  ObjectMapper mapper;
  auto resp = TestReadValue<AdminWriteResponse>(mapper,
                                                "{'timestamp':1570673838543,"
                                                "'endpoints':['https://10.20.30.40:443',"
                                                "'https://10.20.30.31:443']}");
  ASSERT_TRUE(resp);
  EXPECT_EQ(resp->timestamp, 1570673838543);
  EXPECT_EQ(resp->endpoints.size(), 2);
  EXPECT_EQ(resp->endpoints[0], "https://10.20.30.40:443");
  EXPECT_EQ(resp->endpoints[1], "https://10.20.30.31:443");
}

TEST(ObjectMapperTest, ListOfStringReader) {
  ObjectMapper mapper;
  std::string src = "['Portland','Oregon','USA']";
  auto elements = TestReadValue<std::vector<std::string>>(mapper, src);
  ASSERT_TRUE(elements);
  ASSERT_EQ(elements->size(), 3);
  EXPECT_EQ((*elements)[0], "Portland");
  EXPECT_EQ((*elements)[1], "Oregon");
  EXPECT_EQ((*elements)[2], "USA");
}

TEST(ObjectMapperTest, ListOfFlyweightString) {
  ObjectMapper mapper;
  std::string src = "['Portland','Oregon','USA']";
  auto elements = TestReadValue<std::vector<boost::flyweight<std::string>>>(mapper, src);
  ASSERT_TRUE(elements);
  ASSERT_EQ(elements->size(), 3);
  EXPECT_EQ(elements->at(0), "Portland");
  EXPECT_EQ(elements->at(1), "Oregon");
  EXPECT_EQ(elements->at(2), "USA");
}

TEST(ObjectMapperTest, ListOfStringWriter) {
  ObjectMapper mapper;
  std::vector<std::string> data;
  data.push_back("Kuala Lampur");
  data.push_back("Hong Kong");
  data.push_back("Bengaluru");
  data.push_back("Delhi");
  payload_t out = mapper.WriteValue(data);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("['Kuala Lampur','Hong Kong','Bengaluru','Delhi']"));
  Utils::ReleasePayload(&out);
}

TEST(ObjectMapperTest, FailureReport) {
  FailureReport report;
  report.timestamp = 1575949244899;
  report.operation = "POST /tfos/v1/admin/tenants";
  report.payload = "{\"hello\":\"world\"}";
  report.endpoints.push_back("https://node1.isima.io");
  report.endpoints.push_back("https://node3.isima.io");
  report.reasons.push_back("SERVICE_UNAVAILABLE");
  report.reasons.push_back("SERVER_CHANNEL_ERROR");
  report.reporter = "C-SDK 12345";
  ObjectMapper mapper;
  payload_t out = mapper.WriteValue(report);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'timestamp':1575949244899,"
                                       "'operation':'POST /tfos/v1/admin/tenants',"
                                       "'payload':'{\\\"hello\\\":\\\"world\\\"}',"
                                       "'endpoints':['https://node1.isima.io',"
                                       "'https://node3.isima.io'],"
                                       "'reasons':['SERVICE_UNAVAILABLE','SERVER_CHANNEL_ERROR'],"
                                       "'reporter':'C-SDK 12345'"
                                       "}"));
  Utils::ReleasePayload(&out);
}

TEST(ObjectMapperTest, NodeType) {
  ObjectMapper mapper;

  payload_t out_signal = mapper.WriteValue(NodeType::SIGNAL);
  ASSERT_NE(out_signal.data, nullptr);
  EXPECT_EQ(Payload2Str(out_signal), MakeJson("'SIGNAL'"));
  Utils::ReleasePayload(&out_signal);

  out_signal = mapper.WriteValue(static_cast<NodeType>(CSDK_NODE_TYPE_SIGNAL));
  EXPECT_EQ(Payload2Str(out_signal), MakeJson("'SIGNAL'"));
  Utils::ReleasePayload(&out_signal);

  payload_t out_rollup = mapper.WriteValue(NodeType::ROLLUP);
  ASSERT_NE(out_rollup.data, nullptr);
  EXPECT_EQ(Payload2Str(out_rollup), MakeJson("'ROLLUP'"));
  Utils::ReleasePayload(&out_rollup);

  out_rollup = mapper.WriteValue(static_cast<NodeType>(CSDK_NODE_TYPE_ROLLUP));
  EXPECT_EQ(Payload2Str(out_rollup), MakeJson("'ROLLUP'"));
  Utils::ReleasePayload(&out_rollup);

  payload_t out_analysis = mapper.WriteValue(NodeType::ANALYSIS);
  ASSERT_NE(out_analysis.data, nullptr);
  EXPECT_EQ(Payload2Str(out_analysis), MakeJson("'ANALYSIS'"));
  Utils::ReleasePayload(&out_analysis);

  out_analysis = mapper.WriteValue(static_cast<NodeType>(CSDK_NODE_TYPE_ANALYSIS));
  EXPECT_EQ(Payload2Str(out_analysis), MakeJson("'ANALYSIS'"));
  Utils::ReleasePayload(&out_analysis);
}

TEST(ObjectMapperTest, UpdateEndpointRequest) {
  ObjectMapper mapper;

  UpdateEndpointRequest req = {
      .operation = AdminOpType::ADD,
      .endpoint = "https://123.45.67:833",
      .nodeType = NodeType::SIGNAL,
  };

  payload_t out = mapper.WriteValue(req);
  ASSERT_NE(out.data, nullptr);
  EXPECT_EQ(Payload2Str(out), MakeJson("{'operation':'ADD','endpoint':'https://"
                                       "123.45.67:833','nodeType':'SIGNAL'}"));
  Utils::ReleasePayload(&out);
}

TEST(UpstreamFromJson, OperationFromInvalidJson) {
  std::string src = "{'operationPolicy':'ALWAYS''operationType':'SUMMARIZE'}";
  ObjectMapper object_mapper;
  std::unique_ptr<Operation> operation = TestReadValue<Operation>(object_mapper, src);
  EXPECT_EQ(operation, nullptr);
}

TEST(UpstreamFromJson, OperationFromMissingFields) {
  std::string src = "{'operationPolicy':'ALWAYS''operationp':'SUMMARIZE'}";
  ObjectMapper object_mapper;
  std::unique_ptr<Operation> operation = TestReadValue<Operation>(object_mapper, src);
  EXPECT_EQ(operation, nullptr);
}

TEST(UpstreamFromJson, OperationFromJson) {
  std::string src = "{'operationPolicy':'ALWAYS','operationType':'SUMMARIZE'}";
  ObjectMapper object_mapper;
  std::unique_ptr<Operation> operation = TestReadValue<Operation>(object_mapper, src);
  EXPECT_NE(operation, nullptr);
  EXPECT_TRUE(operation->operation_policy() == OperationPolicy::ALWAYS);
  EXPECT_TRUE(operation->operation_type() == OperationType::SUMMARIZE);
}

TEST(UpstreamFromJson, OperationSetFromInvalidJson) {
  std::string src =
      "{'operation':[{'operationPolicy':'UNDER','operationType':"
      "'INGEST'},{'operationPolicy':'ALWAYS','operationType':"
      "'EXTRACT'}]}";

  ObjectMapper object_mapper;
  std::unique_ptr<OperationSet> operation_set = TestReadValue<OperationSet>(object_mapper, src);
  EXPECT_EQ(operation_set, nullptr);
}

TEST(UpstreamFromJson, OperationSetFromMissingFieldsJson) {
  std::string src =
      "{'operation':[{'operationPolicy':'UNDERFAILURE'"
      "},{'operationPolicy':'ALWAYS','operationType':"
      "'EXTRACT'}]}";

  ObjectMapper object_mapper;
  std::unique_ptr<OperationSet> operation_set = TestReadValue<OperationSet>(object_mapper, src);
  EXPECT_EQ(operation_set, nullptr);
}

TEST(UpstreamFromJson, OperationSetFromJson) {
  std::string src =
      "{'operation':[{'operationPolicy':'UNDERFAILURE','operationType':"
      "'INGEST'},{'operationPolicy':'ALWAYS','operationType':"
      "'EXTRACT'}]}";

  ObjectMapper object_mapper;
  auto operation_set = TestReadValue<OperationSet>(object_mapper, src);
  EXPECT_EQ(operation_set->operation()->size(), 2);
  auto operation_vec_ptr = operation_set->operation();
  auto operation = (*operation_vec_ptr)[0].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::UNDERFAILURE);
  EXPECT_EQ(operation->operation_type(), OperationType::INGEST);
  operation = (*operation_vec_ptr)[1].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::EXTRACT);
}

TEST(UpstreamFromJson, UpstreamFromInvalidJson) {
  std::string src =
      "{'hostSet':['bios-signal-1', "
      "'bios-analysis-2'],'operationSet'{'operation':[{"
      "'operationPolicy':'UNDERFAILURE','operationType':'INGEST'},{"
      "'operationPolicy':'ALWAYS','operationType':'CONTEXT_WRITE'}]}}";
  ObjectMapper object_mapper;
  std::unique_ptr<Upstream> upstream = TestReadValue<Upstream>(object_mapper, src);
  EXPECT_EQ(upstream, nullptr);
}

TEST(UpstreamFromJson, UpstreamFromFieldsMissingJson) {
  std::string src =
      "{"
      "'operationSet'{'operation':[{"
      "'operationPolicy':'UNDERFAILURE','operationType':'INGEST'},{"
      "'operationPolicy':'ALWAYS','operationType':'CONTEXT_WRITE'}]}}";
  ObjectMapper object_mapper;
  std::unique_ptr<Upstream> upstream = TestReadValue<Upstream>(object_mapper, src);
  EXPECT_EQ(upstream, nullptr);
}

TEST(UpstreamFromJson, UpstreamFromJson) {
  std::string src =
      "{'hostSet':['bios-signal-1', "
      "'bios-analysis-2'],'operationSet':{'operation':[{"
      "'operationPolicy':'UNDERFAILURE','operationType':'INGEST'},{"
      "'operationPolicy':'ALWAYS','operationType':'CONTEXT_WRITE'}]}}";
  ObjectMapper object_mapper;
  std::unique_ptr<Upstream> upstream = TestReadValue<Upstream>(object_mapper, src);
  auto operation_set = upstream->operation_set();
  EXPECT_EQ(operation_set->operation()->size(), 2);
  auto host_set = upstream->host_set();
  auto operation_vec_ptr = operation_set->operation();
  auto operation = (*operation_vec_ptr)[0].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::UNDERFAILURE);
  EXPECT_EQ(operation->operation_type(), OperationType::INGEST);
  operation = (*operation_vec_ptr)[1].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::CONTEXT_WRITE);
  EXPECT_EQ(host_set.size(), 2);
  EXPECT_EQ(host_set[0], "bios-signal-1");
  EXPECT_EQ(host_set[1], "bios-analysis-2");
  EXPECT_EQ(upstream->NumNodes(), 2);
}

TEST(UpstreamFromJson, UpstreamConfigFromInvalidJson) {
  std::string src =
      "{"
      "  'lbPolicy''ROUND_ROBIN',"
      "  'upstreams': ["
      "    {"
      "      'hostSet': ["
      "        'bios-signal-1'"
      "      ],"
      "      'operationSet': {"
      "        'operation': ["
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'INGEST'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'CONTEXT_WRITE'"
      "          }"
      "        ]"
      "      }"
      "    }"
      "  ]"
      "}";
  ObjectMapper object_mapper;
  std::unique_ptr<UpstreamConfig> upstream_config =
      TestReadValue<UpstreamConfig>(object_mapper, src);
  EXPECT_EQ(upstream_config, nullptr);
}

TEST(UpstreamFromJson, UpstreamConfigFromMissingFieldsJson) {
  std::string src =
      "{"
      "  'upstreams': ["
      "    {"
      "      'hostSet': ["
      "        'bios-signal-1'"
      "      ],"
      "      'operationSet': {"
      "        'operation': ["
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'INGEST'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'CONTEXT_WRITE'"
      "          }"
      "        ]"
      "      }"
      "    }"
      "  ]"
      "}";
  ObjectMapper object_mapper;
  std::unique_ptr<UpstreamConfig> upstream_config =
      TestReadValue<UpstreamConfig>(object_mapper, src);
  EXPECT_EQ(upstream_config, nullptr);
}

TEST(UpstreamFromJson, UpstreamConfigFromJson) {
  std::string src =
      "{"
      "  'lbPolicy': 'ROUND_ROBIN',"
      "  'upstreams': ["
      "    {"
      "      'hostSet': ["
      "        'bios-signal-1'"
      "      ],"
      "      'operationSet': {"
      "        'operation': ["
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'INGEST'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'CONTEXT_WRITE'"
      "          }"
      "        ]"
      "      }"
      "    },"
      "    {"
      "      'hostSet': ["
      "        'bios-analysis-1'"
      "      ],"
      "      'operationSet': {"
      "        'operation': ["
      "          {"
      "            'operationPolicy': 'UNDERFAILURE',"
      "            'operationType': 'INGEST'"
      "          },"
      "          {"
      "            'operationPolicy': 'NEVER',"
      "            'operationType': 'EXTRACT'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'ADMIN_READ'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'ADMIN_WRITE'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'SUMMARIZE'"
      "          }"
      "        ]"
      "      }"
      "    },"
      "    {"
      "      'hostSet': ["
      "        'bios-rollup-1','bios-rollup-2'"
      "      ],"
      "      'operationSet': {"
      "        'operation': ["
      "          {"
      "            'operationPolicy': 'UNDERFAILURE',"
      "            'operationType': 'INGEST'"
      "          },"
      "          {"
      "            'operationPolicy': 'ALWAYS',"
      "            'operationType': 'EXTRACT'"
      "          }"
      "        ]"
      "      }"
      "    }"
      "  ]"
      "}";
  ObjectMapper object_mapper;
  auto upstream_config = TestReadValue<UpstreamConfig>(object_mapper, src);
  ASSERT_NE(upstream_config, nullptr);
  auto upstreams = upstream_config->upstream();
  EXPECT_EQ(upstreams->size(), 3);
  EXPECT_EQ(upstream_config->NumNodes(), 4);
  EXPECT_EQ(upstream_config->lb_policy(), LbPolicy::ROUND_ROBIN);
  auto upstreamVecPtr = upstream_config->upstream();
  auto upstream = (*upstreamVecPtr)[0].get();
  auto operation_set = upstream->operation_set();
  auto host_set = upstream->host_set();
  EXPECT_EQ(operation_set->operation()->size(), 2);
  auto operation_vec_ptr = operation_set->operation();
  auto operation = (*operation_vec_ptr)[0].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::INGEST);
  operation = (*operation_vec_ptr)[1].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::CONTEXT_WRITE);
  EXPECT_EQ(host_set.size(), 1);
  EXPECT_EQ(host_set[0], "bios-signal-1");
  upstream = (*upstreamVecPtr)[1].get();
  operation_set = upstream->operation_set();
  host_set = upstream->host_set();
  EXPECT_EQ(operation_set->operation()->size(), 5);
  operation_vec_ptr = operation_set->operation();
  operation = (*operation_vec_ptr)[0].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::UNDERFAILURE);
  EXPECT_EQ(operation->operation_type(), OperationType::INGEST);
  operation = (*operation_vec_ptr)[1].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::NEVER);
  EXPECT_EQ(operation->operation_type(), OperationType::EXTRACT);
  operation = (*operation_vec_ptr)[2].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::ADMIN_READ);
  operation = (*operation_vec_ptr)[3].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::ADMIN_WRITE);
  operation = (*operation_vec_ptr)[4].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::SUMMARIZE);
  EXPECT_EQ(host_set.size(), 1);
  EXPECT_EQ(host_set[0], "bios-analysis-1");
  upstream = (*upstreamVecPtr)[2].get();
  operation_set = upstream->operation_set();
  host_set = upstream->host_set();
  EXPECT_EQ(operation_set->operation()->size(), 2);
  operation_vec_ptr = operation_set->operation();
  operation = (*operation_vec_ptr)[0].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::UNDERFAILURE);
  EXPECT_EQ(operation->operation_type(), OperationType::INGEST);
  operation = (*operation_vec_ptr)[1].get();
  EXPECT_EQ(operation->operation_policy(), OperationPolicy::ALWAYS);
  EXPECT_EQ(operation->operation_type(), OperationType::EXTRACT);
  EXPECT_EQ(host_set.size(), 2);
  EXPECT_EQ(host_set[0], "bios-rollup-1");
}
}  // namespace tfos::csdk
