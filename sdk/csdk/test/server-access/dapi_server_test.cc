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
#include <condition_variable>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>

#include "gtest/gtest.h"

#include "_bios/data.pb.h"
#include "tfos-common/message_handler.h"
#include "tfos-common/object_mapper.h"
#include "tfos-dapi/dapi_modules.h"
#include "tfos-physical/connection.h"
#include "tfos-physical/connection_manager.h"

namespace tfos::csdk {

class PayloadHolder {
 private:
  payload_t &payload_;

 public:
  explicit PayloadHolder(payload_t &payload) : payload_(payload) {}
  ~PayloadHolder() noexcept { delete[] payload_.data; }
};

static const char *kDefaultUrl = "https://localhost:8443";
static const int64_t kOpTimeoutMillis = 240000;

class DefaultMessageHandler;

ObjectMapper *object_mapper;
static std::array<std::vector<std::string> *, static_cast<int>(OperationType::END)> optype2host_;

static CredentialsBIOS superadmin = {
    .email = "superadmin",
    .password = "superadmin",
};
class DapiServerTest : public ::testing::Test {
 protected:
  capi_t capi_id;
  std::string host_;
  int port_;
  int sadmin_session_id_;
  int admin_session_id_;
  std::unique_ptr<std::vector<std::string>> saved_endpoints_;
  std::unique_ptr<UpstreamConfig> upstream_config_;

  template <typename T>
  std::unique_ptr<T> TestReadValue(const ObjectMapper &object_mapper, const std::string &src) {
    return object_mapper.ReadValue<T>(reinterpret_cast<uint8_t *>(const_cast<char *>(src.c_str())),
                                      src.size());
  }

  void login_and_setup(CredentialsBIOS credentials, std::string scope) {
    capi_id = DaInitialize("");
    ASSERT_GE(capi_id, 0);
    {
      Results<int> results;
      TEST_LOG("Requesting to create the first session");
      DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                     {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
      if (!results.done) {
        std::unique_lock<std::mutex> lock(results.mutex);
        results.cv.wait(lock);
      }
      ASSERT_EQ(results.status, OpStatus::OK);
      ASSERT_GE(results.session_id, 0);

      sadmin_session_id_ = results.session_id;
      results.Clear();

      CredentialsBIOS credentials = superadmin;
      payload_t payload = object_mapper->WriteValue(credentials);
      ASSERT_NE(payload.data, nullptr);
      DaLoginBIOS(capi_id, sadmin_session_id_, payload, 0, CompleteLogin, &results);
      if (!results.done) {
        std::unique_lock<std::mutex> lock(results.mutex);
        results.cv.wait(lock);
      }
      ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;

      delete[] payload.data;
      results.Clear();
    }
  }

  std::unique_ptr<std::vector<std::string>> get_endpoints() {
    Results<std::string> results;
    std::vector<std::string> *ret = nullptr;
    DaSimpleMethod(capi_id, sadmin_session_id_, CSDK_OP_LIST_ENDPOINTS, nullptr, nullptr, {0},
                   nullptr, nullptr, CompleteGenericMethod<std::string>, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    return TestReadValue<std::vector<std::string>>(*object_mapper, results.response_string);
  }

  std::unique_ptr<UpstreamConfig> get_upstream_config() {
    Results<std::string> results;
    std::vector<std::string> *ret = nullptr;
    DaSimpleMethod(capi_id, sadmin_session_id_, CSDK_OP_GET_UPSTREAM_CONFIG, nullptr, nullptr, {0},
                   nullptr, nullptr, CompleteGenericMethod<std::string>, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    return TestReadValue<UpstreamConfig>(*object_mapper, results.response_string);
  }

  void build_upstream_to_host_map() {
    UpstreamConfig *ucp = upstream_config_.get();
    auto upstreams = ucp->upstream();
    EXPECT_NE(upstreams, nullptr);
    for (int i = 0; i < optype2host_.size(); i++) {
      optype2host_[i] = new std::vector<std::string>();
    }
    for (int i = 0; i < upstreams->size(); i++) {
      auto upstream = (*upstreams)[i].get();
      auto op_set = upstream->operation_set();
      auto host_set = upstream->host_set();
      auto operation_vec_ptr = op_set->operation();
      for (auto host : host_set) {
        for (uint64_t j = 0; j < operation_vec_ptr->size(); j++) {
          auto operation = (*operation_vec_ptr)[j].get();
          if (operation->operation_policy() == OperationPolicy::ALWAYS ||
              operation->operation_policy() == OperationPolicy::UNDERFAILURE) {
            auto op_type = operation->operation_type();
            auto host_vec = optype2host_[static_cast<int>(op_type)];
            size_t end = host.find("://");
            if (end == std::string::npos) {
              assert(false);
            }
            auto ep = host.substr(end + 3);
            host_vec->emplace_back(ep);
          }
        }
      }
    }
  }

  void SetUp() override {
    // Get Env TFOS_NODES or something.
    // Save the nodes and type
    // Delete all node.
    // Create your configuration
    // Save Configuration
    object_mapper = new ObjectMapper();
    char *env = getenv("TFOS_ENDPOINT");
    std::string url = env != nullptr ? env : kDefaultUrl;
    std::unique_ptr<Connection> conn(Connection::BuildConnection(url));
    if (!conn) {
      std::cerr << "ERROR: Invalid syntax of environment variable TFOS_ENDPOINT: " << url
                << std::endl;
      throw std::exception();
    }
    host_ = conn->host();
    port_ = conn->port();
    login_and_setup(superadmin, "");
    saved_endpoints_ = get_endpoints();
    ASSERT_NE(saved_endpoints_, nullptr);
    ASSERT_GE(saved_endpoints_->size(), 3);

    upstream_config_ = get_upstream_config();
    EXPECT_NE(upstream_config_, nullptr);
    build_upstream_to_host_map();
    DaTerminate(capi_id);
  }

  void TearDown() override {
    if (capi_id > 0) {
      DaTerminate(capi_id);
    }
    // Restore original configuration
    delete object_mapper;
    for (int i = 0; i < optype2host_.size(); i++) {
      delete optype2host_[i];
    }
  }

  template <typename T>
  struct Results {
    OpStatus status;
    int32_t session_id;
    std::unique_ptr<T> generic_response;
    std::string response_string;
    std::mutex mutex;
    std::condition_variable cv;
    bool done;

    void Clear() {
      status = OpStatus::END;
      session_id = -1;
      done = false;
    }

    Results() { Clear(); }
  };

  static bool verify_routing_endpoint(string_t endpoint, OperationType type) {
    auto endpoints = optype2host_[static_cast<int>(type)];
    std::string ep(endpoint.text);
    for (int i = 0; i < endpoints->size(); i++) {
      if (ep == (*endpoints)[i]) {
        return true;
      }
    }
    return false;
  }

  template <typename T>
  static void CompleteIngestMethod(status_code_t status_code, completion_data_t *completion_data,
                                   payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<Results<T> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    EXPECT_TRUE(verify_routing_endpoint(completion_data->endpoint, OperationType::INGEST));
    if (results->status == OpStatus::OK) {
      results->generic_response = object_mapper->ReadValue<T>(response_data);
    }
    results->response_string.assign((const char *)response_data.data, response_data.length);
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }

  template <typename T>
  static void CompleteTestMethod(status_code_t status_code, completion_data_t *completion_data,
                                 payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<Results<T> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    EXPECT_TRUE(verify_routing_endpoint(completion_data->endpoint, OperationType::ADMIN_READ));
    if (results->status == OpStatus::OK) {
      results->generic_response = object_mapper->ReadValue<T>(response_data);
    }
    results->response_string.assign((const char *)response_data.data, response_data.length);
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }

  static void CompleteSessionStart(status_code_t status_code, completion_data_t *completion_data,
                                   payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<Results<StartSessionResponse> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    TEST_LOG("RESP=%s addr=%p", (const char *)response_data.data, response_data.data);
    auto response = object_mapper->ReadValue<StartSessionResponse>(response_data);
    if (response) {
      results->session_id = response->sessionId;
    }
    std::unique_lock<std::mutex> lock(results->mutex);
    TEST_LOG("signaled");
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }

  static void CompleteLogin(status_code_t status_code, completion_data_t *completion_data,
                            payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<Results<int> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    results->response_string.assign((const char *)response_data.data, response_data.length);
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }

  static void CompleteAdminWriteOp(status_code_t status_code, completion_data_t *completion_data,
                                   payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<Results<int> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    results->response_string.assign((const char *)response_data.data, response_data.length);

    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }

  template <typename T>
  static void CompleteGenericMethod(status_code_t status_code, completion_data_t *completion_data,
                                    payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<Results<T> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    if (results->status == OpStatus::OK) {
      results->generic_response = object_mapper->ReadValue<T>(response_data);
    }
    results->response_string.assign((const char *)response_data.data, response_data.length);
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }

  static void CompleteInsertion(status_code_t status_code, completion_data_t *completion_data,
                                payload_t response_data, void *cb_args) {
    auto results = reinterpret_cast<
      Results<com::isima::bios::models::proto::InsertSuccessResponse> *>(cb_args);
    results->status = Statuses::FromStatusCode(status_code);
    results->response_string.assign((const char *)response_data.data, response_data.length);
    if (results->status == OpStatus::OK) {
      results->generic_response =
          std::make_unique<com::isima::bios::models::proto::InsertSuccessResponse>();
      results->generic_response->ParseFromString(results->response_string);
    }
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
    results->done = true;
    Utils::ReleasePayload(&response_data);
  }
};

TEST_F(DapiServerTest, ModulesStartStop) {
  capi_id = DaInitialize("");
  ASSERT_GE(capi_id, 0);
}

TEST_F(DapiServerTest, BIOSLoginTest) {
  capi_id = DaInitialize("");
  ASSERT_GE(capi_id, 0);
  int sadmin_session_id;
  {
    Results<int> results;
    TEST_LOG("Requesting to create the first session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    ASSERT_GE(results.session_id, 0);

    sadmin_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = {
        .email = "superadmin",
        .password = "superadmin",
    };
    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    DaLoginBIOS(capi_id, sadmin_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    delete[] payload.data;
    results.Clear();
  }
  DaTerminate(capi_id);
}

TEST_F(DapiServerTest, TestIngest) {
  capi_id = DaInitialize("");
  ASSERT_GE(capi_id, 0);

  // Try logging in as superadmin
  int sadmin_session_id;
  int admin_session_id;
  {
    Results<int> results;
    TEST_LOG("Requesting to create the first session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    ASSERT_GE(results.session_id, 0);

    sadmin_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = superadmin;
    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    DaLoginBIOS(capi_id, sadmin_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    // TODO(Naoki): Write object mapper reader for StreamConfig and check
    // decoded values.

    delete[] payload.data;
    results.Clear();
  }

  // Try deleting the test tenant. It's OK to fail for NO_SUCH_TENANT
  {
    Results<int> results;
    TEST_LOG("Start deleteTenant");
    string_t tenant = {.text = "dapi_test", .length = 9};
    string_t resources[] = {{"tenantName", 10}, {"dapi_test", 9}, {nullptr, 0}};
    DaGenericMethod(capi_id, sadmin_session_id, CSDK_OP_DELETE_TENANT_BIOS, resources, nullptr,
                   {}, -1, nullptr, CompleteAdminWriteOp, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    TEST_LOG("End deleteTenant result=%s", Statuses::Name(results.status).c_str());
    ASSERT_TRUE(results.status == OpStatus::OK || results.status == OpStatus::NO_SUCH_TENANT);
    results.Clear();
  }

  // Add the test tenant
  {
    Results<int> results;
    TEST_LOG("Start addTenant");
    const char *src = "{\"tenantName\":\"dapi_test\"}";
    payload_t payload = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>(src)),
                         .length = static_cast<int64_t>(strlen(src))};
    DaSimpleMethod(capi_id, sadmin_session_id, CSDK_OP_CREATE_TENANT_BIOS, nullptr, nullptr,
                   payload, nullptr, nullptr, CompleteAdminWriteOp, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    results.Clear();
    TEST_LOG("End addTenant");
  }

  {
    Results<int> results;
    TEST_LOG("Requesting to create the admin session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    ASSERT_GE(results.session_id, 0);

    admin_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = {
        .email = "admin@dapi_test",
        .password = "admin",
    };
    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    DaLoginBIOS(capi_id, admin_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    // TODO(Naoki): Write object mapper reader for StreamConfig and check
    // decoded values.

    delete[] payload.data;
    results.Clear();
  }

  // Add the test stream
  {
    Results<int> results;
    TEST_LOG("Start addStream");
    const char *src =
        "{"
        "  \"signalName\": \"test_stream\","
        "  \"missingAttributePolicy\": \"Reject\","
        "  \"attributes\": ["
        "      {\"attributeName\":\"one\", \"type\": \"string\"},"
        "      {\"attributeName\":\"two\", \"type\": \"string\"}"
        "  ]"
        "}";
    payload_t payload = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>(src)),
                         .length = static_cast<int64_t>(strlen(src))};
    string_t resources[] = {{"tenantName", 10}, {"dapi_test", 9}, {nullptr, 0}};
    DaGenericMethod(capi_id, admin_session_id, CSDK_OP_CREATE_SIGNAL, resources, nullptr, payload,
                   -1, nullptr, CompleteAdminWriteOp, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    results.Clear();
    TEST_LOG("End addStream");
  }

  // Modify the test stream
  {
    Results<int> results;
    TEST_LOG("Start updateStream");
    const char *src =
        "{"
        "  \"signalName\": \"test_stream\","
        "  \"missingAttributePolicy\": \"Reject\","
        "  \"attributes\": ["
        "      {\"attributeName\":\"one\", \"type\": \"string\"},"
        "      {\"attributeName\":\"two\", \"type\": \"string\"},"
        "      {\"attributeName\":\"three\", \"type\": \"integer\", \"default\":0}"
        "  ]"
        "}";
    payload_t payload = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>(src)),
                         .length = static_cast<int64_t>(strlen(src))};
    string_t tenant = {.text = "dapi_test", .length = 9};
    string_t stream = {.text = "test_stream", .length = 11};
    DaSimpleMethod(capi_id, admin_session_id, CSDK_OP_UPDATE_SIGNAL, &tenant, &stream, payload,
                   nullptr, nullptr, CompleteAdminWriteOp, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    results.Clear();
    TEST_LOG("End updateStream");
  }


  // Create another session for ingest and run ingest
  int ingest_session_id;
  {
    Results<StartSessionResponse> results;
    TEST_LOG("Requesting to create another session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      TEST_LOG("waiting");
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    ASSERT_GT(results.session_id, sadmin_session_id);

    ingest_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = {
        .email = "ingest@dapi_test",
        .password = "ingest",
    };
    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    std::string stream = "test_stream";
    DaLoginBIOS(capi_id, ingest_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    // TODO(Naoki): Write object mapper reader for StreamConfig and check
    // decoded values.

    delete[] payload.data;
    results.Clear();

    com::isima::bios::models::proto::InsertRequest request;
    request.set_content_rep(com::isima::bios::models::proto::ContentRepresentation::CSV);
    auto* record = request.mutable_record();
    record->add_string_values("hello,world,89512");

    std::stringstream ss;
    request.SerializeToOstream(&ss);
    std::string payload_str{ss.str()};
    payload.data = reinterpret_cast<uint8_t*>(const_cast<char*>(payload_str.c_str()));
    payload.length = payload_str.size();

    ASSERT_NE(payload.data, nullptr);
    Results<com::isima::bios::models::proto::InsertSuccessResponse> results2;
    string_t tenant = {.text = "dapi_test", .length = 9};
    string_t resources[] = {
      {"stream", 6}, {stream.c_str(), static_cast<int32_t>(stream.size())},
      {"eventId", 7}, {"77a5db7e-d5b8-11e9-bb65-2a2ae2dbcce4", 36},
      {nullptr, 0}
    };
    DaGenericMethod(capi_id, ingest_session_id, CSDK_OP_INSERT_PROTO, resources, nullptr,
                    payload, -1, nullptr, CompleteInsertion, &results2);

    // for testing channel errors
#if 0
    TEST_LOG("S L E E P  F O R  2 0 s\n");
    sleep(20);
#endif

    if (!results2.done) {
      std::unique_lock<std::mutex> lock(results2.mutex);
      TEST_LOG("waiting for the insertion to complete");
      results2.cv.wait(lock);
    }
    EXPECT_EQ(results2.status, OpStatus::OK) << results2.response_string;
    if (results2.status == OpStatus::OK) {
      const auto& resp = results2.generic_response;
      ASSERT_TRUE(resp);
      // The timestamp is always the same since the provided event ID (is time UUID) is hard coded.
      EXPECT_EQ(resp->insert_timestamp(), 1568332386956);
      // TODO(Naoki): Decode the event ID
      // EXPECT_EQ(resp->eventId, "77a5db7e-d5b8-11e9-bb65-2a2ae2dbcce4");
    }
    results2.Clear();

    // Try ingesting large data
    com::isima::bios::models::proto::InsertRequest request2;
    request2.set_content_rep(com::isima::bios::models::proto::ContentRepresentation::CSV);
    std::string event_text{};
    event_text.reserve(32 * 1024);
    for (int i = 0; i < 30 * 1024; ++i) {
      event_text += static_cast<char>(i % 10) + '0';
    }
    event_text += ",column2,12345";
    request2.mutable_record()->add_string_values(event_text);

    std::stringstream ss2;
    request2.SerializeToOstream(&ss2);
    payload_str = ss2.str();
    payload.data = reinterpret_cast<uint8_t*>(const_cast<char*>(payload_str.c_str()));
    payload.length = payload_str.size();

    string_t resources2[] = {
      {"stream", 6}, {stream.c_str(), static_cast<int32_t>(stream.size())},
      {"eventId", 7}, {"9841e450-0fb9-11ea-8d71-362b9e155667", 36},
      {nullptr, 0}
    };
    DaGenericMethod(capi_id, ingest_session_id, CSDK_OP_INSERT_PROTO, resources2, nullptr,
                    payload, -1, nullptr, CompleteInsertion, &results2);
    if (!results2.done) {
      std::unique_lock<std::mutex> lock(results2.mutex);
      TEST_LOG("waiting for the second insertion to complete");
      results2.cv.wait(lock);
    }
    EXPECT_EQ(results2.status, OpStatus::OK) << results2.response_string;
    results2.Clear();
  }

  // Test operation timeout
  {
    Results<std::string> results;
    TEST_LOG("Testing timeout");
    const char *src = "{\"delay\":2000}";
    bool timeout_changed = DaSetOpTimeoutMillis(capi_id, sadmin_session_id, 1000);
    ASSERT_TRUE(timeout_changed);
    payload_t payload = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>(src)),
                         .length = static_cast<int64_t>(strlen(src))};
    DaSimpleMethod(capi_id, sadmin_session_id, CSDK_OP_TEST, nullptr, nullptr, payload, nullptr,
                   nullptr, CompleteGenericMethod<std::string>, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    TEST_LOG("End timeout test result=%s", Statuses::Name(results.status).c_str());
    EXPECT_TRUE(results.status == OpStatus::TIMEOUT);

    // Wait until server response time and verify the physical pipe does not
    // crash.
    sleep(2);

    results.Clear();
  }

  // Test bios login and get_signals
  {
    DaTerminate(capi_id);
    capi_id = DaInitialize("");
    Results<StartSessionResponse> results;
    TEST_LOG("Requesting to create another session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      TEST_LOG("waiting");
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);

    int ingest_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = {
        .email = "admin@dapi_test",
        .password = "admin",
    };
    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    DaLoginBIOS(capi_id, ingest_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    // TODO(Naoki): Write object mapper reader for StreamConfig and check
    // decoded values.

    delete[] payload.data;
    results.Clear();
    Results<IngestResponse> results2;
    string_t options[] = {{"names", 5}, {"test_stream", 11}, {nullptr, 0}};
    DaGenericMethod(capi_id, ingest_session_id, CSDK_OP_GET_SIGNALS, nullptr, options, {}, -1, {},
                    CompleteGenericMethod<IngestResponse>, &results2);

    // for testing channel errors
#if 0
    TEST_LOG("S L E E P  F O R  2 0 s\n");
    sleep(20);
#endif

    if (!results2.done) {
      std::unique_lock<std::mutex> lock(results2.mutex);
      TEST_LOG("waiting");
      results2.cv.wait(lock);
    }
    EXPECT_EQ(results2.status, OpStatus::OK) << results2.response_string;
    if (results2.status == OpStatus::OK) {
      EXPECT_EQ(results2.response_string, "[{\"signalName\":\"test_stream\"}]");
    }
    results2.Clear();
  }
}

/**
 * This test verifies the behavior of cancelling pending operations by
 * terminating the client.
 *
 * The test program sends two delayed echo operations and terminates the client
 * before these operations complete.  Both operations should end with status
 * OpStatus::OPERATION_CANCELLED.
 */
// TODO(BIOS-4902): Enable the test case after porting the test methods on the server side.
TEST_F(DapiServerTest, DISABLED_TestCancellingOp) {
  capi_id = DaInitialize("");
  ASSERT_GE(capi_id, 0);

  // Try logging in as superadmin
  int sadmin_session_id;
  int admin_session_id;
  {
    Results<int> results;
    TEST_LOG("Requesting to create the first session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    ASSERT_GE(results.session_id, 0);

    sadmin_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = superadmin;
    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    DaLoginBIOS(capi_id, sadmin_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;
    // TODO(Naoki): Write object mapper reader for StreamConfig and check
    // decoded values.

    delete[] payload.data;
    results.Clear();
  }

  // Test operation cancellation
  {
    Results<std::string> results;
    Results<std::string> results2;

    const char *src = "{\"delay\":5000}";
    payload_t payload = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>(src)),
                         .length = static_cast<int64_t>(strlen(src))};

    DaSimpleMethod(capi_id, sadmin_session_id, CSDK_OP_TEST, nullptr, nullptr, payload, nullptr,
                   nullptr, CompleteGenericMethod<std::string>, &results);

    sleep(2);

    payload_t payload2 = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>(src)),
                          .length = static_cast<int64_t>(strlen(src))};
    DaSimpleMethod(capi_id, sadmin_session_id, CSDK_OP_TEST, nullptr, nullptr, payload2, nullptr,
                   nullptr, CompleteGenericMethod<std::string>, &results2);

    TEST_LOG("BEGIN terminating the client");
    DaTerminate(capi_id);
    capi_id = -1;
    TEST_LOG("END terminating the client");

    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    TEST_LOG("End operation cancellation test result=%s", Statuses::Name(results.status).c_str());
    EXPECT_EQ(results.status, OpStatus::OPERATION_CANCELLED);

    if (!results2.done) {
      std::unique_lock<std::mutex> lock(results2.mutex);
      results2.cv.wait(lock);
    }
    TEST_LOG("End operation cancellation test result2=%s", Statuses::Name(results2.status).c_str());
    EXPECT_EQ(results2.status, OpStatus::OPERATION_CANCELLED);

    // Wait until server response time and verify the physical pipe does not crash.
    sleep(5);

    results.Clear();
    results2.Clear();
  }
}

TEST_F(DapiServerTest, TestUpstream) {
  capi_id = DaInitialize("");
  ASSERT_GE(capi_id, 0);

  // Try logging in as superadmin
  int sadmin_session_id;
  int admin_session_id;
  {
    Results<int> results;
    TEST_LOG("Requesting to create the first session");
    DaStartSession(capi_id, {host_.c_str(), static_cast<int32_t>(host_.size())}, port_, true,
                   {nullptr, 0}, {nullptr, 0}, kOpTimeoutMillis, CompleteSessionStart, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK);
    ASSERT_GE(results.session_id, 0);

    sadmin_session_id = results.session_id;
    results.Clear();

    CredentialsBIOS credentials = superadmin;

    payload_t payload = object_mapper->WriteValue(credentials);
    ASSERT_NE(payload.data, nullptr);
    DaLoginBIOS(capi_id, sadmin_session_id, payload, 0, CompleteLogin, &results);
    if (!results.done) {
      std::unique_lock<std::mutex> lock(results.mutex);
      results.cv.wait(lock);
    }
    ASSERT_EQ(results.status, OpStatus::OK) << results.response_string;

    delete[] payload.data;
    results.Clear();
    // TODO(Manish) Continue with upstream testing
  }
}

}  // namespace tfos::csdk
