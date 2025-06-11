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
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <condition_variable>
#include <memory>
#include <mutex>

#if defined(__linux__)
#include <unistd.h>
#elif defined(__APPLE__) && defined(__MACH__)
// TODO(Naoki): Uncomment and try to make this buildable if we need to run this
// on MacOS. #include <mach-o/dyld.h>
#endif

#include "gtest/gtest.h"
#include "tfos-common/object_mapper.h"
#include "tfos-physical/impl/http_resource_resolver.h"
#include "tfos-physical/impl/physical_pipe_impl.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/message_struct.h"

namespace tfos::csdk {

class PhysicalPipeImplTest : public ::testing::Test {
 protected:
  // valid endpoint parameters
  std::string host_;
  int port_;
  bool is_secure_;
  std::string ssl_cert_file_;

  // Operation results
  struct Results {
    OpStatus status;
    std::string response;
    std::mutex mutex;
    std::condition_variable cv;

    void Clear() {
      status = OpStatus::END;
      response.clear();
    }

    Results() { Clear(); }
  };

  const ResourceResolver *resource_resolver_;
  const ObjectMapper *object_mapper_;
  PhysicalPipeImpl *pipe_;

  void SetUp() {
    PhysicalPipeImpl::GlobalInit();

    host_ = "localhost";
    port_ = 443;
    is_secure_ = true;

    resource_resolver_ = new HttpResourceResolver();
    object_mapper_ = new ObjectMapper();
    pipe_ = nullptr;

    const size_t kBufSize = 2048;
    char exe_file_name[kBufSize] = {0};
#if defined(__linux__)
    ssize_t len = readlink("/proc/self/exe", exe_file_name, sizeof(exe_file_name) - 1);
#elif defined(__APPLE__) && defined(__MACH__)
    // TODO(Naoki): Uncomment and try to make this buildable if we need to run
    // this on MacOS. uint32_t size; _NSGetExecutablePath(exe_file_name, &size);
#endif
    if (len > 0) {
      ssl_cert_file_ = dirname(exe_file_name);
      ssl_cert_file_ += "/../../../../test_data/cacerts.pem";
    }
  }

  void TearDown() {
    pipe_->Shutdown();
    delete resource_resolver_;
    delete object_mapper_;
  }

  static void CollectResults(const ResponseMessage &response_message, void *cb_args) {
    auto results = reinterpret_cast<Results *>(cb_args);
    results->status = response_message.status();
    results->response.assign(reinterpret_cast<const char *>(response_message.response_body()),
                             response_message.body_length());
    Utils::ReleasePayload(response_message.response_body());
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.notify_one();
  }
};

TEST_F(PhysicalPipeImplTest, ValidInitializationWithExplicitCert) {
  pipe_ = new PhysicalPipeImpl(host_, port_, is_secure_, resource_resolver_, object_mapper_);
  pipe_->set_ssl_cert_file(ssl_cert_file_);

  std::unique_ptr<Results> results(new Results());
  pipe_->Initialize(CollectResults, results.get());
  {
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.wait(lock);
  }

  EXPECT_EQ(results->status, OpStatus::OK);
  EXPECT_EQ(results->response.size(), 0);
}

TEST_F(PhysicalPipeImplTest, ValidInitializationWithImplicitCert) {
  char buffer[2048];
  snprintf(buffer, sizeof(buffer), "SSL_CERT_FILE=%s", ssl_cert_file_.c_str());
  putenv(buffer);
  pipe_ = new PhysicalPipeImpl(host_, port_, is_secure_, resource_resolver_, object_mapper_);

  std::unique_ptr<Results> results(new Results());
  pipe_->Initialize(CollectResults, results.get());

  {
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.wait(lock);
  }

  EXPECT_EQ(results->status, OpStatus::OK);
  EXPECT_EQ(results->response.size(), 0);
  unsetenv("SSL_CERT_FILE");
}

TEST_F(PhysicalPipeImplTest, InvalidInitializationForWrongCertPath) {
  char buffer[2048];
  snprintf(buffer, sizeof(buffer), "SSL_CERT_FILE=%sBAD", ssl_cert_file_.c_str());
  putenv(buffer);
  pipe_ = new PhysicalPipeImpl(host_, port_, is_secure_, resource_resolver_, object_mapper_);

  std::unique_ptr<Results> results(new Results());
  pipe_->Initialize(CollectResults, results.get());
  {
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.wait(lock);
  }

  EXPECT_EQ(results->status, OpStatus::SERVER_CONNECTION_FAILURE);
  EXPECT_EQ(results->response,
            "{\"message\":\"Failed to verify server certificate: self-signed "
            "certificate (18)\"}");
  unsetenv("SSL_CERT_FILE");
}

TEST_F(PhysicalPipeImplTest, InvalidInitializationForWrongEndpoint) {
  pipe_ =
      new PhysicalPipeImpl(host_ + "bad", port_, is_secure_, resource_resolver_, object_mapper_);
  pipe_->set_ssl_cert_file(ssl_cert_file_);

  std::unique_ptr<Results> results(new Results());
  pipe_->Initialize(CollectResults, results.get());
  {
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.wait(lock);
  }

  EXPECT_EQ(results->status, OpStatus::SERVER_CONNECTION_FAILURE);
}

TEST_F(PhysicalPipeImplTest, ValidLogin) {
  pipe_ = new PhysicalPipeImpl(host_, port_, is_secure_, resource_resolver_, object_mapper_);
  pipe_->set_ssl_cert_file(ssl_cert_file_);

  std::unique_ptr<Results> results(new Results());
  pipe_->Initialize(CollectResults, results.get());
  {
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.wait(lock);
  }
  EXPECT_EQ(results->status, OpStatus::OK);

  results->Clear();
  ObjectMapper mapper;
  payload_t payload = mapper.WriteValue<CredentialsBIOS>({
      .email = "superadmin",
      .password = "superadmin",
  });
  Session session(1, nullptr, 1000, true);
  RequestContext context(&session, session.timeout, nullptr, CollectResults, results.get(), nullptr,
                         false);
  request_message_t request_message = {
      .op_id_ = CSDK_OP_LOGIN_BIOS,
      .payload_ = payload,
      .context_ = &context,
  };

  pipe_->HandleRequest(request_message);
  {
    std::unique_lock<std::mutex> lock(results->mutex);
    results->cv.wait(lock);
  }
  EXPECT_EQ(results->status, OpStatus::OK);
  TEST_LOG("response=%s", results->response.c_str());
  Utils::ReleasePayload(&payload);
}

}  // namespace tfos::csdk
