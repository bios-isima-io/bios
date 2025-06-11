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
#include "tfos-common/message_handler.h"

#include <stdio.h>

#include "gtest/gtest.h"
#include "tfos-common/logger.h"
#include "tfos-common/object_mapper.h"
#include "tfos-common/session.h"
#include "tfos-dapi/dapi_modules.h"
#include "tfos-physical/connection.h"
#include "tfos-physical/connection_manager.h"
#include "tfoscsdk/status.h"

namespace tfos::csdk {

class MessageHandlerTest : public ::testing::Test {
 protected:
  const ObjectMapper *object_mapper_;

  void SetUp() { object_mapper_ = new ObjectMapper(); }

  void TearDown() { delete object_mapper_; }

  struct return_values {
    OpStatus status;
    ConnectionSet *conn_set;
    std::string message;
    const uint8_t *data;
    int64_t length;
  };

  static void CollectGetConnectionSetResults(const ResponseMessage &msg, ConnectionSet *conn_set,
                                             void *cb_args) {
    auto return_values = reinterpret_cast<struct return_values *>(cb_args);
    return_values->status = msg.status();
    return_values->conn_set = conn_set;
    return_values->message.assign((const char *)msg.response_body(), (int64_t)msg.body_length());
    delete[] msg.response_body();
  }

  static void CollectPlaceRequestResults(const ResponseMessage &resp, void *cb_args) {
    auto return_values = reinterpret_cast<struct return_values *>(cb_args);
    return_values->status = resp.status();
    return_values->data = resp.response_body();
    return_values->length = resp.body_length();
  }
};

TEST_F(MessageHandlerTest, GetConnectionSet) {
  ConnectionManager *manager = ConnectionManagerFactory::Create("mock", object_mapper_);
  MessageHandler *handler = MessageHandlerFactory::Create("basic", object_mapper_, manager);
  ASSERT_NE(handler, nullptr);
  ASSERT_EQ(handler->name(), "basic");

  // Try getting a connection set
  struct return_values return_values = {
      .status = OpStatus::END,
      .conn_set = nullptr,
  };
  handler->GetConnectionSet("isima.io", 8432, true, "cafiles.pem", CollectGetConnectionSetResults,
                            &return_values);
  EXPECT_EQ(return_values.status, OpStatus::OK);
  EXPECT_NE(return_values.conn_set, nullptr);

  EXPECT_TRUE(
      return_values.conn_set->Contains(Connection::MakeEndpointString("isima.io", 8432, true)));
  EXPECT_FALSE(
      return_values.conn_set->Contains(Connection::MakeEndpointString("isima.io", 8443, true)));

  // Try getting another connection set
  struct return_values return_values2 = {
      .status = OpStatus::END,
      .conn_set = nullptr,
  };
  handler->GetConnectionSet("elasticflash.com", 8443, true, "", CollectGetConnectionSetResults,
                            &return_values2);
  EXPECT_EQ(return_values2.status, OpStatus::OK);
  EXPECT_NE(return_values2.conn_set, nullptr);
  EXPECT_NE(return_values2.conn_set, return_values.conn_set);

  EXPECT_FALSE(
      return_values2.conn_set->Contains(Connection::MakeEndpointString("isima.io", 8432, true)));
  EXPECT_TRUE(return_values2.conn_set->Contains(
      Connection::MakeEndpointString("elasticflash.com", 8443, true)));

  // try again and verify the method returns the same connection set
  struct return_values return_values3 = {
      .status = OpStatus::END,
      .conn_set = nullptr,
  };
  handler->GetConnectionSet("isima.io", 8432, true, "cafiles.pem", CollectGetConnectionSetResults,
                            &return_values3);
  EXPECT_EQ(return_values3.status, OpStatus::OK);
  EXPECT_EQ(return_values3.conn_set, return_values.conn_set);

  // try invalid host name
  struct return_values return_values4 = {
      .status = OpStatus::END,
      .conn_set = nullptr,
  };
  handler->GetConnectionSet("example.com", 443, true, "cafiles.pem", CollectGetConnectionSetResults,
                            &return_values4);
  EXPECT_EQ(return_values4.status, OpStatus::SERVER_CONNECTION_FAILURE);
  EXPECT_EQ(return_values4.conn_set, nullptr);
  EXPECT_EQ(return_values4.message, "no such host");

  // try insecure connection -- not yet implemented
  struct return_values return_values5 = {
      .status = OpStatus::END,
      .conn_set = nullptr,
  };
  handler->GetConnectionSet("example.com", 8080, false, "", CollectGetConnectionSetResults,
                            &return_values5);
  EXPECT_EQ(return_values5.status, OpStatus::SERVER_CONNECTION_FAILURE);
  EXPECT_EQ(return_values5.conn_set, nullptr);
  EXPECT_EQ(return_values5.message, "{\"message\":\"Insecure connection is not supported\"}");

  delete handler;
  delete manager;
}

TEST_F(MessageHandlerTest, PlaceRequestLogin) {
  ConnectionManager *manager = ConnectionManagerFactory::Create("mock", object_mapper_);
  MessageHandler *handler = MessageHandlerFactory::Create("basic", object_mapper_, manager);
  ASSERT_NE(handler, nullptr);
  ASSERT_EQ(handler->name(), "basic");

  struct return_values return_values;

  // Try getting a connection set
  handler->GetConnectionSet("isima.io", 8432, true, "cafiles.pem", CollectGetConnectionSetResults,
                            &return_values);
  EXPECT_EQ(return_values.status, OpStatus::OK);
  ASSERT_NE(return_values.conn_set, nullptr);

  Session *session = new Session(1, return_values.conn_set, 0, true);

  {
    struct return_values return_values;
    std::unique_ptr<RequestContext> context(new RequestContext());
    context->session = session;
    context->completion_cb = CollectPlaceRequestResults;
    context->completion_cb_args = &return_values;

    request_message_t request = {
        .op_id_ = CSDK_OP_LOGIN_BIOS,
        .payload_ = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>("user=admin")),
                     .length = sizeof("user=admin")},
        .context_ = context.get(),
    };
    std::unique_ptr<RequestMessage> request_message(new RequestMessage(request));
    handler->PlaceRequest(request_message.get());

    EXPECT_EQ(return_values.status, OpStatus::OK);
    delete[] return_values.data;
  }

  {
    struct return_values return_values;
    std::unique_ptr<RequestContext> context(new RequestContext());
    context->session = session;
    context->completion_cb = CollectPlaceRequestResults;
    context->completion_cb_args = &return_values;

    request_message_t request = {
        .op_id_ = CSDK_OP_END,
        .payload_ = {.data = reinterpret_cast<uint8_t *>(const_cast<char *>("user=admin")),
                     .length = sizeof("user=admin")},
        .context_ = context.get(),
    };
    std::unique_ptr<RequestMessage> request_message(new RequestMessage(request));
    handler->PlaceRequest(request_message.get());

    EXPECT_EQ(return_values.status, OpStatus::NOT_FOUND);

    delete[] return_values.data;
  }

  delete session;
  delete handler;
  delete manager;
}

}  // namespace tfos::csdk
