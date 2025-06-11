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
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "tfos-common/object_mapper.h"
#include "tfos-physical/impl/http_resource_resolver.h"
#include "tfos-physical/impl/physical_pipe_impl.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/models.h"
#include "tfoscsdk/status.h"

namespace tfos::csdk {

struct State {
  PhysicalPipe *pipe;
  bool connected;
  std::mutex mutex;
  std::condition_variable cv;
  std::vector<ResponseMessage> replies;
  Session *session;

  State() : pipe(nullptr), connected(false), session(nullptr) {}
} state;

struct GetVersionState {
  State *const global_state;
  const int sequence;

  GetVersionState(State *global_state, int sequence)
      : global_state(global_state), sequence(sequence) {}
};

static void GetVersionCallback(const ResponseMessage &response_message, void *cb_args) {
  auto *op_state = reinterpret_cast<GetVersionState *>(cb_args);
  TEST_LOG("DONE %d", op_state->sequence);
  op_state->global_state->replies.push_back(response_message);
  std::unique_lock<std::mutex> lock(op_state->global_state->mutex);
  op_state->global_state->cv.notify_one();
  delete op_state;
}

struct ConnectionState {
  State *const global_state;
  response_cb const op_callback1;
  void *const op_callback1_args;
  response_cb const op_callback2;
  void *const op_callback2_args;

  ConnectionState(State *global_state, response_cb op_cb1, void *op_cb1_args, response_cb op_cb2,
                  void *op_cb2_args)
      : global_state(global_state),
        op_callback1(op_cb1),
        op_callback1_args(op_cb1_args),
        op_callback2(op_cb2),
        op_callback2_args(op_cb2_args) {}
};

static void ConnectionCallback(const ResponseMessage &response, void *cb_args) {
  auto *conn_state = reinterpret_cast<ConnectionState *>(cb_args);

  TEST_LOG("result=%s message=%s", Statuses::Name(response.status()).c_str(),
           (const char *)response.response_body());

  TEST_LOG("(3)pipe=%p", conn_state->global_state->pipe);
  if (response.status() == OpStatus::OK) {
    RequestContext *context1 = new RequestContext();
    context1->completion_cb = conn_state->op_callback1;
    context1->completion_cb_args = conn_state->op_callback1_args;
    RequestContext *context2 = new RequestContext();
    context2->completion_cb = conn_state->op_callback2;
    context2->completion_cb_args = conn_state->op_callback2_args;
    request_message_t request_message1 = {0};
    request_message1.op_id_ = CSDK_OP_GET_VERSION;
    request_message_t request_message2 = request_message1;
    request_message1.context_ = context1;
    request_message2.context_ = context2;
    conn_state->global_state->pipe->HandleRequest(request_message1);
    conn_state->global_state->pipe->HandleRequest(request_message2);
  } else {
    conn_state->global_state->connected = false;
    std::unique_lock<std::mutex> lock(conn_state->global_state->mutex);
    conn_state->global_state->cv.notify_one();
  }
  delete[] response.response_body();
  delete conn_state;
}

struct CallState {
  State *const global_state;
  RequestContext *const context;
  std::function<void(const rapidjson::Document &)> caller_cb;
  OpStatus *op_status;
  std::string op_name;

  CallState(State *global_state, RequestContext *context,
            std::function<void(const rapidjson::Document &)> cb, OpStatus *op_status,
            const std::string& op_name)
      : global_state(global_state),
        context(context),
        caller_cb(cb),
        op_status(op_status),
        op_name(op_name) {}
};

static void CompleteCall(const ResponseMessage &response, void *cb_args) {
  auto *call_state = reinterpret_cast<CallState *>(cb_args);
  *call_state->op_status = response.status();
  if (*call_state->op_status == OpStatus::OK) {
    rapidjson::Document response_doc;
    response_doc.Parse((const char *)response.response_body());
    call_state->caller_cb(response_doc);
  } else {
    TEST_LOG("%s error: %s -- %s", call_state->op_name.c_str(),
             Statuses::Name(*call_state->op_status).c_str(),
             (const char *)response.response_body());
  }
  std::unique_lock<std::mutex> lock(call_state->global_state->mutex);
  call_state->global_state->cv.notify_one();
  delete call_state->context;
  delete call_state;
}

static OpStatus Call(CSdkOperationId op_id, const rapidjson::Document *request_doc,
                     const std::string& op_name,
                     std::function<void(const rapidjson::Document &)> cb) {
  std::string body;
  if (request_doc != nullptr) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    request_doc->Accept(writer);
    body = buffer.GetString();
    TEST_LOG("body: %s", body.c_str());
  }
  RequestContext *context = new RequestContext();
  context->session = state.session;
  request_message_t request_message = {
      .op_id_ = op_id,
      .payload_ = {reinterpret_cast<uint8_t *>(const_cast<char *>(body.c_str())),
                   static_cast<int64_t>(body.size())},
      .context_ = context,
  };
  OpStatus status;
  auto *call_state = new CallState(&state, context, cb, &status, op_name);
  context->completion_cb = CompleteCall;
  context->completion_cb_args = call_state;
  if (state.session) {
    context->token = state.session->token;
  }
  state.pipe->HandleRequest(request_message);

  std::unique_lock<std::mutex> lock(state.mutex);
  state.cv.wait(lock);
  return status;
}

static int login() {
  rapidjson::Document request_doc;
  request_doc.SetObject();
  request_doc.AddMember("email", "superadmin", request_doc.GetAllocator());
  request_doc.AddMember("password", "superadmin", request_doc.GetAllocator());
  OpStatus status = Call(CSDK_OP_LOGIN_BIOS,
                         &request_doc, "login", [](const rapidjson::Document &response_doc) {
    std::string token = "Bearer ";
    auto it = response_doc.FindMember("token");
    if (it != response_doc.MemberEnd()) {
      token += it->value.GetString();
    }
    state.session = new Session(1, nullptr, 0, true);
    state.session->username = "superadmin";
    state.session->permissions.push_back(Permission::SUPERADMIN);
    state.session->token = std::make_shared<std::string>(token);
    it = response_doc.FindMember("tenant");
    if (it != response_doc.MemberEnd()) {
      state.session->tenant = it->value.GetString();
    }
    state.session->bios = true;
  });
  TEST_LOG("status=%s", Statuses::Name(status).c_str());
  if (status == OpStatus::OK) {
    TEST_LOG("login success, tenant=%s, token=%s",
             state.session->tenant.c_str(), state.session->token->c_str());
  }
  return status == OpStatus::OK ? 0 : -1;
}

static int list_tenants() {
  OpStatus status =
      Call(CSDK_OP_GET_TENANTS_BIOS, nullptr, "get_tenants",
           [](const rapidjson::Document &response_doc) {
        if (response_doc.IsArray()) {
          for (int i = 0; i < response_doc.Size(); ++i) {
            auto& entry = response_doc[i];
            auto it = entry.FindMember("tenantName");
            if (it != entry.MemberEnd()) {
              TEST_LOG("tenant[%d]: %s", i, it->value.GetString());
            }
          }
        }
      });
  TEST_LOG("status=%s", Statuses::Name(status).c_str());
  return status == OpStatus::OK ? 0 : -1;
}

static int run_main(int argc, char *argv[]) {
  std::string host = "localhost";
  int port = 443;
  bool is_secure = true;

  PhysicalPipeImpl::GlobalInit();

  std::unique_ptr<ResourceResolver> resource_resolver(new HttpResourceResolver());
  std::unique_ptr<ObjectMapper> object_mapper(new ObjectMapper());

  PhysicalPipeImpl *pipe =
      new PhysicalPipeImpl(host, port, is_secure, resource_resolver.get(), object_mapper.get());

  int status = 0;
  std::string message;

  state.pipe = pipe;
  state.connected = true;

  TEST_LOG("(1)pipe=%p", state.pipe);
  auto *op_state1 = new GetVersionState(&state, 1);
  auto *op_state2 = new GetVersionState(&state, 1);
  auto *conn_state =
      new ConnectionState(&state, GetVersionCallback, op_state1, GetVersionCallback, op_state2);
  pipe->Initialize(ConnectionCallback, conn_state);

  while (state.connected && state.replies.size() < 2) {
    std::unique_lock<std::mutex> lock(state.mutex);
    state.cv.wait(lock);
  }

  if (!state.connected) {
    TEST_LOG("connection failure");
  }

  // consume messages
  bool ok = true;
  for (auto response : state.replies) {
    TEST_LOG("status=%s %s", Statuses::Name(response.status()).c_str(),
             (const char *)response.response_body());
    if (response.status() != OpStatus::OK) {
      ok = false;
    }
    response.response_message().release_resources();
  }

  if (!ok || login() || list_tenants()) {
    return 1;
  }
  pipe->Shutdown();
  return 0;
}

}  // namespace tfos::csdk

int main(int argc, char *argv[]) { return tfos::csdk::run_main(argc, argv); }
