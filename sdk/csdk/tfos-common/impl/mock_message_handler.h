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
#ifndef TFOS_CSDK_MOCK_MESSAGE_HANDLER_H_
#define TFOS_CSDK_MOCK_MESSAGE_HANDLER_H_

#include <string>

#include "tfos-common/message_handler.h"
#include "tfos-common/utils.h"
#include "tfos-physical/connection_manager.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

class MockMessageHandler final : public MessageHandler {
 public:
  std::string name() { return "mock"; }

  void InjectSessionController(SessionController *session_controller) {}

  void GetConnectionSet(const std::string &host, int port, bool ssl_enabled,
                        const std::string &cert_file,
                        void (*cb)(const ResponseMessage &resp, ConnectionSet *conn_set,
                                   void *cb_args),
                        void *cb_args) {
    ResponseMessage response_message(OpStatus::OK, host + ":" + std::to_string(port), nullptr, 0);
    cb(response_message, new SimpleConnectionSet(nullptr, nullptr), cb_args);
  }

  void PlaceRequest(RequestMessage *request_message) {
    pthread_t tid;
    pthread_create(&tid, nullptr, PlaceRequestCore, request_message);
  }

  void ScheduleTask(void (*run)(void *), void *args, uint64_t millis) {}

 private:
  static void *PlaceRequestCore(void *args) {
    auto *request_message = reinterpret_cast<RequestMessage *>(args);
    DEBUG_LOG("MockMessageHandler::PlaceRequestCore request=%s",
              Utils::GetOperationName(request_message->op_id()).text);
    std::string resp;
    OpStatus status = OpStatus::OK;
    switch (request_message->op_id()) {
      case CSDK_OP_LOGIN_BIOS:
        resp =
            "{\"token\":\"eyJhbGciOiJIUzUxMiJ9"
            ".eyJhdWQiOiJzdXBlcmFkbWluIiwic3ViIjoiU1VQRVJBRE1JTi4vIiwiaWF0IjoxNTY3NTc1"
            "MTMwLCJleHAiOjE1Njc1Nzg3MzB9.6b7C5ZbE-gyvMVWRh1wX6eDLwVFu7vvivxNqw9pqAbLRe"
            "d0oFkOOxvp4nbGoLiWpWlzyJeD-V92Kw5TpbOhoQQ\","
            "\"expiry\":1567578730973,\"sessionTimeoutMillis\":3600000,"
            "\"permissions\":[\"SYSTEM_ADMIN\"]}";
        break;
      case CSDK_OP_LIST_ENDPOINTS:
        resp = "[]";
        break;
      case CSDK_OP_LIST_CONTEXT_ENDPOINTS:
        resp = "[]";
        break;
      default:
        DEBUG_LOG("op=%s requested, but not implemented",
                  Utils::GetOperationName(request_message->op_id()).text);
        status = OpStatus::NOT_IMPLEMENTED;
        resp = "{\"status\":\"NOT_IMPLEMENTED\",\"message\":\"not implemented\"}";
    }

    uint8_t *data = new uint8_t[resp.size() + 1];
    memcpy(data, resp.c_str(), resp.size() + 1);
    ResponseMessage response_message(status, "", data, resp.size());

    request_message->context()->completion_cb(response_message,
                                              request_message->context()->completion_cb_args);

    return nullptr;
  }
};

}  // namespace tfos::csdk

#endif  // TFOS_CSDK_MOCK_MESSAGE_HANDLER_H_
