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
#ifndef TFOS_CSDK_MOCK_CONNECTION_MANAGER_H_
#define TFOS_CSDK_MOCK_CONNECTION_MANAGER_H_

#include "tfos-physical/connection.h"
#include "tfos-physical/connection_manager.h"

namespace tfos::csdk {

class MockPhysicalPipe final : public PhysicalPipe {
 private:
  std::string host_;

 public:
  explicit MockPhysicalPipe(std::string host) : host_(host) {}

  std::string name() override { return "mock"; }

  void Initialize(response_cb cb, void *cb_args) override {
    if (host_ == "isima.io" || host_ == "elasticflash.com") {
      ResponseMessage response(OpStatus::OK, host_, nullptr, 0);
      cb(response, cb_args);
    } else {
      std::string message = "no such host";
      uint8_t *data = new uint8_t[message.size() + 1];
      memcpy(data, message.c_str(), message.size() + 1);
      ResponseMessage response(OpStatus::SERVER_CONNECTION_FAILURE, host_, data,
                               (int64_t)message.size());
      cb(response, cb_args);
    }
  }

  void HandleRequest(const RequestMessage &request_message) override {
    OpStatus status;
    std::string reply;
    if (request_message.op_id() == CSDK_OP_LOGIN_BIOS) {
      status = OpStatus::OK;
      reply =
          "{\"token\":\"eyJhbGciOiJIUzUxMiJ9"
          ".eyJhdWQiOiJzdXBlcmFkbWluIiwic3ViIjoiU1VQRVJBRE1JTi4vIiwiaWF0IjoxNTY"
          "3NTc1"
          "MTMwLCJleHAiOjE1Njc1Nzg3MzB9.6b7C5ZbE-"
          "gyvMVWRh1wX6eDLwVFu7vvivxNqw9pqAbLRe"
          "d0oFkOOxvp4nbGoLiWpWlzyJeD-V92Kw5TpbOhoQQ\","
          "\"expiry\":1567578730973,\"sessionTimeoutMillis\":3600000,\"role\":"
          "\"SYSTEM_ADMIN\"}";
    } else {
      status = OpStatus::NOT_FOUND;
      std::string reply = "{\"message\":\"Unknown operation\"}";
    }
    uint8_t *data = new uint8_t[reply.size() + 1];
    memcpy(data, reply.c_str(), reply.size() + 1);
    ResponseMessage response(status, host_, data, reply.size());
    request_message.context()->completion_cb(response,
                                             request_message.context()->completion_cb_args);
  }

  bool IsActive() override { return true; }

  void Shutdown() override { delete this; }
};

class MockConnectionManager final : public ConnectionManager {
 public:
  std::string name() { return "mock"; }

  void RegisterConnectionListener(ConnectionListener *p_listener) override {}

  struct CreateConnectionSetState {
    std::string host;
    int port;
    bool ssl_enabled;
    std::string ssl_cert_file;
    PhysicalPipe *pipe;
    void (*caller_cb)(const ResponseMessage &, ConnectionSet *, void *);
    void *caller_cb_args;
    CreateConnectionSetState(const std::string &host_, int port_, bool ssl_enabled_,
                             const std::string &ssl_cert_file_, PhysicalPipe *pipe_,
                             void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                             void *cb_args)
        : host(host_),
          port(port_),
          ssl_enabled(ssl_enabled_),
          ssl_cert_file(ssl_cert_file_),
          pipe(pipe_),
          caller_cb(cb),
          caller_cb_args(cb_args) {}
  };

  static void InitializeCb(const ResponseMessage &response, void *cb_args) {
    auto *state = reinterpret_cast<CreateConnectionSetState *>(cb_args);
    SimpleConnectionSet *conn_set;
    if (response.status() == OpStatus::OK) {
      conn_set = new SimpleConnectionSet(nullptr, nullptr);
      Connection *conn = new Connection(state->pipe, state->host, state->port, state->ssl_enabled);
      conn_set->AddConnection(conn);
    } else {
      state->pipe->Shutdown();
      conn_set = nullptr;
    }
    state->caller_cb(response, conn_set, state->caller_cb_args);
    delete state;
  }

  void CreateConnectionSet(const std::string &host, int port, bool ssl_enabled,
                           const std::string &ssl_cert_file,
                           void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                           void *cb_args) {
    PhysicalPipe *pipe = new MockPhysicalPipe(host);
    auto *state =
        new CreateConnectionSetState(host, port, ssl_enabled, ssl_cert_file, pipe, cb, cb_args);
    pipe->Initialize(InitializeCb, state);
  }
};

}  // namespace tfos::csdk

#endif  // TFOS_CSDK_MOCK_CONNECTION_MANAGER_H_
