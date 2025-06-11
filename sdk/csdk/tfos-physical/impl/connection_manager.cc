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
#include "tfos-physical/connection_manager.h"

#include <string.h>

#include <iostream>

#include "tfos-common/object_mapper.h"
#include "tfos-physical/impl/fan_router_connection_set.h"
#include "tfos-physical/impl/http_resource_resolver.h"
#include "tfos-physical/impl/mock_connection_manager.h"
#include "tfos-physical/impl/physical_pipe_impl.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

// SimpleConnectionSet implementation
// /////////////////////////////////////////////////////////

SimpleConnectionSet::SimpleConnectionSet(const ResourceResolver *resource_resolver,
                                         const ObjectMapper *object_mapper)
    : resource_resolver_(resource_resolver), object_mapper_(object_mapper), next_conn_(0) {}

SimpleConnectionSet::~SimpleConnectionSet() {
  for (Connection *conn : connections_) {
    delete conn;
  }
}

struct InitializeState {
  SimpleConnectionSet *const conn_set;
  PhysicalPipeImpl *const pipe;
  void (*const caller_cb)(const ResponseMessage &, ConnectionSet *, void *);
  void *const caller_cb_args;

  InitializeState(SimpleConnectionSet *conn_set, PhysicalPipeImpl *pipe,
                  void (*cb)(const ResponseMessage &, ConnectionSet *, void *), void *cb_args)
      : conn_set(conn_set), pipe(pipe), caller_cb(cb), caller_cb_args(cb_args) {}
};

void SimpleConnectionSet::InitializeCb(const ResponseMessage &response, void *cb_args) {
  auto *state = reinterpret_cast<InitializeState *>(cb_args);
  SimpleConnectionSet *conn_set = state->conn_set;
  if (response.status() == OpStatus::OK) {
    Connection *conn = new Connection(state->pipe, state->pipe->host(), state->pipe->port(),
                                      state->pipe->is_secure());
    conn_set->AddConnection(conn);
  } else if (state->pipe != nullptr) {
    state->pipe->Shutdown();
  }
  state->caller_cb(response, conn_set, state->caller_cb_args);
  delete state;
}

void SimpleConnectionSet::Initialize(const std::string &host, int port, bool ssl_enabled,
                                     const std::string &ssl_cert_file,
                                     void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                                     void *cb_args) {
  PhysicalPipeImpl *pipe =
      new PhysicalPipeImpl(host, port, ssl_enabled, resource_resolver_, object_mapper_);
  if (!ssl_cert_file.empty()) {
    pipe->set_ssl_cert_file(ssl_cert_file);
  }
  auto *state = new InitializeState(this, pipe, cb, cb_args);
  pipe->Initialize(InitializeCb, state);
}

void SimpleConnectionSet::GetOneSignal(const std::string &endpoint_url,
                                       void (*cb)(const ResponseMessage &, Connection *, void *),
                                       void *cb_args) {}

void SimpleConnectionSet::GetOneAdmin(const std::string &endpoint_url,
                                      void (*cb)(const ResponseMessage &, Connection *, void *),
                                      void *cb_args) {}

void SimpleConnectionSet::GetConnectionsForOp(CSdkOperationId id, std::vector<Connection *> *all,
                                              std::vector<Connection *> *backup) {
  all->clear();
  for (auto connection : connections_) {
    if (connection->IsReady()) {
      all->push_back(connection);
    }
  }
}

void SimpleConnectionSet::GetAll(std::vector<Connection *> *all) {
  all->clear();
  for (auto connection : connections_) {
    if (connection->IsReady()) {
      all->push_back(connection);
    }
  }
}

void SimpleConnectionSet::UpdateRoutingTable() {}
void SimpleConnectionSet::SetUpstreamHash(size_t hash) {}
size_t SimpleConnectionSet::GetUpstreamHash() { return 0; }
void SimpleConnectionSet::SetUpstreamConfig(std::unique_ptr<UpstreamConfig> upstream_config) {}

int SimpleConnectionSet::GetActiveSignalsCount() {
  int count = 0;
  for (auto connection : connections_) {
    if (connection->IsReady()) {
      ++count;
    }
  }
  return count;
}

bool SimpleConnectionSet::Contains(const std::string &endpoint) {
  for (auto connection : connections_) {
    if (endpoint == connection->endpoint()) {
      return true;
    }
  }
  return false;
}

Connection *SimpleConnectionSet::GetConnection(const std::string &endpoint_url) {
  for (auto connection : connections_) {
    if (endpoint_url == connection->endpoint()) {
      return connection;
    }
  }
  return nullptr;
}

Connection *SimpleConnectionSet::AddConnection(Connection *conn) {
  connections_.push_back(conn);
  return conn;
}

// BasicConnectionManager implementation
// ////////////////////////////////////////////////////////
class BasicConnectionManager final : public ConnectionManager {
 private:
  const ResourceResolver *const resource_resolver_;
  const ObjectMapper *const object_mapper_;

  std::vector<ConnectionListener *> connection_listeners_;
  // for sequential consistency of threads that come after loading listener
  std::atomic_bool listener_loaded_;

 public:
  explicit BasicConnectionManager(const ObjectMapper *object_mapper)
      : resource_resolver_(new HttpResourceResolver()),
        object_mapper_(object_mapper),
        connection_listeners_{},
        listener_loaded_(false) {
    PhysicalPipeImpl::GlobalInit();
  }
  ~BasicConnectionManager() { delete resource_resolver_; }

  std::string name() { return "basic"; }

  void CreateConnectionSet(const std::string &host, int port, bool ssl_enabled,
                           const std::string &ssl_cert_file,
                           void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                           void *cb_args);

  void RegisterConnectionListener(ConnectionListener *p_listener) override;

  static void notifyListeners(const std::vector<ConnectionListener *> *p_lsnrs,
                              Connection *p_connection);
};

void BasicConnectionManager::CreateConnectionSet(
    const std::string &host, int port, bool ssl_enabled, const std::string &ssl_cert_file,
    void (*cb)(const ResponseMessage &, ConnectionSet *, void *), void *cb_args) {
  ConnectionSet *conn_set = new FanRouterConnectionSet(resource_resolver_, object_mapper_);
  conn_set->Initialize(host, port, ssl_enabled, ssl_cert_file, cb, cb_args);
}

// currently not thread safe.. as only one thread is accessing during initialize
void BasicConnectionManager::RegisterConnectionListener(ConnectionListener *p_listener) {
  connection_listeners_.push_back(p_listener);
  // do a sequential consistent write for ensuring other threads that accesses
  // later sees it
  listener_loaded_.store(true, std::memory_order::memory_order_seq_cst);
}

void BasicConnectionManager::notifyListeners(const std::vector<ConnectionListener *> *p_lsnrs,
                                             Connection *p_connection) {
  if (p_lsnrs != nullptr) {
    for (ConnectionListener *p_lsnr : *p_lsnrs) {
      p_lsnr->ConnectionAdded(p_connection);
    }
  }
}

// ConnectionManagerFactory implementation
// ////////////////////////////////////////////////////

ConnectionManager *ConnectionManagerFactory::Create(const std::string &implementation_name,
                                                    const ObjectMapper *object_mapper) {
  if (implementation_name == "") {
    return new BasicConnectionManager(object_mapper);
  } else if (implementation_name == "mock") {
    DEBUG_LOG("Creating MockConnectionManager");
    return new MockConnectionManager();
  }
  DEBUG_LOG("Unknown implementation: %s", implementation_name.c_str());
  return nullptr;
}

}  // namespace tfos::csdk
