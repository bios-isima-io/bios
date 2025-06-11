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
#include "tfos-physical/impl/fan_router_connection_set.h"

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "tfos-common/event_loop.h"
#include "tfos-common/object_mapper.h"
#include "tfos-common/utils.h"
#include "tfos-physical/connection.h"
#include "tfos-physical/impl/physical_pipe_impl.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

FanRouterConnectionSet::FanRouterConnectionSet(const ResourceResolver *resource_resolver,
                                               const ObjectMapper *object_mapper)
    : resource_resolver_(resource_resolver),
      object_mapper_(object_mapper),
      upstreams_(new std::vector<Connection *>()),
      next_index_(0) {
  for (int i = 0; i < static_cast<int>(OperationType::END); i++) {
    routing_indexes_[i] = 0;
  }
  upstream_config_ = nullptr;
  for (int i = 0; i < static_cast<int>(OperationPolicy::NEVER); i++) {
    routing_table_[i] = nullptr;
  }
  upstream_hash_ = 0;
}

FanRouterConnectionSet::~FanRouterConnectionSet() {
  for (auto it : endpoints_) {
    delete it.second;
  }
  delete upstreams_;
  std::unique_lock lock(upstreams_mutex_);
  for (auto it = upstreams_attic_.begin(); it != upstreams_attic_.end(); ++it) {
    delete *it;
  }
  // We don't lock the table here, ensure no one else would touch the table by design
  ClearRoutingTable();
}

struct InitializeState {
  FanRouterConnectionSet *const conn_set;
  PhysicalPipeImpl *const pipe;
  void (*const caller_cb)(const ResponseMessage &, ConnectionSet *, void *);
  void *const caller_cb_args;

  InitializeState(FanRouterConnectionSet *conn_set, PhysicalPipeImpl *pipe,
                  void (*cb)(const ResponseMessage &, ConnectionSet *, void *), void *cb_args)
      : conn_set(conn_set), pipe(pipe), caller_cb(cb), caller_cb_args(cb_args) {}
};

void FanRouterConnectionSet::InitialConnectionCb(const ResponseMessage &response, void *cb_args) {
  auto *state = reinterpret_cast<InitializeState *>(cb_args);
  FanRouterConnectionSet *conn_set = state->conn_set;
  Connection *conn = new Connection(state->pipe, state->pipe->host(), state->pipe->port(),
                                    state->pipe->is_secure());
  conn_set->AddConnection(conn, true);
  state->caller_cb(response, conn_set, state->caller_cb_args);
  delete state;
}

struct RegisterState {
  Connection *const conn;
  void (*const caller_cb)(const ResponseMessage &, Connection *, void *);
  void *const caller_cb_args;

  RegisterState(Connection *conn, void (*cb)(const ResponseMessage &, Connection *, void *),
                void *cb_args)
      : conn(conn), caller_cb(cb), caller_cb_args(cb_args) {}
};

void FanRouterConnectionSet::RegisterConnectionCb(const ResponseMessage &response, void *cb_args) {
  auto *state = reinterpret_cast<RegisterState *>(cb_args);
  Connection *conn = state->conn;
  if (conn->IsMarked()) {
    ResponseMessage resp(OpStatus::SERVICE_UNAVAILABLE, response.endpoint(), {0});
    state->caller_cb(resp, conn, state->caller_cb_args);
    payload_t payload = response.payload();
    Utils::ReleasePayload(&payload);
  } else {
    state->caller_cb(response, conn, state->caller_cb_args);
  }
  delete state;
}  // namespace tfos::csdk

void FanRouterConnectionSet::Initialize(
    const std::string &host, int port, bool ssl_enabled, const std::string &ssl_cert_file,
    void (*cb)(const ResponseMessage &, ConnectionSet *, void *), void *cb_args) {
  PhysicalPipeImpl *pipe =
      new PhysicalPipeImpl(host, port, ssl_enabled, resource_resolver_, object_mapper_);
  if (!ssl_cert_file.empty()) {
    pipe->set_ssl_cert_file(ssl_cert_file);
  }
  ssl_cert_file_ = ssl_cert_file;
  auto *state = new InitializeState(this, pipe, cb, cb_args);
  pipe->Initialize(InitialConnectionCb, state);
}

void FanRouterConnectionSet::UpdateRoutingTable() {
  DEBUG_LOG("Updating routing table");
  std::unique_lock lock(upstreams_mutex_);
  ClearRoutingTable();
  for (uint64_t i_policy = 0; i_policy < routing_table_.size(); i_policy++) {
    routing_table_[i_policy] = new OpTypeToConnections();
  }
  lb_policy_ = upstream_config_->lb_policy();
  auto upstreams = upstream_config_->upstream();
  for (const auto &upstream : *upstreams) {
    const auto &host_set = upstream->host_set();
    const auto &operations = upstream->operation_set()->operation();
    for (const auto &operation : *operations) {
      auto policy = operation->operation_policy();
      if (policy == OperationPolicy::ALWAYS || policy == OperationPolicy::UNDERFAILURE) {
        int i_policy = static_cast<int>(policy);
        int i_op_type = static_cast<int>(operation->operation_type());
        Connections *connections = (*routing_table_[i_policy])[i_op_type];
        if (connections == nullptr) {
          connections = new Connections();
          (*routing_table_[i_policy])[i_op_type] = connections;
        }
        for (const auto &host : host_set) {
          auto endpoint = Utils::UrlToEndpoint(host);
          assert(!endpoint.empty());
          auto it = endpoints_.find(endpoint);
          if (it == endpoints_.end()) {
            DEBUG_LOG("Node %s is missing yet!!!", endpoint.c_str());
            continue;
          }
          auto *conn = it->second;
          connections->emplace_back(conn);
        }
      }
    }
  }
}

bool FanRouterConnectionSet::IsAnyUpstreamNodeAvailable() {
  auto upstreams = upstream_config_->upstream();
  for (const auto &upstream : *upstreams) {
    auto host_set = upstream->host_set();
    for (const auto &host : host_set) {
      auto endpoint = Utils::UrlToEndpoint(host);
      {
        std::shared_lock lock(upstreams_mutex_);
        auto it = endpoints_.find(endpoint);
        if (it != endpoints_.end()) {
          auto* conn = it->second;
          if (conn->IsReady() && conn->pipe() && conn->pipe()->IsActive()) {
            return true;
          }
        }
      }
    }
  }
  return false;
}

void FanRouterConnectionSet::GetOneSignal(const std::string &endpoint_url,
                                          void (*cb)(const ResponseMessage &, Connection *, void *),
                                          void *cb_args) {
  GetOne(endpoint_url, true, cb, cb_args);
}

void FanRouterConnectionSet::GetOneAdmin(const std::string &endpoint_url,
                                         void (*cb)(const ResponseMessage &, Connection *, void *),
                                         void *cb_args) {
  GetOne(endpoint_url, false, cb, cb_args);
}

void FanRouterConnectionSet::GetOne(const std::string &endpoint_url, bool is_signal,
                                    void (*cb)(const ResponseMessage &, Connection *, void *),
                                    void *cb_args) {
  Connection *conn = Connection::BuildConnection(endpoint_url);
  if (conn == nullptr) {
    ErrorResponse reply("Invalid endpoint syntax: " + endpoint_url);
    payload_t payload = object_mapper_->WriteValue(reply);
    ResponseMessage resp(OpStatus::SERVER_DATA_ERROR, "", payload);
    cb(resp, nullptr, cb_args);
    return;
  }
  std::shared_lock lock(upstreams_mutex_);
  const std::string &endpoint = conn->endpoint();
  Connection *fetched_conn;
  auto it = endpoints_.find(endpoint);
  if (it != endpoints_.end()) {
    fetched_conn = it->second;
    lock.unlock();
  } else {
    lock.unlock();
    fetched_conn = AddConnection(conn, is_signal);
  }
  // Update the error status
  fetched_conn->CopyErrorMark(*conn);

  if (fetched_conn != conn) {
    // Some other threads has initiated the connection already. We'll just use
    // it.
    OpStatus status = OpStatus::OK;
    if (fetched_conn->pipe() == nullptr) {
      // TODO(Naoki): We should back off and retry in this case.
      status = OpStatus::SERVER_CONNECTION_FAILURE;
    } else if (fetched_conn->IsMarked()) {
      status = OpStatus::SERVICE_UNAVAILABLE;
    }
    ResponseMessage resp(status, endpoint, {0});
    cb(resp, fetched_conn, cb_args);
    delete conn;
    return;
  }

  // This method is responsible to initiate the new connection.
  PhysicalPipeImpl *pipe = new PhysicalPipeImpl(conn->host(), conn->port(), conn->ssl_enabled(),
                                                resource_resolver_, object_mapper_);
  conn->set_pipe(pipe);
  if (!ssl_cert_file_.empty()) {
    pipe->set_ssl_cert_file(ssl_cert_file_);
  }
  auto *state = new RegisterState(conn, cb, cb_args);
  pipe->Initialize(RegisterConnectionCb, state);
}

void FanRouterConnectionSet::GetAll(std::vector<Connection *> *all) {
  all->clear();
  auto connections = upstreams_;
  uint32_t index = next_index_++;
  for (uint32_t i = 0; i < connections->size(); ++i) {
    auto conn = connections->at((index + i) % connections->size());
    if (conn->IsReady() && conn->pipe() && conn->pipe()->IsActive()) {
      all->push_back(conn);
    }
  }
}

void FanRouterConnectionSet::RoundRobinRoutes(OperationType optype, Connections *connections,
                                              std::vector<Connection *> *all) {
  uint32_t index = routing_indexes_[static_cast<int>(optype)]++;
  DEBUG_LOG("Optype: %d, index: %d", static_cast<int>(optype), index);
  for (uint32_t i = 0; i < connections->size(); ++i) {
    auto conn = connections->at((index + i) % connections->size());
    if (conn->IsReady() && conn->pipe() && conn->pipe()->IsActive()) {
      all->push_back(conn);
    }
  }
}

void FanRouterConnectionSet::GetRoutes(OperationType optype, Connections *connections,
                                       std::vector<Connection *> *all) {
  if (lb_policy_ == LbPolicy::ROUND_ROBIN) {
    RoundRobinRoutes(optype, connections, all);
  }
}

/**
 * Get all available nodes for a operation type.
 * If all primary nodes are working, then pushes them into a vector
 * for round robin scheduler.
 * If n connections are down and there are more than n backup connections,
 * then uses pushes n connections from backup.
 * If n connections are down and there are less than n in backup, then pushes
 * what ever backup connections are working.
 */
void FanRouterConnectionSet::GetConnectionsForOp(CSdkOperationId id, std::vector<Connection *> *all,
                                                 std::vector<Connection *> *backup) {
  all->clear();
  backup->clear();
  std::shared_lock lock(upstreams_mutex_);
  if (routing_table_[0]) {
    auto index = static_cast<int>(Utils::GetOperationType(id));
    auto primary_connections = (*routing_table_[static_cast<int>(OperationPolicy::ALWAYS)])[index];
    auto backup_connections =
        (*routing_table_[static_cast<int>(OperationPolicy::UNDERFAILURE)])[index];
    if (primary_connections) {
      Connections *connections = new Connections();
      int backup_index = 0;
      int backup_size = 0;
      int failed_nodes = 0;
      if (backup_connections) {
        backup_size = static_cast<int>(backup_connections->size());
      }
      for (auto conn : *primary_connections) {
        if (conn->IsReady() && conn->pipe() && conn->pipe()->IsActive()) {
          connections->push_back(conn);
        } else {
          failed_nodes++;
        }
      }

      if (backup_size && failed_nodes) {
        for (int i = 0; i < failed_nodes; i++) {
          if (backup_index >= backup_size) {
            break;
          }
          auto backup_conn = backup_connections->at(backup_index);
          if (backup_conn && backup_conn->IsReady() && backup_conn->pipe() &&
              backup_conn->pipe()->IsActive()) {
            connections->push_back(backup_conn);
            break;
          }
        }

        backup_index++;
      }

      for (int i = backup_index; i < backup_size; i++) {
        auto backup_conn = backup_connections->at(i);
        if (backup_conn && backup_conn->IsReady() && backup_conn->pipe() &&
            backup_conn->pipe()->IsActive()) {
          backup->push_back(backup_conn);
        }
      }

      GetRoutes(Utils::GetOperationType(id), connections, all);
#if defined(DEBUG_LOG_ENABLED)
      std::stringstream buffer;
      buffer << "Endpoints for request: " << Utils::GetOperationName(id).text << ": ";
      buffer << static_cast<int>(Utils::GetOperationType(id)) << " ";
      auto delim = "";
      for (auto conn : *all) {
        buffer << delim << conn->host();
        delim = ", ";
      }
      DEBUG_LOG("%s", buffer.str().c_str());
#endif
      delete connections;
      return;
    } else {
      GetAll(all);
    }
  } else {
    // If routing_table_ is null, it means we still have not called login
    GetAll(all);
    return;
  }
}

/**
 * Returns number of active signal nodes
 */
int FanRouterConnectionSet::GetActiveSignalsCount() {
  int count = 0;
  auto connections = upstreams_;
  for (uint32_t i = 0; i < connections->size(); ++i) {
    auto conn = connections->at(i);
    if (conn->IsReady() && conn->pipe() && conn->pipe()->IsActive()) {
      ++count;
    }
  }
  return count;
}

bool FanRouterConnectionSet::Contains(const std::string &endpoint) {
  std::shared_lock lock(upstreams_mutex_);
  return endpoints_.find(endpoint) != endpoints_.end();
}

Connection *FanRouterConnectionSet::GetConnection(const std::string &endpoint) {
  std::shared_lock lock(upstreams_mutex_);
  auto it = endpoints_.find(endpoint);
  return it != endpoints_.end() ? it->second : nullptr;
}

std::string FanRouterConnectionSet::CollectErrors() {
  std::stringstream msg;
  std::string delimiter = "";
  auto connections = upstreams_;
  for (uint32_t i = 0; i < connections->size(); ++i) {
    auto conn = connections->at(i);
    auto pipe = conn->pipe();
    if (pipe != nullptr) {
      auto error = pipe->GetLastConnectionError();
      if (error.empty()) {
        continue;
      }
      msg << delimiter << conn->host() << ":" << conn->port() << ":" << error;
      delimiter = ", ";
    }
  }
  return msg.str();
}

Connection *FanRouterConnectionSet::AddConnection(Connection *conn, bool is_signal) {
  std::unique_lock lock(upstreams_mutex_);
  auto it = endpoints_.find(conn->endpoint());
  if (it != endpoints_.end()) {
    return it->second;
  }
  if (is_signal) {
    // Add the connection to the load balancing array.
    // NOTE that readers would access to the connection array without locking
    // during this operation.
    const size_t attic_size_limit = 8;
    while (upstreams_attic_.size() >= attic_size_limit) {
      delete upstreams_attic_.front();
      upstreams_attic_.pop_front();
    }
    upstreams_attic_.push_back(upstreams_);
    auto new_upstreams = new std::vector<Connection *>();
    *new_upstreams = *upstreams_;
    new_upstreams->push_back(conn);
    upstreams_ = new_upstreams;
#if defined(DEBUG_LOG_ENABLED)
    std::string joined;
    const char *delim = "";
    for (auto elem : *new_upstreams) {
      joined += delim;
      joined += elem->endpoint();
      delim = ", ";
    }
    DEBUG_LOG("AddConnection -- connection %s added, they are now [%s]", conn->endpoint().c_str(),
              joined.c_str());
#endif
  }
  // Add the connection to the connection resolver table.
  endpoints_[conn->endpoint()] = conn;
  lock.unlock();
  return conn;
}

void FanRouterConnectionSet::ClearRoutingTable(void) {
  for (uint64_t itype = 0; itype < routing_table_.size(); ++itype) {
    auto *connections = routing_table_[itype];
    if (connections != nullptr) {
      for (auto *conn : *connections) {
        delete conn;
      }
      delete connections;
      routing_table_[itype] = nullptr;
    }
  }
}

void FanRouterConnectionSet::SetUpstreamHash(size_t hash) { upstream_hash_ = hash; }

size_t FanRouterConnectionSet::GetUpstreamHash() { return upstream_hash_; }
}  // namespace tfos::csdk
