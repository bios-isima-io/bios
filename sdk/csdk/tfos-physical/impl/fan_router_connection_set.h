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
#ifndef TFOS_PHYSICAL_FAN_ROUTER_CONNECTION_SET_H_
#define TFOS_PHYSICAL_FAN_ROUTER_CONNECTION_SET_H_

#include <algorithm>
#include <array>
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "tfos-physical/connection_manager.h"
#include "tfoscsdk/models.h"

namespace tfos::csdk {

class EventLoop;

class FanRouterConnectionSet final : public ConnectionSet {
 private:
  const ResourceResolver *const resource_resolver_;
  const ObjectMapper *const object_mapper_;

  /**
   * Array of connections. This is used for load balancing the signal nodes.
   * Readers access this array without lock while writers would mutex lock each
   * other.
   */
  std::vector<Connection *> *upstreams_;
  std::list<std::vector<Connection *> *> upstreams_attic_;
  typedef std::vector<Connection *> Connections;
  typedef std::array<Connections *, static_cast<size_t>(OperationType::END)> OpTypeToConnections;
  std::array<OpTypeToConnections *, static_cast<int>(OperationPolicy::NEVER)> routing_table_;
  /**
   * Map to be used to resolve a connection by an endpoint name.
   */
  // TODO(Naoki): We may want to use concurrent hash map
  std::unordered_map<std::string, Connection *> endpoints_;

  std::shared_mutex upstreams_mutex_;

  /**
   * The index that indicates next position in the connection array to pick up.
   * The value does not wrap at the array size but keeps increasing up to the
   * maximum value.
   */
  volatile uint32_t next_index_;

  std::string ssl_cert_file_;
  LbPolicy lb_policy_;
  std::unique_ptr<UpstreamConfig> upstream_config_;
  uint32_t routing_indexes_[static_cast<int>(OperationType::END)];
  size_t upstream_hash_;

 public:
  FanRouterConnectionSet(const ResourceResolver *resource_resolver,
                         const ObjectMapper *object_mapper);

  ~FanRouterConnectionSet();

  void Initialize(const std::string &host, int port, bool ssl_enabled,
                  const std::string &ssl_cert_file,
                  void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                  void *cb_args) override;

  void UpdateRoutingTable() override;
  void SetUpstreamConfig(std::unique_ptr<UpstreamConfig> upstream_config) override {
    upstream_config_ = std::move(upstream_config);
  }
  bool IsAnyUpstreamNodeAvailable();

  void GetOneSignal(const std::string &endpoint,
                    void (*cb)(const ResponseMessage &, Connection *, void *),
                    void *cb_args) override;

  void GetOneAdmin(const std::string &endpoint,
                   void (*cb)(const ResponseMessage &, Connection *, void *),
                   void *cb_args) override;

  /**
   * Returns physical pipes for all available nodes. The returned pipes must
   * connect to one available endpoint for each.
   */
  void GetAll(std::vector<Connection *> *all) override;

  void GetConnectionsForOp(CSdkOperationId id, std::vector<Connection *> *all,
                           std::vector<Connection *> *backup) override;

  /**
   * Returns number of active signal nodes.
   */
  virtual int GetActiveSignalsCount() override;

  /**
   * @return True if the connection set covers the specified endpoint.
   */
  bool Contains(const std::string &endpoint) override;

  Connection *GetConnection(const std::string &endpoint) override;

  std::string CollectErrors() override;

  void GetRoutes(OperationType, Connections *, std::vector<Connection *> *);
  void RoundRobinRoutes(OperationType, Connections *, std::vector<Connection *> *);
  void SetUpstreamHash(size_t hash) override;
  size_t GetUpstreamHash() override;

 private:
  void GetOne(const std::string &endpoint, bool is_signal,
              void (*cb)(const ResponseMessage &, Connection *, void *), void *cb_args);
  Connection *AddConnection(Connection *conn, bool is_signal);

  static void InitialConnectionCb(const ResponseMessage &response, void *cb_args);
  static void RegisterConnectionCb(const ResponseMessage &response, void *cb_args);
  void ClearRoutingTable();
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_FAN_ROUTER_CONNECTION_SET_H_
