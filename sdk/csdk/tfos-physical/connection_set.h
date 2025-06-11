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
#ifndef TFOS_PHYSICAL_CONNECTION_SET_H_
#define TFOS_PHYSICAL_CONNECTION_SET_H_

#include <string>
#include <vector>

#include "tfos-physical/physical_pipe.h"

namespace tfos::csdk {

class Connection;
class Logger;
class ObjectMapper;
class ResourceResolver;

/**
 * Abstract class that defines the interface to access to physical pipes.
 *
 * An instance of ConnectionSet holds multiple physical pipes, at least one pipe
 * per endpoint.
 */
class ConnectionSet {
 public:
  virtual ~ConnectionSet() = default;

  virtual void Initialize(const std::string &host, int port, bool ssl_enabled,
                          const std::string &ssl_cert_file,
                          void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                          void *cb_args) = 0;

  /**
   * Updates upstream routing table.
   */
  virtual void UpdateRoutingTable() = 0;

  /**
   * Moves an upstream config into the instance.
   */
  virtual void SetUpstreamConfig(std::unique_ptr<UpstreamConfig> upstream_config) = 0;

  /**
   * Replies whether at least one upstream node is connected.
   */
  virtual bool IsAnyUpstreamNodeAvailable() = 0;

  /**
   * Returns a physical pipe for the specified endpoint as a signal node.
   * If the connection for the specified endpoint is missing in the connection
   * set, the method creates one and register it as a signal node. The
   * connection would appear in the signal connections available via method
   * GetOne().
   */
  virtual void GetOneSignal(const std::string &endpoint_url,
                            void (*cb)(const ResponseMessage &, Connection *, void *),
                            void *cb_args) = 0;

  /**
   * Returns a physical pipe for the specified endpoint as an admin node.
   * If the connection for specified endpoint is missing in the connection set,
   * the method creates one and register it as an admin node.  The connection
   * regisred by this method won't appear in the signal connections available
   * via method GetOne().
   */
  virtual void GetOneAdmin(const std::string &endpoint_url,
                           void (*cb)(const ResponseMessage &, Connection *, void *),
                           void *cb_args) = 0;

  /**
   * Returns available connections for an operation.
   */
  virtual void GetConnectionsForOp(CSdkOperationId id, std::vector<Connection *> *all,
                                   std::vector<Connection *> *backup) = 0;

  /**
   * Returns physical pipes for all available signal nodes.
   */
  virtual void GetAll(std::vector<Connection *> *all) = 0;

  virtual void SetUpstreamHash(size_t hash) = 0;
  virtual size_t GetUpstreamHash() = 0;

  /**
   * Returns number of active signal nodes.
   */
  virtual int GetActiveSignalsCount() = 0;

  /**
   * @param endpoint Endpoint string
   * @return True if the connection set covers the specified endpoint string.
   */
  virtual bool Contains(const std::string &endpoint) = 0;

  virtual Connection *GetConnection(const std::string &endpoint) = 0;

  virtual std::string CollectErrors() { return ""; }
};

/**
 * Simple connection set that holds one physical pipe per endpoint and
 * round-robin them to determine the pipe to return via GetOne().
 */
class SimpleConnectionSet final : public ConnectionSet {
 private:
  const ResourceResolver *const resource_resolver_;
  const ObjectMapper *const object_mapper_;

  std::vector<Connection *> connections_;
  int next_conn_;

 public:
  SimpleConnectionSet(const ResourceResolver *resource_resolver, const ObjectMapper *object_mapper);
  ~SimpleConnectionSet();

  void Initialize(const std::string &host, int port, bool ssl_enabled,
                  const std::string &ssl_cert_file,
                  void (*cb)(const ResponseMessage &, ConnectionSet *, void *), void *cb_args);

  void UpdateRoutingTable() override;
  void SetUpstreamConfig(std::unique_ptr<UpstreamConfig> upstream_config) override;
  bool IsAnyUpstreamNodeAvailable() { return true; }

  void GetOneSignal(const std::string &endpoint_url,
                    void (*cb)(const ResponseMessage &, Connection *, void *), void *cb_args);
  void GetOneAdmin(const std::string &endpoint_url,
                   void (*cb)(const ResponseMessage &, Connection *, void *), void *cb_args);
  void GetConnectionsForOp(CSdkOperationId id, std::vector<Connection *> *all,
              std::vector<Connection *> *backup) override;
  void GetAll(std::vector<Connection *> *all) override;
  void SetUpstreamHash(size_t hash) override;
  size_t GetUpstreamHash() override;
  int GetActiveSignalsCount() override;
  bool Contains(const std::string &endpoint);
  Connection *GetConnection(const std::string &endpoint);

  /**
   * Try adding a connection.
   *
   * If the ConnectionSet has the connection for the endpoint already, the
   * method skips adding the specified connection and returns existing one.
   * Otherwise the method adds the specified connection and return it.
   *
   * @param conn The connection object ot add
   * @return Corresponding connection object to be used by this ConnectionSet.
   */
  Connection *AddConnection(Connection *conn);

 private:
  static void InitializeCb(const ResponseMessage &response, void *cb_args);
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_CONNECTION_SET_H_
