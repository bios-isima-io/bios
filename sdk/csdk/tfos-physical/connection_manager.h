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
#ifndef TFOS_PHYSICAL_CONNECTION_MANAGER_H_
#define TFOS_PHYSICAL_CONNECTION_MANAGER_H_

#include <string>
#include <vector>

#include "tfos-physical/connection_set.h"
#include "tfos-physical/physical_pipe.h"

namespace tfos::csdk {

class ObjectMapper;

class ConnectionListener {
 public:
  virtual ~ConnectionListener() = default;
  DISABLE_COPY_MOVE_AND_ASSIGN(ConnectionListener);

  /**
   * Called by the connection manager on all registered listeners when a new
   * connection is added.
   */
  virtual void ConnectionAdded(const Connection *p_connection) = 0;

  /**
   * Called by the connection manager on all registered listeners when a
   * connection is closed before the connection is destroyed and deleted.
   */
  virtual void ConnectionRemoved(const std::string &endpoint) = 0;

 protected:
  ConnectionListener() = default;
};

class ConnectionManager {
 public:
  virtual ~ConnectionManager() = default;

  virtual std::string name() = 0;

  virtual void CreateConnectionSet(const std::string &host, int port, bool ssl_enabled,
                                   const std::string &ssl_cert_file,
                                   void (*cb)(const ResponseMessage &, ConnectionSet *, void *),
                                   void *cb_args) = 0;

  // register connection listeners that are notified.
  // as of now this should be done during initialize
  virtual void RegisterConnectionListener(ConnectionListener *p_listener) = 0;
};

class ConnectionManagerFactory {
 public:
  static ConnectionManager *Create(const std::string &implementation_name,
                                   const ObjectMapper *object_mapper);
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_CONNECTION_MANAGER_H_
