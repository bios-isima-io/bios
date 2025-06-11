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
#ifndef TFOS_PHYSICAL_CONNECTION_H_
#define TFOS_PHYSICAL_CONNECTION_H_

#include <string>

namespace tfos::csdk {

class PhysicalPipe;

/**
 * Class that keeps connection information for an endpoint.
 *
 * The difference from PhysicalPipe is that this class understands C-SDK
 * semantics, such as signal nodes, error mark, etc. On the other hand, this
 * class does not know anything about communication protocol handling against
 * the endpoint. The PhysicalPipe takes care of it.
 *
 * In this way, we can replace the communication protocol against the endpoint
 * without losing the C-SDK mechanisms such as load balancing, fan routing,
 * failure recovery, etc.
 */
class Connection final {
  // properties //////////////////////
  // corresponding physical pipe
  PhysicalPipe *pipe_;
  // endpoint name of this connection
  std::string endpoint_;
  // connection parameters
  std::string host_;
  int port_;
  bool ssl_enabled_;
  std::string protocol_;

  // statuses ////////////////////////
  // whether this is a signal node
  bool is_signal_;
  // indicates with timestamp whether the server marks the node as error. 0
  // means false
  volatile uint64_t is_marked_;
  // indicates with timestamp whehter the client encountered a retriable error.
  volatile uint64_t had_error_;

  // interval milliseconds to resume operation after error flags are set, i.e.,
  // error expiry
  uint64_t error_expiry_;

  static const uint64_t kDefaultErrorExpiry;

 public:
  Connection(PhysicalPipe *pipe, const std::string &host, int port, bool ssl_enabled);
  ~Connection();

  // getters and setters
  inline PhysicalPipe *pipe() const { return pipe_; }
  inline void set_pipe(PhysicalPipe *pipe) { pipe_ = pipe; }
  inline const std::string &endpoint() const { return endpoint_; }
  inline const std::string &host() const { return host_; }
  inline int port() const { return port_; }
  inline bool ssl_enabled() const { return ssl_enabled_; }
  inline const std::string &protocol() { return protocol_; }
  inline bool is_signal() const { return is_signal_; }
  inline void set_signal(bool is_signal) { is_signal_ = is_signal; }
  inline uint64_t error_expiry() const { return error_expiry_; }
  inline void set_error_expiry(uint64_t interval) { error_expiry_ = interval; }

  // server marks error
  inline bool IsMarked() { return is_marked_ > 0; }
  // client should try operationp
  bool IsReady();

  void CopyErrorMark(const Connection &src);
  void UpdateServerMark(bool is_marked);
  void UpdateErrorStatus(bool is_error);

  static std::string MakeEndpointString(const std::string &host, int port, bool ssl_enabled);
  static std::string MakeEndpointString(const std::string &url);
  static uint64_t GetDefaultErrorExpiry();

  /**
   * Static method to generate a Connection object by parsing a URL.
   *
   * The method fills the information available from the URL into the returning
   * object. Other Connection parameters are kept in default.
   *
   * @param url Input URL. This URL is meant to be given by ListEndPoint or
   * ListContextEndpoint TFOS operation. The URL, thus, may starts with dot
   * ('.') in case of the node is marked error.
   * @return Generated Connection object or nullptr in case of syntax error.
   */
  static Connection *BuildConnection(const std::string &url);

 private:
  // Should be used only by the parser
  Connection();

  static bool ParseUrl(const std::string &url, size_t index_start, std::string *protocol,
                       std::string *host, int *port, bool *ssl_enabled);
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_CONNECTION_H_
