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

/**
 * Physical pipe implementation
 */
#ifndef TFOS_PHYSICAL_PHYSICAL_PIPE_IMPL_H_
#define TFOS_PHYSICAL_PHYSICAL_PIPE_IMPL_H_

#include <event.h>
#include <event2/event.h>
#include <openssl/ssl.h>

#include <condition_variable>
#include <mutex>
#include <string>

#include "tfos-common/event_loop.h"
#include "tfos-physical/physical_pipe.h"

namespace tfos::csdk {

class EventLoop;
class Http2Session;
class Http2Stream;
class ResourceResolver;
class ObjectMapper;

// TODO(Naoki): Rename to Http2PhysicalPipe
class PhysicalPipeImpl final : public PhysicalPipe {
 private:
  std::string host_;
  uint16_t port_;
  bool is_secure_;
  std::string cafile_;

  SSL_CTX *ssl_ctx_;
  Http2Session *session_data_;
  std::string *last_connection_error_;

  EventLoop *event_loop_;
  const ResourceResolver *const resource_resolver_;
  const ObjectMapper *const object_mapper_;

  bool shutdown_;
  std::mutex *shutdown_mutex_;
  std::condition_variable *shutdown_cv_;

 public:
  static void GlobalInit();

  PhysicalPipeImpl(std::string host, uint16_t port, bool is_secure,
                   const ResourceResolver *resource_resolver, const ObjectMapper *object_mapper);

  std::string name() override { return "http2"; }
  void Initialize(response_cb cb, void *cb_args) override;
  void HandleRequest(const RequestMessage &request_message) override;
  bool IsActive() override { return session_data_ != nullptr; }
  void Shutdown() override;
  std::string GetLastConnectionError() override;
  void SetConnectionError(const std::string &error_message);
  void ClearConnectionError();

  // Getters
  inline const std::string &host() const { return host_; }
  inline uint16_t port() const { return port_; }
  inline bool is_secure() const { return is_secure_; }
  inline const std::string &cafile() const { return cafile_; }
  inline EventLoop *event_loop() const { return event_loop_; }
  inline const ObjectMapper *object_mapper() const { return object_mapper_; }
  inline SSL_CTX *ssl_ctx() const { return ssl_ctx_; }
  inline Http2Session *session_data() const { return session_data_; }
  inline bool shutdown() const { return shutdown_; }

  // Setters
  inline void set_ssl_cert_file(const std::string cafile) { cafile_ = cafile; }

  void AttachSession(Http2Session *session_data);

  /**
   * Detaches the session from the pipe if attached one matches the specified
   * one.
   */
  void DetachSession(Http2Session *session_data);

  int HandleSslConnectionError(response_cb cb, void *cb_args, std::string message);

  void HandleDisconnection(Http2Session *session_data);

 protected:
  ~PhysicalPipeImpl() override;

 private:
  int CreateSslContext(response_cb cb, void *cb_args);

  static void InvokeStream(void *args);

  static void TerminatePipe(void *args);
  static void DiscardPipe(void *args);
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_PHYSICAL_PIPE_H_
