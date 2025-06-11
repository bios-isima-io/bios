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
#include "tfos-physical/connection.h"

#include <string.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <iostream>

#include "tfos-common/utils.h"
#include "tfos-physical/physical_pipe.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

// Connection implementation
// //////////////////////////////////////////////////////////////////

const uint64_t Connection::kDefaultErrorExpiry = 5000;

Connection::Connection(PhysicalPipe *pipe, const std::string &host, int port, bool ssl_enabled)
    : pipe_(pipe),
      host_(host),
      port_(port),
      ssl_enabled_(ssl_enabled),
      is_signal_(false),
      is_marked_(0),
      had_error_(0),
      error_expiry_(kDefaultErrorExpiry) {
  endpoint_ = MakeEndpointString(host_, port_, ssl_enabled_);
}

Connection::Connection()
    : pipe_(nullptr),
      port_(0),
      ssl_enabled_(false),
      is_signal_(false),
      is_marked_(0),
      had_error_(0),
      error_expiry_(kDefaultErrorExpiry) {}

Connection::~Connection() {
  if (pipe_) {
    pipe_->Shutdown();
  }
}

bool Connection::IsReady() {
  if (had_error_ > 0 || is_marked_ > 0) {
    uint64_t now = Utils::CurrentTimeMillis();
    return (now - had_error_) > error_expiry_ && (now - is_marked_) > error_expiry_;
  }
  return true;
}

void Connection::CopyErrorMark(const Connection &src) { is_marked_ = src.is_marked_; }

void Connection::UpdateServerMark(bool is_marked) {
  is_marked_ = is_marked ? Utils::CurrentTimeMillis() : 0;
}

void Connection::UpdateErrorStatus(bool is_error) {
  if (is_error) {
    had_error_ = Utils::CurrentTimeMillis();
  } else {
    had_error_ = 0;
    is_marked_ = 0;
  }
}

std::string Connection::MakeEndpointString(const std::string &host, int port, bool ssl_enabled) {
  // TODO(Naoki): This has to be a normlized URL for multi-protocol support. But
  // current
  //     implementation is fine as long as we use only HTTP/HTTPS protocol.
  //     Actually, C-SDK supports only HTTPS for the first version.
  return host + ":" + std::to_string(port) + ":" + std::to_string(ssl_enabled);
}

std::string Connection::MakeEndpointString(const std::string &url) {
  size_t index_start = 0;
  std::string protocol;
  std::string host;
  int port;
  bool ssl_enabled;
  return ParseUrl(url, index_start, &protocol, &host, &port, &ssl_enabled)
             ? MakeEndpointString(host, port, ssl_enabled)
             : "";
}

uint64_t Connection::GetDefaultErrorExpiry() { return kDefaultErrorExpiry; }

Connection *Connection::BuildConnection(const std::string &url) {
  Connection *conn = new Connection();
  size_t index_start = 0;
  // check the error mark
  if (url.at(0) == '.') {
    conn->UpdateServerMark(true);
    ++index_start;
  } else {
    conn->UpdateServerMark(false);
  }

  if (!ParseUrl(url, index_start, &conn->protocol_, &conn->host_, &conn->port_,
                &conn->ssl_enabled_)) {
    delete conn;
    return nullptr;
  }

  // put the normlized endpoint name
  conn->endpoint_ = MakeEndpointString(conn->host_, conn->port_, conn->ssl_enabled_);
  return conn;
}

bool Connection::ParseUrl(const std::string &url, size_t index_start, std::string *protocol,
                          std::string *host, int *port, bool *ssl_enabled) {
  // parse the protocol part
  size_t index_end = url.find("://", index_start);
  if (index_end == std::string::npos) {
    return false;
  }
  *protocol = url.substr(index_start, index_end - index_start);
  boost::algorithm::to_lower(*protocol);
  *ssl_enabled = *protocol == "https";

  // parse the authority part
  index_start = index_end + 3;
  index_end = url.find("/", index_start);
  if (index_end == std::string::npos) {
    index_end = url.size();
  }
  std::string hostport = url.substr(index_start, index_end - index_start);
  index_end = hostport.find(":");
  if (index_end != std::string::npos) {
    *host = hostport.substr(0, index_end);
    ++index_end;
    *port = index_end < hostport.size() ? atoi(hostport.substr(index_end).c_str()) : 0;
  } else {
    *host = hostport;
    *port = 0;
  }
  boost::algorithm::to_lower(*host);
  if (*port == 0) {
    *port = *ssl_enabled ? 443 : 80;  // TODO(Naoki): Invalid for non-HTTP protocols
  }
  return true;
}

}  // namespace tfos::csdk
