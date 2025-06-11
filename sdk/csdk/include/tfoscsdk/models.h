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
#ifndef TFOSCSDK_MODELS_H_
#define TFOSCSDK_MODELS_H_

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "tfos-common/logger.h"
#include "tfoscsdk/csdk.h"
#include "tfoscsdk/types.h"

namespace tfos::csdk {

class ConnectionSet;

struct Credentials {
  std::string username;
  std::string password;
  std::string scope;
};

struct CredentialsBIOS {
  std::string email;
  std::string password;
  std::string app_name;
  std::string app_type;
};

enum class Permission {
  EXTRACT,
  INGEST,
  INGEST_EXTRACT,
  SUPERADMIN,
  ADMIN,
  BI_REPORT,
  DEVELOPER,
  UNKNOWN
};

struct ErrorResponse {
  std::string message;
  std::string error_code;

  ErrorResponse() {}

  explicit ErrorResponse(const std::string &message) : message(message) {}

  explicit ErrorResponse(const std::string &message, const std::string &error_code)
      : message(message), error_code(error_code) {}
};

struct IngestResponseOrError {
  std::string event_id;
  int64_t timestamp;
  int32_t status_code;
  std::string error_message;
  std::string server_error_code;
};

struct IngestBulkErrorResponse {
  std::string message;
  std::string error_code;
  std::vector<IngestResponseOrError> results_with_error;
};

struct StartSessionResponse {
  int32_t sessionId;
};

struct SessionAttribute {
  std::string key;
  std::string value;
};

struct LoginResponse {
  std::string token;
  int64_t expiry;
  int64_t sessionTimeoutMillis;
  std::vector<Permission> permissions;
  std::vector<SessionAttribute> session_attributes;
};

struct LoginResponseBIOS {
  std::string tenant;
  std::string app_name;
  std::string app_type;
  std::string upstreams;
  std::string dev_instance;
  std::string home_page_config;
  std::string token;
  std::vector<SessionAttribute> session_attributes;
  int64_t expiry;
};

enum class AdminOpType { ADD, REMOVE, UPDATE, UNKNOWN };

enum class RequestPhase { INITIAL, FINAL };

struct AdminWriteRequest {
  AdminOpType operation;
  RequestPhase phase;
  int64_t timestamp;
  payload_t payload;
  bool force;

  AdminWriteRequest(AdminOpType op, RequestPhase phase, int64_t timestamp, payload_t payload,
                    bool force)
      : operation(op), phase(phase), timestamp(timestamp), payload(payload), force(force) {}

  AdminWriteRequest()
      : operation(AdminOpType::UNKNOWN),
        phase(RequestPhase::INITIAL),
        timestamp(0),
        payload({0}),
        force(false) {}
};

struct AdminWriteResponse {
  long timestamp;
  std::vector<std::string> endpoints;
};

struct FailureReport {
  int64_t timestamp;
  std::string operation;
  std::string payload;
  std::vector<std::string> endpoints;
  std::vector<std::string> reasons;
  std::string reporter;
};

// Not an accurate definition. Only for testing via JSON encoding.
struct IngestRequest {
  std::string eventId;
  std::string eventText;
  int64_t streamVersion;
};

// Not an accurate definition. Only for testing via JSON decoding.
struct IngestResponse {
  std::string eventId;
  int64_t ingestTimestamp;
};

enum class NodeType { SIGNAL = 1, ROLLUP = 2, ANALYSIS = 3 };

struct UpdateEndpointRequest {
  AdminOpType operation;
  std::string endpoint;
  NodeType nodeType;
};

/**
 * Struct to carry resources of a method call.
 *
 * Since required resources and its are limited, we just hard code
 * them in this struct rather than using a map to generalize parameter store.
 */
struct Resources {
  std::string tenant;
  std::string stream;
  std::map<std::string, std::string> path_params;
  std::map<std::string, std::string> options;
};

/**
 * Struct to carry context of a session.
 */
class Session final {
 public:
  // TODO(Naoki): Make these parameters private and provide setters and getters
  const int32_t session_id;
  ConnectionSet *const connection_set;
  std::string tenant;
  // std::string stream;
  std::string username;
  std::string home_page_config;
  std::string dev_instance;
  std::string upstreams;
  std::unordered_map<std::string, std::string> session_attributes;
  payload_t loginResponseJson;
  bool bios;
  std::vector<Permission> permissions;
  int64_t timeout;  // Operation timeout millis; Never timeouts when zero.
  std::shared_ptr<std::string> token;
  int64_t expiry;                    // session expiry
  int64_t renewal_time;              // session expiry - lead time
  std::atomic_int64_t renewal_sent;  // timestamp of the latest renewal request
  bool signed_in;
  bool is_expired;
  bool is_realtime;
  std::unique_ptr<std::string> endpoint;
  bool is_internal_network;
  int64_t closed_timestamp;
  Logger *logger;

 public:
  Session(int32_t _session_id, ConnectionSet *_connection_set, int64_t _timeout,
          bool _is_internal_network)
      : session_id(_session_id),
        connection_set(_connection_set),
        tenant(),
        username(),
        permissions(),
        timeout(_timeout),
        token(),
        expiry(0),
        renewal_time(0),
        renewal_sent(0),
        signed_in(false),
        is_expired(false),
        is_realtime(false),
        is_internal_network(_is_internal_network),
        closed_timestamp(0) {
    logger = new Logger();
  }

  Session()
      : session_id(0),
        connection_set(nullptr),
        tenant(),
        username(),
        permissions(),
        timeout(0),
        token(),
        expiry(0),
        renewal_time(0),
        renewal_sent(0),
        signed_in(false),
        is_expired(false),
        is_realtime(false),
        is_internal_network(false),
        closed_timestamp(0) {
    logger = new Logger();
  }

  ~Session() { delete logger; }

  int32_t GetLookupQosThresholdMillis() {
    int32_t lookup_qos_threshold_millis = 0;
    auto attr_pair = session_attributes.find("prop.lookupQosThresholdMillis");
    if (attr_pair != session_attributes.end()) {
      try {
        lookup_qos_threshold_millis = std::stoi(attr_pair->second);
      } catch (...) {
        lookup_qos_threshold_millis = 0;
      }
    }

    return lookup_qos_threshold_millis;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "{tenant=" << tenant << ", username=" << username
       << ", timeout=" << timeout << ", token=" << (token ? *token : std::string("<null>"))
       << ", expiry=" << expiry << ", renewal_time=" << renewal_time
       << ", renewal_sent=" << renewal_sent << ", is_internal_network=" << is_internal_network;
    if (endpoint) {
      ss << ", endpoint=" << *endpoint;
    }
    ss << ", permissions=[";
    bool init = true;
    for (auto perm : permissions) {
      if (!init) {
        ss << ", ";
        init = false;
      }
      ss << static_cast<int>(perm);
    }
    ss << "]}";
    return ss.str();
  }
};

/**
 * Callback function that is invoked when the server sends back the response.
 * Since the function would be called after this method completes, all resources
 * used by the function must be available until the function is invoked. Also,
 * assume that the thread that runs the callback function is different from one
 * that runs this method.
 */
class ResponseMessage;
typedef void (*response_cb)(const ResponseMessage &, void *cb_args);

/**
 * Struct to carry internal metadata for a method call.
 */
struct RequestContext {
  // Resources specifier for the request.
  Resources resources;
  // The session where the request is placed.
  Session *session;
  int32_t session_id;
  // Operation timeout
  int64_t timeout;

  // Explicit target endpoint
  const std::string *endpoint;

  // Callback function and args
  response_cb completion_cb;
  void *completion_cb_args;

  // Metric timestamps -- is nullptr when not counting metrics. TODO(Naoki):
  // hacky. do something
  payload_t *metric_timestamps;

  // Flag to indicate that the request is for logging client metrics.
  bool is_metrics_logging;

  // Session token
  std::shared_ptr<std::string> token;

  RequestContext(Session *session, int64_t timeout, const std::string *endpoint,
                 response_cb completion_cb, void *completion_cb_args, payload_t *metric_timestamps,
                 bool is_metrics_logging)
      : session(session),
        session_id(session ? session->session_id : -1),
        timeout(timeout),
        endpoint(endpoint),
        completion_cb(completion_cb),
        completion_cb_args(completion_cb_args),
        metric_timestamps(metric_timestamps),
        is_metrics_logging(is_metrics_logging),
        token() {
    if (session) {
      token = session->token;
    }
  }

  RequestContext()
      : session(nullptr),
        timeout(0),
        endpoint(nullptr),
        completion_cb(nullptr),
        completion_cb_args(nullptr),
        metric_timestamps(nullptr),
        is_metrics_logging(false),
        token() {}
};

enum class OperationType {
  INGEST,
  EXTRACT,
  CONTEXT_WRITE,
  ADMIN_WRITE,
  ADMIN_READ,
  SUMMARIZE,
  END
};

enum class OperationPolicy { ALWAYS, UNDERFAILURE, NEVER };

enum class LbPolicy {
  ROUND_ROBIN,
};

class Operation {
 public:
  Operation() = default;
  virtual ~Operation() = default;

 private:
  OperationPolicy operation_policy_;
  OperationType operation_type_;

 public:
  const OperationPolicy &operation_policy() const { return operation_policy_; }
  void set_operation_policy(const OperationPolicy &value) { this->operation_policy_ = value; }

  const OperationType &operation_type() const { return operation_type_; }
  void set_operation_type(const OperationType &value) { this->operation_type_ = value; }
};

class OperationSet {
 public:
  OperationSet() = default;
  virtual ~OperationSet() = default;

 private:
  std::vector<std::unique_ptr<Operation>> operation_;

 public:
  const std::vector<std::unique_ptr<Operation>> *operation() { return &operation_; }
  void set_operation(std::vector<std::unique_ptr<Operation>> *value) {
    this->operation_ = std::move(*value);
  }
};

class Upstream {
 public:
  Upstream() = default;
  ~Upstream() { delete operation_set_; }

 private:
  std::vector<std::string> host_set_;
  OperationSet *operation_set_;

 public:
  const std::vector<std::string> &host_set() const { return host_set_; }
  void set_host_set(const std::vector<std::string> &value) { this->host_set_ = value; }

  OperationSet *operation_set() { return operation_set_; }
  void set_operation_set(OperationSet *value) { this->operation_set_ = value; }
  uint64_t NumNodes() { return static_cast<uint64_t>(host_set_.size()); }
};

class UpstreamConfig {
 public:
  UpstreamConfig() = default;
  virtual ~UpstreamConfig() = default;

 private:
  LbPolicy lb_policy_;
  std::vector<std::unique_ptr<Upstream>> upstream_;

 public:
  const LbPolicy &lb_policy() const { return lb_policy_; }
  void set_lb_policy(const LbPolicy &value) { this->lb_policy_ = value; }

  const std::vector<std::unique_ptr<Upstream>> *upstream() { return &upstream_; }
  void set_upstream(std::vector<std::unique_ptr<Upstream>> *value) {
    this->upstream_ = std::move(*value);
  }
  uint64_t NumNodes() {
    uint64_t nodes = 0;
    for (uint64_t i = 0; i < upstream_.size(); i++) {
      nodes += upstream_[i].get()->NumNodes();
    }
    return nodes;
  }
};

}  // namespace tfos::csdk
#endif  // TFOSCSDK_MODELS_H_
