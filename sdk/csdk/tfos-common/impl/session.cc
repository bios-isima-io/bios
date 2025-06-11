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
#include "tfos-common/session.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>

#include "tfos-common/impl/ingest_bulk_ctx.h"
#include "tfos-common/keywords.h"
#include "tfos-common/logger.h"
#include "tfos-common/message_handler.h"
#include "tfos-common/object_mapper.h"
#include "tfos-physical/connection.h"
#include "tfos-physical/connection_set.h"
#include "tfoscsdk/csdk.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

const char *SessionController::SUPER_ADMIN_SCOPE = "/";
const int64_t SessionController::MIN_RENEW_SESSION_LEAD_TIME = 10000;
const int64_t SessionController::RENEWAL_BLACKOUT_MILLIS = 5000;

SessionController::SessionController(const ObjectMapper *object_mapper,
                                     MessageHandler *message_handler)
    : object_mapper_{object_mapper}, message_handler_{message_handler} {
  next_session_id_ = 1;
  if (message_handler_ && (message_handler_->name() != "mock")) {
    message_handler_->ScheduleTask(FlushClientMetricsTask, FlushClientMetricsFinalTask, this, 1000);
  }
}

SessionController::~SessionController() {
  for (auto entry : sessions_) {
    delete entry.second;
  }
  for (auto entry : stale_sessions_) {
    delete entry;
  }
}

class MethodCallState final {
 public:
  capi_completion_cb const api_cb;
  void *const api_cb_args;
  int64_t timeout;
  SessionController *const controller;

  RequestMessage *request_message;

  std::atomic_bool *responded;
  std::atomic_int *remaining;

  std::unique_ptr<std::string> endpoint;  // Original request endpoint, used by StartSession
  bool disable_dynamic_routing;           // Used by login method; Don't use routing
                                          // config after login

  uint64_t start_time;

  MethodCallState(capi_completion_cb api_cb, void *api_cb_args, int64_t timeout,
                  SessionController *controller)
      : api_cb(api_cb),
        api_cb_args(api_cb_args),
        timeout(timeout),
        controller(controller),
        request_message(nullptr),
        responded(nullptr),
        remaining(nullptr),
        disable_dynamic_routing(false),
        start_time{0} {}

  MethodCallState(capi_completion_cb api_cb, void *api_cb_args, int64_t timeout,
                  SessionController *controller, uint64_t start_time)
      : api_cb(api_cb),
        api_cb_args(api_cb_args),
        timeout(timeout),
        controller(controller),
        request_message(nullptr),
        responded(nullptr),
        remaining(nullptr),
        disable_dynamic_routing(false),
        start_time{start_time} {}

  MethodCallState(const MethodCallState &src)
      : api_cb(src.api_cb),
        api_cb_args(src.api_cb_args),
        timeout(src.timeout),
        controller(src.controller),
        request_message(src.request_message),
        responded(src.responded),
        remaining(src.remaining),
        disable_dynamic_routing(src.disable_dynamic_routing),
        start_time{src.start_time} {
    if (src.endpoint) {
      endpoint = std::make_unique<std::string>(*src.endpoint.get());
    }
  }

  ~MethodCallState() {
    assert(responded == nullptr);
    assert(remaining == nullptr);
  }
};

void SessionController::CompleteGetConnectionSet(const ResponseMessage &response,
                                                 ConnectionSet *connection_set, void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);

  OpStatus status = response.status();
  payload_t reply_payload;
  if (status != OpStatus::OK) {
    // Opening connection failed. We'll just forward the server response to the
    // caller.
    reply_payload = {
        .data = const_cast<uint8_t *>(response.response_body()),
        .length = response.body_length(),
    };
  } else {
    // register a session
    int32_t session_id = state->controller->GetNextSessionId();
    Session *session =
        new Session(session_id, connection_set, state->timeout, !state->disable_dynamic_routing);
    session->endpoint = std::move(state->endpoint);
    if (!state->controller->AddSession(session_id, session)) {
      status = OpStatus::GENERIC_CLIENT_ERROR;
      std::stringstream ss;
      ss << "Failed to add the new session -- ID " << session_id << " has been registered already";
      ErrorResponse resp(ss.str());
      reply_payload = state->controller->object_mapper_->WriteValue(resp);
      delete session;
    } else {
      // send back the response object to the caller.
      StartSessionResponse response = {
          .sessionId = session_id,
      };
      reply_payload = state->controller->object_mapper_->WriteValue(response);
    }
  }
  completion_data_t completion_data = Utils::get_basic_completion_data();
  completion_data.endpoint = Utils::ToStringType(response.endpoint());
  state->api_cb(Statuses::StatusCode(status), &completion_data, reply_payload, state->api_cb_args);
  delete state;
}

void SessionController::StartSession(const std::string &host, int port, bool ssl_enabled,
                                     const std::string &journal_dir, const std::string &cert_file,
                                     capi_completion_cb api_cb, void *api_cb_args,
                                     int64_t timeout) {
  auto *state = new MethodCallState(api_cb, api_cb_args, timeout, this);
  std::string endpoint = ssl_enabled ? "https://" : "http://";
  endpoint += host;
  endpoint += ":";
  endpoint += std::to_string(port);
  state->endpoint = std::make_unique<std::string>(endpoint);
  message_handler_->GetConnectionSet(host, port, ssl_enabled, cert_file, CompleteGetConnectionSet,
                                     state);
}

bool SessionController::SetOpTimeout(int32_t session_id, int64_t timeout) {
  Session *session = FindSession(session_id, nullptr, nullptr);
  if (session == nullptr) {
    return false;
  }
  session->timeout = timeout;
  return true;
}

void SessionController::CloseSession(int32_t session_id) {
  DEBUG_LOG("SessionController::CloseSession; session_id=%d", session_id);
  Session *session = FindSession(session_id, nullptr, nullptr);
  if (session == nullptr) {
    return;
  }
  invalidated_sessions_.insert(session_id);
  // TODO(Naoki): Cancel or wait pending operations
  bool session_removed = RemoveSession(session_id);

  // Clear
  auto now = Utils::CurrentTimeMillis();
  session->closed_timestamp = now;
  DEBUG_LOG("SessionController::CloseSession; session=%p, timestamp=%ld", session, now);
  std::shared_lock lock(sessions_mutex_);
  for (auto it = stale_sessions_.begin(); it != stale_sessions_.end();) {
    if ((*it)->closed_timestamp < now - 60000) {
      DEBUG_LOG("Deleting session %p, id=%d, timestamp=%ld, now=%ld",
       *it, (*it)->session_id, (*it)->closed_timestamp, now);
      delete *it;
      it = stale_sessions_.erase(it);
    } else {
      ++it;
    }
  }

  // If ID removal happened in this execution, obsolete the session object.
  // Otherwise, another execution would do it.
  if (session_removed) {
    stale_sessions_.insert(session);
  }
}

bool SessionController::CheckLoginResponse(const ResponseMessage &response, MethodCallState *state,
                                           bool v2 = false) {
  SessionController *controller = state->controller;
  // Login succeeded.  We tap the server response and put the parameters to the
  // session.
  if (v2) {
    auto login_response = controller->object_mapper_->ReadValue<LoginResponseBIOS>(
        response.response_body(), response.body_length());

    if (login_response) {
      Utils::ReleasePayload(response.response_body());
      auto *session = state->request_message->context()->session;
      session->token = std::make_shared<std::string>("Bearer " + login_response->token);
      session->expiry = login_response->expiry;
      session->tenant = login_response->tenant;
      session->logger->SetAppName(login_response->app_name);
      session->logger->SetAppType(login_response->app_type);
      if (boost::iequals(login_response->app_type, "Realtime")) {
        session->is_realtime = true;
      }
      for (const auto &attribute : login_response->session_attributes) {
        session->session_attributes.insert_or_assign(attribute.key, attribute.value);
      }
      if (session->tenant.empty()) {
        return false;
      }
      if (session->is_internal_network) {
        session->upstreams = login_response->upstreams;
      }
      session->bios = true;
      session->loginResponseJson =
          state->controller->object_mapper_->WriteValue(*(login_response.get()));
      int64_t lead_time = (session->expiry - Utils::CurrentTimeMillis()) * 0.5;
      session->renewal_time = session->expiry - std::max(lead_time, MIN_RENEW_SESSION_LEAD_TIME);
      return true;
    }
  } else {
    auto login_response = controller->object_mapper_->ReadValue<LoginResponse>(
        response.response_body(), response.body_length());
    if (login_response) {
      Utils::ReleasePayload(response.response_body());
      auto *session = state->request_message->context()->session;
      session->permissions = login_response->permissions;
      session->token = std::make_shared<std::string>("Bearer " + login_response->token);
      session->expiry = login_response->expiry;
      int64_t lead_time = login_response->sessionTimeoutMillis * 0.5;
      session->renewal_time = session->expiry - std::max(lead_time, MIN_RENEW_SESSION_LEAD_TIME);
      session->bios = false;
      return true;
    }
  }
  // The server response has been consumed.

  Utils::ReleasePayload(response.response_body());

  std::stringstream ss;
  ss << "Failed to parse login response; response="
     << std::string((const char *)response.response_body(), response.body_length());
  ErrorResponse error_response(ss.str());
  payload_t reply_payload = state->controller->object_mapper_->WriteValue(error_response);
  completion_data_t completion_data = Utils::get_basic_completion_data();
  completion_data.endpoint = Utils::ToStringType(response.endpoint());
  state->api_cb(Statuses::StatusCode(OpStatus::SERVER_CHANNEL_ERROR),
                &completion_data, reply_payload, state->api_cb_args);
  CleanupRequestMessage(state);
  delete state;
  return false;
}

void SessionController::HandleSuccessfulLogin(const ResponseMessage &response,
                                              MethodCallState *state) {
  auto *session = state->request_message->context()->session;
  auto *controller = state->controller;
  if (!state->disable_dynamic_routing) {
    CleanupRequestMessage(state);
    controller->SendRequest(CSDK_OP_GET_UPSTREAM_CONFIG, nullptr, nullptr, {0}, session, nullptr,
                            nullptr, CompleteGetUpstreamPostLogin, state);
  } else {
    // Asking for routing config is skipped; We proceed to the post login
    // operation
    HandleSuccessfulGetEndpointsPostLogin(response, state);
  }
  // The operation continues. We don't send reply and will keep the call state
  // object.
}

void SessionController::CompleteConnRegistration(const ResponseMessage &response, Connection *conn,
                                                 void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);
  if (response.status() == OpStatus::OK) {
    DEBUG_LOG("endpoint registration was successful; endpoint=%s:%d", conn->host().c_str(),
              conn->port());
  } else {
    DEBUG_LOG("endpoint registration was fail; endpoint=%s:%d status=%s", conn->host().c_str(),
              conn->port(), Statuses::Name(response.status()).c_str());
  }
  Utils::ReleasePayload(response.response_body());

  // Respond when at least one upstream node becomes available
  auto *connection_set = state->request_message->context()->session->connection_set;
  auto *responded = state->responded;
  connection_set->UpdateRoutingTable();
  if (responded != nullptr && !responded->load() && connection_set->IsAnyUpstreamNodeAvailable()) {
    bool prev = false;
    bool to_respond = responded->compare_exchange_strong(prev, true);
    if (to_respond) {
      DEBUG_LOG("At least one upstream node is available, responding early");
      payload_t final_payload = {0};
      ResponseMessage final_response(OpStatus::OK, "", final_payload);
      HandleSuccessfulGetEndpointsPostLogin(final_response, state, false, to_respond);
    }
  }

  int current = state->remaining->fetch_sub(1);
  if (current == 1) {
    delete state->remaining;
    state->remaining = nullptr;
    OpStatus final_status;
    payload_t final_payload;
    if (connection_set->IsAnyUpstreamNodeAvailable()) {
      final_status = OpStatus::OK;
      final_payload = {0};
    } else {
      final_status = OpStatus::SERVICE_UNAVAILABLE;
      ErrorResponse error("No signal hosts are available");
      final_payload = state->controller->object_mapper_->WriteValue(error);
    }
    ResponseMessage final_response(final_status, "", final_payload);
    bool prev = false;
    bool to_respond = responded == nullptr || responded->compare_exchange_strong(prev, true);
    DEBUG_LOG("All attempts to connect upstreams are done, status=%s, to_respond=%d",
              Statuses::Name(final_status).c_str(), to_respond);
    HandleSuccessfulGetEndpointsPostLogin(final_response, state, true, to_respond);
  }
}

void SessionController::CompleteGetEndpointsPostLogin(const ResponseMessage &response,
                                                      void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);
  auto *controller = state->controller;

  // Login response handler
  OpStatus status = response.status();
  if (status == OpStatus::OK) {
    auto endpoints = controller->object_mapper_->ReadValue<std::vector<std::string>>(
        response.response_body(), response.body_length());
    Utils::ReleasePayload(response.response_body());
    if (endpoints && !endpoints->empty()) {
      state->remaining = new std::atomic_int(static_cast<int>(endpoints->size()));
      for (size_t i = 0; i < endpoints->size(); ++i) {
        const std::string &endpoint = (*endpoints)[i];
        DEBUG_LOG("endpoint[%ld]=%s", i, endpoint.c_str());
        state->request_message->context()->session->connection_set->GetOneSignal(
            endpoint, CompleteConnRegistration, state);
      }
    } else {
      DEBUG_LOG("empty endpoint, returning %s", Statuses::Name(response.status()).c_str());
      HandleSuccessfulGetEndpointsPostLogin(response, state);
    }
  } else {
    CompleteOperationGeneric(response, state);
  }
}

void SessionController::CompleteGetUpstreamPostLogin(const ResponseMessage &response,
                                                     void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);
  auto *controller = state->controller;
  auto *connection_set = state->request_message->context()->session->connection_set;
  auto upstream_hash = connection_set->GetUpstreamHash();
  // Login response handler
  OpStatus status = response.status();
  if (status == OpStatus::OK) {
    std::string upstream_json(reinterpret_cast<const char *>(response.response_body()),
                              response.body_length());
    DEBUG_LOG("UPSTREAM: %s", upstream_json.c_str());
    std::hash<std::string> hfunction;
    auto h = hfunction(upstream_json);
    if (h != upstream_hash) {
      connection_set->SetUpstreamHash(h);
      auto upstream_config = controller->object_mapper_->ReadValue<UpstreamConfig>(
          response.response_body(), response.body_length());
      Utils::ReleasePayload(response.response_body());
      if (upstream_config) {
        DEBUG_LOG("Got upstream: %d", static_cast<int>(h));
        state->responded = new std::atomic_bool(false);
        state->remaining = new std::atomic_int(static_cast<int>(upstream_config->NumNodes()));
        auto upstreams = upstream_config->upstream();
        connection_set->SetUpstreamConfig(std::move(upstream_config));
        for (uint64_t i = 0; i < upstreams->size(); i++) {
          auto upstream = (*upstreams)[i].get();
          auto host_set = upstream->host_set();
          for (auto host : host_set) {
            connection_set->GetOneSignal(host, CompleteConnRegistration, state);
          }
        }
      } else {
        DEBUG_LOG("empty upstream, returning %s", Statuses::Name(response.status()).c_str());
        HandleSuccessfulGetEndpointsPostLogin(response, state);
      }
    } else {
      Utils::ReleasePayload(response.response_body());
      DEBUG_LOG("Upstream has not change, skipping creation on connections");
      HandleSuccessfulGetEndpointsPostLogin(response, state);
    }
  } else {
    CompleteOperationGeneric(response, state);
  }
}

void SessionController::HandleSuccessfulGetEndpointsPostLogin(const ResponseMessage &response,
                                                              MethodCallState *state,
                                                              bool is_final,
                                                              bool to_respond) {
  auto *session = state->request_message->context()->session;

  // Admin login. Reply the response immediately.
  DEBUG_LOG("Done admin login; status=%s, is_final=%d",
            Statuses::Name(response.status()).c_str(), is_final);
  if (to_respond) {
    std::stringstream responseStream;
    session->signed_in = response.status() == OpStatus::OK;
    completion_data_t completion_data = Utils::get_basic_completion_data();
    completion_data.endpoint = Utils::ToStringType(response.endpoint());
    DEBUG_LOG("Responding");
    if (session->bios) {
      state->api_cb(Statuses::StatusCode(response.status()),
                    &completion_data, session->loginResponseJson,
                    state->api_cb_args);
      session->loginResponseJson = {};  // Once the payload is passed, it will be freed.
    } else {
      state->api_cb(Statuses::StatusCode(response.status()),
                    &completion_data, {}, state->api_cb_args);
    }
  }
  // The operation terminates here.
  if (is_final) {
    DEBUG_LOG("Releasing resources");
    delete state->responded;
    state->responded = nullptr;
    CleanupRequestMessage(state);
    delete state;
  }
}

void SessionController::CompleteLoginBIOS(const ResponseMessage &response, void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);
  auto status = response.status();

  payload_t reply_payload = {
      .data = const_cast<uint8_t *>(response.response_body()),
      .length = response.body_length(),
  };

  // Login response handler
  if (status == OpStatus::OK) {
    if (CheckLoginResponse(response, state, true)) {
      HandleSuccessfulLogin(response, state);
    }
  } else {
    // Forward reply to the caller.
    completion_data_t completion_data = Utils::get_basic_completion_data();
    completion_data.endpoint = Utils::ToStringType(response.endpoint());
    state->api_cb(Statuses::StatusCode(status), &completion_data,
                  reply_payload, state->api_cb_args);
    CleanupRequestMessage(state);
    delete state;
  }
}

void SessionController::CompleteGetStreamPostLogin(const ResponseMessage &response, void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);

  // Login response handler
  OpStatus status = response.status();
  if (status == OpStatus::OK) {
    state->request_message->context()->session->signed_in = true;
  }

  CompleteOperationGeneric(response, state);
}

/**
 * Method to start the session.
 */
void SessionController::Login(int32_t session_id, payload_t request_payload,
                              bool disable_routing, capi_completion_cb api_cb, void *api_cb_args) {
  Session *session = FindSession(session_id, api_cb, api_cb_args);
  if (session == nullptr) {
    return;
  }

  // Parse the credentials in the payload. Its parameters would be used on
  // completion.
  auto credentials =
      object_mapper_->ReadValue<CredentialsBIOS>(request_payload.data, request_payload.length);
  if (credentials) {
    session->username = credentials->email;
  }

  // Send the request asynchronously. New session will have necessary session
  // parameters
  //  on successful server call in the completion handler.
  auto *state = new MethodCallState(api_cb, api_cb_args, session->timeout, this);
  char *const connection_type = getenv(BIOS_CONNECTION_TYPE);
  state->disable_dynamic_routing =
      connection_type == nullptr || strcasecmp(connection_type, "internal") != 0 || disable_routing;
  DEBUG_LOG("connection_type=%s, disable_dynamic_routing=%d", connection_type,
            state->disable_dynamic_routing);
  std::string *endpoint;
  if (disable_routing) {
    endpoint = session->endpoint.get();
  } else {
    endpoint = nullptr;
    session->endpoint.reset();
  }
  SendRequest(CSDK_OP_LOGIN_BIOS, nullptr, nullptr, request_payload, session, endpoint, nullptr,
              CompleteLoginBIOS, state);
  DEBUG_LOG("Login() -- end");
}

void SessionController::CheckSessionExpiry(Session *session) {
  int64_t current_time = Utils::CurrentTimeMillis();
  int64_t renewal_sent = session->renewal_sent;
  if (current_time <= session->renewal_time ||
      current_time - renewal_sent < RENEWAL_BLACKOUT_MILLIS) {
    return;
  }
  if (!session->renewal_sent.compare_exchange_weak(renewal_sent, current_time)) {
    // another thread has sent a renewal request already. backing off.
    return;
  }
  auto *state =
      new MethodCallState(nullptr, session, session->timeout, this, Utils::CurrentTimeMicros());
  SendRequest(CSDK_OP_RENEW_SESSION, nullptr, nullptr, {0}, session, nullptr, nullptr,
              CompleteRenewSession, state);
}

void SessionController::CompleteRenewSession(const ResponseMessage &response, void *cb_args) {
  auto *state = reinterpret_cast<MethodCallState *>(cb_args);
  auto *session = reinterpret_cast<Session *>(state->api_cb_args);
  auto *controller = state->controller;
  OpStatus op_status = response.status();
  if (op_status == OpStatus::OK) {
    auto login_response = controller->object_mapper_->ReadValue<LoginResponse>(
        response.response_body(), response.body_length());
    if (login_response) {
      session->token = std::make_shared<std::string>("Bearer " + login_response->token);
      session->expiry = login_response->expiry;
      int64_t lead_time = (session->expiry - Utils::CurrentTimeMillis()) * 0.5;
      session->renewal_time = session->expiry - std::max(lead_time, MIN_RENEW_SESSION_LEAD_TIME);
      for (const auto &attribute : login_response->session_attributes) {
        auto attr_pair = session->session_attributes.find(attribute.key);
        if (attr_pair != session->session_attributes.end()) {
          DEBUG_LOG("Updated sessionAttribute=%s from %s to %s", attribute.key.c_str(),
                    attr_pair->second.c_str(), attribute.value.c_str());
        }
        session->session_attributes.insert_or_assign(attribute.key, attribute.value);
      }
    }
    uint64_t latency_us = Utils::CurrentTimeMicros() - state->start_time;
    session->logger->Log(true, "", CSDK_OP_RENEW_SESSION, response.endpoint(), latency_us,
                         latency_us, 0, 0, 0, 0, 0);
  } else {
    auto error_response = controller->object_mapper_->ReadValue<ErrorResponse>(
        response.response_body(), response.body_length());
    uint64_t latency_us = Utils::CurrentTimeMicros() - state->start_time;
    session->logger->Log((Statuses::Name(op_status) == "OK"), "", CSDK_OP_RENEW_SESSION,
                         response.endpoint(), latency_us, latency_us, 0, 0, 0, 0, 0);
  }
  CleanupRequestMessage(state);
  delete state;
}

void SessionController::AddEndpoint(int32_t session_id, const std::string &endpoint,
                                    CSdkNodeType node_type, capi_completion_cb api_cb,
                                    void *api_cb_args) {
  DEBUG_LOG("AddEndpoint -- request received; session_id=%d endpoint=%s node_type=%d", session_id,
            endpoint.c_str(), node_type);

  Session *session = FindSession(session_id, api_cb, api_cb_args);
  if (session == nullptr) {
    return;
  }
  CSdkOperationId op_id = CSDK_OP_UPDATE_ENDPOINTS;
  if (!IsOperationAllowed(session, op_id, api_cb, api_cb_args)) {
    return;
  }
  CheckSessionExpiry(session);
  auto state = new MethodCallState(api_cb, api_cb_args, session->timeout, this);
  UpdateEndpointRequest req = {
      .operation = AdminOpType::ADD,
      .endpoint = endpoint,
      .nodeType = static_cast<NodeType>(node_type),
  };
  payload_t request_payload = object_mapper_->WriteValue(req);
  std::map<std::string, std::string> params;
  // TODO(Naoki): Parameterize the key
  params[CSDK_KEY_NODE_TYPE] = std::to_string(node_type);
  SendRequest(op_id, nullptr, nullptr, request_payload, session, &endpoint, nullptr,
              CompleteOperationGeneric, state, nullptr, &params);
}

void SessionController::CompleteOperationGeneric(const ResponseMessage &response, void *cb_args) {
  auto state = reinterpret_cast<MethodCallState *>(cb_args);

  auto session = state->request_message->context()->session;
  OpStatus status = response.status();
  if (status == OpStatus::SESSION_EXPIRED) {
    session->is_expired = true;
    // for backward compatibility
    if (!session->bios) {
      status = OpStatus::UNAUTHORIZED;
    }
  }

  payload_t reply_payload = {
      .data = const_cast<uint8_t *>(response.response_body()),
      .length = response.body_length(),
  };
  completion_data_t completion_data = {
    .endpoint = Utils::ToStringType(response.endpoint()),
    .qos_retry_considered = response.qos_retry_considered(),
    .qos_retry_sent = response.qos_retry_sent(),
    .qos_retry_response_used = response.qos_retry_response_used(),
  };
  state->api_cb(Statuses::StatusCode(status), &completion_data, reply_payload, state->api_cb_args);

  CleanupRequestMessage(state);
  delete state;
}

void SessionController::SimpleMethod(int32_t session_id, CSdkOperationId op_id,
                                     const std::string *tenant_name, const std::string *stream_name,
                                     payload_t request_payload, const std::string *endpoint,
                                     payload_t *metric_timestamps, capi_completion_cb api_cb,
                                     void *api_cb_args, bool is_metrics_logging) {
  DEBUG_LOG("SimpleMethod -- request received; session_id=%d op=%s tenant=%s stream=%s", session_id,
            Utils::GetOperationName(op_id).text, tenant_name ? tenant_name->c_str() : nullptr,
            stream_name ? stream_name->c_str() : nullptr);
  Session *session = FindSession(session_id, api_cb, api_cb_args);
  if (session == nullptr) {
    return;
  }
  if (!IsOperationAllowed(session, op_id, api_cb, api_cb_args)) {
    return;
  }
  CheckSessionExpiry(session);
  auto state = new MethodCallState(api_cb, api_cb_args, session->timeout, this);
  if (endpoint == nullptr) {
    endpoint = session->endpoint.get();
  }
  SendRequest(op_id, tenant_name, stream_name, request_payload, session, endpoint,
              metric_timestamps, CompleteOperationGeneric, state, nullptr, nullptr,
              is_metrics_logging);
}

void SessionController::GenericMethod(int32_t session_id, CSdkOperationId op_id,
                                      string_t resources_src[], string_t options_src[],
                                      payload_t request_payload, int64_t timeout,
                                      const std::string *endpoint, payload_t *metric_timestamps,
                                      capi_completion_cb api_cb, void *api_cb_args) {
  DEBUG_LOG("GenericMethod -- request received; session_id=%d op=%s", session_id,
            Utils::GetOperationName(op_id).text);
  Session *session = FindSession(session_id, api_cb, api_cb_args);
  if (session == nullptr) {
    return;
  }
  if (!IsOperationAllowed(session, op_id, api_cb, api_cb_args)) {
    return;
  }
  CheckSessionExpiry(session);

  std::string tenant;
  std::string stream;
  std::map<std::string, std::string> path_params;
  std::map<std::string, std::string> options;
  if (resources_src) {
    DEBUG_LOG("GenericMethod -- resource_src is specified");
    for (int i = 0; resources_src[i].text != nullptr; i += 2) {
      assert(resources_src[i + 1].text != nullptr);
      DEBUG_LOG(" key=%s value=%s", resources_src[i].text, resources_src[i + 1].text);
      if (strncmp(resources_src[i].text, CSDK_KEY_TENANT, resources_src[i].length) == 0) {
        tenant.assign(resources_src[i + 1].text, resources_src[i + 1].length);
      } else if (strncmp(resources_src[i].text, CSDK_KEY_STREAM, resources_src[i].length) == 0) {
        stream.assign(resources_src[i + 1].text, resources_src[i + 1].length);
      } else {
        path_params[std::string(resources_src[i].text, resources_src[i].length)] =
            std::string(resources_src[i + 1].text, resources_src[i + 1].length);
      }
    }
  }
  if (options_src) {
    for (int i = 0; options_src[i].text != nullptr; i += 2) {
      assert(options_src[i + 1].text != nullptr);
      options[std::string(options_src[i].text, options_src[i].length)] =
          std::string(options_src[i + 1].text, options_src[i + 1].length);
    }
  }

#if defined(DEBUG_LOG_ENABLED)
  DEBUG_LOG("GenericMethod -- tenant=%s stream=%s", tenant.c_str(), stream.c_str());
  for (auto it = path_params.begin(); it != path_params.end(); ++it) {
    DEBUG_LOG("path_params[%s]=%s", it->first.c_str(), it->second.c_str());
  }
  for (auto it = options.begin(); it != options.end(); ++it) {
    DEBUG_LOG("options[%s]=%s", it->first.c_str(), it->second.c_str());
  }
#endif

  auto state =
      new MethodCallState(api_cb, api_cb_args, timeout >= 0 ? timeout : session->timeout, this);
  if (endpoint == nullptr) {
    endpoint = session->endpoint.get();
  }
  SendRequest(op_id, tenant.empty() ? nullptr : &tenant, stream.empty() ? nullptr : &stream,
              request_payload, session, endpoint, metric_timestamps, CompleteOperationGeneric,
              state, options.empty() ? nullptr : &options,
              path_params.empty() ? nullptr : &path_params);
}

void SessionController::IngestBulkStart(int32_t session_id, CSdkOperationId op_id, int num_events,
                                        capi_completion_cb api_cb,
                                        void (*ask_cb)(int64_t, int, int, void *), void *cb_args) {
  Session *session = FindSession(session_id, api_cb, cb_args);
  if (session == nullptr) {
    return;
  }
  auto *ingest_bulk_ctx = new IngestBulkCtx(num_events, ask_cb, cb_args, session, op_id);
  ingest_bulk_ctx->AskMore();
}

struct IngestBulkState {
  IngestBulkCtx *ingest_bulk_ctx;
  int from_index;
  int to_index;
  capi_completion_cb api_cb;
  void *api_cb_args;

  IngestBulkState(IngestBulkCtx *ingest_bulk_ctx, int from_index, int to_index,
                  capi_completion_cb api_cb, void *api_cb_args)
      : ingest_bulk_ctx(ingest_bulk_ctx),
        from_index(from_index),
        to_index(to_index),
        api_cb(api_cb),
        api_cb_args(api_cb_args) {}
};

void SessionController::CompleteIngestBulk(status_code_t status_code,
                                           completion_data_t *completion_data,
                                           payload_t response_data, void *cb_args) {
  auto *ingest_bulk_state = reinterpret_cast<IngestBulkState *>(cb_args);
  auto *ingest_bulk_ctx = ingest_bulk_state->ingest_bulk_ctx;
  if (status_code == Statuses::StatusCode(OpStatus::REQUEST_TOO_LARGE)) {
    ingest_bulk_ctx->SqueezeBatchSize();
  } else {
    ingest_bulk_ctx->MarkDone(ingest_bulk_state->from_index, ingest_bulk_state->to_index);
  }
  bool would_ask = ingest_bulk_ctx->WouldAskMore();
  ingest_bulk_state->api_cb(status_code, completion_data, response_data,
      ingest_bulk_state->api_cb_args);
  // The caller would call IngestBulkEnd from a different thread when no more
  // records to ingest. In such a case, ingest_bulk_ctx shouldn't be touched
  // beyond this line.
  if (would_ask) {
    ingest_bulk_ctx->AskMore();
  }
  delete ingest_bulk_state;
}

void SessionController::IngestBulk(int32_t session_id, int64_t ingest_bulk_ctx_id, int from_index,
                                   int to_index, string_t resources[], string_t options[],
                                   payload_t payload, payload_t *metric_timestamps,
                                   capi_completion_cb api_cb, void *api_cb_args) {
  // TODO(Naoki): Hack, this is really dangerous
  auto *ingest_bulk_ctx = reinterpret_cast<IngestBulkCtx *>(ingest_bulk_ctx_id);
  auto *ingest_bulk_state =
      new IngestBulkState(ingest_bulk_ctx, from_index, to_index, api_cb, api_cb_args);
  GenericMethod(session_id, ingest_bulk_ctx->op_id(), resources, options, payload, 0, nullptr,
               metric_timestamps, CompleteIngestBulk, ingest_bulk_state);
}

void SessionController::IngestBulkEnd(int32_t session_id, int64_t ingest_bulk_ctx_id) {
  // TODO(Naoki): Hack, this is really dangerous
  auto *ingest_bulk_ctx = reinterpret_cast<IngestBulkCtx *>(ingest_bulk_ctx_id);
  DEBUG_LOG("terminating ingest bulk ctx %p", ingest_bulk_ctx);
  delete ingest_bulk_ctx;
}

bool SessionController::CheckOperationPermission(int32_t session_id, CSdkOperationId op_id,
                                                 capi_completion_cb cb, void *cb_args) {
  auto *session = FindSession(session_id, cb, cb_args);
  if (session == nullptr) {
    return false;
  }
  return IsOperationAllowed(session, op_id, cb, cb_args);
}

void SessionController::CompleteLogging(status_code_t status_code,
                                        completion_data_t *completion_data,
                                        payload_t response_data, void *cb_args) {
  // we can't do much for any response. just release resources
  auto *payload_string = reinterpret_cast<std::string *>(cb_args);
  if (status_code != 0) {
    auto status = Statuses::FromStatusCode(status_code);
    std::cerr << "Logger Error: " << Statuses::Name(status);
    ObjectMapper mapper;
    auto response = mapper.ReadValue<ErrorResponse>(response_data);
    if (response) {
      std::cerr << "; " << response->message;
    }
    if (status == OpStatus::NO_SUCH_STREAM) {
      std::cerr << "; Couldn't find signal to put client metrics into";
    }
    std::cerr << std::endl;
  }
  delete payload_string;
  Utils::ReleasePayload(&response_data);
}

void SessionController::FlushClientMetricsTask(void *cb_args) {
  auto session_controller = reinterpret_cast<SessionController *>(cb_args);
  session_controller->FlushClientMetrics();
  session_controller->message_handler_->ScheduleTask(FlushClientMetricsTask,
                                                     FlushClientMetricsFinalTask, cb_args, 1000);
}

void SessionController::FlushClientMetricsFinalTask(void *cb_args) {
  auto session_controller = reinterpret_cast<SessionController *>(cb_args);
  session_controller->FlushClientMetrics();
}

void SessionController::FlushClientMetrics(void) {
  std::shared_lock lock(sessions_mutex_);
  for (auto it = sessions_.begin(); it != sessions_.end(); it++) {
    auto *session = it->second;
    if (session == nullptr) {
      continue;
    }

    auto *str = session->logger->Flush();
    if (str != nullptr) {
      if (session->bios) {
        payload_t payload = {
            .data = reinterpret_cast<uint8_t *>(const_cast<char *>(str->c_str())),
            .length = static_cast<int64_t>(str->size()),
        };
        SimpleMethod(session->session_id, CSDK_OP_INSERT_BULK_PROTO, nullptr, nullptr, payload,
                     nullptr, nullptr, CompleteLogging, str, true);
      } else {
        delete str;
      }
    }
  }
}

bool SessionController::IsOperationAllowed(Session *session, CSdkOperationId op_id,
                                           capi_completion_cb api_cb, void *api_cb_args) {
  assert(session != nullptr);
  if (!session->signed_in) {
    if (api_cb != nullptr) {
      OpStatus status = OpStatus::UNAUTHORIZED;
      std::stringstream ss;
      ss << Statuses::Description(status) << ": Login first";
      Utils::ReplyError(status, ss.str(), api_cb, api_cb_args);
    }
    return false;
  } else if (session->is_expired) {
    if (api_cb != nullptr) {
      std::string message = Statuses::Description(OpStatus::SESSION_EXPIRED);
      Utils::ReplyError(session->bios ? OpStatus::SESSION_EXPIRED : OpStatus::UNAUTHORIZED, message,
                        api_cb, api_cb_args);
    }
    DEBUG_LOG("FindSession() -- session is expired");
    return false;
  }
  return true;
}

bool SessionController::AddSession(int32_t session_id, Session *session) {
  std::unique_lock lock(sessions_mutex_);
  if (sessions_.find(session_id) != sessions_.end()) {
    return false;
  }
  sessions_[session_id] = session;
  return true;
}

bool SessionController::RemoveSession(int32_t session_id) {
  std::unique_lock lock(sessions_mutex_);
  return sessions_.erase(session_id) > 0;
}

Session *SessionController::FindSession(int32_t session_id, capi_completion_cb api_cb,
                                        void *api_cb_args) {
  std::shared_lock lock(sessions_mutex_);
  auto it = sessions_.find(session_id);
  auto session = it != sessions_.end() ? it->second : nullptr;
  if (session == nullptr) {
    if (api_cb != nullptr) {
      std::stringstream ss;
      ss << "Session not found; session_id=" << session_id;
      Utils::ReplyError(OpStatus::SESSION_INACTIVE, ss.str(), api_cb, api_cb_args);
    }
    DEBUG_LOG("FindSession() -- session not found; session_id=%d", session_id);
  } else {
    DEBUG_LOG("FindSession() -- found session; session_id=%d", session_id);
  }
  return session;
}

int32_t SessionController::GetNextSessionId() {
  int32_t next_id;
  do {
    next_id = next_session_id_.load();
  } while (!next_session_id_.compare_exchange_weak(next_id, next_id + 1));
  return next_id;
}

void SessionController::SendRequest(CSdkOperationId op_id, const std::string *tenant_name,
                                    const std::string *stream_name, const payload_t &payload,
                                    Session *session, const std::string *endpoint,
                                    payload_t *metric_timestamps, response_cb completion_cb,
                                    MethodCallState *state,
                                    std::map<std::string, std::string> *options,
                                    std::map<std::string, std::string> *path_params,
                                    bool is_metrics_logging) {
  RequestContext *context = new RequestContext(session, state->timeout, endpoint, completion_cb,
                                               state, metric_timestamps, is_metrics_logging);
  context->resources.tenant = tenant_name != nullptr ? *tenant_name : session->tenant;
  if (stream_name != nullptr) {
    context->resources.stream =  *stream_name;
  }
  if (options != nullptr) {
    context->resources.options = *options;
  }
  if (path_params != nullptr) {
    context->resources.path_params = *path_params;
  }

  request_message_t request = {
      .op_id_ = op_id,
      .payload_ = payload,
      .context_ = context,
  };

  auto *request_message = new RequestMessage(request);

  state->request_message = request_message;
  message_handler_->PlaceRequest(request_message);
}

void SessionController::CleanupRequestMessage(MethodCallState *state) {
  if (state->request_message) {
    delete state->request_message->context();
  }
  delete state->request_message;
  state->request_message = nullptr;
}

Session *SessionController::GetSession(int32_t session_id) {
  std::shared_lock lock(sessions_mutex_);
  auto it = sessions_.find(session_id);
  return it != sessions_.end() ? it->second : nullptr;
}

bool SessionController::CheckSessionValidity(int32_t session_id) const {
  std::shared_lock lock(sessions_mutex_);
  auto it = invalidated_sessions_.find(session_id);
  return it == invalidated_sessions_.end();
}

}  // namespace tfos::csdk
