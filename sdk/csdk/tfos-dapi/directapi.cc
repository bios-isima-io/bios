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
#include "tfoscsdk/directapi.h"

#include <iostream>
#include <string>

#include "tfos-common/logger.h"
#include "tfos-common/object_mapper.h"
#include "tfos-common/session.h"
#include "tfos-common/utils.h"
#include "tfos-dapi/dapi_modules.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/models.h"
#include "tfoscsdk/status.h"

using tfos::csdk::DirectApiModules;
using tfos::csdk::ErrorResponse;
using tfos::csdk::ObjectMapper;
using tfos::csdk::OpStatus;
using tfos::csdk::SessionController;
using tfos::csdk::Statuses;
using tfos::csdk::Utils;

capi_t DaInitialize(const char *modules_profile) {
  std::string name;
  if (modules_profile != nullptr) {
    name = modules_profile;
  }
  return DirectApiModules::Create(name);
}

void DaTerminate(capi_t capi) { DirectApiModules::Terminate(capi); }

void DaStartSession(capi_t capi, string_t host_api, int32_t port, bool ssl_enabled,
                    string_t journal_dir_api, string_t cert_file_api, int64_t op_timeout_millis,
                    capi_completion_cb cb, void *cb_args) {
  DEBUG_LOG("DaStartSession() -- start");
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  std::string host(host_api.text, host_api.length);
  std::string journal_dir(journal_dir_api.text, journal_dir_api.length);
  std::string cert_file(cert_file_api.text, cert_file_api.length);
  modules->session_controller()->StartSession(host, port, ssl_enabled, journal_dir, cert_file, cb,
                                              cb_args, op_timeout_millis);
}

bool DaSetOpTimeoutMillis(capi_t capi, int32_t session_id, int64_t op_timeout_millis) {
  auto *modules = DirectApiModules::Get(capi, nullptr, nullptr);
  if (modules == nullptr) {
    return false;
  }
  return modules->session_controller()->SetOpTimeout(session_id, op_timeout_millis);
}

void DaCloseSession(capi_t capi, int32_t session_id) {
  auto *modules = DirectApiModules::Get(capi, nullptr, nullptr);
  if (modules == nullptr) {
    return;
  }
  modules->session_controller()->CloseSession(session_id);
}

void DaLoginBIOS(capi_t capi, int32_t session_id, payload_t payload, int disable_routing,
                 capi_completion_cb cb, void *cb_args) {
  DEBUG_LOG("DaLoginBIOS() -- start");
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  modules->session_controller()->Login(session_id, payload, disable_routing != 0, cb, cb_args);
}

void DaAddEndpoint(capi_t capi, int32_t session_id, string_t endpoint_url, CSdkNodeType node_type,
                   capi_completion_cb cb, void *cb_args) {
  DEBUG_LOG("DaAddEndpoint() -- start");
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  // copy the strings in the parameter since the contents would be gone after
  // this method.
  std::string endpoint(endpoint_url.text, endpoint_url.length);
  modules->session_controller()->AddEndpoint(session_id, endpoint, node_type, cb, cb_args);
}

void DaSimpleMethod(capi_t capi, int32_t session_id, CSdkOperationId op_id,
                    string_t *tenant_name_api, string_t *stream_name_api, payload_t payload,
                    payload_t *metric_timestamps, string_t *endpoint_api, capi_completion_cb cb,
                    void *cb_args) {
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  std::string tenant_name;
  if (tenant_name_api != nullptr) {
    tenant_name.assign(tenant_name_api->text, tenant_name_api->length);
  }
  std::string stream_name;
  if (stream_name_api != nullptr) {
    stream_name.assign(stream_name_api->text, stream_name_api->length);
  }
  std::string endpoint;
  if (endpoint_api != nullptr) {
    endpoint.assign(endpoint_api->text, endpoint_api->length);
  }
  modules->session_controller()->SimpleMethod(
      session_id, op_id, tenant_name_api != nullptr ? &tenant_name : nullptr,
      stream_name_api != nullptr ? &stream_name : nullptr, payload,
      endpoint_api != nullptr ? &endpoint : nullptr, metric_timestamps, cb, cb_args);
}

void DaGenericMethod(capi_t capi, int32_t session_id, CSdkOperationId op_id, string_t resources[],
                     string_t options[], payload_t payload, int64_t timeout,
                     payload_t *metric_timestamps, capi_completion_cb cb, void *cb_args) {
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  modules->session_controller()->GenericMethod(session_id, op_id, resources, options, payload,
                                               timeout, nullptr, metric_timestamps, cb, cb_args);
}

void DaIngestBulkStart(capi_t capi, int32_t session_id, CSdkOperationId op_id, int num_events,
                       capi_completion_cb api_cb, void (*ask_cb)(int64_t, int, int, void *),
                       void *cb_args) {
  auto *modules = DirectApiModules::Get(capi, api_cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  modules->session_controller()->IngestBulkStart(session_id, op_id, num_events, api_cb, ask_cb,
                                                 cb_args);
}

void DaIngestBulk(capi_t capi, int32_t session_id, int64_t bulk_ingest_ctx, int from_index,
                  int to_index, string_t resources[], string_t options[], payload_t payload,
                  payload_t *metric_timestamps, capi_completion_cb cb, void *cb_args) {
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return;
  }
  modules->session_controller()->IngestBulk(session_id, bulk_ingest_ctx, from_index, to_index,
                                            resources, options, payload, metric_timestamps,
                                            cb, cb_args);
}

void DaIngestBulkEnd(capi_t capi, int32_t session_id, int64_t bulk_ingest_ctx) {
  auto *modules = DirectApiModules::Get(capi);
  if (modules == nullptr) {
    return;
  }
  modules->session_controller()->IngestBulkEnd(session_id, bulk_ingest_ctx);
}

int8_t DaCheckOperationPermission(capi_t capi, int32_t session_id, CSdkOperationId op_id,
                                  capi_completion_cb cb, void *cb_args) {
  auto *modules = DirectApiModules::Get(capi, cb, cb_args);
  if (modules == nullptr) {
    return 0;
  }
  return modules->session_controller()->CheckOperationPermission(session_id, op_id, cb, cb_args);
}

void DaLog(capi_t capi, int32_t session_id, const char *stream, const char *request,
           const char *status, const char *server_endpoint, uint64_t latency_us,
           uint64_t latency_internal_us, uint64_t num_reads, uint64_t num_writes,
           uint64_t num_qos_retry_considered, uint64_t num_qos_retry_sent,
           uint64_t num_qos_retry_response_used) {
  auto *modules = DirectApiModules::Get(capi);
  if (modules == nullptr) {
    return;
  }
  auto *session = modules->session_controller()->GetSession(session_id);
  if (session == nullptr) {
    return;
  }
  session->logger->Log((std::string(status) == "OK"), std::string(stream),
                       Utils::OperationNameToId(request), std::string(server_endpoint), latency_us,
                       latency_internal_us, num_reads, num_writes, num_qos_retry_considered,
                       num_qos_retry_sent, num_qos_retry_response_used);
}
