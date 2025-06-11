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
#ifndef TFOSCSDK_DIRECTAPI_H_
#define TFOSCSDK_DIRECTAPI_H_

#include <sys/types.h>

#include "tfoscsdk/csdk.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initializes the DirectAPI C-SDK modules. Call this method once on startup.
 *
 * @params modules_profile "" for default, "mock" for testing without going to
 * the server.
 * @return Generated modules object.
 */
capi_t DaInitialize(const char *modules_profile);

void DaTerminate(capi_t capi);

void DaStartSession(capi_t capi, string_t host, int32_t port, bool ssl_enabled,
                    string_t journal_dir, string_t cert_file, int64_t op_timeout_millis,
                    capi_completion_cb cb, void *cb_args);

bool DaSetOpTimeoutMillis(capi_t capi, int32_t session_id, int64_t op_timeout_millis);

void DaCloseSession(capi_t, int32_t session_id);

/**
 * Method to execute bios login operation.
 *
 * @param capi C-API modules handle
 * @param session_id Session ID
 * @param payload Operatin parameters payload
 * @param disable_routing Flag to disable dynamic routing; Specify 1 to disable,
 * 0 otherwise.
 * @param cb Login completion calback
 * @param cb_args Login completion callback arguments
 */
void DaLoginBIOS(capi_t capi, int32_t session_id, payload_t payload, int32_t disable_routing,
                 capi_completion_cb cb, void *cb_args);

/**
 * Method to add an endpoint.
 *
 * @param capi C-API modules handle
 * @param session_id Session ID
 * @param endpoint_url Endpoint URL to register
 * @param node_type Node type of the new endpoint
 * @param cb Operation completion callback
 * @param cb_args Arguments for the operation completion callback
 */
void DaAddEndpoint(capi_t capi, int32_t session_id, string_t endpoint_url, CSdkNodeType node_type,
                   capi_completion_cb cb, void *cb_args);

/**
 * Executes a simple method asynchronously.
 *
 * @param capi C-API modules handle
 * @param session_id Session ID
 * @param op_id Operation ID
 * @param tenant_name Target tenant name. Put NULL to unspecify
 * @param stream_name Target stream name. Put NULL to unspecify
 * @param payload Data payload
 * @param metric_timestamps Byte buffer to keep timestamps of steps in the
 * operation. Used for performance instrumentation. Put NULL to unspecify
 * @param endpoint The endpoint to send the request to. SDK does not do load
 * balancing when this parameter is specified. Put NULL to unspecify
 * @param cb Operation completion callback
 * @param cb_args Arguments for the operation completion callback
 */
void DaSimpleMethod(capi_t capi, int32_t session_id, CSdkOperationId op_id, string_t *tenant_name,
                    string_t *stream_name, payload_t payload, payload_t *metric_timestamps,
                    string_t *endpoint, capi_completion_cb cb, void *cb_args);

/**
 * Generic method that can issue any operation.
 *
 * @param resources Pairs of resource name and values. The list member must have
 * even number of elements that consist of { key, value }.
 * @param options Pairs of call options. The list member must have even number
 * of elements that consist of { key, value }.
 * @param payload Request payload
 * @param timeout Operation timeout milliseconds, no timeout if 0 is specified,
 * use the session default if -1 is specified
 */
void DaGenericMethod(capi_t capi, int32_t session_id, CSdkOperationId op_id, string_t resources[],
                     string_t options[], payload_t payload, int64_t timeout,
                     payload_t *metric_timestamps, capi_completion_cb cb, void *cb_args);

void DaIngestBulkStart(capi_t capi, int32_t session_id, CSdkOperationId op_id, int num_events,
                       capi_completion_cb api_cb, void (*ask_cb)(int64_t, int, int, void *),
                       void *cb_args);

void DaIngestBulk(capi_t capi, int32_t session_id, int64_t bulk_ingest_ctx, int from_index,
                  int to_index, string_t resources[], string_t options[], payload_t payload,
                  payload_t *metric_timestamps, capi_completion_cb cb, void *cb_args);

void DaIngestBulkEnd(capi_t capi, int32_t session_id, int64_t bulk_ingest_ctx);

/**
 * Method that checks whether the specified session has permission to run an
 * operation.
 *
 * The method executes the call back function in the same thread only when the
 * specified operation is not permitted.
 *
 * @param capi CAPI modules handle
 * @param session_id Session ID
 * @param cb Error callback function; Specify NULL if you don't want to call
 * back
 * @param cb_args Error callback application data
 * @return 1 if the operation is permitted, 0 otherwise
 */
int8_t DaCheckOperationPermission(capi_t capi, int32_t session_id, CSdkOperationId op_id,
                                  capi_completion_cb cb, void *cb_args);

void DaLog(capi_t capi, int32_t session_id, const char *stream, const char *request,
           const char *status, const char *server_endpoint, uint64_t latency_us,
           uint64_t latency_internal_us, uint64_t num_reads, uint64_t num_writes,
           uint64_t num_qos_retry_considered, uint64_t num_qos_retry_sent,
           uint64_t num_qos_retry_response_used);

#ifdef __cplusplus
}  // extern "C"
#endif
#endif  // TFOSCSDK_DIRECTAPI_H_
