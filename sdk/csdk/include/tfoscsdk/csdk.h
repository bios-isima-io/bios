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
#ifndef TFOSCSDK_CSDK_H_
#define TFOSCSDK_CSDK_H_

#include <stddef.h>

#ifdef __cplusplus
#include <cstdint>
extern "C" {
#else
#include <stdint.h>
#endif

#include "tfoscsdk/types.h"

enum CSdkOperationId {
  CSDK_OP_NOOP = 0,
  CSDK_OP_GET_VERSION,
  CSDK_OP_START_SESSION,
  CSDK_OP_RENEW_SESSION,
  CSDK_OP_SELECT_CONTEXT_ENTRIES,
  CSDK_OP_LIST_ENDPOINTS,
  CSDK_OP_LIST_CONTEXT_ENDPOINTS,
  CSDK_OP_UPDATE_ENDPOINTS,
  CSDK_OP_GET_PROPERTY,
  CSDK_OP_SET_PROPERTY,
  CSDK_OP_GET_UPSTREAM_CONFIG,
  CSDK_OP_SELECT_PROTO,
  CSDK_OP_LOGIN_BIOS,
  CSDK_OP_RESET_PASSWORD,
  CSDK_OP_GET_SIGNALS,
  CSDK_OP_CREATE_SIGNAL,
  CSDK_OP_UPDATE_SIGNAL,
  CSDK_OP_DELETE_SIGNAL,
  CSDK_OP_GET_CONTEXTS,
  CSDK_OP_GET_CONTEXT,
  CSDK_OP_CREATE_CONTEXT,
  CSDK_OP_UPDATE_CONTEXT,
  CSDK_OP_DELETE_CONTEXT,
  CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS,
  CSDK_OP_GET_CONTEXT_ENTRIES_BIOS,
  CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS,
  CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS,
  CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS,
  CSDK_OP_GET_TENANTS_BIOS,
  CSDK_OP_GET_TENANT_BIOS,
  CSDK_OP_CREATE_TENANT_BIOS,
  CSDK_OP_UPDATE_TENANT_BIOS,
  CSDK_OP_DELETE_TENANT_BIOS,
  CSDK_OP_LIST_APP_TENANTS,
  CSDK_OP_INSERT_PROTO,
  CSDK_OP_INSERT_BULK_PROTO,
  CSDK_OP_REPLACE_CONTEXT_ENTRY_BIOS,
  CSDK_OP_CREATE_EXPORT_DESTINATION,
  CSDK_OP_GET_EXPORT_DESTINATION,
  CSDK_OP_UPDATE_EXPORT_DESTINATION,
  CSDK_OP_DELETE_EXPORT_DESTINATION,
  CSDK_OP_DATA_EXPORT_START,
  CSDK_OP_DATA_EXPORT_STOP,
  CSDK_OP_CREATE_TENANT_APPENDIX,
  CSDK_OP_GET_TENANT_APPENDIX,
  CSDK_OP_UPDATE_TENANT_APPENDIX,
  CSDK_OP_DELETE_TENANT_APPENDIX,
  CSDK_OP_DISCOVER_IMPORT_SUBJECTS,
  CSDK_OP_CREATE_USER,
  CSDK_OP_GET_USERS,
  CSDK_OP_MODIFY_USER,
  CSDK_OP_DELETE_USER,
  CSDK_OP_REGISTER_APPS_SERVICE,
  CSDK_OP_GET_APPS_INFO,
  CSDK_OP_DEREGISTER_APPS_SERVICE,
  CSDK_OP_MAINTAIN_KEYSPACES,
  CSDK_OP_MAINTAIN_TABLES,
  CSDK_OP_MAINTAIN_CONTEXT,
  CSDK_OP_GET_REPORT_CONFIGS,
  CSDK_OP_PUT_REPORT_CONFIG,
  CSDK_OP_DELETE_REPORT,
  CSDK_OP_GET_INSIGHT_CONFIGS,
  CSDK_OP_PUT_INSIGHT_CONFIGS,
  CSDK_OP_DELETE_INSIGHT_CONFIGS,
  CSDK_OP_FEATURE_STATUS,
  CSDK_OP_FEATURE_REFRESH,
  CSDK_OP_GET_ALL_CONTEXT_SYNOPSES,
  CSDK_OP_GET_CONTEXT_SYNOPSIS,
  CSDK_OP_REGISTER_FOR_SERVICE,
  CSDK_OP_STORES_QUERY,
  CSDK_OP_TEST,
  CSDK_OP_TEST_BIOS,
  CSDK_OP_TEST_LOG,
  CSDK_OP_END
};

static inline bool IsOperationProto(CSdkOperationId op_id) {
  return op_id == CSDK_OP_SELECT_PROTO || op_id == CSDK_OP_INSERT_PROTO ||
          op_id == CSDK_OP_INSERT_BULK_PROTO;
}

static inline bool IsOperationBulk(CSdkOperationId op_id) {
  return op_id == CSDK_OP_INSERT_BULK_PROTO;
}

enum CSdkNodeType {
  CSDK_NODE_TYPE_SIGNAL = 1,
  CSDK_NODE_TYPE_ROLLUP,
  CSDK_NODE_TYPE_ANALYSIS,
};

// Environment variable names
#define BIOS_CONNECTION_TYPE "BIOS_CONNECTION_TYPE"

payload_t CsdkAllocatePayload(int capacity);

/**
 * Releases allocated memory for the specified payload.
 *
 * The method clears the pointer to data after freeing the memory.
 *
 * Use the method carefully since wrongful usage of this method would cause
 * immediate crash.
 *
 * TODO(TFOS-1730): We may want to abstract the pointer to the payload to avoid
 * crash, though it would take some cost to keep track of allocated memory.
 */
void CsdkReleasePayload(payload_t *payload);

// Methods for testing ////////////////////////////////
string_t CsdkGetOperationName(CSdkOperationId id);

payload_t CsdkListStatusCodes();

string_t CsdkGetStatusName(status_code_t code);

int CsdkWriteHello(payload_t *payload);

#ifdef __cplusplus
}  // extern "C"
#endif
#endif  // TFOSCSDK_CSDK_H_
