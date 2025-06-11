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
#include "tfos-physical/impl/http_resource_resolver.h"

#include <assert.h>
#include <string.h>

#include <cstddef>
#include <string>

#include "tfos-common/keywords.h"
#include "tfos-common/utils.h"
#include "tfoscsdk/csdk.h"
#include "tfoscsdk/models.h"

namespace tfos::csdk {

// path parameter keywords
#define KW_TENANT "{tenant}"
#define KW_SIGNAL "{signal}"
#define KW_STREAM "{stream}"
#define KW_ATTRIBUTE "{attribute}"
#define KW_PREPROCESS "{preprocess}"
#define KW_PROPERTY "{property}"
#define KW_TENANT_NAME "{tenantName}"
#define KW_EVENT_ID "{eventId}"
#define KW_STORAGE_NAME "{storageName}"
#define KW_CATEGORY "{category}"
#define KW_ENTRY_ID "{entryId}"
#define KW_IMPORT_SOURCE_ID "{importSourceId}"
#define KW_USER_ID "{userId}"
#define KW_REPORT_ID "{reportId}"
#define KW_INSIGHT_NAME "{insightName}"

// path entity names
#define DELIM "/"
#define BIOS "/bios/v1"
#define AUTH DELIM "auth"
#define SESSION DELIM "session"
#define RENEW DELIM "renew"
#define LOGIN DELIM "login"
#define CHANGE_PASSWORD DELIM "change-password"
#define ADMIN DELIM "admin"
#define TENANTS DELIM "tenants"
#define TENANT DELIM KW_TENANT
#define STREAMS DELIM "streams"
#define STREAM DELIM KW_STREAM
#define ATTRIBUTES DELIM "attributes"
#define ATTRIBUTE DELIM KW_ATTRIBUTE
#define PREPROCESSES DELIM "preprocesses"
#define PREPROCESS DELIM KW_PREPROCESS
#define CONTEXTS DELIM "contexts"
#define CONTEXT DELIM KW_STREAM
#define MAINTAIN_CONTEXT DELIM "context"
#define ENDPOINTS DELIM "endpoints"
#define UPSTREAM DELIM "upstream"
#define FAILURES DELIM "failures"
#define PROPERTIES DELIM "properties"
#define PROPERTY DELIM KW_PROPERTY
#define EVENTS DELIM "events"
#define MULTI_GET DELIM "multi-get"
#define ENTRIES DELIM "entries"
#define BULK DELIM "bulk"
#define SUMMARY DELIM "summary"
#define USERS DELIM "users"
#define TEST DELIM "test" DELIM "delay"
#define TEST_BIOS DELIM "test" DELIM "jsontype"
#define TEST_LOG DELIM "test" DELIM "log"
#define SELECT DELIM "select"
#define SIGNALS DELIM "signals"
#define SIGNAL DELIM KW_SIGNAL
#define DELETE DELIM "delete"
#define REPLACE DELIM "replace"
#define FETCH DELIM "fetch"
#define TENANT_NAME DELIM KW_TENANT_NAME
#define EVENT_ID DELIM KW_EVENT_ID
#define EXPORTS DELIM "exports"
#define STORAGE_NAME DELIM KW_STORAGE_NAME
#define APPENDIXES DELIM "appendixes"
#define APPENDIX DELIM KW_CATEGORY
#define APPENDIX_ENTRIES DELIM "entries"
#define APPENDIX_ENTRY DELIM KW_ENTRY_ID
#define IMPORT_SOURCES DELIM "importSources"
#define IMPORT_SOURCE_ID DELIM KW_IMPORT_SOURCE_ID
#define SUBJECTS DELIM "subjects"
#define USERS DELIM "users"
#define START DELIM "start"
#define STOP DELIM "stop"
#define APPS DELIM "apps"
#define USER_ID DELIM KW_USER_ID
#define KEYSPACES DELIM "keyspaces"
#define TABLES DELIM "tables"
#define REPORTS DELIM "reports"
#define REPORT_ID DELIM KW_REPORT_ID
#define INSIGHTS DELIM "insights"
#define INSIGHT_NAME DELIM KW_INSIGHT_NAME
#define FEATURE_STATUS DELIM "feature" DELIM "status"
#define FEATURE_REFRESH DELIM "feature" DELIM "refresh"
#define SYNOPSES DELIM "synopses"
#define SERVICES DELIM "services"
#define REGISTER DELIM "register"
#define STORES DELIM "stores"
#define QUERY DELIM "query"
#define APP_TENANTS DELIM "app-tenants"

const char *kMethGet = "GET";
const char *kMethPut = "PUT";
const char *kMethPost = "POST";
const char *kMethFetch = "FETCH";
const char *kMethErase = "ERASE";
const char *kMethUpdate = "UPDATE";
const char *kMethModAttrs = "FILTERUPDATE";
const char *kMethDelete = "DELETE";
const char *kMethPatch = "PATCH";

static const struct operation_info {
  const char *method;
  const char *url;
  const char *path_params;
} kOpInfo[] = {
    {kMethGet, BIOS, nullptr},
    {kMethGet, BIOS "/version", nullptr},                                  // GET_VERSION
    {kMethGet, BIOS, nullptr},                                             // START_SESSION
    {kMethGet, BIOS AUTH SESSION RENEW, nullptr},                          // RENEW_SESSION
    {kMethPost, BIOS TENANTS TENANT CONTEXTS CONTEXT ENTRIES SELECT,
     nullptr},  // SELECT_CONTEXT_ENTRIES
    {kMethGet, BIOS ADMIN ENDPOINTS, "endpoint_type=all"},             // LIST_ENDPOINTS
    {kMethGet, BIOS ADMIN ENDPOINTS, "endpoint_type=context"},         // LIST_CONTEXT_ENDPOINTS
    {kMethPost, BIOS ADMIN ENDPOINTS, nullptr},                        // UPDATE_ENDPOINTS
    {kMethGet, BIOS ADMIN PROPERTIES PROPERTY, nullptr},               // GET_PROPERTY
    {kMethPut, BIOS ADMIN PROPERTIES PROPERTY, nullptr},               // SET_PROPERTY
    {kMethGet, BIOS ADMIN UPSTREAM, nullptr},                              // GET_UPSTREAM_CONFIG
    {kMethPost, BIOS TENANTS TENANT EVENTS SELECT, nullptr},               // SELECT_PROTO
    {kMethPost, BIOS AUTH LOGIN, nullptr},                                 // LOGIN_BIOS
    {kMethPost, BIOS AUTH CHANGE_PASSWORD, nullptr},                       // RESET_PASSWORD
    {kMethGet, BIOS TENANTS TENANT SIGNALS, nullptr},                      // GET_SIGNALS
    {kMethPost, BIOS TENANTS TENANT SIGNALS, nullptr},                     // CREATE_SIGNAL
    {kMethPost, BIOS TENANTS TENANT SIGNALS STREAM, nullptr},              // UPDATE_SIGNAL
    {kMethDelete, BIOS TENANTS TENANT SIGNALS STREAM, nullptr},            // DELETE_SIGNAL
    {kMethGet, BIOS TENANTS TENANT CONTEXTS, nullptr},                     // GET_CONTEXTS
    {kMethGet, BIOS TENANTS TENANT CONTEXTS, nullptr},                     // GET_CONTEXT
    {kMethPost, BIOS TENANTS TENANT CONTEXTS, nullptr},                    // CREATE_CONTEXT
    {kMethPost, BIOS TENANTS TENANT CONTEXTS CONTEXT, nullptr},            // UPDATE_CONTEXT
    {kMethDelete, BIOS TENANTS TENANT CONTEXTS CONTEXT, nullptr},          // DELETE_CONTEXT
    {kMethPost, BIOS TENANTS TENANT MULTI_GET, nullptr},                // MULTI_GET_CONTEXT_ENTRIES
    {kMethPost, BIOS TENANTS TENANT CONTEXTS CONTEXT ENTRIES FETCH,
     nullptr},                                                            // GET_CONTEXT_ENTRIES
    {kMethPost, BIOS TENANTS TENANT CONTEXTS CONTEXT ENTRIES, nullptr},   // CREATE_CONTEXT_ENTRY
    {kMethPatch, BIOS TENANTS TENANT CONTEXTS CONTEXT ENTRIES, nullptr},  // UPDATE_CONTEXT_ENTRY
    {kMethPost, BIOS TENANTS TENANT CONTEXTS CONTEXT ENTRIES DELETE,
     nullptr},                                         // DELETE_CONTEXT_ENTRY
    {kMethGet, BIOS TENANTS, nullptr},                 // GET_TENANTS
    {kMethGet, BIOS TENANTS TENANT_NAME, nullptr},     // GET_TENANT
    {kMethPost, BIOS TENANTS, nullptr},                // CREATE_TENANT
    {kMethPost, BIOS TENANTS TENANT, nullptr},         // UPDATE_TENANT
    {kMethDelete, BIOS TENANTS TENANT_NAME, nullptr},  // DELETE_TENANT
    {kMethGet, BIOS TENANTS TENANT_NAME APP_TENANTS, nullptr},  // LIST_APP_TENANTS
    {kMethPut, BIOS TENANTS TENANT SIGNALS STREAM EVENTS EVENT_ID, nullptr},  // INSERT_PROTO
    {kMethPost, BIOS TENANTS TENANT EVENTS BULK, nullptr},                    // INSERT_BULK_PROTO
    {kMethPost, BIOS TENANTS TENANT CONTEXTS CONTEXT ENTRIES REPLACE,
     nullptr},                                                         // REPLACE_CONTEXT_ENTRY
    {kMethPost, BIOS TENANTS TENANT EXPORTS, nullptr},                 // CREATE_EXPORT_DESTINATION
    {kMethGet, BIOS TENANTS TENANT EXPORTS STORAGE_NAME, nullptr},     // GET_EXPORT_DESTINATION
    {kMethPut, BIOS TENANTS TENANT EXPORTS STORAGE_NAME, nullptr},     // UPDATE_EXPORT_DESTINATION
    {kMethDelete, BIOS TENANTS TENANT EXPORTS STORAGE_NAME, nullptr},  // DELETE_EXPORT_DESTINATION
    {kMethPost, BIOS TENANTS TENANT EXPORTS STORAGE_NAME START, nullptr},  // EXPORT START
    {kMethPost, BIOS TENANTS TENANT EXPORTS STORAGE_NAME STOP, nullptr},   // EXPORT STOP
    {kMethPost, BIOS TENANTS TENANT APPENDIXES APPENDIX, nullptr},  // CREATE_TENANT_APPENDIX
    {kMethGet, BIOS TENANTS TENANT APPENDIXES APPENDIX APPENDIX_ENTRIES APPENDIX_ENTRY,
     nullptr},  // GET_TENANT_APPENDIX
    {kMethPost, BIOS TENANTS TENANT APPENDIXES APPENDIX APPENDIX_ENTRIES APPENDIX_ENTRY,
     nullptr},  // UPDATE_TENANT_APPENDIX
    {kMethDelete, BIOS TENANTS TENANT APPENDIXES APPENDIX APPENDIX_ENTRIES APPENDIX_ENTRY,
     nullptr},  // DELETE_TENANT_APPENDIX
    {kMethGet, BIOS TENANTS TENANT IMPORT_SOURCES IMPORT_SOURCE_ID SUBJECTS,
     nullptr},                                          // DISCOVER_IMPORT_SUBJECTS
    {kMethPost, BIOS USERS, nullptr},                   // CREATE_USER
    {kMethGet, BIOS USERS, nullptr},                    // GET_USERS
    {kMethPatch, BIOS USERS USER_ID, nullptr},          // MODIFY_USER
    {kMethDelete, BIOS USERS, nullptr},                 // DELETE_USER
    {kMethPost, BIOS ADMIN APPS, nullptr},              // REGISTER_APP_SERVICE
    {kMethGet, BIOS ADMIN APPS TENANT, nullptr},        // GET_APP_INFO
    {kMethDelete, BIOS ADMIN APPS TENANT, nullptr},     // DEREGISTER_APP_SERVICE
    {kMethPost, BIOS ADMIN KEYSPACES, nullptr},         // MAINTAIN_KEYSPACES
    {kMethPost, BIOS ADMIN TABLES, nullptr},            // MAINTAIN_TABLES
    {kMethPost, BIOS ADMIN MAINTAIN_CONTEXT, nullptr},  // MAINTAIN_CONTEXT
    {kMethGet, BIOS TENANTS TENANT REPORTS, nullptr},   // GET_REPORT_CONFIGS
    {kMethPut, BIOS TENANTS TENANT REPORTS REPORT_ID, nullptr},     // PUT_REPORT_CONFIG
    {kMethDelete, BIOS TENANTS TENANT REPORTS REPORT_ID, nullptr},  // DELETE_REPORT
    {kMethGet, BIOS TENANTS TENANT INSIGHTS INSIGHT_NAME, nullptr},     // GET_INSIGHT_CONFIGS
    {kMethPost, BIOS TENANTS TENANT INSIGHTS INSIGHT_NAME, nullptr},    // PUT_INSIGHT_CONFIGS
    {kMethDelete, BIOS TENANTS TENANT INSIGHTS INSIGHT_NAME, nullptr},  // DELETE_INSIGHT_CONFIGS
    {kMethPost, BIOS TENANTS TENANT FEATURE_STATUS, nullptr},         // FEATURE_STATUS
    {kMethPost, BIOS TENANTS TENANT FEATURE_REFRESH, nullptr},        // FEATURE_REFRESH
    {kMethPost, BIOS TENANTS TENANT SYNOPSES CONTEXTS, nullptr},      // GET_ALL_CONTEXT_SYNOPSES
    {kMethPost, BIOS TENANTS TENANT SYNOPSES CONTEXTS CONTEXT, nullptr},  // GET_CONTEXT_SYNOPSIS
    {kMethPost, BIOS SERVICES REGISTER, nullptr},       // REGISTER_FOR_SERVICE
    {kMethPost, BIOS STORES QUERY, nullptr},            // STORES_QUERY
    {kMethPost, BIOS TEST, nullptr},              // TEST
    {kMethPost, BIOS TEST_BIOS, nullptr},               // TEST_BIOS
    {kMethPost, BIOS TEST_LOG, nullptr},               // TEST_LOG
    {"", nullptr, nullptr},                             // END
};

static_assert(sizeof(kOpInfo) / sizeof(kOpInfo[0]) == static_cast<int>(CSDK_OP_END) + 1,
              "size of kOperationUrls must be the same with number of operations");

const std::string HttpResourceResolver::GetMethod(CSdkOperationId id) const {
  if (id < CSDK_OP_NOOP || id >= CSDK_OP_END) {
    id = CSDK_OP_END;
  }
  return kOpInfo[id].method;
}

const std::string HttpResourceResolver::GetContentType(CSdkOperationId id) const {
  if (Utils::IsProtobufOperation(id)) {
    return "application/x-protobuf";
  } else {
    return "application/json";
  }
}

/**
 * Builds URL path.
 */
const std::string HttpResourceResolver::GetResource(CSdkOperationId id,
                                                    const Resources &resources) const {
  if (id < CSDK_OP_NOOP || id >= CSDK_OP_END) {
    id = CSDK_OP_END;
  }
  std::string out;
  out.reserve(120);
  // resolve the URL path template from the operation ID
  const auto &op_info = kOpInfo[id];
  const char *source = op_info.url;

  // scan the template and replace parameters by given resources.
  int left = 0;
  int i;
  for (i = 0; source[i] != '\0'; ++i) {
    char ch = source[i];
    switch (ch) {
      case '{':
        out.append(source + left, i - left);
        left = i;
        break;
      case '}':
        out += Substitute(source + left + 1, i - left - 1, resources);
        left = i + 1;
        break;
    }
  }
  out.append(source + left, i - left);

  // add options if any
  char prefix = '?';
  for (auto it : resources.options) {
    out += prefix;
    out += it.first;
    out += '=';
    out += Utils::UrlEncode(it.second);
    prefix = '&';
  }
  if (op_info.path_params) {
    out += prefix;
    out += op_info.path_params;
  }

  return out;
}

const std::string HttpResourceResolver::Substitute(const char *str, size_t len,
                                                   const Resources &resources) const {
  if (strncasecmp(str, CSDK_KEY_TENANT, len) == 0) {
    return resources.tenant;
  }
  if (strncasecmp(str, CSDK_KEY_STREAM, len) == 0) {
    return resources.stream;
  }
  // find from generic path parameters
  auto it = resources.path_params.find(std::string(str, len));
  if (it != resources.path_params.end()) {
    return Utils::UrlEncode(it->second);
  }
  // no match
  return "";
}

}  // namespace tfos::csdk
