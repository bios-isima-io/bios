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
#include "tfos-common/utils.h"

#include <assert.h>
#include <string.h>

#include <iomanip>
#include <sstream>
#include <unordered_map>

#include "tfos-common/object_mapper.h"
#include "tfoscsdk/log.h"
#include "tfoscsdk/version.h"

namespace tfos::csdk {

std::string Utils::GetSdkName() {
  return "C-SDK v" BIOS_VERSION;
}

std::string Utils::GetVersion() {
  char *env = getenv("TEST_OVERRIDE_CSDK_VERSION");
  std::string version = env != nullptr ? env : BIOS_VERSION;
  return version;
}

#define REGISTER_STRING(x) \
  { x, sizeof(x) - 1 }

static string_t kOperationNames[] = {
    REGISTER_STRING("NOOP"),
    REGISTER_STRING("GET_VERSION"),
    REGISTER_STRING("START_SESSION"),
    REGISTER_STRING("RENEW_SESSION"),
    REGISTER_STRING("SELECT_CONTEXT_ENTRIES"),
    REGISTER_STRING("LIST_ENDPOINTS"),
    REGISTER_STRING("LIST_CONTEXT_ENDPOINTS"),
    REGISTER_STRING("UPDATE_ENDPOINTS"),
    REGISTER_STRING("GET_PROPERTY"),
    REGISTER_STRING("SET_PROPERTY"),
    REGISTER_STRING("GET_UPSTREAM_CONFIG"),
    REGISTER_STRING("SELECT_PROTO"),
    REGISTER_STRING("LOGIN_BIOS"),
    REGISTER_STRING("RESET_PASSWORD"),
    REGISTER_STRING("GET_SIGNALS"),
    REGISTER_STRING("CREATE_SIGNAL"),
    REGISTER_STRING("UPDATE_SIGNAL"),
    REGISTER_STRING("DELETE_SIGNAL"),
    REGISTER_STRING("GET_CONTEXTS"),
    REGISTER_STRING("GET_CONTEXT"),
    REGISTER_STRING("CREATE_CONTEXT"),
    REGISTER_STRING("UPDATE_CONTEXT"),
    REGISTER_STRING("DELETE_CONTEXT"),
    REGISTER_STRING("MULTI_GET_CONTEXT_ENTRIES_BIOS"),
    REGISTER_STRING("GET_CONTEXT_ENTRIES_BIOS"),
    REGISTER_STRING("CREATE_CONTEXT_ENTRY_BIOS"),
    REGISTER_STRING("UPDATE_CONTEXT_ENTRY_BIOS"),
    REGISTER_STRING("DELETE_CONTEXT_ENTRY_BIOS"),
    REGISTER_STRING("GET_TENANTS_BIOS"),
    REGISTER_STRING("GET_TENANT_BIOS"),
    REGISTER_STRING("CREATE_TENANT_BIOS"),
    REGISTER_STRING("UPDATE_TENANT_BIOS"),
    REGISTER_STRING("DELETE_TENANT_BIOS"),
    REGISTER_STRING("LIST_APP_TENANTS"),
    REGISTER_STRING("INSERT_PROTO"),
    REGISTER_STRING("INSERT_BULK_PROTO"),
    REGISTER_STRING("REPLACE_CONTEXT_ENTRY_BIOS"),
    REGISTER_STRING("CREATE_EXPORT_DESTINATION"),
    REGISTER_STRING("GET_EXPORT_DESTINATION"),
    REGISTER_STRING("UPDATE_EXPORT_DESTINATION"),
    REGISTER_STRING("DELETE_EXPORT_DESTINATION"),
    REGISTER_STRING("DATA_EXPORT_START"),
    REGISTER_STRING("DATA_EXPORT_STOP"),
    REGISTER_STRING("CREATE_TENANT_APPENDIX"),
    REGISTER_STRING("GET_TENANT_APPENDIX"),
    REGISTER_STRING("UPDATE_TENANT_APPENDIX"),
    REGISTER_STRING("DELETE_TENANT_APPENDIX"),
    REGISTER_STRING("DISCOVER_IMPORT_SUBJECTS"),
    REGISTER_STRING("CREATE_USER"),
    REGISTER_STRING("GET_USERS"),
    REGISTER_STRING("MODIFY_USER"),
    REGISTER_STRING("DELETE_USER"),
    REGISTER_STRING("REGISTER_APPS_SERVICE"),
    REGISTER_STRING("GET_APPS_INFO"),
    REGISTER_STRING("DEREGISTER_APPS_SERVICE"),
    REGISTER_STRING("MAINTAIN_KEYSPACES"),
    REGISTER_STRING("MAINTAIN_TABLES"),
    REGISTER_STRING("MAINTAIN_CONTEXT"),
    REGISTER_STRING("GET_REPORT_CONFIGS"),
    REGISTER_STRING("PUT_REPORT_CONFIG"),
    REGISTER_STRING("DELETE_REPORT"),
    REGISTER_STRING("GET_INSIGHT_CONFIGS"),
    REGISTER_STRING("PUT_INSIGHT_CONFIGS"),
    REGISTER_STRING("DELETE_INSIGHT_CONFIGS"),
    REGISTER_STRING("FEATURE_STATUS"),
    REGISTER_STRING("FEATURE_REFRESH"),
    REGISTER_STRING("GET_ALL_CONTEXT_SYNOPSES"),
    REGISTER_STRING("GET_CONTEXT_SYNOPSIS"),
    REGISTER_STRING("REGISTER_FOR_SERVICE"),
    REGISTER_STRING("STORES_QUERY"),
    REGISTER_STRING("TEST"),
    REGISTER_STRING("TEST_BIOS"),
    REGISTER_STRING("TEST_LOG"),
    REGISTER_STRING("END"),
};

static_assert(sizeof(kOperationNames) / sizeof(string_t) == static_cast<int>(CSDK_OP_END) + 1);

static string_t kPermissionNames[] = {
    REGISTER_STRING("EXTRACT"),    REGISTER_STRING("INGEST"),  REGISTER_STRING("INGEST_EXTRACT"),
    REGISTER_STRING("SUPERADMIN"), REGISTER_STRING("ADMIN"),   REGISTER_STRING("BI_REPORT"),
    REGISTER_STRING("DEVELOPER"),  REGISTER_STRING("UNKNOWN"),
};

static_assert(sizeof(kPermissionNames) / sizeof(string_t) ==
              static_cast<int>(Permission::UNKNOWN) + 1);

enum class ActualNameIndex {
  LOGIN,
  INSERT,
  INSERT_BULK,
  SELECT,
  PUT_CONTEXT_ENTRIES,
  MULTI_GET_CONTEXT_ENTRIES,
  GET_CONTEXT_ENTRIES,
  UPDATE_CONTEXT_ENTRY,
  DELETE_CONTEXT_ENTRIES,
  GET_TENANTS,
  GET_TENANT,
  CREATE_TENANT,
  UPDATE_TENANT,
  DELETE_TENANT,
  OUT_OF_RANGE,
};

static constexpr int Index(ActualNameIndex idx) { return static_cast<int>(idx); }

static string_t kActualOperationNames[] = {
    REGISTER_STRING("LOGIN"),         REGISTER_STRING("INSERT"),
    REGISTER_STRING("INSERT_BULK"),   REGISTER_STRING("SELECT"),
    REGISTER_STRING("UPSERT"),        REGISTER_STRING("MULTI_GET_CONTEXT"),
    REGISTER_STRING("SELECT_CONTEXT"),
    REGISTER_STRING("UPDATE"),        REGISTER_STRING("DELETE"),
    REGISTER_STRING("GET_TENANTS"),   REGISTER_STRING("GET_TENANT"),
    REGISTER_STRING("CREATE_TENANT"), REGISTER_STRING("UPDATE_TENANT"),
    REGISTER_STRING("DELETE_TENANT"),
};

static_assert(sizeof(kActualOperationNames) / sizeof(string_t) ==
              Index(ActualNameIndex::OUT_OF_RANGE));

static OperationType kOperationTypeFromOpId[] = {
    OperationType::ADMIN_READ,     //  CSDK_OP_NOOP,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_VERSION,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_START_SESSION,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_RENEW_SESSION,
    OperationType::ADMIN_READ,     //  CSDK_OP_SELECT_CONTEXT_ENTRIES,
    OperationType::ADMIN_READ,     //  CSDK_OP_LIST_ENDPOINTS,
    OperationType::ADMIN_READ,     //  CSDK_OP_LIST_CONTEXT_ENDPOINTS,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_UPDATE_ENDPOINTS,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_PROPERTY,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_SET_PROPERTY,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_UPSTREAM_CONFIG,
    OperationType::EXTRACT,        //  CSDK_OP_SELECT_PROTO,
    OperationType::ADMIN_READ,     //  CSDK_OP_LOGIN_BIOS,
    OperationType::ADMIN_READ,     //  CSDK_OP_RESET_PASSWORD,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_SIGNALS,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_CREATE_SIGNAL,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_UPDATE_SIGNAL,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DELETE_SIGNAL,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_CONTEXTS,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_CONTEXT,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_CREATE_CONTEXT,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_UPDATE_CONTEXT,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DELETE_CONTEXT,
    OperationType::ADMIN_READ,     //  CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_CONTEXT_ENTRIES_BIOS,
    OperationType::CONTEXT_WRITE,  //  CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS,
    OperationType::CONTEXT_WRITE,  //  CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS,
    OperationType::CONTEXT_WRITE,  //  CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_TENANTS_BIOS,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_TENANT_BIOS,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_CREATE_TENANT_BIOS,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_UPDATE_TENANT_BIOS,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DELETE_TENANT_BIOS,
    OperationType::ADMIN_READ,     //  CSDK_OP_LIST_APP_TENANTS,
    OperationType::INGEST,         //  CSDK_OP_INSERT_PROTO,
    OperationType::INGEST,         //  CSDK_OP_INSERT_BULK_PROTO,
    OperationType::CONTEXT_WRITE,  //  CSDK_OP_REPLACE_CONTEXT_ENTRY_BIOS,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_CREATE_EXPORT_DESTINATION,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_GET_EXPORT_DESTINATION,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_UPDATE_EXPORT_DESTINATION,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DELETE_EXPORT_DESTINATION,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DATA_EXPORT_START,
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DATA_EXPORT_STOP,
    OperationType::ADMIN_READ,     //  CSDK_OP_CREATE_TENANT_APPENDIX,
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_TENANT_APPENDIX,
    OperationType::ADMIN_READ,     //  CSDK_OP_UPDATE_TENANT_APPENDIX,
    OperationType::ADMIN_READ,     //  CSDK_OP_DELETE_TENANT_APPENDIX,
    OperationType::ADMIN_READ,     //  CSDK_OP_DISCOVER_IMPORT_SUBJECTS,
    OperationType::ADMIN_READ,     //  CSDK_OP_CREATE_USER
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_USERS
    OperationType::ADMIN_READ,     //  CSDK_OP_MODIFY_USER
    OperationType::ADMIN_READ,     //  CSDK_OP_DELETE_USER
    OperationType::ADMIN_READ,     //  CSDK_OP_REGISTER_APPS_SERVICE
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_APPS_INFO
    OperationType::ADMIN_READ,     //  CSDK_OP_DEREGISTER_APPS_SERVICE
    OperationType::ADMIN_READ,     //  CSDK_OP_MAINTAIN_KEYSPACES
    OperationType::ADMIN_READ,     //  CSDK_OP_MAINTAIN_TABLES
    OperationType::ADMIN_READ,     //  CSDK_OP_MAINTAIN_CONTEXT
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_REPORT_CONFIGS
    OperationType::ADMIN_WRITE,    //  CSDK_OP_PUT_REPORT_CONFIG
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DELETE_REPORT
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_INSIGHT_CONFIGS
    OperationType::ADMIN_WRITE,    //  CSDK_OP_PUT_INSIGHT_CONFIGS
    OperationType::ADMIN_WRITE,    //  CSDK_OP_DELETE_INSIGHT_CONFIGS
    OperationType::ADMIN_READ,     //  CSDK_OP_FEATURE_STATUS
    OperationType::ADMIN_WRITE,    //  CSDK_OP_FEATURE_REFRESH
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_ALL_CONTEXT_SYNOPSES
    OperationType::ADMIN_READ,     //  CSDK_OP_GET_CONTEXT_SYNOPSIS
    OperationType::ADMIN_READ,     //  CSDK_OP_REGISTER_FOR_SERVICE
    OperationType::ADMIN_READ,     //  CSDK_OP_STORES_QUERY
    OperationType::ADMIN_READ,     //  CSDK_OP_TEST
    OperationType::ADMIN_READ,     //  CSDK_OP_TEST_BIOS
    OperationType::ADMIN_READ,     //  CSDK_OP_TEST_LOG
    OperationType::ADMIN_READ,     //  CSDK_OP_END
};

static_assert(static_cast<size_t>(CSdkOperationId::CSDK_OP_END + 1 ==
                                  sizeof(kOperationTypeFromOpId) /
                                      sizeof(kOperationTypeFromOpId[0])));

string_t Utils::GetOperationName(CSdkOperationId id) {
  if (id < CSDK_OP_NOOP || id >= CSDK_OP_END) {
    id = CSDK_OP_END;
  }
  return kOperationNames[id];
}

static std::unordered_map<std::string, CSdkOperationId> *name_to_id = nullptr;

static std::unordered_map<std::string, CSdkOperationId> *InitNameToId() {
  auto *map = new std::unordered_map<std::string, CSdkOperationId>();
  for (int id = CSDK_OP_NOOP; id <= CSDK_OP_END; ++id) {
    (*map)[std::string(kOperationNames[id].text)] = static_cast<CSdkOperationId>(id);
  }
  return map;
}

CSdkOperationId Utils::OperationNameToId(const std::string &name) {
  if (name_to_id == nullptr) {
    // This method is meant to be called by Logger which runs in a single thread, so we don't use
    // mutext lock here
    name_to_id = InitNameToId();
  }
  auto it = name_to_id->find(name);
  return it != name_to_id->end() ? it->second : CSDK_OP_END;
}

const char *Utils::GetActualOperationName(CSdkOperationId id) {
  if (id < CSDK_OP_NOOP || id >= CSDK_OP_END) {
    id = CSDK_OP_END;
  }
  switch (id) {
    case CSDK_OP_LOGIN_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::LOGIN)].text;
    case CSDK_OP_INSERT_PROTO:
      return kActualOperationNames[Index(ActualNameIndex::INSERT)].text;
    case CSDK_OP_INSERT_BULK_PROTO:
      return kActualOperationNames[Index(ActualNameIndex::INSERT_BULK)].text;
    case CSDK_OP_SELECT_PROTO:
      return kActualOperationNames[Index(ActualNameIndex::SELECT)].text;
    case CSDK_OP_CREATE_CONTEXT_ENTRY_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::PUT_CONTEXT_ENTRIES)].text;
    case CSDK_OP_MULTI_GET_CONTEXT_ENTRIES_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::MULTI_GET_CONTEXT_ENTRIES)].text;
    case CSDK_OP_GET_CONTEXT_ENTRIES_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::GET_CONTEXT_ENTRIES)].text;
    case CSDK_OP_UPDATE_CONTEXT_ENTRY_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::UPDATE_CONTEXT_ENTRY)].text;
    case CSDK_OP_DELETE_CONTEXT_ENTRY_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::DELETE_CONTEXT_ENTRIES)].text;
    case CSDK_OP_GET_TENANTS_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::GET_TENANTS)].text;
    case CSDK_OP_GET_TENANT_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::GET_TENANT)].text;
    case CSDK_OP_CREATE_TENANT_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::CREATE_TENANT)].text;
    case CSDK_OP_UPDATE_TENANT_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::UPDATE_TENANT)].text;
    case CSDK_OP_DELETE_TENANT_BIOS:
      return kActualOperationNames[Index(ActualNameIndex::DELETE_TENANT)].text;
    default:
      return kOperationNames[id].text;
  }
}

OperationType Utils::GetOperationType(CSdkOperationId id) {
  assert(id <= CSDK_OP_END);
  assert(id >= CSDK_OP_NOOP);
  return kOperationTypeFromOpId[id];
}

std::string Utils::GetOperationInfo(const RequestMessage *request_message) {
  assert(request_message != nullptr);

  std::stringstream ss;
  ss << GetOperationName(request_message->op_id()).text;
  auto context = request_message->context();
  if (context) {
    if (!context->resources.tenant.empty()) {
      ss << " tenant=" << context->resources.tenant;
    }
    if (!context->resources.stream.empty()) {
      ss << " stream=" << context->resources.stream;
    }
    for (auto it : context->resources.options) {
      ss << " " << it.first << "=" << it.second;
    }
  }
  return ss.str();
}

bool Utils::IsRetriableFailure(OpStatus status) {
#ifdef DEBUG_LOG_ENABLED
  if (status != OpStatus::OK) {
    auto status_name = Statuses::Name(status);
    DEBUG_LOG("status=%s", status_name.c_str());
  }
#endif
  switch (status) {
    case OpStatus::SERVER_CONNECTION_FAILURE:
    case OpStatus::SERVER_CHANNEL_ERROR:
    case OpStatus::BAD_GATEWAY:
    case OpStatus::SERVICE_UNAVAILABLE:
    case OpStatus::SERVICE_UNDEPLOYED:
    case OpStatus::GENERIC_SERVER_ERROR:
      return true;
    default:
      return false;
  }
}

Permission Utils::ResolvePermission(string_t src) {
  int idx;
  for (idx = 0; idx < static_cast<int>(Permission::UNKNOWN); ++idx) {
    const string_t &entry = kPermissionNames[idx];
    if (src.length == entry.length && strncasecmp(src.text, entry.text, entry.length) == 0) {
      break;
    }
  }
  return static_cast<Permission>(idx);
}

payload_t Utils::MakePayload(const char *text) {
  int64_t len = strlen(text);
  return {reinterpret_cast<uint8_t *>(const_cast<char *>(text)), len};
}

string_t Utils::ToStringType(const std::string &src) {
  return {
      .text = src.c_str(),
      .length = static_cast<int32_t>(src.size()),
  };
}

payload_t Utils::AllocatePayload(int64_t capacity) {
  return {
      .data = new uint8_t[capacity],
      .length = capacity,
  };
}

void Utils::ReleasePayload(payload_t *payload) {
  assert(payload != nullptr);
  delete[] payload->data;
  payload->data = nullptr;
  payload->length = 0;
}

void Utils::ReleasePayload(const uint8_t *data) { delete[] data; }

/**
 *   Decodes a endpoint to search in the endpoints_ map.
 *   The endpoint format can be
 *   "https://foo.bar:443"
 *   for
 *   "https://foo.bar"
 *   for both it should return
 *   "foo.bar:443:1"
 *   The 443 is the port and the :1 is  that
 *   ssl  is  enabled on the port.
 *
 */

std::string Utils::UrlToEndpoint(std::string endpoint) {
  std::string ret = "";
  std::string port = ":443";
  size_t end = endpoint.find("https://");
  if (end == std::string::npos) {
    return ret;
  }
  auto ep = endpoint.substr(end + 8);
  end = ep.find(":");
  if (end == std::string::npos) {
    return ep + port + ":1";
  } else {
    return ep + ":1";
  }
}

void Utils::ReplyError(OpStatus status, const std::string &error_message, capi_completion_cb cb,
                       void *cb_args) {
  assert(cb != nullptr);
  // This is a little expensive but it's fince since the method won't be used
  // often.
  ErrorResponse response(error_message);
  ObjectMapper object_mapper;
  payload_t payload = object_mapper.WriteValue(response);
  completion_data_t completion_data = {
    .endpoint = {0},
    .qos_retry_considered = false,
    .qos_retry_sent = false,
    .qos_retry_response_used = false,
  };
  cb(Statuses::StatusCode(status), &completion_data, payload, cb_args);
}

bool Utils::IsProtobufOperation(CSdkOperationId id) {
  switch (id) {
    case CSDK_OP_SELECT_PROTO:
    case CSDK_OP_INSERT_BULK_PROTO:
    case CSDK_OP_INSERT_PROTO:
      return true;
    default:
      return false;
  }
}

std::string Utils::UrlEncode(const std::string &src) {
  // const char *str = src.c_str();
  std::string result;
  result.reserve(/*strlen(str) + 1*/ src.size() + 1);
  // for (char ch = *str; ch != '\0'; ch = *++str) {
  for (char ch : src) {
    switch (ch) {
      case '!':
      case '*':
      case '\'':
      case '(':
      case ')':
      case ';':
      case ':':
      case '@':
      case '&':
      case '=':
      case '+':
      case '$':
      case '/':
      case '?':
      case '%':
      case '#':
      case '\n':
        AppendHexChar(ch, &result);
        break;
      default:
        result.push_back(ch);
        break;
    }
  }
  return result;
}

void Utils::AppendHexChar(char ch, std::string *str) {
  str->push_back('%');
  unsigned char hex1 = ch / 16;
  hex1 += hex1 < 10 ? '0' : 'A' - 10;
  str->push_back(hex1);
  unsigned char hex2 = ch % 16;
  hex2 += hex2 < 10 ? '0' : 'A' - 10;
  str->push_back(hex2);
}

completion_data_t Utils::get_basic_completion_data() {
  completion_data_t completion_data = {
    .endpoint = {0},
    .qos_retry_considered = false,
    .qos_retry_sent = false,
    .qos_retry_response_used = false,
  };
  return completion_data;
}

}  // namespace tfos::csdk
