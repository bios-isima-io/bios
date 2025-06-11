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
#include "tfos-common/object_mapper.h"

#include <algorithm>
#include <boost/flyweight.hpp>
#include <iostream>

#include "include/tfoscsdk/models.h"
#include "rapidjson/rapidjson.h"
#include "tfos-common/utils.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

template <typename V>
std::function<void *(const ObjectMapper *, const rapidjson::Value *)> ObjectMapper::vector_reader =
    [](const ObjectMapper *mapper, const rapidjson::Value *value) {
      std::vector<V> *values;
      if (value->IsArray()) {
        auto it = mapper->readers.find(std::type_index(typeid(V)));
        if (it == mapper->readers.end()) {
          values = nullptr;
        } else {
          values = new std::vector<V>();
          for (rapidjson::SizeType i = 0; i < value->Size(); ++i) {
            V *element = reinterpret_cast<V *>(it->second(mapper, &(*value)[i]));
            if (element != nullptr) {
              values->push_back(*element);
            } else {
              values->push_back(V());
            }
            delete element;
          }
        }
      } else {
        values = nullptr;
      }
      return values;
    };

template <typename V>
std::function<void(const ObjectMapper *mapper, const std::vector<V> &, JsonWriter *)>
    ObjectMapper::vector_writer =
        [](const ObjectMapper *mapper, const std::vector<V> &values, JsonWriter *writer) {
          writer->StartArray();
          auto it = mapper->writers.find(std::type_index(typeid(V)));
          if (it != mapper->writers.end()) {
            for (auto itv = values.begin(); itv != values.end(); ++itv) {
              it->second(mapper, &(*itv), writer);
            }
          }
          // TODO(Naoki): Handle else case
          writer->EndArray();
        };

std::function<void(const ObjectMapper *, const AdminOpType &, JsonWriter *)>
    ObjectMapper::admin_op_writer =
        [](const ObjectMapper *, const AdminOpType &admin_op_type, JsonWriter *writer) {
          const char *name = "UNKNOWN";
          if (admin_op_type == AdminOpType::ADD) {
            name = "ADD";
          } else if (admin_op_type == AdminOpType::REMOVE) {
            name = "REMOVE";
          } else if (admin_op_type == AdminOpType::UPDATE) {
            name = "UPDATE";
          } else if (admin_op_type == AdminOpType::UNKNOWN) {
            name = "UNKNOWN";
          }
          writer->String(name);
        };

std::function<void(const ObjectMapper *, const RequestPhase &, JsonWriter *)>
    ObjectMapper::request_phase_writer =
        [](const ObjectMapper *, const RequestPhase &request_phase, JsonWriter *writer) {
          const char *name = "UNKNOWN";
          if (request_phase == RequestPhase::INITIAL) {
            name = "INITIAL";
          } else if (request_phase == RequestPhase::FINAL) {
            name = "FINAL";
          }
          writer->String(name);
        };

std::function<void(const ObjectMapper *, const NodeType &, JsonWriter *)>
    ObjectMapper::node_type_writer =
        [](const ObjectMapper *, const NodeType &node_type, JsonWriter *writer) {
          const char *name;
          switch (node_type) {
            case NodeType::SIGNAL:
              name = "SIGNAL";
              break;
            case NodeType::ROLLUP:
              name = "ROLLUP";
              break;
            case NodeType::ANALYSIS:
              name = "ANALYSIS";
              break;
            default:
              name = "UNKNOWN";
          }
          writer->String(name);
        };

ObjectMapper::ObjectMapper() {
  // Register readers
  // ////////////////////////////////////////////////////////////////////////////

  AddReader<CredentialsBIOS>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    CredentialsBIOS *credentials;
    if (value->IsObject()) {
      credentials = new CredentialsBIOS();
      FetchString(value, "email", &credentials->email);
      FetchString(value, "password", &credentials->password);
      FetchString(value, "appName", &credentials->app_name);
      FetchString(value, "appType", &credentials->app_type);

    } else {
      // TODO(Naoki): We may want to give more detail error info later on,
      // but it is good enough to return nullptr for now since it's unlikely that
      // the JSON from the server is corrupted when it arrives at this method.
      credentials = nullptr;
    }
    return credentials;
  });

  AddReader<Credentials>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    Credentials *credentials;
    if (value->IsObject()) {
      credentials = new Credentials();
      FetchString(value, "username", &credentials->username);
      FetchString(value, "password", &credentials->password);
      FetchString(value, "scope", &credentials->scope);
    } else {
      // TODO(Naoki): We may want to give more detail error info later on,
      // but it is good enough to return nullptr for now since it's unlikely that
      // the JSON from the server is corrupted when it arrives at this method.
      credentials = nullptr;
    }
    return credentials;
  });

  AddReader<StartSessionResponse>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    StartSessionResponse *start_session_response;
    if (value->IsObject()) {
      start_session_response = new StartSessionResponse();
      FetchInt(value, "sessionId", &start_session_response->sessionId);
    } else {
      start_session_response = nullptr;
    }
    return start_session_response;
  });

  AddReader<Permission>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    Permission *permission = new Permission(Permission::UNKNOWN);
    if (value->IsString()) {
      string_t src = {value->GetString(), static_cast<int32_t>(value->GetStringLength())};
      *permission = Utils::ResolvePermission(src);
    }
    return permission;
  });

  AddReader<ErrorResponse>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    ErrorResponse *error_response;
    if (value->IsObject()) {
      error_response = new ErrorResponse();
      FetchString(value, "message", &error_response->message);
      FetchString(value, "errorCode", &error_response->error_code);
    } else {
      error_response = nullptr;
    }
    return error_response;
  });

  AddReader<IngestResponseOrError>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    IngestResponseOrError *response_or_error = nullptr;
    if (value->IsObject()) {
      response_or_error = new IngestResponseOrError();
      FetchString(value, "eventId", &response_or_error->event_id);
      FetchLong(value, "timestamp", &response_or_error->timestamp);
      FetchInt(value, "statusCode", &response_or_error->status_code);
      FetchString(value, "errorMessage", &response_or_error->error_message);
      FetchString(value, "serverErrorCode", &response_or_error->server_error_code);
    }
    return response_or_error;
  });

  AddReader<IngestBulkErrorResponse>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    IngestBulkErrorResponse *error_response = nullptr;
    if (value->IsObject()) {
      error_response = new IngestBulkErrorResponse();
      FetchString(value, "message", &error_response->message);
      FetchString(value, "errorCode", &error_response->error_code);
      // parse property 'resultsWithError'
      auto it = value->FindMember("resultsWithError");
      if (it != value->MemberEnd()) {
        auto mit =
            mapper->readers.find(std::type_index(typeid(std::vector<IngestResponseOrError>)));
        if (mit != mapper->readers.end()) {
          auto *response_or_error = reinterpret_cast<std::vector<IngestResponseOrError> *>(
              mit->second(mapper, &it->value));
          error_response->results_with_error = *response_or_error;
          delete response_or_error;
        }
      }
    }
    return error_response;
  });

  AddReader<LoginResponse>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    LoginResponse *login_response;
    if (value->IsObject()) {
      login_response = new LoginResponse();
      FetchString(value, "token", &login_response->token);
      FetchLong(value, "expiry", &login_response->expiry);
      FetchLong(value, "sessionTimeoutMillis", &login_response->sessionTimeoutMillis);
      // parse property 'permission'
      auto it1 = value->FindMember("permissions");
      if (it1 != value->MemberEnd()) {
        auto mit = mapper->readers.find(std::type_index(typeid(std::vector<Permission>)));
        if (mit != mapper->readers.end()) {
          auto *permissions =
              reinterpret_cast<std::vector<Permission> *>(mit->second(mapper, &it1->value));
          login_response->permissions = *permissions;
          delete permissions;
        }
      }
      // parse property 'sessionAttributes'
      auto it2 = value->FindMember("sessionAttributes");
      if (it2 != value->MemberEnd()) {
        auto mit = mapper->readers.find(std::type_index(typeid(std::vector<SessionAttribute>)));
        if (mit != mapper->readers.end()) {
          auto *session_attributes =
              reinterpret_cast<std::vector<SessionAttribute> *>(mit->second(mapper, &it2->value));
          login_response->session_attributes = *session_attributes;
          delete session_attributes;
        }
      }
    } else {
      login_response = nullptr;
    }
    return login_response;
  });

  AddReader<SessionAttribute>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    SessionAttribute *attribute = nullptr;
    if (value->IsObject()) {
      attribute = new SessionAttribute();
      FetchString(value, "key", &attribute->key);
      FetchString(value, "value", &attribute->value);
    }
    return attribute;
  });

  AddReader<LoginResponseBIOS>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    LoginResponseBIOS *login_response;
    if (value->IsObject()) {
      login_response = new LoginResponseBIOS();
      FetchString(value, "token", &login_response->token);
      FetchString(value, "tenant", &login_response->tenant);
      FetchString(value, "appName", &login_response->app_name);
      FetchString(value, "appType", &login_response->app_type);
      FetchString(value, "devInstance", &login_response->dev_instance);
      FetchString(value, "homePageConfig", &login_response->home_page_config);
      FetchString(value, "upstreams", &login_response->upstreams);
      auto attr_it = value->FindMember("sessionAttributes");
      if (attr_it != value->MemberEnd()) {
        auto mit = mapper->readers.find(std::type_index(typeid(std::vector<SessionAttribute>)));
        if (mit != mapper->readers.end()) {
          auto *session_attributes = reinterpret_cast<std::vector<SessionAttribute> *>(
              mit->second(mapper, &attr_it->value));
          login_response->session_attributes = *session_attributes;
          delete session_attributes;
        }
      }
      FetchLong(value, "expiry", &login_response->expiry);
      // parse property 'permission'
    } else {
      login_response = nullptr;
    }
    return login_response;
  });

  AddReader<IngestResponse>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    IngestResponse *ingest_response;
    if (value->IsObject()) {
      ingest_response = new IngestResponse();
      FetchString(value, "eventId", &ingest_response->eventId);
      FetchLong(value, "ingestTimestamp", &ingest_response->ingestTimestamp);
    } else {
      ingest_response = nullptr;
    }
    return ingest_response;
  });

  AddReader<AdminWriteResponse>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    AdminWriteResponse *response;
    if (value->IsObject()) {
      response = new AdminWriteResponse();
      FetchLong(value, "timestamp", &response->timestamp);
      auto it = value->FindMember("endpoints");
      if (it != value->MemberEnd()) {
        auto mit = mapper->readers.find(std::type_index(typeid(std::vector<std::string>)));
        if (mit != mapper->readers.end()) {
          auto *endpoints =
              reinterpret_cast<std::vector<std::string> *>(mit->second(mapper, &it->value));
          response->endpoints = *endpoints;
          delete endpoints;
        }
      }
    } else {
      response = nullptr;
    }
    return response;
  });

  AddReader<LbPolicy>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    static std::unordered_map<std::string, LbPolicy> const str_to_policy = {
        {"ROUND_ROBIN", LbPolicy::ROUND_ROBIN},
    };
    LbPolicy *policy = new LbPolicy();
    if (value->IsObject()) {
      std::string *op = new std::string();
      FetchString(value, "lbPolicy", op);
      if (auto it = str_to_policy.find(*op); it != str_to_policy.end()) {
        *policy = it->second;
      } else {
        *policy = LbPolicy::ROUND_ROBIN;
      }
    } else {
      *policy = LbPolicy::ROUND_ROBIN;
    }
    return policy;
  });

  AddReader<Operation>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    static std::unordered_map<std::string, OperationPolicy> const str_to_policy = {
        {"ALWAYS", OperationPolicy::ALWAYS},
        {"NEVER", OperationPolicy::NEVER},
        {"UNDERFAILURE", OperationPolicy::UNDERFAILURE},
    };
    static std::unordered_map<std::string, OperationType> const str_to_type = {
        {"INGEST", OperationType::INGEST},
        {"ADMIN_READ", OperationType::ADMIN_READ},
        {"ADMIN_WRITE", OperationType::ADMIN_WRITE},
        {"CONTEXT_WRITE", OperationType::CONTEXT_WRITE},
        {"SUMMARIZE", OperationType::SUMMARIZE},
        {"EXTRACT", OperationType::EXTRACT},
    };
    if (value->IsObject()) {
      Operation *operation = new Operation();
      std::string *op = new std::string();
      FetchString(value, "operationPolicy", op);
      if (auto it = str_to_policy.find(*op); it != str_to_policy.end()) {
        operation->set_operation_policy(it->second);
      } else {
        delete op;
        delete operation;
        return (tfos::csdk::Operation *)nullptr;
      }
      FetchString(value, "operationType", op);
      if (auto it = str_to_type.find(*op); it != str_to_type.end()) {
        operation->set_operation_type(it->second);
      } else {
        delete op;
        delete operation;
        return (tfos::csdk::Operation *)nullptr;
      }
      delete op;
      return operation;
    } else {
      return (tfos::csdk::Operation *)nullptr;
    }
  });

  AddReader<OperationSet>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    if (!value->IsObject()) {
      return (tfos::csdk::OperationSet *)nullptr;
    }
    auto it = value->FindMember("operation");
    if (it == value->MemberEnd()) {
      return (tfos::csdk::OperationSet *)nullptr;
    }
    auto op_value = &it->value;
    if (op_value->IsArray()) {
      std::vector<std::unique_ptr<Operation>> operations;
      auto mit = mapper->readers.find(std::type_index(typeid(Operation)));
      if (mit == mapper->readers.end()) {
        return (tfos::csdk::OperationSet *)nullptr;
      }
      for (rapidjson::SizeType i = 0; i < op_value->Size(); i++) {
        auto element = reinterpret_cast<Operation *>(mit->second(mapper, &(*op_value)[i]));
        if (element == nullptr) {
          return (tfos::csdk::OperationSet *)nullptr;
        }
        operations.emplace_back(element);
      }
      OperationSet *operation_set = new OperationSet();
      operation_set->set_operation(&operations);
      return operation_set;
    } else {
      return (tfos::csdk::OperationSet *)nullptr;
    }
  });

  AddReader<Upstream>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    if (!value->IsObject()) {
      return (tfos::csdk::Upstream *)nullptr;
    }
    auto os_it = value->FindMember("operationSet");
    auto hs_it = value->FindMember("hostSet");
    if (os_it == value->MemberEnd() || hs_it == value->MemberEnd()) {
      return (tfos::csdk::Upstream *)nullptr;
    }
    auto os_mit = mapper->readers.find(std::type_index(typeid(OperationSet)));
    auto hs_mit = mapper->readers.find(std::type_index(typeid(std::vector<std::string>)));
    if (os_mit == mapper->readers.end() || hs_mit == mapper->readers.end()) {
      return (tfos::csdk::Upstream *)nullptr;
    }
    Upstream *upstream = new Upstream();
    auto hs = reinterpret_cast<std::vector<std::string> *>(hs_mit->second(mapper, &hs_it->value));
    auto os = reinterpret_cast<OperationSet *>(os_mit->second(mapper, &os_it->value));
    upstream->set_host_set(*hs);
    upstream->set_operation_set(os);
    delete (hs);
    return upstream;
  });

  AddReader<UpstreamConfig>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    if (!value->IsObject()) {
      return (tfos::csdk::UpstreamConfig *)nullptr;
    }
    auto lb_it = value->FindMember("lbPolicy");
    auto up_it = value->FindMember("upstreams");
    if (lb_it == value->MemberEnd() || up_it == value->MemberEnd()) {
      return (tfos::csdk::UpstreamConfig *)nullptr;
    }
    auto lb_mit = mapper->readers.find(std::type_index(typeid(LbPolicy)));
    if (lb_mit == mapper->readers.end()) {
      return (tfos::csdk::UpstreamConfig *)nullptr;
    }
    UpstreamConfig *upstream_config = new UpstreamConfig();
    auto lb_policy = reinterpret_cast<LbPolicy *>(lb_mit->second(mapper, &lb_it->value));
    upstream_config->set_lb_policy(*lb_policy);
    delete lb_policy;
    auto up_value = &up_it->value;
    if (up_value->IsArray()) {
      std::vector<std::unique_ptr<Upstream>> upstreams;
      auto mit = mapper->readers.find(std::type_index(typeid(Upstream)));
      if (mit == mapper->readers.end()) {
        delete upstream_config;
      }
      for (rapidjson::SizeType i = 0; i < up_value->Size(); i++) {
        auto element = reinterpret_cast<Upstream *>(mit->second(mapper, &(*up_value)[i]));
        if (element == nullptr) {
          delete upstream_config;
          return (tfos::csdk::UpstreamConfig *)nullptr;
          break;
        }
        upstreams.emplace_back(element);
      }
      upstream_config->set_upstream(&upstreams);
      return upstream_config;
    }
    return (tfos::csdk::UpstreamConfig *)nullptr;
  });

  AddReader<std::string>([](const ObjectMapper *mapper, const rapidjson::Value *value) {
    std::string *out;
    if (value->IsString()) {
      out = new std::string(value->GetString(), value->GetStringLength());
    } else {
      out = nullptr;
    }
    return out;
  });

  AddReader<boost::flyweight<std::string>>(
      [](const ObjectMapper *mapper, const rapidjson::Value *value) {
        boost::flyweight<std::string> *out;
        if (value->IsString()) {
          out = new boost::flyweight<std::string>(
              std::string(value->GetString(), value->GetStringLength()));
        } else {
          out = nullptr;
        }
        return out;
      });

  AddReader<std::vector<std::string>>(vector_reader<std::string>);
  AddReader<std::vector<boost::flyweight<std::string>>>(
      vector_reader<boost::flyweight<std::string>>);
  AddReader<std::vector<Permission>>(vector_reader<Permission>);
  AddReader<std::vector<IngestResponseOrError>>(vector_reader<IngestResponseOrError>);
  AddReader<std::vector<SessionAttribute>>(vector_reader<SessionAttribute>);

  // Register writers
  // ////////////////////////////////////////////////////////////////////////////

  AddWriter<StartSessionResponse>([](const StartSessionResponse *start_session_response,
                                     rapidjson::Value *value,
                                     rapidjson::MemoryPoolAllocator<> &allocator) {
    value->SetObject();
    PutInt(value, "sessionId", start_session_response->sessionId, allocator);
  });

  AddWriter<CredentialsBIOS>([](const CredentialsBIOS *credentials, rapidjson::Value *value,
                                rapidjson::MemoryPoolAllocator<> &allocator) {
    value->SetObject();
    PutString(value, "email", credentials->email, allocator);
    PutString(value, "password", credentials->password, allocator);
    if (!credentials->app_name.empty()) {
      PutString(value, "appName", credentials->app_name, allocator);
    }
    if (!credentials->app_type.empty()) {
      PutString(value, "appType", credentials->app_type, allocator);
    }
  });

  AddWriter<LoginResponseBIOS>([](const LoginResponseBIOS *login_response, rapidjson::Value *value,
                                  rapidjson::MemoryPoolAllocator<> &allocator) {
    /*
     * I have taken a shortcut here and excluded upstreams and token
     * while writing LoginResponse, since we do not want to return
     * token and upstreams to other language layers.
     * TODO(Manish) Fix this by excluding the fileds in a new LoginResponse object
     */
    value->SetObject();
    PutString(value, "tenant", login_response->tenant, allocator);
    if (!login_response->app_name.empty()) {
      PutString(value, "appName", login_response->app_name, allocator);
    }
    if (!login_response->app_type.empty()) {
      PutString(value, "appType", login_response->app_type, allocator);
    }
    if (!login_response->dev_instance.empty()) {
      PutString(value, "devInstance", login_response->dev_instance, allocator);
    }
    if (!login_response->home_page_config.empty()) {
      PutString(value, "homePageConfig", login_response->home_page_config, allocator);
    }
  });

  AddWriter<Credentials>([](const Credentials *credentials, rapidjson::Value *value,
                            rapidjson::MemoryPoolAllocator<> &allocator) {
    value->SetObject();
    PutString(value, "username", credentials->username, allocator);
    PutString(value, "password", credentials->password, allocator);
    PutString(value, "scope", credentials->scope, allocator);
  });

  AddWriter<ErrorResponse>(
      [](const ObjectMapper *, const ErrorResponse &error_response, JsonWriter *writer) {
        writer->StartObject();

        if (!error_response.message.empty()) {
          PutString("message", error_response.message, writer);
        }
        if (!error_response.error_code.empty()) {
          PutString("errorCode", error_response.error_code, writer);
        }

        writer->EndObject();
      });

  AddWriter<IngestResponseOrError>(
      [](const ObjectMapper *, const IngestResponseOrError &response, JsonWriter *writer) {
        writer->StartObject();
        PutString("eventId", response.event_id, writer);
        PutLong("timestamp", response.timestamp, writer);
        PutInt("statusCode", response.status_code, writer);
        if (!response.error_message.empty()) {
          PutString("errorMessage", response.error_message, writer);
        }
        // no need to write server_error_code as it is already processed
        writer->EndObject();
      });

  AddWriter<IngestBulkErrorResponse>([](const ObjectMapper *mapper,
                                        const IngestBulkErrorResponse &error_response,
                                        JsonWriter *writer) {
    writer->StartObject();

    if (!error_response.message.empty()) {
      PutString("message", error_response.message, writer);
    }
    if (!error_response.error_code.empty()) {
      PutString("errorCode", error_response.error_code, writer);
    }

    if (!error_response.results_with_error.empty()) {
      writer->Key("resultsWithError");
      vector_writer<IngestResponseOrError>(mapper, error_response.results_with_error, writer);
    }

    writer->EndObject();
  });

  AddWriter<IngestRequest>([](const IngestRequest *ingest_request, rapidjson::Value *value,
                              rapidjson::MemoryPoolAllocator<> &allocator) {
    value->SetObject();
    PutString(value, "eventId", ingest_request->eventId, allocator);
    PutString(value, "eventText", ingest_request->eventText, allocator);
    if (ingest_request->streamVersion > 0) {
      PutLong(value, "streamVersion", ingest_request->streamVersion, allocator);
    }
  });

  AddWriter(admin_op_writer);

  AddWriter(request_phase_writer);

  AddWriter<AdminWriteRequest>(
      [](const ObjectMapper *mapper, const AdminWriteRequest &request, JsonWriter *writer) {
        writer->StartObject();

        writer->Key("operation");
        admin_op_writer(mapper, request.operation, writer);

        writer->Key("phase");
        request_phase_writer(mapper, request.phase, writer);

        if (request.timestamp > 0) {
          PutLong("timestamp", request.timestamp, writer);
        }

        if (request.force) {
          PutBool("force", request.force, writer);
        }

        writer->Key("payload");
        if (request.payload.data != nullptr) {
          writer->RawValue(reinterpret_cast<const char *>(request.payload.data),
                           request.payload.length, rapidjson::kObjectType);
        } else {
          writer->StartObject();
          writer->EndObject();
        }

        writer->EndObject();
      });

  AddWriter<FailureReport>(
      [](const ObjectMapper *mapper, const FailureReport &report, JsonWriter *writer) {
        writer->StartObject();

        PutLong("timestamp", report.timestamp, writer);
        PutString("operation", report.operation, writer);
        PutString("payload", report.payload, writer);
        writer->Key("endpoints");
        vector_writer<std::string>(mapper, report.endpoints, writer);
        writer->Key("reasons");
        vector_writer<std::string>(mapper, report.reasons, writer);
        PutString("reporter", report.reporter, writer);

        writer->EndObject();
      });

  AddWriter(node_type_writer);

  AddWriter<UpdateEndpointRequest>(
      [](const ObjectMapper *mapper, const UpdateEndpointRequest &request, JsonWriter *writer) {
        writer->StartObject();

        writer->Key("operation");
        admin_op_writer(mapper, request.operation, writer);

        PutString("endpoint", request.endpoint, writer);

        writer->Key("nodeType");
        node_type_writer(mapper, request.nodeType, writer);

        writer->EndObject();
      });

  AddWriter<std::string>([](const ObjectMapper *mapper, const std::string &value,
                            JsonWriter *writer) { writer->String(value.c_str(), value.size()); });

  AddWriter<std::vector<std::string>>(vector_writer<std::string>);
  AddWriter<std::vector<IngestResponseOrError>>(vector_writer<IngestResponseOrError>);
}

void ObjectMapper::FetchString(const rapidjson::Value *value, const char *name,
                               std::string *target) {
  auto it = value->FindMember(name);
  if (it != value->MemberEnd()) {
    if (it->value.IsString()) {
      target->assign(it->value.GetString(), it->value.GetStringLength());
    } else if (it->value.IsInt64()) {
      *target = std::to_string(it->value.GetInt64());
    }
  }
}

void ObjectMapper::FetchLong(const rapidjson::Value *value, const char *name, int64_t *target) {
  auto it = value->FindMember(name);
  if (it != value->MemberEnd() && it->value.IsInt64()) {
    *target = it->value.GetInt64();
  }
}

void ObjectMapper::FetchInt(const rapidjson::Value *value, const char *name, int32_t *target) {
  auto it = value->FindMember(name);
  if (it != value->MemberEnd() && it->value.IsInt()) {
    *target = it->value.GetInt();
  }
}

void ObjectMapper::PutString(rapidjson::Value *value, const char *name,
                             const std::string &str_value,
                             rapidjson::MemoryPoolAllocator<> &allocator) {
  value->AddMember(rapidjson::Value().SetString(name, allocator),
                   rapidjson::Value().SetString(str_value.c_str(), str_value.size()), allocator);
}

void ObjectMapper::PutString(const char *name, const std::string &str_value, JsonWriter *writer) {
  writer->Key(name);
  writer->String(str_value.c_str(), str_value.size());
}

void ObjectMapper::PutInt(rapidjson::Value *value, const char *name, int32_t int_value,
                          rapidjson::MemoryPoolAllocator<> &allocator) {
  value->AddMember(rapidjson::Value().SetString(name, allocator),
                   rapidjson::Value().SetInt(int_value), allocator);
}

void ObjectMapper::PutInt(const char *name, int32_t int_value, JsonWriter *writer) {
  writer->Key(name);
  writer->Int(int_value);
}

void ObjectMapper::PutLong(rapidjson::Value *value, const char *name, int64_t long_value,
                           rapidjson::MemoryPoolAllocator<> &allocator) {
  value->AddMember(rapidjson::Value().SetString(name, allocator),
                   rapidjson::Value().SetInt64(long_value), allocator);
}

void ObjectMapper::PutLong(const char *name, int64_t long_value, JsonWriter *writer) {
  writer->Key(name);
  writer->Int64(long_value);
}

void ObjectMapper::PutBool(rapidjson::Value *value, const char *name, bool bool_value,
                           rapidjson::MemoryPoolAllocator<> &allocator) {
  value->AddMember(rapidjson::Value().SetString(name, allocator),
                   rapidjson::Value().SetBool(bool_value), allocator);
}

void ObjectMapper::PutBool(const char *name, bool bool_value, JsonWriter *writer) {
  writer->Key(name);
  writer->Bool(bool_value);
}

template <typename T>
void ObjectMapper::PutAny(
    rapidjson::Value *value, const char *name, const T *src,
    std::function<void(const T *, rapidjson::Value *, rapidjson::MemoryPoolAllocator<> &)>
        value_writer,
    rapidjson::MemoryPoolAllocator<> &allocator) {  // NOLINT
  rapidjson::Value newValue;
  value_writer(src, &newValue, allocator);
  value->AddMember(rapidjson::Value().SetString(name, allocator), newValue, allocator);
}

}  // namespace tfos::csdk
