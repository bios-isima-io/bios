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
package io.isima.bios.service;

/** Collection of biOS REST API keywords. */
public class Keywords {
  // HTTP methods //////////////////////////////////////////////////////////////
  public static final String DELETE = "DELETE";
  public static final String PATCH = "PATCH";
  public static final String POST = "POST";

  // header fields /////////////////////////////////////////////////////////////
  public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  public static final String AUTHORIZATION = "Authorization";
  public static final String METHOD = ":method"; // HTTP2 method header field
  public static final String PATH = ":path"; // HTTP2 path header field
  public static final String AUTHORITY = ":authority"; // HTTP2 authority header field
  public static final String STATUS = ":status"; // HTTP2 status header field
  public static final String X_CONTENT_TYPE_OPTIONS = "x-content-type-options";
  public static final String ORIGIN = "Origin";
  public static final String X_BIOS_REQUEST_PHASE = "x-bios-request-phase";
  public static final String X_BIOS_SKIP_AUDIT_VALIDATION = "x-bios-skip-audit-validation";
  public static final String X_BIOS_AUDIT_ENABLED = "x-bios-audit-enabled";
  public static final String X_BIOS_STREAM_VERSION = "x-bios-stream-version";
  public static final String X_BIOS_TIMESTAMP = "x-bios-timestamp";
  public static final String X_BIOS_EXECUTION_ID = "x-bios-execution-id";
  // HACK: Used for passing allowed methods from a V2 HandleNode to the Options operation handler
  // over request headers
  public static final String X_BIOS_ALLOWED_METHODS = "x-bios-allowed-methods";
  // Used to inform the client version to the server
  public static final String X_BIOS_VERSION = "x-bios-version";
  public static final String X_BIOS_INTERNAL_REQUEST = "x-bios-internal-request";
  public static final String X_BIOS_CLIENT_VERSION = "x-bios-client-version";

  // auth related parameters //////////////////////////////////////////////////
  public static final String TOKEN = "token";
  public static final String BEARER = "Bearer";

  // path parameters //////////////////////////////////////////////////////////
  public static final String TENANT_NAME = "tenantName";
  public static final String SIGNAL_NAME = "signalName";
  public static final String CONTEXT_NAME = "contextName";
  public static final String REPORT_ID = "reportId";
  public static final String EVENT_ID = "eventId";
  public static final String STORAGE_NAME = "storageName";
  public static final String CATEGORY = "category";
  public static final String ENTRY_ID = "entryId";
  public static final String KEY = "key";
  public static final String IMPORT_SOURCE_ID = "importSourceId";
  public static final String EMAIL = "email";
  public static final String USER_ID = "userId";
  public static final String KEYSPACE = "keyspace";
  public static final String INSIGHT_NAME = "insightName";

  // query parameters ////////////////////////////////////////////////////////
  public static final String DETAIL = "detail";
  public static final String ENDPOINT_TYPE = "endpoint_type";
  public static final String INCLUDE_INTERNAL = "includeInternal";
  public static final String INCLUDE_INFERRED_TAGS = "includeInferredTags";
  public static final String NAMES = "names";
  public static final String IDS = "ids";
  public static final String TIMEOUT = "timeout";
  public static final String SSL_ENABLE = "sslEnable";
  public static final String TABLES = "tables";

  // Used for atomic data write operations
  public static final String ATOMIC_OP_ID = "atomicOpId";
  public static final String ATOMIC_OP_COUNT = "atomicOpCount";

  // fan routing parameters /////////////////////////////////////////////////
  public static final String INITIAL = "INITIAL";

  // special header values /////////////////////////////////////////////////
  public static final String METRICS = "metrics";
}
