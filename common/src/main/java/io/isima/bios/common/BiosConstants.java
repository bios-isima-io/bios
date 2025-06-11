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
package io.isima.bios.common;

/** Constant values provider for biOS. */
public class BiosConstants {

  /**
   * Prefix of failed node name returned via get_endpoints API method. Admin and context methods in
   * SDK must consider that the service is unavailable on a host with this prefix.
   */
  public static final String PREFIX_FAILED_NODE_NAME = ".";

  /** System tenant name. */
  public static final String TENANT_SYSTEM = "_system";

  /** Failure report stream name. */
  public static final String STREAM_FAILURE_REPORT = "_failureReport";

  /** Query logger signal name. */
  public static final String QUERY_SIGNAL = "_query";

  /** operation failure signal name. */
  public static final String STREAM_OPERATION_FAILURE = "_operationFailure";

  public static final String STREAM_ALL_OPERATION_FAILURE = "_allOperationFailure";

  /** Audit stream name. */
  public static final String STREAM_AUDIT_LOG = "audit_log";

  public static final String STREAM_CLIENT_METRICS = "_clientMetrics";
  public static final String STREAM_ALL_CLIENT_METRICS = "_allClientMetrics";
  public static final int CLIENT_METRICS_NUM_ATTRIBUTES_BEFORE_QOS = 16;

  // Failure report attribute names /////////////////////////////
  public static final String ATTR_NODE_ID = "nodeId";
  public static final String ATTR_TIMESTAMP = "timestamp";
  public static final String ATTR_ENDPOINT = "endpoint";
  public static final String ATTR_REASON = "reason";
  public static final String ATTR_OPERATION = "operation";
  public static final String ATTR_PAYLOAD = "payload";
  public static final String ATTR_REPORTER = "reporter";

  public static final String APP_TFOS = "biOS";

  public static final String SSL_RMI_CLIENT_SOCKET_FACTORY = "com.sun.jndi.rmi.factory.socket";
  public static final String AUTH_HEADER = "Authorization";
  public static final String TOKEN_SEPARATOR = "Bearer ";
  public static final String ACCEPT_ENCODING = "accept-encoding";
  public static final String SCHEME = "x-http2-scheme";
  public static final String CONTENT_TYPE = "content-type";
  public static final String APPLICATION_JSON = "application/json; charset=utf-8";
  public static final String DEFLATE = "deflate";
  public static final String GZIP = "gzip";
  public static final String HTTPS = "https";

  // LastN TTL
  private static final long DEFAULT_COLLECTION_DAYS = 15;
  public static final long DEFAULT_LAST_N_COLLECTION_TTL =
      DEFAULT_COLLECTION_DAYS * 24 * 60 * 60 * 1000;

  // Email domain check
  public static final String STREAM_EMAIL_DOMAINS = "_emailDomains";
  public static final String ATTR_APPROVAL_ACTION = "approvalAction";
}
