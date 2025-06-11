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
package io.isima.bios.server.services;

/** Class that provides service paths of BIOS API. */
public class BiosServicePath {

  public static final String ROOT = "/bios/v1";
  public static final String TFOS = "/tfos/v1";
  public static final String SELFSERVE = "/selfserve/v1";

  public static final String PATH_VERSION = ROOT + "/version";

  // auth
  public static final String PATH_AUTH = ROOT + "/auth";
  public static final String PATH_AUTH_LOGIN = PATH_AUTH + "/login";
  public static final String PATH_AUTH_LOGOUT = PATH_AUTH + "/logout";
  public static final String PATH_AUTH_INFO = PATH_AUTH + "/info";
  public static final String PATH_AUTH_CHANGE_PASSWORD = PATH_AUTH + "/change-password";
  public static final String PATH_AUTH_CHANGE_PASSWORD_DEPRECATED = PATH_AUTH + "/resetpassword";
  public static final String PATH_AUTH_RENEW = ROOT + "/auth/session/renew";
  public static final String PATH_AUTH_RENEW_DEPRECATED = TFOS + "/auth/session/renew";
  public static final String PATH_AUTH_FORGOT_PASSWORD = ROOT + "/auth/forgotpassword/initiate";
  public static final String PATH_AUTH_FORGOT_PASSWORD_DEPRECATED =
      SELFSERVE + "/auth/forgotpassword";
  public static final String PATH_RESET_PASSWORD = ROOT + "/auth/forgotpassword/reset";
  public static final String PATH_RESET_PASSWORD_DEPRECATED = SELFSERVE + "/auth/resetpassword";

  // tenants
  public static final String PATH_TENANTS = ROOT + "/tenants";
  public static final String PATH_TENANT = PATH_TENANTS + "/{tenantName}";

  // tenants: mainly control plane
  public static final String PATH_SIGNALS = PATH_TENANT + "/signals";
  public static final String PATH_SIGNAL = PATH_SIGNALS + "/{signalName}";

  public static final String PATH_CONTEXTS = PATH_TENANT + "/contexts";
  public static final String PATH_CONTEXT = PATH_CONTEXTS + "/{contextName}";
  public static final String PATH_CONTEXT_ENTRIES = PATH_CONTEXT + "/entries";
  public static final String PATH_CONTEXT_ENTRIES_FETCH = PATH_CONTEXT_ENTRIES + "/fetch";
  public static final String PATH_CONTEXT_ENTRIES_SELECT = PATH_CONTEXT_ENTRIES + "/select";
  public static final String PATH_CONTEXT_ENTRIES_DELETE = PATH_CONTEXT_ENTRIES + "/delete";
  public static final String PATH_CONTEXT_ENTRIES_REPLACE = PATH_CONTEXT_ENTRIES + "/replace";

  public static final String PATH_CONTEXTS_SYNOPSES = PATH_TENANT + "/synopses/contexts";
  public static final String PATH_CONTEXT_SYNOPSIS = PATH_CONTEXTS_SYNOPSES + "/{contextName}";

  public static final String PATH_AVAILABLE_METRICS = PATH_TENANT + "/availableMetrics";
  public static final String PATH_FEATURE = ROOT + "/tenants/{tenantName}/feature";
  public static final String PATH_FEATURE_STATUS = PATH_FEATURE + "/status";
  public static final String PATH_FEATURE_REFRESH = PATH_FEATURE + "/refresh";

  public static final String PATH_TEACH_BIOS = ROOT + "/tenants/{tenantName}/teachbios";
  public static final String PATH_TEACH_BIOS_DEPRECATED = TFOS + "/tenants/{tenantName}/teachbios";

  public static final String PATH_APP_TENANTS = PATH_TENANT + "/app-tenants";

  // tenants: integrations
  public static final String PATH_EXPORTS = PATH_TENANT + "/exports";
  public static final String PATH_EXPORT = PATH_EXPORTS + "/{storageName}";
  public static final String PATH_EXPORT_START = PATH_EXPORT + "/start";
  public static final String PATH_EXPORT_STOP = PATH_EXPORT + "/stop";
  public static final String PATH_APPENDIXES = PATH_TENANT + "/appendixes/{category}";
  public static final String PATH_APPENDIX = PATH_APPENDIXES + "/entries/{entryId}";
  public static final String PATH_DISCOVER =
      PATH_TENANT + "/importSources/{importSourceId}/subjects";

  // tenants: data plane
  public static final String PATH_MULTI_GET = PATH_TENANT + "/multi-get";
  public static final String PATH_EVENT = PATH_SIGNAL + "/events/{eventId}";
  public static final String PATH_INSERT_BULK = PATH_TENANT + "/events/bulk";
  public static final String PATH_SELECT = PATH_TENANT + "/events/select";

  // tenants: BI
  public static final String PATH_REPORTS = PATH_TENANT + "/reports";
  public static final String PATH_REPORT = PATH_REPORTS + "/{reportId}";
  public static final String PATH_INSIGHTS = PATH_TENANT + "/insights/{insightName}";

  // system admin
  public static final String PATH_ADMIN = ROOT + "/admin";
  public static final String PATH_APPS = PATH_ADMIN + "/apps";
  public static final String PATH_APPS_FOR_TENANT = PATH_ADMIN + "/apps/{tenantName}";
  public static final String PATH_SYSADMIN_CONTEXT = PATH_ADMIN + "/context";
  public static final String PATH_ENDPOINTS = PATH_ADMIN + "/endpoints";
  public static final String PATH_KEYSPACES = PATH_ADMIN + "/keyspaces";
  public static final String PATH_PROPERTIES = PATH_ADMIN + "/properties/{key}";
  public static final String PATH_INFERRED_TAGS = "/admin/system/inferredTags";
  public static final String FULL_PATH_INFERRED_TAGS = ROOT + PATH_INFERRED_TAGS;
  public static final String PATH_TABLES = PATH_ADMIN + "/tables";
  public static final String PATH_UPSTREAM = PATH_ADMIN + "/upstream";
  public static final String PATH_UPSTREAM_DEPRECATED = TFOS + "/admin/upstream";
  // exceptionally, this is a control plane operation
  public static final String PATH_SUPPORTED_TAGS = PATH_ADMIN + "/supportedTags";

  // testing
  public static final String PATH_TEST_JSON_TYPE = ROOT + "/test/jsontype";
  public static final String PATH_TEST_DELAY = ROOT + "/test/delay";
  public static final String PATH_TEST_LOG = ROOT + "/test/log";

  // signup
  public static final String PATH_SIGNUP = ROOT + "/signup";
  public static final String PATH_SIGNUP_INITIATE = PATH_SIGNUP + "/initiate";
  public static final String PATH_SIGNUP_VERIFY = PATH_SIGNUP + "/verify";
  public static final String PATH_SIGNUP_APPROVE = PATH_SIGNUP + "/approve";
  public static final String PATH_SIGNUP_COMPLETE = PATH_SIGNUP + "/complete";
  public static final String PATH_SIGNUP_INVITE = PATH_SIGNUP + "/invite";
  public static final String PATH_SIGNUP_VERIFY_EMAIL = PATH_SIGNUP + "/verify-email";

  public static final String PATH_ANALYTICS = ROOT + "analytics/{tenantName}";

  public static final String PATH_SERVICE_REGISTRATION = ROOT + "/services/register";
}
