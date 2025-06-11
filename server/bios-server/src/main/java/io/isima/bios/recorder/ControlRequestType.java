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
package io.isima.bios.recorder;

import static io.isima.bios.recorder.RecorderConstants.CONTROL_BASE;

public enum ControlRequestType implements RequestType {
  CREATE_TENANT(true),
  CREATE_TENANT_INITIAL(true),
  CREATE_TENANT_FINAL(true),
  DELETE_TENANT,
  DELETE_TENANT_INITIAL,
  DELETE_TENANT_FINAL,
  UPDATE_TENANT,
  UPDATE_TENANT_INITIAL,
  UPDATE_TENANT_FINAL,

  GET_TENANT_NAMES(2),
  GET_TENANT_CONFIG(2),

  GET_SIGNALS(2),
  GET_CONTEXTS(2),
  GET_REPORT_CONFIGS(2),
  PUT_REPORT_CONFIG,
  DELETE_REPORT,
  GET_INSIGHT_CONFIGS(2),
  PUT_INSIGHT_CONFIGS,
  DELETE_INSIGHT_CONFIGS,
  OPERATE_SYNTHETIC_LOAD(3),
  GET_APP_TENANT_NAMES,

  // for old API only
  GET_STREAM_CONFIG(2),
  CREATE_STREAM_CONFIG_INITIAL(true),
  CREATE_STREAM_CONFIG_FINAL(true),
  UPDATE_STREAM_CONFIG_INITIAL,
  UPDATE_STREAM_CONFIG_FINAL,
  DELETE_STREAM_CONFIG_INITIAL,
  DELETE_STREAM_CONFIG_FINAL,
  GET_ENDPOINTS,
  UPDATE_ENDPOINTS,
  GET_UPSTREAM_CONFIG,

  // for new API only
  CREATE_SIGNAL(true),
  CREATE_SIGNAL_INITIAL(true),
  CREATE_SIGNAL_FINAL(true),
  UPDATE_SIGNAL,
  UPDATE_SIGNAL_INITIAL,
  UPDATE_SIGNAL_FINAL,
  DELETE_SIGNAL,
  DELETE_SIGNAL_INITIAL,
  DELETE_SIGNAL_FINAL,

  CREATE_CONTEXT(true),
  CREATE_CONTEXT_INITIAL(true),
  CREATE_CONTEXT_FINAL(true),
  UPDATE_CONTEXT,
  UPDATE_CONTEXT_INITIAL,
  UPDATE_CONTEXT_FINAL,
  DELETE_CONTEXT,
  DELETE_CONTEXT_INITIAL,
  DELETE_CONTEXT_FINAL,

  GET_AVAILABLE_METRICS,

  CREATE_EXPORT_DESTINATION,
  GET_EXPORT_DESTINATION,
  UPDATE_EXPORT_DESTINATION,
  START_EXPORT,
  STOP_EXPORT,

  CREATE_APPENDIX,
  GET_APPENDIX,
  UPDATE_APPENDIX,
  DELETE_APPENDIX,
  DISCOVER_IMPORT_SOURCE,

  GET_PROPERTY,
  SET_PROPERTY,

  MAINTAIN_KEYSPACES,
  MAINTAIN_TABLES,
  MAINTAIN_CONTEXT,

  TEACH_BIOS,

  LOGIN,
  RENEW_SESSION,
  GET_USER_INFO,
  CHANGE_PASSWORD,
  ;

  private final int priority;
  private final boolean rateLimit;

  ControlRequestType() {
    this.priority = 1000;
    this.rateLimit = false;
  }

  ControlRequestType(boolean rateLimit) {
    this.priority = 1000;
    this.rateLimit = rateLimit;
  }

  ControlRequestType(int priority) {
    this.priority = priority;
    this.rateLimit = false;
  }

  @Override
  public String getRequestName() {
    return this.name();
  }

  @Override
  public int getRequestNumber() {
    return CONTROL_BASE + this.ordinal();
  }

  @Override
  public boolean isControl() {
    return true;
  }

  @Override
  public int priority() {
    return priority;
  }

  @Override
  public boolean shouldRateLimit() {
    return rateLimit;
  }
}
