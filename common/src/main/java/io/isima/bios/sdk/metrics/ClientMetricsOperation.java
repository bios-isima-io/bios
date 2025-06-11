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
package io.isima.bios.sdk.metrics;

import io.isima.bios.metrics.MetricsOperation;

public enum ClientMetricsOperation implements MetricsOperation {
  INGEST_SYNC,
  INGEST_ASYNC,
  INGEST_REQUEST_DISPATCH,
  INGEST_REQUEST_SUBMIT,
  INGEST_SERVER_PROCESS,
  INGEST_RECEIVE_RESPONSE,
  INGEST_COMPLETION_DISPATCH,
  INGEST_COMPLETION_PROCESS,

  EXTRACT_SYNC,
  EXTRACT_ASYNC,
  EXTRACT_REQUEST_DISPATCH,
  EXTRACT_REQUEST_SUBMIT,
  EXTRACT_SERVER_PROCESS,
  EXTRACT_RECEIVE_RESPONSE,
  EXTRACT_COMPLETION_DISPATCH,
  EXTRACT_COMPLETION_PROCESS,

  PUT_CONTEXT_COMPLETE,
  PUT_CONTEXT_NODE_,
  LIST_CONTEXT_ENTRIES,
  GET_CONTEXT_ENTRIES,
  SELECT_CONTEXT_ENTRIES,
  DELETE_CONTEXT_ENTRIES,
  UPDATE_CONTEXT_ENTRIES,
  REPLACE_CONTEXT_ENTRIES,

  POST_CONTEXT_INITIAL,
  POST_CONTEXT_FINAL,

  ERASE_CONTEXT_INITIAL,
  ERASE_CONTEXT_FINAL,

  UPDATE_CONTEXT_INITIAL,
  UPDATE_CONTEXT_FINAL,

  FILTERUPDATE_CONTEXT_INITIAL,
  FILTERUPDATE_CONTEXT_FINAL;

  @Override
  public String getOperationName() {
    return name();
  }

  @Override
  public String getSubOperationName() {
    return "";
  }

  @Override
  public boolean isInputOperation() {
    return false;
  }
}
