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
package io.isima.bios.deli.importer;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.isima.bios.models.CdcOperationType;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntityStateChangeCapture {
  private CdcOperationType operationType;
  private Map<String, Object> preOpState;
  private Map<String, Object> postOpState;

  public EntityStateChangeCapture() {}

  public CdcOperationType getOperationType() {
    return operationType;
  }

  public void setOperationType(final CdcOperationType operationType) {
    this.operationType = operationType;
  }

  public Map<String, Object> getPreOpState() {
    return preOpState;
  }

  public void setPreOpState(final Map<String, Object> preOpState) {
    this.preOpState = preOpState;
  }

  public Map<String, Object> getPostOpState() {
    return postOpState;
  }

  public void setPostOpState(final Map<String, Object> postOpState) {
    this.postOpState = postOpState;
  }
}
