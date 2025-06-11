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
package io.isima.bios.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class UpdateEndpointsRequest {

  @NotNull private Operation operation;

  @NotNull
  @Size(min = 1, max = 2048)
  @Pattern(regexp = ValidatorConstants.URL_PATTERN)
  private String endpoint;

  private NodeType nodeType = NodeType.SIGNAL;

  public UpdateEndpointsRequest() {}

  public UpdateEndpointsRequest(Operation operation, String endpoint) {
    this.operation = operation;
    this.endpoint = endpoint;
  }

  public UpdateEndpointsRequest(Operation operation, String endpoint, NodeType nodeType) {
    this.operation = operation;
    this.endpoint = endpoint;
    this.nodeType = nodeType;
  }

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public NodeType getNodeType() {
    return nodeType;
  }

  public void setNodeType(NodeType nodeType) {
    this.nodeType = nodeType;
  }
}
