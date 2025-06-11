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
package io.isima.bios.deli.models;

import io.isima.bios.deli.importer.CdcKeywords;
import io.isima.bios.models.CdcOperationType;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Data flow context.
 *
 * <p>
 * An instance is created when a data importer receives an event. The instance brings current state
 * of the data flow until delivery.
 * </p>
 */
@Getter
@Setter
@ToString
public class FlowContext {

  // Time stamped when the context is created
  private final long timestamp0;

  // Record to insert
  private Map<String, Object> record;

  // Keep additional attributes such as operation type and elapsed time here
  private final Map<String, Object> metadata = new HashMap<>();

  // for InMessage authentication
  private String user;
  private String password;

  // CDC Specific //////////////////////////////////////////////////////////
  private String database;
  private String tableName;
  private Map<String, Object> before;
  private Map<String, Object> after;

  // Forwarding only once for a table is good enough in proxy mode.
  private boolean sentAlready = false;

  /**
   * The ctor.
   */
  public FlowContext(long timestamp0) {
    this.timestamp0 = timestamp0;
  }

  /**
   * Gets an additional attribute
   *
   * @param <T>  Return type
   * @param name Attribute name
   * @return Value of the return type
   */
  @SuppressWarnings("unchecked")
  public <T> T getAdditionalAttribute(String name) {
    return (T) metadata.get(name);
  }

  /**
   * Adds an additional attribute.
   *
   * @param name  Attribute name
   * @param value Attribute value
   */
  public void setAdditionalAttribute(String name, Object value) {
    metadata.put(name, value);
  }

  public long getElapsedTime() {
    return System.currentTimeMillis() - timestamp0;
  }

  // Special interfaces for frequently-used additional attributes /////////

  public void setOperationType(CdcOperationType operationType) {
    metadata.put(CdcKeywords.OPERATION_TYPE, operationType);
  }

  public CdcOperationType getOperationType() {
    return getAdditionalAttribute(CdcKeywords.OPERATION_TYPE);
  }
}
