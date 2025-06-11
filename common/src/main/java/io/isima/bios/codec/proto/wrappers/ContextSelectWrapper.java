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
package io.isima.bios.codec.proto.wrappers;

import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.isql.ContextSelectStatement;
import java.util.List;

public class ContextSelectWrapper implements ContextSelectStatement {
  private final String contextName;
  private final List<List<Object>> primaryKeys;
  private final List<String> attributeList;

  public ContextSelectWrapper(
      String contextName, List<List<Object>> keys, List<String> attributeList) {
    this.contextName = contextName;
    this.primaryKeys = keys;
    this.attributeList = attributeList;
  }

  @Override
  public ContentRepresentation getContentRepresentation() {
    return ContentRepresentation.UNTYPED;
  }

  @Override
  public String getContextName() {
    return contextName;
  }

  @Override
  public List<List<Object>> getPrimaryKeys() {
    return primaryKeys;
  }

  @Override
  public List<String> getSelectAttributes() {
    return attributeList;
  }
}
