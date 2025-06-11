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
package io.isima.bios.codec.proto.isql;

import io.isima.bios.models.isql.WhereClause;

/** Builds keys.in().. clause. */
public class WhereClauseBuilder
    implements WhereClause.KeysSpecifier, WhereClause.KeysFinalSpecifier {
  private final BasicBuilderValidator validator;
  private Object[] primaryKeys;

  WhereClauseBuilder(BasicBuilderValidator validator) {
    this.validator = validator;
    this.primaryKeys = null;
  }

  @Override
  public WhereClause.KeysFinalSpecifier in(Object... primaryKeys) {
    validator.validateObject(primaryKeys, "keys::in");
    this.primaryKeys = primaryKeys;
    return this;
  }

  @Override
  public WhereClause.KeysFinalSpecifier eq(Object primaryKey) {
    validator.validateObject(primaryKey, "keys::eq");
    this.primaryKeys = new Object[1];
    this.primaryKeys[0] = primaryKey;
    return this;
  }

  BasicBuilderValidator getValidator() {
    return validator;
  }

  Object[] getPrimaryKeys() {
    return primaryKeys;
  }
}
