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

import io.isima.bios.codec.proto.wrappers.ContextSelectWrapper;
import io.isima.bios.models.isql.ContextSelectStatement;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.WhereClause;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builds context select statements.
 *
 * <p>As of now the underlying messages are not protobuf enabled. Also, the POJO objects that is
 * sent on the wire as JSON does not contain sufficient information for building these query (e.g
 * fromContext). So normal wrappers are built that is then used to build the POJO objects that are
 * then sent over the wire.
 */
public class DefaultContextSelectBuilder implements ISqlStatement.ContextSelectBuilder {
  private final BasicBuilderValidator validator;
  private final String contextName;
  private final List<List<Object>> keys;
  private final List<String> attributesToSelect;

  public DefaultContextSelectBuilder(
      BasicBuilderValidator validator, String fromContext, List<String> attributesToSelect) {
    this.validator = validator;
    this.validator.validateStringParam(fromContext, "fromContext");
    this.validator.validateStringList(attributesToSelect, "attributes", 1);
    this.contextName = fromContext;
    this.keys = Collections.emptyList();
    this.attributesToSelect = attributesToSelect;
  }

  public DefaultContextSelectBuilder(BasicBuilderValidator validator, String fromContext) {
    this.validator = validator;
    this.validator.validateStringParam(fromContext, "fromContext");
    this.contextName = fromContext;
    this.keys = Collections.emptyList();
    this.attributesToSelect = Collections.emptyList();
  }

  private DefaultContextSelectBuilder(DefaultContextSelectBuilder other, Object[] primaryKeys) {
    this.validator = other.validator;
    this.contextName = other.contextName;
    this.keys = Stream.of(primaryKeys).map(Collections::singletonList).collect(Collectors.toList());
    this.attributesToSelect = other.attributesToSelect;
  }

  @Override
  public ISqlStatement.ContextSelectBuilder where(WhereClause.KeysFinalSpecifier specifier) {
    validator.validateObject(specifier, "keys");
    if (specifier instanceof WhereClauseBuilder) {
      WhereClauseBuilder concreteBuilder = (WhereClauseBuilder) specifier;
      return new DefaultContextSelectBuilder(this, concreteBuilder.getPrimaryKeys());
    }
    return this;
  }

  @Override
  public ContextSelectStatement build() {
    return new ContextSelectWrapper(contextName, keys, attributesToSelect);
  }
}
