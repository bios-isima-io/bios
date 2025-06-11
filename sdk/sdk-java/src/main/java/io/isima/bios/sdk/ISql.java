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
package io.isima.bios.sdk;

import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.models.isql.StatementType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

public class ISql {

  /** Starts a select statement for Signals and Contexts. */
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.From select(
      String... columns) {
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.From(columns);
  }

  /** Starts a select statement for Signals and Contexts. */
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.From select(
      List<String> columns) {
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.From(columns);
  }

  /** Starts an insert statement for Signals. */
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.InsertInto insert() {
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.InsertInto();
  }

  /** Starts an upsert statement for Contexts. */
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextUpsertInto upsert() {
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextUpsertInto();
  }

  /** Starts an update statement for Contexts. */
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextUpdateSet update(
      String contextName) {
    var updateContext = new ContextUpdate();
    updateContext.setContextName(contextName);
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextUpdateSet(
        updateContext);
  }

  /** Deprecated. Use Bios.isql().update() instead. Starts an update statement for Contexts. */
  @Deprecated
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextUpdateSet
      updateContext(String contextName) {
    return update(contextName);
  }

  /** Starts a delete statement for Contexts. */
  public ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextDeleteFrom delete() {
    return new ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication.ContextDeleteFrom();
  }

  @ToString
  @Getter
  @Setter(AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class SignalSelect implements Statement {
    private List<String> selectColumns;
    private String signalName;
    private String where;
    private List<String> groupByColumns;
    private String orderByColumn;
    private Boolean orderByReverse;
    private Boolean orderByCaseSensitive;
    private Integer limit;
    private Long windowSizeMs;
    private Long hopSizeMs;
    private Long originTimeEpochMs;
    private Long deltaMs;
    private Long alignmentDurationMs;
    private Boolean onTheFly;

    @Override
    public StatementType getStatementType() {
      return StatementType.SELECT;
    }
  }

  @ToString
  @Getter
  @Setter(AccessLevel.PACKAGE)
  public static class ContextSelect implements Statement {
    private List<String> selectColumns;
    private String contextName;
    private List<CompositeKey> primaryKeyValues;
    private SelectContextRequest exRequest;
    private boolean isExtendedRequest;
    private Boolean onTheFly;

    ContextSelect() {
      this.exRequest = new SelectContextRequest();
    }

    @Override
    public StatementType getStatementType() {
      return StatementType.CONTEXT_SELECT;
    }
  }

  @ToString
  @Getter
  @Setter(AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class Insert implements Statement {
    private String signalName;
    private List<String> csvs;

    @Override
    public StatementType getStatementType() {
      return StatementType.INSERT;
    }

    public boolean isBulk() {
      return (csvs.size() > 1);
    }
  }

  @ToString
  @Getter
  @Setter(AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpsert implements Statement {
    private String contextName;
    private List<String> csvs;

    @Override
    public StatementType getStatementType() {
      return StatementType.UPSERT;
    }
  }

  @ToString
  @Getter
  @Setter(AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpdate implements Statement {
    private String contextName;
    private Map<String, Object> attributesToUpdate;
    private List<CompositeKey> primaryKeyValues;

    @Override
    public StatementType getStatementType() {
      return StatementType.CONTEXT_UPDATE;
    }
  }

  @ToString
  @Getter
  @Setter(AccessLevel.PACKAGE)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextDelete implements Statement {
    private String contextName;
    private List<CompositeKey> primaryKeyValues;

    @Override
    public StatementType getStatementType() {
      return StatementType.DELETE;
    }
  }

  @ToString
  @Getter
  public static class AtomicMutation implements Statement {
    private final List<Statement> statements;

    AtomicMutation(Statement statement) {
      Objects.requireNonNull(statement);
      if (statement.getStatementType() == StatementType.ATOMIC_MUTATION) {
        throw new IllegalArgumentException("rollbackOnFailure must not be nested");
      }
      statements = List.of(statement);
    }

    @Override
    public StatementType getStatementType() {
      return StatementType.ATOMIC_MUTATION;
    }
  }
}
