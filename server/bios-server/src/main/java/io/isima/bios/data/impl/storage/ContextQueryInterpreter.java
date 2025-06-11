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
package io.isima.bios.data.impl.storage;

import static io.isima.bios.data.impl.storage.ContextCassStream.COLUMN_ENTRY_ID;
import static io.isima.bios.data.impl.storage.ContextCassStream.COLUMN_WRITE_TIME;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.data.filter.FilterElement;
import io.isima.bios.data.impl.models.ContextSelectOpResources;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextQueryInterpreter {
  private static final Logger logger = LoggerFactory.getLogger(ContextQueryInterpreter.class);
  private static final long ALLOWED_NUM_POTENTIALLY_MISS = 100;

  private final ContextCassStream cassStream;
  private final SelectContextRequest request;
  private final ContextQueryState queryState;
  private final long dbSelectContextLimit;

  private final StreamDesc streamDesc;
  private final List<SingleColumnRelation> filter;
  private final List<CassAttributeDesc> filterAttributes = new ArrayList<>();

  private final StringBuilder statement;

  public ContextQueryInterpreter(
      ContextCassStream cassStream,
      SelectContextRequest request,
      long dbSelectContextLimit,
      ContextQueryState queryState)
      throws InvalidFilterException {
    this.cassStream = cassStream;
    this.request = request;
    this.queryState = queryState;
    this.dbSelectContextLimit = dbSelectContextLimit;

    streamDesc = cassStream.getStreamDesc();

    if (queryState.getFilter() != null) {
      filter = validateFilter(queryState.getFilter());
    } else {
      filter = List.of();
    }

    // Initial (common) query statement setup
    statement =
        new StringBuilder(String.format("SELECT %s, %s", COLUMN_WRITE_TIME, COLUMN_ENTRY_ID));

    for (final var attribute : streamDesc.getAttributes()) {
      CassAttributeDesc desc = cassStream.getAttributeDesc(attribute.getName());
      statement.append(", ").append(desc.getColumn());
    }
    statement.append(
        String.format(" FROM %s.%s ", cassStream.getKeyspaceName(), cassStream.getTableName()));
  }

  private List<SingleColumnRelation> validateFilter(List<SingleColumnRelation> filter)
      throws InvalidFilterException {
    final var attributeNames = new HashSet<>();
    final var validated = new ArrayList<SingleColumnRelation>();
    final String whereClause = request.getWhere();
    for (var relation : filter) {
      final var attributeName = relation.getEntity().rawText();
      if (attributeNames.contains(attributeName.toLowerCase())) {
        throw new InvalidFilterException(
            "Where clause may not have the same attribute twice; attribute=" + attributeName);
      }
      final var attributeDesc = cassStream.getAttributeDesc(attributeName);
      if (attributeDesc == null) {
        throw new InvalidFilterException(
            String.format("Attribute %s not found; where=%s", attributeName, whereClause));
      }
      filterAttributes.add(attributeDesc);
      validated.add(relation);
    }
    return validated;
  }

  public ContextSelectOpResources chooseExtractMethodology()
      throws InvalidFilterException, InvalidValueException {
    final Integer limit = request.getLimit();

    if (limit != null) {
      queryState.setLimit(limit);
    } else {
      queryState.setLimit(dbSelectContextLimit + 1);
    }

    final List<List<Object>> primaryKeys = tryMakingPrimaryKeys();
    if (primaryKeys != null) {
      return ContextSelectOpResources.of(primaryKeys);
    }

    addWhereClause();

    final var statement = this.statement.toString();
    logger.debug("Select context statement={}", statement);
    return ContextSelectOpResources.of(statement);
  }

  private void addWhereClause() throws InvalidFilterException {
    if (filter.isEmpty()) {
      queryState.setFilterElements(List.of());
      return;
    }

    int primaryKeySize = streamDesc.getPrimaryKey().size();
    final var primaryKeyUsed = new Boolean[primaryKeySize];
    final var primaryKeyFilterElements = new List[primaryKeySize]; // of FilterElement
    final var primaryKeyTerms = new List[primaryKeySize]; // of String
    for (int i = 0; i < primaryKeySize; ++i) {
      primaryKeyFilterElements[i] = new ArrayList<FilterElement>();
      primaryKeyTerms[i] = new ArrayList<String>();
    }
    final var filterElements = new ArrayList<FilterElement>();

    for (int i = 0; i < filter.size(); ++i) {
      final var relation = filter.get(i);
      final var attributeDesc = filterAttributes.get(i);

      interpretRelation(
          relation,
          attributeDesc,
          primaryKeyUsed,
          primaryKeyFilterElements,
          primaryKeyTerms,
          filterElements);
    }

    // Finalize DB qury statement
    boolean canDeferBackend = true;
    String joiner = " WHERE ";
    for (int i = 0; i < primaryKeyUsed.length; ++i) {
      if (primaryKeyUsed[i] == Boolean.TRUE) {
        if (canDeferBackend) {
          for (Object term : primaryKeyTerms[i]) {
            statement.append(joiner).append((String) term);
            joiner = " AND ";
          }
        } else {
          for (Object filterElement : primaryKeyFilterElements[i]) {
            filterElements.add((FilterElement) filterElement);
          }
        }
      } else {
        canDeferBackend = false;
      }
    }

    if (request.getLimit() != null && filterElements.isEmpty() && request.getOrderBy() == null) {
      statement.append(" LIMIT ").append(request.getLimit());
      queryState.setLimitSpecifiedInQuery(true);
    } else {
      // We'll fetch only up to the DB limitation. If number of returned rows exceeds the
      // limitation, the query will be rejected.
      statement.append(" LIMIT ").append(dbSelectContextLimit + 10);
    }

    queryState.setFilterElements(filterElements);
  }

  private void interpretRelation(
      SingleColumnRelation relation,
      CassAttributeDesc attributeDesc,
      Boolean[] primaryKeyUsed,
      List[] primaryKeyFilterElements, // of FilterElement
      List[] primaryKeyTerms, // of String
      ArrayList<FilterElement> restFilterElements)
      throws InvalidFilterException {

    final Operator operator = relation.operator();
    final String column = attributeDesc.getColumn();
    final String attributeName = attributeDesc.getName();

    // Checks if the filtering attribute is part of the primary key
    final int index = streamDesc.getPrimaryKey().indexOf(attributeName);

    // This is a special case. The backend DB can't run a range query on a partition key
    if (index == 0 && operator != Operator.EQ && operator != Operator.IN) {
      // We can't defer the filter to the backend DB.
      restFilterElements.add(new FilterElement(attributeDesc, relation));
      primaryKeyUsed[index] = Boolean.FALSE;
      return;
    }

    // The relation is for a primary key element
    if (index >= 0) {
      // we raise the flag unless false is set explicitly
      if (primaryKeyUsed[index] != Boolean.FALSE) {
        primaryKeyUsed[index] = Boolean.TRUE;
      }
      if (operator == Operator.IN) {
        final var sb =
            new StringBuilder(column).append(" ").append(operator).append(" ").append("(");
        String delimiter = "";
        for (var value : relation.getInValues()) {
          if (attributeDesc.getAttributeType() == InternalAttributeType.ENUM) {
            sb.append(delimiter).append(CqlFilterCompiler.getValue(value, attributeDesc));
          } else {
            sb.append(delimiter).append(value);
          }
          delimiter = ", ";
        }
        primaryKeyTerms[index].add(sb.append(")").toString());
      } else {
        final var sb = new StringBuilder(column).append(" ").append(operator).append(" ");
        if (attributeDesc.getAttributeType() == InternalAttributeType.ENUM) {
          sb.append(CqlFilterCompiler.getValue(relation.getValue(), attributeDesc));
        } else {
          sb.append(relation.getValue());
        }
        primaryKeyTerms[index].add(sb.toString());
      }
      primaryKeyFilterElements[index].add(new FilterElement(attributeDesc, relation));
      return;
    }

    // The relation is for a value column. We'll filter the entries as long as fetched entries are
    // within limitation.
    restFilterElements.add(new FilterElement(attributeDesc, relation));
  }

  /**
   * Parse filter and returns a list of primary keys if the query can be fulfilled by {@link
   * ContextCassStream#getContextEntriesAsync}.
   *
   * <p>NOTE: We assume the filter relations are validated already and there are no duplicate
   * columns.
   *
   * @return List of primary keys if running {@link ContextCassStream#getContextEntriesAsync} is
   *     feasible. Returns null otherwise.
   */
  private List<List<Object>> tryMakingPrimaryKeys() throws InvalidFilterException {
    final int primaryKeySize = streamDesc.getPrimaryKey().size();
    if (filter.size() != primaryKeySize) {
      return null;
    }

    // Retrieve target primary key values for each element.
    final var targetValues = new Set[primaryKeySize];

    // minimum number of target entries assuming the specified value always exists in the context
    long minimumPossibleNumEntries = 0;

    // all primary key element combinations, i.e., number of generated primary keys
    long numAllCombinations = 1;

    for (int i = 0; i < filter.size(); ++i) {
      final var relation = filter.get(i);
      final var attributeDesc = filterAttributes.get(i);
      final int index = streamDesc.getPrimaryKey().indexOf(attributeDesc.getName());
      if (index < 0) {
        // not for primary key
        return null;
      }
      if (relation.isEQ()) {
        targetValues[index] =
            Set.of(cassStream.getTypeConvertedFilterValue(relation.getValue(), attributeDesc));
        minimumPossibleNumEntries = Math.max(minimumPossibleNumEntries, 1);
      } else if (relation.isIN()) {
        final var values = new HashSet<>();
        targetValues[index] = values;
        for (var value : relation.getInValues()) {
          values.add(cassStream.getTypeConvertedFilterValue(value, attributeDesc));
        }
        minimumPossibleNumEntries = Math.max(minimumPossibleNumEntries, values.size());
        numAllCombinations *= values.size();
      } else {
        // primary key query supports only exact match
        return null;
      }
    }

    // Give up if the fetching by exact match may be too inefficient
    final long potentialMiss = numAllCombinations - minimumPossibleNumEntries;
    if (potentialMiss > ALLOWED_NUM_POTENTIALLY_MISS) {
      return null;
    }

    // Build primary keys and return
    final var currentValues = new Object[primaryKeySize];
    final var primaryKeys = new ArrayList<List<Object>>();
    buildPrimaryKeys(0, targetValues, currentValues, primaryKeys);
    return primaryKeys;
  }

  private void buildPrimaryKeys(
      int index, Set[] targetValues, Object[] currentValues, ArrayList<List<Object>> primaryKeys) {
    if (index == targetValues.length) {
      primaryKeys.add(List.of(currentValues));
      return;
    }
    for (var value : targetValues[index]) {
      currentValues[index] = value;
      buildPrimaryKeys(index + 1, targetValues, currentValues, primaryKeys);
    }
  }
}
