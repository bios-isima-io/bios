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

import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_EVENT_ID;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_TIME_INDEX;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.maintenance.DigestSpecifier;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Event;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.Range;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.req.Aggregate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewCassStream extends SignalCassStream {
  private static final Logger logger = LoggerFactory.getLogger(ViewCassStream.class);

  private static final String FORMAT_CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s (%s timestamp, %s timeuuid%s,"
          + " PRIMARY KEY (%s%s%s, %s))"
          + " WITH comment = 'tenant=%s (version=%d), view=%s (version=%d)'"
          + " AND default_time_to_live = %d"
          + " AND gc_grace_seconds = %d"
          + " AND compaction = %s"
          + " AND bloom_filter_fp_chance = %s";

  protected ViewCassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    super(cassTenant, streamDesc);
  }

  @Override
  public String makeCreateTableStatement(final String keyspaceName) {

    final StringBuilder columnsBuilder = new StringBuilder();

    // add attributes to column
    // TODO(TFOS-1080): View stream includes all attributes (view GroupBys and attributes)
    streamDesc
        .getAttributes()
        .forEach(
            (attrDesc) -> {
              final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());
              columnsBuilder
                  .append(", ")
                  .append(cassAttrDesc.getColumn())
                  .append(" ")
                  .append(valueTypeToCassDataTypeName(cassAttrDesc.getAttributeType()));
            });

    final StringJoiner partitionKeyJoiner = new StringJoiner(", ").add(COL_TIME_INDEX);

    // groupBy (additional attributes of view's StreamConfig) used as partition keys
    // TODO(TFOS-1080): View stream has groupBy keys in additionalAttributes.
    if (streamDesc.getAdditionalAttributes() != null) {
      streamDesc
          .getAdditionalAttributes()
          .forEach(
              (attrDesc) -> {
                final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());
                partitionKeyJoiner.add(cassAttrDesc.getColumn());
              });
    }

    final String partitionKeys = partitionKeyJoiner.toString();

    final var indexType = streamDesc.getViews().get(0).getIndexType();
    boolean isExactMatch = indexType == null || indexType == IndexType.EXACT_MATCH;

    // For a range query index, only the first entry in partitionKeys is actually the partition key,
    // so we don't wrap the "partitionKeys" by parenthesis in the case.
    return String.format(
        FORMAT_CREATE_TABLE,
        keyspaceName,
        getTableName(),
        COL_TIME_INDEX,
        COL_EVENT_ID,
        columnsBuilder, // columns
        isExactMatch ? "(" : "",
        partitionKeys,
        isExactMatch ? ")" : "",
        COL_EVENT_ID, // keys
        cassTenant.getName(),
        cassTenant.getVersion(),
        streamDesc.getName(),
        streamDesc.getVersion(),
        TfosConfig.featureRecordsDefaultTimeToLive(),
        TfosConfig.eventsGcGraceSeconds(),
        TfosConfig.eventsCompactionConfig(),
        TfosConfig.eventsBloomFilterFpChance());
  }

  @Override
  protected String generateIngestQueryString(final String keyspaceName) {
    final StringBuilder statementBuilder =
        new StringBuilder(
            String.format(
                "INSERT INTO %s.%s (%s, %s",
                keyspaceName, tableName, COL_TIME_INDEX, COL_EVENT_ID));
    final StringBuilder valuesPartBuilder = new StringBuilder(") VALUES (?, ?");

    streamDesc
        .getAttributes()
        .forEach(
            (attrDesc) -> {
              final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());
              statementBuilder.append(", ").append(cassAttrDesc.getColumn());
              valuesPartBuilder.append(", ?");
            });

    statementBuilder.append(valuesPartBuilder).append(")");
    return statementBuilder.toString();
  }

  @Override
  public Statement makeInsertStatement(final Event event) throws ApplicationException {
    // reserved columns are 'time_index' and 'event_id'
    final int numReservedColumns = 2;
    final int numAttributes = listSize(streamDesc.getAttributes());

    final Object[] values = new Object[numReservedColumns + numAttributes];
    int index = 0;
    values[index++] = makeTimeIndex(event.getEventId());
    values[index++] = event.getEventId();
    if (numAttributes > 0) {
      index = populateIngestValues(index, event, streamDesc.getAttributes(), values);
    }

    final PreparedStatement prepared = getPreparedIngest();
    if (prepared != null) {
      return prepared.bind(values);
    } else {
      return new SimpleStatement(getIngestQueryString(), values);
    }
  }

  @Override
  public List<QueryInfo> makeExtractStatements(
      QueryExecutionState state,
      Collection<CassAttributeDesc> attributes,
      final Long startTime,
      final Long endTime) {
    return null;
  }

  /**
   * Make a statement for group view extract query.
   *
   * <p>The method builds statement as follows with binding specified parameters:
   *
   * <pre>
   * SELECT time_index, event_id, &lt;partition_keys&gt;, &lt;aggregates&gt;
   *   FROM &lt;table&gt;
   *     WHERE time_index = ? AND &ltpartition keys spec&gt;
   *       AND event_id &gt;= ? AND event_id &lt; ?
   * </pre>
   *
   * @param partitionKeyColumns Partition key column names. The name should not include the first
   *     (known) partition key, time_index.
   * @param partitionKeys Values of partition keys. The entries should start with time_index value
   *     followed by other partition key values in the same order with parameter
   *     partitionKeyColumns.
   * @param attributes Other attributes to request.
   * @param aggregates Aggregates to retrieve.
   * @param start start time as Epoch time in milliseconds
   * @param end end time as Epoch time in milliseconds
   * @return Group view extraction statement or null if the specified time range is out of scope of
   *     the time index window.
   */
  public Statement makeGroupViewExtractStatement(
      List<String> partitionKeyColumns,
      List<Object> partitionKeys,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      long start,
      long end) {
    if (partitionKeys == null || partitionKeys.isEmpty()) {
      throw new IllegalArgumentException("partitionKeys may not be null or empty");
    }

    // assume the first partition key is time_index
    final long timeIndex = ((Date) partitionKeys.get(0)).getTime();

    // no need to create the statement if the time range is out of scope of the time index window
    final long timeIndexEnd = timeIndex + timeIndexWindowWidth;
    if (end <= timeIndex || start > timeIndexEnd) {
      return null;
    }

    // resolve column names for requested attributes
    final List<String> columns = new ArrayList<>();
    if (attributes != null) {
      attributes.forEach(
          attribute -> {
            final CassAttributeDesc attrDesc = getAttributeDesc(attribute);
            if (attrDesc != null) {
              columns.add(attrDesc.getColumn());
            }
          });
    }

    // build columns based on the request
    StringJoiner columnsJoiner = new StringJoiner(", ");
    columnsJoiner.add(COL_TIME_INDEX);
    columnsJoiner.add(COL_EVENT_ID);
    partitionKeyColumns.forEach(column -> columnsJoiner.add(column));
    columns.forEach(column -> columnsJoiner.add(column));
    if (aggregates != null) {
      aggregates.forEach(
          aggregate -> {
            final MetricFunction func = aggregate.getFunction();
            switch (func) {
              case COUNT:
                columnsJoiner.add("COUNT(" + COL_EVENT_ID + ")");
                break;
              case LAST:
                final CassAttributeDesc attrDesc = getAttributeDesc(aggregate.getBy());
                columnsJoiner.add(attrDesc.getColumn());
                break;
              default:
                final String column = getAttributeDesc(aggregate.getBy()).getColumn();
                columnsJoiner.add(func.name() + "(" + column + ")");
            }
          });
    }

    StringBuilder sb =
        new StringBuilder("SELECT ")
            .append(columnsJoiner)
            .append(" FROM ")
            .append(cassTenant.getKeyspaceName())
            .append(".")
            .append(tableName)
            .append(" WHERE ")
            .append(COL_TIME_INDEX)
            .append(" = ?");
    for (String partitionKey : partitionKeyColumns) {
      sb.append(" AND ").append(partitionKey).append(" = ?");
    }
    List<Object> values = new ArrayList<>(partitionKeys);
    if (start > timeIndex) {
      sb.append(" AND ").append(COL_EVENT_ID).append(" >= ?");
      values.add(UUIDs.startOf(start));
    }
    if (end < timeIndex + timeIndexWindowWidth) {
      sb.append(" AND ").append(COL_EVENT_ID).append(" < ?");
      values.add(UUIDs.startOf(end));
    }
    final Statement statement = new SimpleStatement(sb.toString(), values.toArray());
    logger.trace("view extract statement={} values=", statement, values);
    return statement;
  }

  /**
   * Method to populate ingest view group query result to the provided viewEntries map.
   *
   * <p>The specified row is a result from statement generated by method {@link
   * #makeGroupViewExtractStatement}. That method generates a SELECT statement columns order is as
   * follows:
   *
   * <p>time_index, event_id, &lt;partition keys&gt;, &lt;attributes&gt;, &lt;aggregates&gt;
   *
   * <p>This method does following:
   *
   * <dl>
   *   <li>Fetch the event_id from the specified row and calculate ingest timestamp
   *   <li>Fetch partition keys from the specified row
   *   <li>Fetch specified attributes from the row
   *   <li>Make a group view scope using the partition keys and given view dimensions; Find existing
   *       view entry for the scope
   *   <li>fetch aggregate results and combine them with existing values
   *   <li>Put the built entry to view entries map if necessary.
   * </dl>
   *
   * <p>When the viewEntries map has an existing entry from other queries, attributes of the older
   * event wins and the entry in the map would be replaced. For the aggregate results, existing ones
   * and new ones are combined.
   *
   * @param viewEntries Map of group view scope and its entry
   * @param dimensions Attribute names to form the group view scope
   * @param row Cassandra row to process
   * @param attributes Requested attributes
   * @param aggregates Requested aggregates
   * @param state Execution state
   * @param doSetEventId Flag to determine whether the method shall put event ID to the generated
   *     events. Setting this to false is useful for building summarize operation output.
   */
  public void handleGroupViewQueryRow(
      Map<List<Object>, Event> viewEntries,
      List<String> dimensions,
      Row row,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      QueryExecutionState state,
      boolean doSetEventId) {
    final Event event = state.getEventFactory().create();
    event.setAttributes(new LinkedHashMap<>());

    // fetch event ID and calculate ingest timestamp
    int index = 1;
    final UUID eventId = row.getUUID(index++);
    if (eventId == null) {
      // this may happen when columns include an aggregate function and no rows were hit
      return;
    }
    final long timestamp = UUIDs.unixTimestamp(eventId);
    if (doSetEventId) {
      event.setEventId(eventId);
    }
    event.setIngestTimestamp(new Date(timestamp));

    // fetch partition keys
    for (AttributeDesc partitionKeyDesc : streamDesc.getAdditionalAttributes()) {
      final String name = partitionKeyDesc.getName();
      if (!dimensions.contains(name)) {
        index++;
        continue;
      }
      final Object value = cassandraToDataEngine(row.getObject(index++), partitionKeyDesc);
      event.getAttributes().put(name, value);
    }

    // fetch specified attributes
    if (attributes != null) {
      for (String attr : attributes) {
        CassAttributeDesc desc = getAttributeDesc(attr);
        final String name = desc.getName();
        final Object value = cassandraToDataEngine(row.getObject(desc.getColumn()), desc);
        event.getAttributes().put(name, value);
      }
    }

    // make a group view scope and find existing view entry for the scope
    final List<Object> keys = new ArrayList<>();
    dimensions.forEach(attr -> keys.add(event.getAttributes().get(attr)));
    Event existing = viewEntries.putIfAbsent(keys, event);
    if (existing == null) {
      existing = event;
    }

    // TODO(Naoki): Locking the event is not pretty. We want to queue the retrieved event here and
    // merge them at the end.
    synchronized (existing) {
      if (timestamp < existing.getIngestTimestamp().getTime()) {
        existing.setIngestTimestamp(event.getIngestTimestamp());
      }

      // fetch aggregate results and combine them with existing values
      if (attributes != null) {
        index += attributes.size();
      }
      if (aggregates != null) {
        for (Aggregate aggregate : aggregates) {
          final MetricFunction function = aggregate.getFunction();
          Object value;
          final InternalAttributeType type;
          switch (function) {
            case COUNT:
              value = row.getObject(index++);
              type = InternalAttributeType.LONG;
              break;
            default:
              CassAttributeDesc desc = getAttributeDesc(aggregate.getBy());
              value = cassandraToDataEngine(row.getObject(index++), desc);
              type = desc.getAttributeType();
          }
          final String name = aggregate.getOutputAttributeName();
          final Comparable oldValue = (Comparable) existing.getAttributes().get(name);
          if (oldValue != null) {
            switch (function) {
              case MIN:
                if (oldValue.compareTo(value) < 0) {
                  value = oldValue;
                }
                break;
              case MAX:
                if (oldValue.compareTo(value) > 0) {
                  value = oldValue;
                }
                break;
              case LAST:
                if (timestamp >= existing.getIngestTimestamp().getTime()) {
                  value = oldValue;
                }
                break;
              default:
                // count or sum
                value = type.add(value, oldValue);
            }
          }
          existing.getAttributes().put(name, value);
        }
      }
    }
  }

  @Override
  public Object makePartitionKey(Event event) {
    List<Object> key = new ArrayList<>();
    key.add(Long.valueOf(makeTimeIndex(event.getEventId())));
    if (streamDesc.getAdditionalAttributes() != null) {
      streamDesc
          .getAdditionalAttributes()
          .forEach(
              (attrDesc) -> {
                key.add(event.get(attrDesc.getName()));
              });
    }
    return key;
  }

  public CompletableFuture<List<QueryInfo>> makeExtractFilterWithViewStatements(
      ExtractState state,
      List<CassAttributeDesc> requestedAttributes,
      List<SingleColumnRelation> filter,
      Long startTime,
      Long endTime)
      throws InvalidFilterException {

    state.addHistory("makeStatements{");

    final var columns = new ArrayList<String>();
    columns.add(COL_EVENT_ID);
    for (var attrDesc : requestedAttributes) {
      columns.add(attrDesc.getColumn());
    }

    var filterBuilder = new StringBuilder();
    for (final var relation : filter) {
      filterBuilder.append(" AND ");
      final String dimension = relation.getEntity().rawText();
      final CassAttributeDesc attributeDesc = getAttributeDesc(dimension);
      var columnName = attributeDesc.getColumn();
      filterBuilder.append(columnName);
      final var operator = relation.operator();
      if (Set.of(Operator.EQ, Operator.GT, Operator.GTE, Operator.LT, Operator.LTE)
          .contains(operator)) {
        filterBuilder.append(" ").append(operator).append(" ");
        filterBuilder.append(getFilterValue(relation.getValue().getText(), attributeDesc));
      } else if (operator == Operator.IN) {
        filterBuilder.append(" IN (");
        String delimiter = "";
        for (var term : relation.getInValues()) {
          filterBuilder.append(delimiter).append(getFilterValue(term.getText(), attributeDesc));
          delimiter = ", ";
        }
        filterBuilder.append(")");
      } else {
        throw new InvalidFilterException(
            String.format("Unsupported %s relation: %s", operator, relation));
      }
    }
    final var columnsString = String.join(",", columns);
    final var commonFilterString = filterBuilder.toString();

    final List<QueryInfo> queries = new ArrayList<>();

    final CompletableFuture<DigestSpecifier> scheduleFuture;
    if (streamDesc.getViews().get(0).getWriteTimeIndexing() == Boolean.TRUE) {
      scheduleFuture =
          CompletableFuture.completedFuture(
              new DigestSpecifier(
                  getStreamName(), getStreamVersion(), startTime, endTime, startTime, endTime));
    } else {
      scheduleFuture =
          dataEngine
              .getPostProcessScheduler()
              .getDigestionCoverage(state.getStreamDesc(), streamDesc, state);
    }

    return scheduleFuture.thenApply(
        (coverage) -> {
          final long coveredSince = coverage.getDoneSince();
          final long coveredUntil = coverage.getDoneUntil();

          long left = Math.max(startTime, coveredSince);
          long right = Math.min(endTime, coveredUntil);
          long timeIndex = makeTimeIndex(left);

          while (left < right) {
            final var where = new StringBuilder(COL_TIME_INDEX).append(" = ").append(timeIndex);
            where.append(commonFilterString);
            long queryLeft = timeIndex;
            if (left > timeIndex) {
              where.append(" AND ").append(COL_EVENT_ID).append(" >= ").append(UUIDs.startOf(left));
              queryLeft = left;
            }
            timeIndex += timeIndexWindowWidth;
            long queryRight = timeIndex;
            if (right < timeIndex) {
              where.append(" AND ").append(COL_EVENT_ID).append(" < ").append(UUIDs.startOf(right));
              queryRight = right;
            }
            left = timeIndex;

            final var statement =
                String.format(
                    "SELECT %s FROM %s.%s WHERE %s ALLOW FILTERING",
                    columnsString, keyspaceName, tableName, where);
            logger.debug("STATEMENT: {}", statement);
            queries.add(
                new QueryInfo(
                    statement, new Range(queryLeft, queryRight), this, requestedAttributes));
          }

          state.addHistory("}");
          return queries;
        });
  }
}
