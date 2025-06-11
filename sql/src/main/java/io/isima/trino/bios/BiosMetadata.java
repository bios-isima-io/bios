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
package io.isima.trino.bios;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.IndexType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import org.apache.commons.lang3.tuple.Triple;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.isima.trino.bios.BiosClient.COLUMN_PARAM_DURATION_OFFSET_SECONDS;
import static io.isima.trino.bios.BiosClient.COLUMN_PARAM_DURATION_SECONDS;
import static io.isima.trino.bios.BiosClient.COLUMN_PARAM_WINDOW_SIZE_SECONDS;
import static io.isima.trino.bios.BiosClient.COLUMN_SIGNAL_TIMESTAMP;
import static io.isima.trino.bios.BiosClient.COLUMN_SIGNAL_TIME_EPOCH_MS;
import static io.isima.trino.bios.BiosClient.COLUMN_WINDOW_BEGIN_EPOCH;
import static io.isima.trino.bios.BiosClient.COLUMN_WINDOW_BEGIN_TIMESTAMP;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static java.util.Objects.requireNonNull;

public class BiosMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(BiosMetadata.class);

    private final BiosClient biosClient;
    private final ConnectorTableProperties connectorTableProperties;

    @Inject
    public BiosMetadata(BiosClient biosClient)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
        connectorTableProperties = new ConnectorTableProperties();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        // logger.debug("session %s, identity %s, principal %s", session, session.getIdentity(),
        //         session.getIdentity().getPrincipal());
        return ImmutableList.copyOf(biosClient.getSchemaNames());
    }

    @Override
    public BiosTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        // logger.debug("getTableHandle");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            logger.debug("getTableHandle called for non-existent schema.");
            throw new SchemaNotFoundException(tableName.getSchemaName());
        }

        return biosClient.getTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        // logger.debug("getTableMetadata");
        return getTableMetadata(session, ((BiosTableHandle) table).toSchemaTableName());
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                    SchemaTableName schemaTableName)
    {
        // logger.debug("getTableMetadata");
        if (!listSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            logger.debug("getTableMetadata called for non-existent schema.");
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        var columnHandles = biosClient.getColumnHandles(schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
        if (columnHandles == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        List<ColumnMetadata> columns = columnHandles.stream()
                .map(BiosColumnHandle::getColumnMetadata)
                .collect(Collectors.toUnmodifiableList());

        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        logger.debug("listTables");
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(biosClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : biosClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // logger.debug("getColumnHandles");
        BiosTableHandle biosTableHandle = (BiosTableHandle) tableHandle;

        var columns = biosClient.getColumnHandles(biosTableHandle.getSchemaName(),
                biosTableHandle.getTableName());
        if (columns == null) {
            throw new TableNotFoundException(biosTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (BiosColumnHandle column : columns) {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        logger.debug("listTableColumns");
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName schemaTableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, schemaTableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(schemaTableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        // logger.debug("getColumnMetadata");
        return ((BiosColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        // logger.debug("getTableProperties");
        return connectorTableProperties;
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        BiosTableHandle tableHandle = (BiosTableHandle) handle;
        Long timeRangeStart = tableHandle.getTimeRangeStart();
        Long timeRangeDelta = tableHandle.getTimeRangeDelta();
        Long windowSizeSeconds = tableHandle.getWindowSizeSeconds();
        Long durationSeconds = tableHandle.getDurationSeconds();
        Long durationOffsetSeconds = tableHandle.getDurationOffsetSeconds();
        boolean somePushdownApplied = false;
        Map<ColumnHandle, Domain> remainingDomains = new HashMap<>();

        if (constraint.getSummary().getDomains().isEmpty()) {
            return Optional.empty();
        }
        var regularColumnConditions = new ArrayList<Entry<ColumnHandle, Domain>>();

        // Push down virtual columns and collect entries for regular columns into a list.
        for (var entry : constraint.getSummary().getDomains().orElseGet(() -> Map.of()).entrySet()) {
            String columnName = ((BiosColumnHandle) entry.getKey()).getColumnName();

            switch (columnName) {
                case COLUMN_SIGNAL_TIMESTAMP:
                case COLUMN_SIGNAL_TIME_EPOCH_MS:
                case COLUMN_WINDOW_BEGIN_EPOCH:
                case COLUMN_WINDOW_BEGIN_TIMESTAMP:
                    // If we have not already set the time range, pushdown time range.
                    if ((timeRangeStart == null)) {
                        long timeRangeLow;
                        long timeRangeHigh;
                        double scalingFactor = 1;   // To convert to milliseconds.
                        switch (columnName) {
                            case COLUMN_SIGNAL_TIMESTAMP:
                            case COLUMN_WINDOW_BEGIN_TIMESTAMP:
                                // Trino uses microseconds since epoch for timestamps, whereas
                                // bios v1 uses milliseconds.
                                scalingFactor = 1.0 / 1000;
                                break;
                            case COLUMN_WINDOW_BEGIN_EPOCH:
                                scalingFactor = 1000;
                                break;
                        }

                        // Currently, we only support pushdown of single range predicates on signal timestamp.
                        var valueSet = entry.getValue().getValues();
                        if (!(valueSet instanceof SortedRangeSet)) {
                            continue;
                        }
                        var sortedRangeSet = (SortedRangeSet) valueSet;
                        if ((sortedRangeSet.getRangeCount() != 1) || sortedRangeSet.isAll() || sortedRangeSet.isNone() || sortedRangeSet.isDiscreteSet()) {
                            continue;
                        }
                        var range = sortedRangeSet.getOrderedRanges().get(0);
                        if (range.getLowValue().isPresent()) {
                            timeRangeLow =
                                    (long) ((Long) range.getLowValue().get() * scalingFactor);
                            // logger.debug("timeRangeLow provided: %d", timeRangeLow);
                        }
                        else {
                            timeRangeLow = 0L;
                        }
                        if (range.getHighValue().isPresent()) {
                            timeRangeHigh =
                                    (long) ((Long) range.getHighValue().get() * scalingFactor);
                            // logger.debug("timeRangeHigh provided: %d", timeRangeHigh);
                        }
                        else {
                            timeRangeHigh = System.currentTimeMillis();
                        }
                        timeRangeStart = timeRangeLow;
                        timeRangeDelta = timeRangeHigh - timeRangeLow;
                        somePushdownApplied = true;
                        logger.debug("pushdown filter: timeRangeStart %d, timeRangeDelta %d",
                                timeRangeStart, timeRangeDelta);
                    }
                    break;

                case COLUMN_PARAM_WINDOW_SIZE_SECONDS:
                    windowSizeSeconds = getLongValuePredicate(entry.getValue(),
                            COLUMN_PARAM_WINDOW_SIZE_SECONDS);
                    somePushdownApplied = true;
                    logger.debug("pushdown filter: windowSizeSeconds %d", windowSizeSeconds);
                    break;

                case COLUMN_PARAM_DURATION_SECONDS:
                    durationSeconds = getLongValuePredicate(entry.getValue(),
                        COLUMN_PARAM_DURATION_SECONDS);
                    somePushdownApplied = true;
                    logger.debug("pushdown filter: durationSeconds %d", durationSeconds);
                    break;

                case COLUMN_PARAM_DURATION_OFFSET_SECONDS:
                    durationOffsetSeconds = getLongValuePredicate(entry.getValue(),
                        COLUMN_PARAM_DURATION_OFFSET_SECONDS);
                    somePushdownApplied = true;
                    logger.debug("pushdown filter: durationOffsetSeconds %d", durationOffsetSeconds);
                    break;

                default:
                    // This is not a virtual column - it is a regular column.
                    regularColumnConditions.add(entry);
                    break;
            }
        }

        // Next try to push down filters on regular columns.
        String where = null;
        if (tableHandle.getTableKind() == BiosTableKind.CONTEXT_RAW) {
            where = pushdownContextRaw(tableHandle, constraint, regularColumnConditions, remainingDomains);
            if (where != null) {
                somePushdownApplied = true;
            }
        }
        else if (tableHandle.getTableKind() == BiosTableKind.SIGNAL_RAW) {
            where = pushdownSignalRaw(tableHandle, constraint, regularColumnConditions, remainingDomains);
            if (where != null) {
                somePushdownApplied = true;
            }
        }
        else {
            logger.debug("Currently only contexts_raw and signals_raw are supported for pushdown");
            for (var entry : regularColumnConditions) {
                remainingDomains.put(entry.getKey(), entry.getValue());
            }
        }

        if (somePushdownApplied) {
            var newTableHandle = new BiosTableHandle(tableHandle.getSchemaName(),
                    tableHandle.getTableName(), timeRangeStart, timeRangeDelta, windowSizeSeconds,
                    durationSeconds, durationOffsetSeconds, tableHandle.getGroupBy(), where);
            logger.debug("pushdown filter done; tableHandle: %s  constraint: %s  Assignments: %s",
                    newTableHandle, constraint.toString(), constraint.getAssignments());

            TupleDomain<ColumnHandle> remainingFilter = withColumnDomains(remainingDomains);
            return Optional.of(
                    new ConstraintApplicationResult<>(newTableHandle, remainingFilter, false));
        }
        else {
            return Optional.empty();
        }
    }

    private String pushdownContextRaw(BiosTableHandle tableHandle, Constraint constraint,
            List<Entry<ColumnHandle, Domain>> regularColumnConditions,
            Map<ColumnHandle, Domain> remainingDomains)
    {
        String where = null;
        if (regularColumnConditions.isEmpty()) {
            return null;
        }

        // First process each index in the table:
        //  1. Check whether it can be used to satisfy the query (includes all columns)
        //  2. Gather the conditions that can be pushed down and create a where clause string
        //  3. Store the remaining conditions in a variable
        //  4. Score the pushdown: 2 points for an exact match clause, 1 point for range query
        //  5. Store the score, where clause string, and remaining conditions in a map entry associated with that index
        //
        // Next find the index with the highest score (if any) and apply pushdown for that index
        // by using the associated where clause string add the remaining conditions to
        // remainingDomains map.

        // Create a list of all attributes needed by the query.
        final List<String> attributesNeeded = constraint.getAssignments().values().stream()
                .map(ch -> (BiosColumnHandle) ch)
                .filter(ch -> !ch.getIsVirtual())
                .map(ch -> ch.getColumnName())
                .collect(Collectors.toList());
        List<String> neededAttributes = new ArrayList<>();
        for (var entry : regularColumnConditions) {
            String columnName = ((BiosColumnHandle) entry.getKey()).getColumnName();
            neededAttributes.add(columnName);
        }

        // Create a map of index name to a tuple of (score, where clause string, remaining conditions)
        Map<String, Triple<Integer, String, Map<ColumnHandle, Domain>>> indexResults = new HashMap<>();

        // Get the table, and loop through the list of indexes for this table.
        ContextConfig contextConfig = biosClient.getContextConfig(tableHandle.getSchemaName(),
                tableHandle.getTableName());
        if (contextConfig == null) {
            throw new TableNotFoundException(new SchemaTableName(tableHandle.getSchemaName(),
                tableHandle.getTableName()));
        }
        if (contextConfig.getFeatures() == null) {
            return null;
        }
        for (var featureConfig : contextConfig.getFeatures()) {
            if ((featureConfig.getDimensions() == null) ||
                    (featureConfig.getDimensions().size() == 0)) {
                continue;
            }

            // Check whether the index contains all the attributes needed by the query.
            boolean allNeededAttributesPresent = true;
            for (final var attributeNeeded : attributesNeeded) {
                if (contextConfig.getPrimaryKey().contains(attributeNeeded)) {
                    continue;
                }
                if (featureConfig.getDimensions().contains(attributeNeeded)) {
                    continue;
                }
                if ((featureConfig.getAttributes() != null) &&
                        featureConfig.getAttributes().contains(attributeNeeded)) {
                    continue;
                }
                // This attribute is not included in this feature; skip this feature.
                allNeededAttributesPresent = false;
                break;
            }
            if (!allNeededAttributesPresent) {
                continue;
            }

            Map<ColumnHandle, Domain> remainingConditions = new HashMap<>();
            for (var entry : regularColumnConditions) {
                remainingConditions.put(entry.getKey(), entry.getValue());
            }
            if (featureConfig.getIndexType() == IndexType.EXACT_MATCH) {
                // All the dimensions must be present in the query with equality values.
                boolean allDimensionsPresent = true;
                int score = 0;
                List<String> whereParts = new ArrayList<>();
                for (String dimension : featureConfig.getDimensions()) {
                    boolean dimensionPresent = false;
                    for (var entry : regularColumnConditions) {
                        String columnName = ((BiosColumnHandle) entry.getKey()).getColumnName();
                        if (!columnName.equals(dimension)) {
                            continue;
                        }
                        var attributeConfig = biosClient.getAttributeConfig(tableHandle.getSchemaName(),
                                tableHandle.getTableName(), columnName);
                        SortedRangeSet valueSet = (SortedRangeSet) entry.getValue().getValues();
                        if (valueSet.isDiscreteSet()) {
                            dimensionPresent = true;
                            score += 2;
                            remainingConditions.remove(entry.getKey());
                            final String wherePart = getWherePartDiscreteValues(attributeConfig, valueSet);
                            whereParts.add(wherePart);
                            break;
                        }
                    }
                    if (!dimensionPresent) {
                        allDimensionsPresent = false;
                        break;
                    }
                }
                if (!allDimensionsPresent) {
                    continue;
                }
                indexResults.put(featureConfig.getName(),
                        Triple.of(score,
                                String.join(" AND ", whereParts),
                                remainingConditions));
            }
            else if (featureConfig.getIndexType() == IndexType.RANGE_QUERY) {
                // One or more dimensions from the beginning of the list must be present.
                List<String> whereParts = new ArrayList<>();
                int score = 0;
                for (String dimension : featureConfig.getDimensions()) {
                    boolean continueToNextDimension = false;
                    for (var entry : regularColumnConditions) {
                        String columnName = ((BiosColumnHandle) entry.getKey()).getColumnName();
                        if (!columnName.equals(dimension)) {
                            continue;
                        }
                        var attributeConfig = biosClient.getAttributeConfig(tableHandle.getSchemaName(),
                                tableHandle.getTableName(), columnName);
                        SortedRangeSet valueSet = (SortedRangeSet) entry.getValue().getValues();
                        if (valueSet.isDiscreteSet()) {
                            continueToNextDimension = true;
                            score += 2;
                            remainingConditions.remove(entry.getKey());
                            final String wherePart = getWherePartDiscreteValues(attributeConfig, valueSet);
                            whereParts.add(wherePart);
                        }
                        else if (valueSet.getRangeCount() == 1) {
                            // bi(OS) only supports a single range - the where clause doesn't support OR.

                            continueToNextDimension = false;
                            score += 1;
                            remainingConditions.remove(entry.getKey());
                            final String wherePart = getWherePartInequality(attributeConfig, valueSet);
                            whereParts.add(wherePart);
                        }
                    }
                    if (!continueToNextDimension) {
                        break;
                    }
                }
                if (whereParts.isEmpty()) {
                    continue;
                }
                indexResults.put(featureConfig.getName(),
                        Triple.of(score,
                                String.join(" AND ", whereParts),
                                remainingConditions));
            }
        }

        // Find the index with the highest score.
        String bestIndex = null;
        int bestScore = 0;
        for (var entry : indexResults.entrySet()) {
            logger.debug("index %s: score %d, where %s",
                    entry.getKey(), entry.getValue().getLeft(),
                    entry.getValue().getMiddle());
            if (entry.getValue().getLeft() > bestScore) {
                bestIndex = entry.getKey();
                bestScore = entry.getValue().getLeft();
            }
        }

        // If we found a good index, apply the pushdown.
        if (bestIndex != null) {
            where = indexResults.get(bestIndex).getMiddle();
            remainingDomains.putAll(indexResults.get(bestIndex).getRight());
            logger.debug("pushdown filter: %s", where);
        }

        return where;
    }

    private String pushdownSignalRaw(BiosTableHandle tableHandle, Constraint constraint,
            List<Entry<ColumnHandle, Domain>> regularColumnConditions,
            Map<ColumnHandle, Domain> remainingDomains)
    {
        String where = null;
        if (regularColumnConditions.isEmpty()) {
            return null;
        }

        // First add all the given conditions as remaining conditions; we will remove each one
        // as we add it to the where clause.
        Map<ColumnHandle, Domain> remainingConditions = new HashMap<>();
        for (var entry : regularColumnConditions) {
            remainingConditions.put(entry.getKey(), entry.getValue());
        }

        // Bios only supports pushing down where clauses with "and" connectors, no "or" allowed.
        // Currently Trino also only allows pushing down "and" clauses, so we don't need to handle
        // "or" clauses here.
        List<String> whereParts = new ArrayList<>();
        for (var entry : regularColumnConditions) {
            String columnName = ((BiosColumnHandle) entry.getKey()).getColumnName();
            var attributeConfig = biosClient.getAttributeConfig(tableHandle.getSchemaName(),
                    tableHandle.getTableName(), columnName);
            SortedRangeSet valueSet = (SortedRangeSet) entry.getValue().getValues();
            if (valueSet.isSingleValue()) {
                // For equality clauses, bios only supports a single value, not a list of values.
                remainingConditions.remove(entry.getKey());
                final var value = valueSet.getSingleValue();
                whereParts.add(String.format("%s = %s", attributeConfig.getName(), valueToString(attributeConfig, value)));
            }
            else if (valueSet.getRangeCount() == 1) {
                // bi(OS) only supports a single range - the where clause doesn't support OR.
                remainingConditions.remove(entry.getKey());
                final String wherePart = getWherePartInequality(attributeConfig, valueSet);
                whereParts.add(wherePart);
            }
        }

        if (whereParts.isEmpty()) {
            return null;
        }

        where = String.join(" AND ", whereParts);
        remainingDomains.putAll(remainingConditions);
        logger.debug("pushdown filter: %s", where);

        return where;
    }

    private String getWherePartDiscreteValues(AttributeConfig attributeConfig, SortedRangeSet valueSet)
    {
        final var values = valueSet.getDiscreteSet();
        final List<String> valueStrings = values.stream()
                .map(value -> valueToString(attributeConfig, value))
                .collect(Collectors.toList());
        final String wherePart = String.format("%s IN (%s)", attributeConfig.getName(), String.join(", ", valueStrings));
        return wherePart;
    }

    private String getWherePartInequality(AttributeConfig attributeConfig, SortedRangeSet valueSet)
    {
        final var range = valueSet.getOrderedRanges().get(0);
        final String wherePart;
        if (range.isLowUnbounded()) {
            final String optionalEquals = range.isHighInclusive() ? "=" : "";
            wherePart = String.format("%s <%s %s", attributeConfig.getName(), optionalEquals,
                    valueToString(attributeConfig, range.getHighBoundedValue()));
        }
        else if (range.isHighUnbounded()) {
            final String optionalEquals = range.isLowInclusive() ? "=" : "";
            wherePart = String.format("%s >%s %s", attributeConfig.getName(), optionalEquals,
                    valueToString(attributeConfig, range.getLowBoundedValue()));
        }
        else {
            final String optionalLowEquals = range.isLowInclusive() ? "=" : "";
            final String optionalHighEquals = range.isHighInclusive() ? "=" : "";
            wherePart = String.format("%s >%s %s AND %s <%s %s", attributeConfig.getName(),
                    optionalLowEquals, valueToString(attributeConfig, range.getLowBoundedValue()),
                    attributeConfig.getName(), optionalHighEquals,
                    valueToString(attributeConfig, range.getHighBoundedValue()));
        }
        return wherePart;
    }

    private String valueToString(AttributeConfig attributeConfig, final Object value)
    {
        final String valueString;
        if (attributeConfig.getType() == AttributeType.STRING) {
            var valueSlice = (Slice) value;
            valueString = String.format("'%s'", valueSlice.toStringUtf8());
        }
        else {
            valueString = value.toString();
        }
        return valueString;
    }

    private long getLongValuePredicate(final Domain domain, final String columnName)
    {
        if (!domain.isSingleValue()) {
            logger.debug("columnName %s domain %s", columnName, domain);
            throw new TrinoException(GENERIC_USER_ERROR, columnName +
                    " can only be equated to a single value.");
        }
        return (Long) domain.getSingleValue();
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        BiosTableHandle tableHandle = (BiosTableHandle) handle;
        if ((tableHandle.getTableKind() != BiosTableKind.CONTEXT) &&
                (tableHandle.getTableKind() != BiosTableKind.SIGNAL)) {
            return Optional.empty();
        }

        // Group by has already been applied once, cannot aggregate again.
        if (tableHandle.getGroupBy() != null) {
            return Optional.empty();
        }

        // Not sure how to handle multiple grouping sets.
        if (groupingSets.size() >= 2) {
            return Optional.empty();
        }

        // logger.debug("applyAggregation %s: aggregates: %s  assignments: %s  groupingSets:%s",
        //         tableHandle.toSchemaTableName(), aggregates, assignments, groupingSets);

        List<ConnectorExpression> outProjections = new ArrayList<>();
        List<Assignment> outAssignments = new ArrayList<>();
        List<String> internalAggregateNames = new ArrayList<>();
        for (var aggregate : aggregates) {
            if (!biosClient.isFeatureSupportedAggregate(aggregate.getFunctionName())) {
                return Optional.empty();
            }
            String name;
            String aggregateSourceName;
            if (aggregate.getFunctionName().equalsIgnoreCase("count")) {
                name = "count()";
                aggregateSourceName = null;
            }
            else {
                if (aggregate.getArguments().size() != 1) {
                    throw new TrinoException(GENERIC_USER_ERROR, aggregate.getFunctionName() +
                            " requires exactly 1 input, got " +
                            aggregate.getArguments().size());
                }
                var input = (Variable) aggregate.getArguments().get(0);
                // Get the real case-sensitive name of the input column.
                var attributeConfig = biosClient.getAttributeConfig(tableHandle.getSchemaName(),
                        tableHandle.getTableName(), input.getName());
                aggregateSourceName = attributeConfig.getName();
                name = aggregate.getFunctionName().toLowerCase(Locale.getDefault()) + "(" +
                        aggregateSourceName + ")";
            }
            outProjections.add(new Variable(name, aggregate.getOutputType()));
            outAssignments.add(new Assignment(name,
                    new BiosColumnHandle(name, aggregate.getOutputType(), null, false,
                            aggregate.getFunctionName(), aggregateSourceName),
                    aggregate.getOutputType()));
            internalAggregateNames.add(name);
        }

        List<String> groupBy = null;
        if (groupingSets.size() > 0) {
            groupBy = groupingSets.get(0).stream()
                    .filter(ch -> !((BiosColumnHandle) ch).getIsVirtual())
                    .map(ch -> ((BiosColumnHandle) ch).getColumnName())
                    .collect(Collectors.toList());
        }
        var outTableHandle = new BiosTableHandle(tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getTimeRangeStart(), tableHandle.getTimeRangeDelta(),
                tableHandle.getWindowSizeSeconds(), tableHandle.getDurationSeconds(),
                tableHandle.getDurationOffsetSeconds(), groupBy, tableHandle.getWhere());

        logger.debug("pushdown aggregates: %s,  groupBy: %s", internalAggregateNames, groupBy);

        var outResult = new AggregationApplicationResult<ConnectorTableHandle>(outTableHandle,
                outProjections, outAssignments, new HashMap<>(), false);
        return Optional.of(outResult);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        // BiosTableHandle tableHandle = (BiosTableHandle) handle;
        // logger.debug("applyProjection %s: projections: %s  assignments: %s",
        //         tableHandle, projections, assignments);

        return Optional.empty();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        // BiosTableHandle tableHandle = (BiosTableHandle) handle;
        // logger.debug("applyLimit %s: limit: %d", tableHandle, limit);

        return Optional.empty();
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        // BiosTableHandle tableHandle = (BiosTableHandle) handle;
        // logger.debug("applyTopN %s: topNCount: %d  sortItems: %s  assignments: %s",
        //         tableHandle, topNCount, sortItems, assignments);

        return Optional.empty();
    }
}
