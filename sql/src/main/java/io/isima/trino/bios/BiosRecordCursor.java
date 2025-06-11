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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.isima.bios.sdk.errors.BiosClientError.GENERIC_SERVER_ERROR;
import static io.isima.trino.bios.BiosClient.COLUMN_CONTEXT_TIMESTAMP;
import static io.isima.trino.bios.BiosClient.COLUMN_CONTEXT_TIME_EPOCH_MS;
import static io.isima.trino.bios.BiosClient.COLUMN_PARAM_DURATION_OFFSET_SECONDS;
import static io.isima.trino.bios.BiosClient.COLUMN_PARAM_DURATION_SECONDS;
import static io.isima.trino.bios.BiosClient.COLUMN_PARAM_WINDOW_SIZE_SECONDS;
import static io.isima.trino.bios.BiosClient.COLUMN_SIGNAL_TIMESTAMP;
import static io.isima.trino.bios.BiosClient.COLUMN_SIGNAL_TIME_EPOCH_MS;
import static io.isima.trino.bios.BiosClient.COLUMN_WINDOW_BEGIN_EPOCH;
import static io.isima.trino.bios.BiosClient.COLUMN_WINDOW_BEGIN_TIMESTAMP;
import static io.isima.trino.bios.BiosClient.SKETCH_COUNT_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class BiosRecordCursor
        implements RecordCursor
{
    private static final Logger logger = Logger.get(BiosRecordCursor.class);

    private final BiosClient biosClient;
    private final BiosTableHandle tableHandle;
    private final List<BiosColumnHandle> columnHandles;
    private final BiosSplit biosSplit;
    private final long timeRangeStart;
    private final long timeRangeEnd;

    private boolean queryStarted;
    private Iterator<DataWindow> windows;
    private Iterator<? extends Record> records;
    private DataWindow currentWindow;
    private Record currentRecord;
    private List<Object> allContextKeys;
    private int keysQueried;

    public BiosRecordCursor(BiosClient biosClient, BiosTableHandle tableHandle,
                            List<BiosColumnHandle> columnHandles, BiosSplit biosSplit)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.biosSplit = requireNonNull(biosSplit, "biosSplit is null");

        this.timeRangeStart = biosClient.getEffectiveTimeRangeStart(tableHandle);
        this.timeRangeEnd =
                this.timeRangeStart + biosClient.getEffectiveTimeRangeDelta(tableHandle);
        this.queryStarted = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    private boolean isContextWithFilter()
    {
        return (BiosTableKind.getTableKind(tableHandle.getSchemaName()) == BiosTableKind.CONTEXT_RAW) &&
                (tableHandle.getWhere() != null);
    }

    private boolean isContextRawWithoutFilter()
    {
        return (BiosTableKind.getTableKind(tableHandle.getSchemaName()) == BiosTableKind.CONTEXT_RAW) &&
                (tableHandle.getWhere() == null);
    }

    private boolean isContext()
    {
        var tableKind = BiosTableKind.getTableKind(tableHandle.getSchemaName());
        return ((tableKind == BiosTableKind.CONTEXT) ||
                (tableKind == BiosTableKind.CONTEXT_RAW) ||
                (tableKind == BiosTableKind.CONTEXT_SKETCHES));
    }

    private boolean isWithinRequestedTimeRange(long recordTime)
    {
        // For raw signals, the window time is set to 0; we need to check the record time instead.
        // For features, the record time is set to null; we need to check the window time instead.
        if (recordTime == 0) {
            return true;
        }
        return (recordTime >= timeRangeStart) && (recordTime < timeRangeEnd);
    }

    @Override
    public boolean advanceNextPosition()
    {
        boolean morePresent;
        boolean matchesTimeRange = false;

        do {
            morePresent = advanceNextPositionInternal();
            if (morePresent) {
                // For raw signals, check the time range. For features/sketches window time is
                // already checked, and time range is not applicable to contexts.
                if (tableHandle.getTableKind() == BiosTableKind.SIGNAL_RAW) {
                    matchesTimeRange = isWithinRequestedTimeRange(currentRecord.getTimestamp());
                }
                else {
                    matchesTimeRange = true;
                }
            }
        }
        while (morePresent && !matchesTimeRange);

        return morePresent;
    }

    private boolean advanceNextPositionInternal()
    {
        // Start running the query if it has not already been started.
        if (!queryStarted) {
            startQuery();
        }

        if (records == null) {
            // We have no more records left.
            return false;
        }

        // Get the next record if present.

        // If the current set of records is exhausted, get the next one.
        if (!records.hasNext()) {
            final boolean moreRecordsPresent;

            if (isContextRawWithoutFilter()) {
                moreRecordsPresent = moveToNextContextRawBatch();
            }
            else if (isContext()) {
                // For context sketches / features / indexes there is no batching currently.
                moreRecordsPresent = false;
            }
            else {
                // Signals and derivatives have time windows - move to the next one.
                moreRecordsPresent = moveToNextApplicableWindow();
            }
            if (!moreRecordsPresent) {
                records = null;
                return false;
            }
        }
        currentRecord = records.next();
        return true;
    }

    private void startQuery()
    {
        if (isContextRawWithoutFilter()) {
            startQueryContextRaw();
        }
        else if (isContextWithFilter()) {
            startQueryContextWithFilter();
        }
        else {
            startQueryNonContextRaw();
        }
        queryStarted = true;
    }

    private void startQueryContextRaw()
    {
        // If there is no filter, contexts only support listing the primary key attribute directly -
        // we cannot list all the attributes of all context entries directly. First get all the
        // primary key values, and then issue a second query to get all the attributes for each of
        // those keys.

        BiosQuery query = new BiosQuery(tableHandle.getSchemaName(), tableHandle.getTableName(),
                null, null, null, null, null, null, null, null, null);
        final ISqlResponse preliminaryResponse;
        try {
            preliminaryResponse = biosClient.getQueryResponse(query);
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof BiosClientException) {
                BiosClientException biosClientException = (BiosClientException) e.getCause();
                if (biosClientException.getCode() == GENERIC_SERVER_ERROR) {
                    throw new TrinoException(NOT_SUPPORTED,
                        "Context likely has too many entries to list; querying large contexts without filter is not supported yet by bi(OS) Trino connector.");
                }
            }
            throw e;
        }
        var keyRecords = preliminaryResponse.getRecords();

        var attributes = biosClient.getAttributeConfigs(tableHandle.getSchemaName(),
                tableHandle.getTableName());
        String keyColumnName = attributes.get(0).getName();
        var keyColumnType = attributes.get(0).getType();
        switch (keyColumnType) {
            case STRING:
                allContextKeys = keyRecords.stream()
                    .map(r -> r.getAttribute(keyColumnName).asString())
                    .collect(Collectors.toList());
                break;

            case INTEGER:
                allContextKeys = keyRecords.stream()
                    .map(r -> r.getAttribute(keyColumnName).asLong())
                    .collect(Collectors.toList());
                break;

            case DECIMAL:
                allContextKeys = keyRecords.stream()
                    .map(r -> r.getAttribute(keyColumnName).asDouble())
                    .collect(Collectors.toList());
                break;

            case BOOLEAN:
                allContextKeys = keyRecords.stream()
                    .map(r -> r.getAttribute(keyColumnName).asBoolean())
                    .collect(Collectors.toList());
                break;

            case BLOB:
                allContextKeys = keyRecords.stream()
                    .map(r -> r.getAttribute(keyColumnName).asByteArray())
                    .collect(Collectors.toList());
                break;

            default:
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "Unsupported context key type: " + keyColumnType);
        }

        // We got all the keys in the context.
        // We need to get the full records in batches, to fit within the response size limits.
        // Get the first batch now.
        keysQueried = 0;
        moveToNextContextRawBatch();
    }

    private boolean moveToNextContextRawBatch()
    {
        while (keysQueried < allContextKeys.size()) {
            int keysToQueryUpperBoundExclusive = Math.min(allContextKeys.size(),
                    keysQueried + biosClient.getBiosConfig().getContextQueryBatchSizeInRows());
            String contextKeyRange = String.format("[%d, %d)", keysQueried,
                    keysToQueryUpperBoundExclusive);
            List<Object> contextKeysToBeQueried = allContextKeys.subList(keysQueried,
                    keysToQueryUpperBoundExclusive);
            BiosQuery query = new BiosQuery(tableHandle.getSchemaName(), tableHandle.getTableName(),
                    null, null, null, null, null, null, null, contextKeyRange, contextKeysToBeQueried);

            final ISqlResponse response = biosClient.getQueryResponse(query);
            keysQueried = keysToQueryUpperBoundExclusive;
            records = response.getRecords().iterator();
            if (records.hasNext()) {
                return true;
            }
        }
        return false;
    }

    private void startQueryContextWithFilter()
    {
        List<String> attributes = columnHandles.stream()
                .filter(ch -> !ch.getIsVirtual())
                .filter(ch -> !ch.getIsAggregate())
                .map(BiosColumnHandle::getColumnName)
                .collect(Collectors.toList());
        BiosQuery query = new BiosQuery(tableHandle.getSchemaName(), tableHandle.getTableName(),
                null, null, null, null, tableHandle.getWhere(), attributes, null, null, null);
        final ISqlResponse response = biosClient.getQueryResponse(query);
        records = response.getRecords().iterator();
    }

    private void startQueryNonContextRaw()
    {
        List<String> attributes = columnHandles.stream()
                .filter(ch -> !ch.getIsVirtual())
                .filter(ch -> !ch.getIsAggregate())
                .filter(ch -> !ch.getIsSketch())
                .map(BiosColumnHandle::getColumnName)
                .collect(Collectors.toList());
        List<String> simpleAggregates = columnHandles.stream()
                .filter(ch -> !ch.getIsVirtual())
                .filter(ch -> ch.getIsAggregate())
                .map(ch ->
                    ch.getAggregateFunction().toLowerCase(Locale.getDefault())
                    + "("
                    + (ch.getAggregateSource() == null ? "" : ch.getAggregateSource())
                    + ")")
                .collect(Collectors.toList());
        List<String> sketches = columnHandles.stream()
                .filter(ch -> !ch.getIsVirtual())
                .filter(ch -> !ch.getIsAggregate())
                .filter(ch -> ch.getIsSketch())
                .map(ch ->
                    ch.getSketchFunction().toLowerCase(Locale.getDefault())
                    + "("
                    + ch.getSketchSource()
                    + ") as "
                    + ch.getColumnName())
                .collect(Collectors.toList());
        final List<String> aggregates;
        if ((tableHandle.getTableKind() == BiosTableKind.CONTEXT_SKETCHES) ||
                (tableHandle.getTableKind() == BiosTableKind.SIGNAL_SKETCHES)) {
            if (!simpleAggregates.isEmpty()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Aggregate functions "
                    + "found with sketch table: " + tableHandle.getTableName()
                    + "; aggregates: " + simpleAggregates);
            }
            // If no column has been specified, add the simple count.
            if (sketches.isEmpty()) {
                sketches.add("count() as " + SKETCH_COUNT_COLUMN_NAME);
            }
            aggregates = sketches;
        }
        else {
            aggregates = simpleAggregates;
        }

        BiosQuery query = new BiosQuery(tableHandle.getSchemaName(), tableHandle.getTableName(),
                biosSplit.getTimeRangeStart(), biosSplit.getTimeRangeDelta(),
                biosClient.getEffectiveWindowSizeSeconds(tableHandle),
                tableHandle.getGroupBy(), tableHandle.getWhere(), attributes, aggregates, null, null);
        ISqlResponse response = biosClient.getQueryResponse(query);

        if (isContext()) {
            records = response.getRecords().iterator();
        }
        else {
            windows = response.getDataWindows().iterator();
            moveToNextApplicableWindow();
        }
    }

    private boolean moveToNextApplicableWindow()
    {
        while (windows.hasNext()) {
            currentWindow = windows.next();
            if (!isWithinRequestedTimeRange(currentWindow.getWindowBeginTime())) {
                continue;
            }
            records = currentWindow.getRecords().iterator();
            if (records.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asBoolean();
    }

    @Override
    public long getLong(int field)
    {
        String columnName = columnHandles.get(field).getColumnName();

        switch (columnName.toLowerCase(Locale.getDefault())) {
            case COLUMN_SIGNAL_TIMESTAMP:
            case COLUMN_CONTEXT_TIMESTAMP:
                // bios v1 uses milliseconds since epoch, but Trino uses
                // microseconds since epoch for timestamps; convert to micros.
                return currentRecord.getTimestamp() * 1000;

            case COLUMN_SIGNAL_TIME_EPOCH_MS:
            case COLUMN_CONTEXT_TIME_EPOCH_MS:
                checkFieldType(field, BIGINT);
                return currentRecord.getTimestamp();

            case COLUMN_WINDOW_BEGIN_EPOCH:
            case "min(" + COLUMN_WINDOW_BEGIN_EPOCH + ")":
            case "max(" + COLUMN_WINDOW_BEGIN_EPOCH + ")":
                // bios v1 uses milliseconds since epoch, but this virtual column is in seconds.
                return currentWindow.getWindowBeginTime() / 1000;

            case COLUMN_WINDOW_BEGIN_TIMESTAMP:
            case "min(" + COLUMN_WINDOW_BEGIN_TIMESTAMP + ")":
            case "max(" + COLUMN_WINDOW_BEGIN_TIMESTAMP + ")":
                // bios v1 uses milliseconds since epoch, but Trino uses
                // microseconds since epoch for timestamps; convert to micros.
                return currentWindow.getWindowBeginTime() * 1000;

            case COLUMN_PARAM_WINDOW_SIZE_SECONDS:
            case COLUMN_PARAM_DURATION_SECONDS:
            case COLUMN_PARAM_DURATION_OFFSET_SECONDS:
                return 0;

            default:
                checkFieldType(field, BIGINT);
                return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asLong();
        }
    }

    @Override
    public double getDouble(int field)
    {
        String columnName = columnHandles.get(field).getColumnName();

        switch (columnName.toLowerCase(Locale.getDefault())) {
            case "avg(" + COLUMN_WINDOW_BEGIN_EPOCH + ")":
                // bios v1 uses milliseconds since epoch, but this virtual column is in seconds.
                return currentWindow.getWindowBeginTime() / 1000.0;

            default:
                checkFieldType(field, DOUBLE);
                return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asDouble();
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        if (type.equals(VARCHAR)) {
            return Slices.utf8Slice(
                    currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asString());
        }
        else if (type.equals(VARBINARY)) {
            return Slices.EMPTY_SLICE;
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format(
                    "Expected field %s (%s) to be type VARCHAR or VARBINARY, but is %s", field,
                    columnHandles.get(field).getColumnName(), type));
        }
    }

    @Override
    public Object getObject(int field)
    {
        throw new TrinoException(NOT_SUPPORTED, "Blobs not supported by bi(OS) yet.");
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return false;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s (%s) to be type %s but is %s",
                field, columnHandles.get(field).getColumnName(), expected, actual);
    }

    @Override
    public void close()
    {
    }
}
