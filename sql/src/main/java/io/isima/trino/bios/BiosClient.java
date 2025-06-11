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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.collect.cache.SafeCaches;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.Type;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.isima.bios.sdk.Bios.keys;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("NullableProblems")
public class BiosClient
{
    public static final String SIGNAL_TABLE_NAME_SUFFIX_RAW = "_raw";
    public static final String SIGNAL_TABLE_NAME_SUFFIX_SKETCHES = "_sketches";
    public static final String VIRTUAL_PREFIX = "__";
    public static final String PARAMETER_PREFIX = VIRTUAL_PREFIX + "param_";

    public static final String COLUMN_PARAM_WINDOW_SIZE_SECONDS = PARAMETER_PREFIX + "window_size_seconds";
    public static final String COLUMN_PARAM_DURATION_SECONDS = PARAMETER_PREFIX + "duration_seconds";
    public static final String COLUMN_PARAM_DURATION_OFFSET_SECONDS = PARAMETER_PREFIX + "duration_offset_seconds";

    public static final String COLUMN_CONTEXT_TIMESTAMP = VIRTUAL_PREFIX + "upsert_timestamp";
    public static final String COLUMN_CONTEXT_TIME_EPOCH_MS = VIRTUAL_PREFIX + "upsert_time_epoch_ms";
    public static final String COLUMN_SIGNAL_TIMESTAMP = VIRTUAL_PREFIX + "event_timestamp";
    public static final String COLUMN_SIGNAL_TIME_EPOCH_MS = VIRTUAL_PREFIX + "event_time_epoch_ms";
    public static final String COLUMN_WINDOW_BEGIN_TIMESTAMP = VIRTUAL_PREFIX + "window_begin_timestamp";
    public static final String COLUMN_WINDOW_BEGIN_EPOCH = VIRTUAL_PREFIX + "window_begin_epoch";

    public static final String SKETCH_FUNCTION_SEPARATOR = "__";
    public static final String SKETCH_COUNT_COLUMN_NAME = "count" + SKETCH_FUNCTION_SEPARATOR;

    private static final Logger logger = Logger.get(BiosClient.class);
    private static final Map<String, Type> biosTypeMap = new HashMap<>();
    private static final List<String> featureAggregates;
    private static final List<String> numberTypeSketches;
    private static final List<String> anyTypeSketches;
    public static final String SCHEMA_CONTEXTS = "contexts";
    public static final String SCHEMA_CONTEXTS_RAW = "contexts_raw";
    public static final String SCHEMA_CONTEXTS_SKETCHES = "contexts_sketches";
    public static final String SCHEMA_SIGNALS = "signals";
    public static final String SCHEMA_SIGNALS_RAW = "signals_raw";
    public static final String SCHEMA_SIGNALS_SKETCHES = "signals_sketches";

    static {
        biosTypeMap.put("integer", BIGINT);
        biosTypeMap.put("boolean", BOOLEAN);
        biosTypeMap.put("string", VARCHAR);
        biosTypeMap.put("decimal", DOUBLE);
        biosTypeMap.put("blob", VARBINARY);

        featureAggregates = List.of("count", "sum", "min", "max");

        numberTypeSketches = List.of(
                "avg", "stddev", "variance", "skewness", "kurtosis", "sum", "sum2", "sum3", "sum4", "min", "max",
                "median", "p0_01", "p0_1", "p1", "p10", "p25", "p50", "p75", "p90", "p99", "p99_9", "p99_99");
        anyTypeSketches = List.of(
                "distinctcount", "dclb1", "dcub1", "dclb2", "dcub2", "dclb3", "dcub3");
    }

    private final BiosConfig biosConfig;
    private Supplier<Session> session;
    private final Supplier<TenantConfig> tenantConfig;
    private final NonEvictableLoadingCache<BiosQuery, ISqlResponse> contextCache;
    private final NonEvictableLoadingCache<BiosQuery, ISqlResponse> rawSignalCache;
    private final NonEvictableLoadingCache<BiosQuery, ISqlResponse> featureCache;

    @Inject
    public BiosClient(BiosConfig config)
    {
        requireNonNull(config, "config is null");

        requireNonNull(config.getUrl(), "url is null");
        checkArgument(!isNullOrEmpty(config.getEmail()), "email is null");
        checkArgument(!isNullOrEmpty(config.getPassword()), "password is null");

        this.biosConfig = config;

        session = Suppliers.memoize(sessionSupplier(config));
        tenantConfig = Suppliers.memoizeWithExpiration(tenantConfigSupplier(this),
                config.getMetadataCacheSeconds(), TimeUnit.SECONDS);
        tenantConfig.get();

        contextCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getContextCacheSizeInRows())
                .weigher((Weigher<BiosQuery, ISqlResponse>) (query, response) -> getTotalRows(response))
                .expireAfterWrite(config.getContextCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });

        rawSignalCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getRawSignalCacheSizeInRows())
                .weigher((Weigher<BiosQuery, ISqlResponse>) (query, response) -> getTotalRows(response))
                .expireAfterWrite(config.getRawSignalCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });

        featureCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getFeatureCacheSizeInRows())
                .weigher((Weigher<BiosQuery, ISqlResponse>) (query, response) -> getTotalRows(response))
                .expireAfterWrite(config.getFeatureCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });
    }

    private int getTotalRows(final ISqlResponse response)
    {
        int numRows = 0;
        numRows += response.getRecords().size();
        for (var window : response.getDataWindows()) {
            numRows += window.getRecords().size();
        }
        return numRows;
    }

    /**
     * This method does one of the following:
     * 1. If doNotThrowIfRetryable is true and the exception indicates that the operation is
     * retryable, returns normally after reestablishing a new session.
     * 2. If doNotThrowIfRetryable is false, or if the exception does not look retryable, throws:
     *      a) TrinoException for known/handled exceptions, or
     *      b) RuntimeException for unhandled exceptions.
     */
    private void handleException(Throwable t, boolean doNotThrowIfRetryable)
    {
        if (t instanceof BiosClientException) {
            BiosClientException biosClientException = (BiosClientException) t;
            switch (biosClientException.getCode()) {
                case BAD_INPUT:
                    throw new TrinoException(GENERIC_USER_ERROR, biosClientException.getMessage());

                case SESSION_INACTIVE:
                case CLIENT_CHANNEL_ERROR:
                case SERVER_CONNECTION_FAILURE:
                case SERVER_CHANNEL_ERROR:
                case SERVICE_UNAVAILABLE:
                case SERVICE_UNDEPLOYED:
                case SESSION_EXPIRED:
                    if (doNotThrowIfRetryable) {
                        logger.debug("Possibly retryable error code: %s. \n\n "
                                + "Attempting to create a new session... \n", biosClientException.getCode());
                        session = Suppliers.memoize(sessionSupplier(biosConfig));
                        session.get();
                        return;
                    }
                    // Fallthrough to default.

                default:
                    logger.debug("bi(OS) got exception: %s", t);
                    throw new RuntimeException(biosClientException);
            }
        }
        else {
            logger.debug("bi(OS) got exception: %s", t);
            throw new RuntimeException(t);
        }
    }

    private static Supplier<Session> sessionSupplier(BiosConfig biosConfig)
    {
        return () -> {
            Session session;
            try {
                logger.debug("sessionSupplier: %s (%s, port: %s), %s", biosConfig.getUrl().toString(),
                        biosConfig.getUrl().getHost(),
                        biosConfig.getUrl().getPort(),
                        biosConfig.getEmail());

                var newSession = Bios.newSession(biosConfig.getUrl().getHost());
                if (biosConfig.getUrl().getPort() != -1) {
                    session = newSession
                                .port(biosConfig.getUrl().getPort())
                                .user(biosConfig.getEmail())
                                .password(biosConfig.getPassword())
                                .sslCertFile(null)
                                .appName("trino-bios")
                                .appType(AppType.ADHOC)
                                .connect();
                }
                else {
                    session = newSession
                                .user(biosConfig.getEmail())
                                .password(biosConfig.getPassword())
                                .sslCertFile(null)
                                .appName("trino-bios")
                                .appType(AppType.ADHOC)
                                .connect();
                }

                logger.debug("sessionSupplier: done");
                return session;
            }
            catch (BiosClientException e) {
                // Cannot call handleException() here because it may cause recursion.
                throw new RuntimeException(e);
            }
        };
    }

    private static Supplier<TenantConfig> tenantConfigSupplier(final BiosClient client)
    {
        return () -> {
            TenantConfig tenantConfig;
            try {
                logger.info("----------> bios network request : getTenant");
                tenantConfig = client.getSession().getTenant(true, true);
            }
            catch (BiosClientException e) {
                client.handleException(e, true);
                // If handleException did not throw an exception, it means we can retry.
                try {
                    logger.info("retrying ----------> bios network request : getTenant");
                    tenantConfig = client.getSession().getTenant(true, true);
                }
                catch (BiosClientException e2) {
                    client.handleException(e2, false);  // This should always throw.
                    throw new RuntimeException("This should never happen - gap in bios connector");
                }
            }

            logger.info("<---------- bios network response: getTenant %s returned %d signals, %d "
                            + "contexts",
                    tenantConfig.getName(), tenantConfig.getSignals().size(),
                    tenantConfig.getContexts().size());
            return tenantConfig;
        };
    }

    public static Long floor(Long toBeFloored, long divisor)
    {
        if (toBeFloored == null) {
            return null;
        }
        return divisor * (toBeFloored / divisor);
    }

    public static Long ceiling(Long toBeCeiled, long divisor)
    {
        if (toBeCeiled == null) {
            return null;
        }
        return (long) Math.signum(toBeCeiled) * divisor * (((Math.abs(toBeCeiled) - 1) / divisor) + 1);
    }

    public long getCurrentTimeWithLag(BiosTableHandle tableHandle)
    {
        long lag = (tableHandle.getTableKind() == BiosTableKind.SIGNAL_RAW) ?
                biosConfig.getRawSignalLagSeconds() * 1000 :
                biosConfig.getFeatureLagSeconds() * 1000;
        return System.currentTimeMillis() - lag;
    }

    public long getEffectiveTimeRangeStart(BiosTableHandle tableHandle)
    {
        long currentTimeWithLag = getCurrentTimeWithLag(tableHandle);
        long start;

        if (tableHandle.getTimeRangeStart() != null) {
            start = tableHandle.getTimeRangeStart();
        }
        else {
            if (tableHandle.getDurationOffsetSeconds() != null) {
                start = currentTimeWithLag - tableHandle.getDurationOffsetSeconds() * 1000;
            }
            else {
                start = currentTimeWithLag;
            }
        }

        // If this is a feature, "snap" it to the window size.
        if (tableHandle.getTableKind() != BiosTableKind.SIGNAL_RAW) {
            start = floor(start, getEffectiveWindowSizeSeconds(tableHandle) * 1000);
        }

        // Return the lower bound of the time range.
        long delta = getEffectiveTimeRangeDeltaSigned(tableHandle);
        if (delta >= 0) {
            return start;
        }
        else {
            return start + delta;
        }
    }

    public long getEffectiveTimeRangeDelta(BiosTableHandle tableHandle)
    {
        long delta = getEffectiveTimeRangeDeltaSigned(tableHandle);
        return Math.abs(delta);
    }

    private long getEffectiveTimeRangeDeltaSigned(BiosTableHandle tableHandle)
    {
        long delta;

        if (tableHandle.getTimeRangeDelta() != null) {
            delta = tableHandle.getTimeRangeDelta();
        }
        else {
            if (tableHandle.getDurationSeconds() != null) {
                delta = -1000 * tableHandle.getDurationSeconds();
            }
            else {
                delta = -1000 * biosConfig.getDefaultTimeRangeDeltaSeconds();
            }
        }
        return delta;
    }

    // Get window size - relevant for features; use placeholder 1 for raw signals / contexts.
    public long getEffectiveWindowSizeSeconds(BiosTableHandle tableHandle)
    {
        long out;
        if (tableHandle.getWindowSizeSeconds() != null) {
            out = tableHandle.getWindowSizeSeconds();
        }
        else {
            if ((tableHandle.getTableKind() == BiosTableKind.SIGNAL) ||
                    (tableHandle.getTableKind() == BiosTableKind.SIGNAL_SKETCHES)) {
                out = biosConfig.getDefaultWindowSizeSeconds();
            }
            else {
                out = 1;
            }
        }
        return out;
    }

    public ISqlResponse getQueryResponse(BiosQuery query)
    {
        switch (query.getTableKind()) {
            case SIGNAL_RAW:
                // For raw signals, get all attributes so that we don't have many queries with
                //      different subsets of attributes.
                query.setAttributes(null);
                break;

            case CONTEXT:
            case SIGNAL:
                // Ensure main signals are only used for aggregated results, not raw rows.
                if (query.getAggregates() == null) {
                    throw new TrinoException(GENERIC_USER_ERROR, "Query has no aggregate or has "
                            + "unsupported complex transformation; use raw signals or contexts for "
                            + "such queries. Query: " + query);
                }
                break;

            default:
                break;
        }

        logger.debug("Request : %s", query);
        NonEvictableLoadingCache<BiosQuery, ISqlResponse> cache;
        switch (query.getTableKind()) {
            case CONTEXT:
            case CONTEXT_RAW:
            case CONTEXT_SKETCHES:
                cache = contextCache;
                break;
            case SIGNAL_RAW:
                cache = rawSignalCache;
                break;
            case SIGNAL:
            case SIGNAL_SKETCHES:
                cache = featureCache;
                break;
            default:
                return null;
        }
        ISqlResponse response;
        try {
            response = cache.get(query);
        }
        catch (Exception e) {
            if (e.getCause() instanceof TrinoException) {
                throw (TrinoException) e.getCause();
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e.getMessage(), e.getCause());
            }
        }
        logger.debug("Response: %d total rows in %d windows", getTotalRows(response),
                response.getDataWindows().size());

        return response;
    }

    private ISqlResponse executeInternal(BiosQuery query)
    {
        Statement isqlStatement;

        switch (query.getTableKind()) {
            case CONTEXT_RAW:
                var attributes = getAttributeConfigs(query.getSchemaName(), query.getTableName());
                String keyColumnName = attributes.get(0).getName();

                if (query.getWhere() == null) {
                    // There are 2 kinds of raw context queries without filter:
                    //  1. Preliminary query to list all keys: contextKeyRange and contextKeys are null.
                    //  2. Query to get records for a key range: contextKeyRange and contextKeys are populated.
                    if (query.getContextKeyRange() == null) {
                        isqlStatement = Bios.isql().select(keyColumnName)
                                .fromContext(query.getUnderlyingTableName())
                                .build();
                    }
                    else {
                        isqlStatement = Bios.isql().select()
                                .fromContext(query.getUnderlyingTableName())
                                .where(keys().in(query.getContextKeys()))
                                .build();
                    }
                }
                else {
                    isqlStatement = Bios.isql()
                            .select(query.getAttributes())
                            .fromContext(query.getUnderlyingTableName())
                            .where(query.getWhere())
                            .build();
                }
                break;

            case CONTEXT:
            case CONTEXT_SKETCHES:
                isqlStatement = Bios.isql()
                        .select(query.getAttributesAndAggregates())
                        .fromContext(query.getUnderlyingTableName())
                        .groupBy(query.getGroupBy())
                        .build();
                break;

            case SIGNAL:
            case SIGNAL_SKETCHES:
                isqlStatement = Bios.isql()
                        .select(query.getAttributesAndAggregates())
                        .fromSignal(query.getUnderlyingTableName())
                        .groupBy(query.getGroupBy())
                        .tumblingWindow(Duration.ofSeconds(query.getWindowSizeSeconds()))
                        .timeRange(query.getTimeRangeStart(), query.getTimeRangeDelta())
                        .build();
                break;

            case SIGNAL_RAW:
                isqlStatement = Bios.isql()
                        .select()
                        .fromSignal(query.getUnderlyingTableName())
                        .where(query.getWhere())
                        .timeRange(query.getTimeRangeStart(), query.getTimeRangeDelta())
                        .build();
                break;

            default:
                return null;
        }

        logger.info("----------> bios network request : %s", query);
        ISqlResponse response = executeStatement(isqlStatement);
        logger.info("<---------- bios network response: query returned %d records, %d windows, "
                        + "%d total rows",
                response.getRecords().size(), response.getDataWindows().size(), getTotalRows(response));

        return response;
    }

    private ISqlResponse executeStatement(Statement statement)
    {
        ISqlResponse response;
        try {
            response = session.get().execute(statement);
        }
        catch (BiosClientException e) {
            handleException(e, true);
            // If handleException did not throw an exception, it means we can retry.
            try {
                logger.info("retrying ----------> bios network request : query");
                response = session.get().execute(statement);
            }
            catch (BiosClientException e2) {
                handleException(e2, false);
                throw new RuntimeException("This should never happen - gap in bios connector");
            }
        }
        return response;
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of(SCHEMA_CONTEXTS, SCHEMA_CONTEXTS_RAW, SCHEMA_CONTEXTS_SKETCHES,
                SCHEMA_SIGNALS, SCHEMA_SIGNALS_RAW, SCHEMA_SIGNALS_SKETCHES);
    }

    private static String getTableSuffix(String schemaName)
    {
        switch (schemaName) {
            case SCHEMA_SIGNALS_RAW:
            case SCHEMA_CONTEXTS_RAW:
                return SIGNAL_TABLE_NAME_SUFFIX_RAW;

            case SCHEMA_SIGNALS_SKETCHES:
            case SCHEMA_CONTEXTS_SKETCHES:
                return SIGNAL_TABLE_NAME_SUFFIX_SKETCHES;

            default:
                return "";
        }
    }

    public static String addTableSuffix(String tableName, String schemaName)
    {
        return tableName + getTableSuffix(schemaName);
    }

    public static String removeTableSuffix(String tableName, String schemaName)
    {
        final String suffix = getTableSuffix(schemaName);
        if (suffix.isEmpty()) {
            return tableName;
        }
        if (!tableName.endsWith(suffix)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "bi(OS) got invalid table"
                    + " name: " + tableName + " in schema: " + schemaName
                    + "; expected it to end with: " + suffix);
        }
        return tableName.substring(0, tableName.length() - suffix.length());
    }

    public Set<String> getTableNames(String schemaName)
    {
        // logger.debug("getTableNames: %s", schemaName);
        requireNonNull(schemaName, "schemaName is null");

        List<String> tableNames = new ArrayList<>();

        switch (schemaName) {
            case SCHEMA_SIGNALS:
            case SCHEMA_SIGNALS_RAW:
            case SCHEMA_SIGNALS_SKETCHES:
                var signalsToHide = Arrays.stream(biosConfig.getSignalsToHide()
                        .toLowerCase(Locale.getDefault()).split(",")).collect(Collectors.toList());
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    if (signalsToHide.contains(signalConfig.getName().toLowerCase(Locale.getDefault()))) {
                        continue;
                    }
                    tableNames.add(addTableSuffix(signalConfig.getName(), schemaName));
                }
                break;
            case SCHEMA_CONTEXTS:
            case SCHEMA_CONTEXTS_RAW:
            case SCHEMA_CONTEXTS_SKETCHES:
                var contextsToHide = Arrays.stream(biosConfig.getContextsToHide()
                        .toLowerCase(Locale.getDefault()).split(",")).collect(Collectors.toList());
                for (ContextConfig contextConfig : tenantConfig.get().getContexts()) {
                    if (contextsToHide.contains(contextConfig.getName().toLowerCase(Locale.getDefault()))) {
                        continue;
                    }
                    tableNames.add(addTableSuffix(contextConfig.getName(), schemaName));
                }
                break;
        }
        logger.debug("getTableNames: %s", tableNames.toString());
        return ImmutableSet.copyOf(tableNames);
    }

    public BiosTableHandle getTableHandle(String schemaName, String tableName)
    {
        return new BiosTableHandle(schemaName, tableName);
    }

    public List<BiosColumnHandle> getColumnHandles(String schemaName, String tableName)
    {
        // logger.debug("getColumnHandles: %s.%s", schemaName, tableName);
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        List<AttributeConfig> attributes = getAttributeConfigs(schemaName, tableName);
        if (attributes == null) {
            throw new TableNotFoundException(new SchemaTableName(schemaName, tableName));
        }

        var tableHandle = new BiosTableHandle(schemaName, tableName);
        BiosTableKind kind = tableHandle.getTableKind();
        ImmutableList.Builder<BiosColumnHandle> columns = ImmutableList.builder();

        String defaultValue;
        boolean isFirstAttribute = true;
        if (schemaName.equals(SCHEMA_SIGNALS_SKETCHES) || schemaName.equals(SCHEMA_CONTEXTS_SKETCHES)) {
            // For sketches, we need to create one column for every sketch function for
            // every attribute (m * n columns), plus one column for count.
            BiosColumnHandle countColumnHandle =
                    new BiosColumnHandle(SKETCH_COUNT_COLUMN_NAME, BIGINT, null, false, null, null);
            columns.add(countColumnHandle);
            for (var sketch : numberTypeSketches) {
                for (AttributeConfig attributeConfig : attributes) {
                    if ((attributeConfig.getType() == AttributeType.INTEGER) ||
                            (attributeConfig.getType() == AttributeType.DECIMAL)) {
                        String attributeName = attributeConfig.getName();
                        String columnName = sketch + SKETCH_FUNCTION_SEPARATOR + attributeName;
                        BiosColumnHandle columnHandle = new BiosColumnHandle(columnName, DOUBLE,
                                null, false, null, null);
                        columns.add(columnHandle);
                    }
                }
            }
            for (var sketch : anyTypeSketches) {
                for (AttributeConfig attributeConfig : attributes) {
                    if (attributeConfig.getType() != AttributeType.BLOB) {
                        String attributeName = attributeConfig.getName();
                        String columnName = sketch + SKETCH_FUNCTION_SEPARATOR + attributeName;
                        BiosColumnHandle columnHandle = new BiosColumnHandle(columnName, DOUBLE,
                                null, false, null, null);
                        columns.add(columnHandle);
                    }
                }
            }
        }
        else {
            for (AttributeConfig attributeConfig : attributes) {
                String attributeName = attributeConfig.getName();
                Type columnType = biosTypeMap.get(attributeConfig.getType().name().toLowerCase(Locale.getDefault()));
                if (attributeConfig.getDefaultValue() != null) {
                    defaultValue = attributeConfig.getDefaultValue().asString();
                }
                else {
                    defaultValue = null;
                }
                BiosColumnHandle columnHandle = new BiosColumnHandle(attributeName, columnType,
                        defaultValue,
                        ((kind == BiosTableKind.CONTEXT) || (kind == BiosTableKind.CONTEXT_RAW)) && isFirstAttribute,
                        null, null);
                isFirstAttribute = false;
                columns.add(columnHandle);
            }
        }

        switch (schemaName) {
            case SCHEMA_SIGNALS:
            case SCHEMA_SIGNALS_SKETCHES:
                columns.add(new BiosColumnHandle(COLUMN_WINDOW_BEGIN_TIMESTAMP, TIMESTAMP_SECONDS, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_WINDOW_BEGIN_EPOCH, BIGINT, null, false, null, null));

                columns.add(new BiosColumnHandle(COLUMN_PARAM_DURATION_SECONDS, BIGINT,
                        biosConfig.getDefaultTimeRangeDeltaSeconds().toString(), false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_PARAM_DURATION_OFFSET_SECONDS, BIGINT,
                        "0 (from current time)", false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_PARAM_WINDOW_SIZE_SECONDS, BIGINT,
                        biosConfig.getDefaultWindowSizeSeconds().toString(), false, null, null));
                break;

            case SCHEMA_SIGNALS_RAW:
                columns.add(new BiosColumnHandle(COLUMN_SIGNAL_TIMESTAMP, TIMESTAMP_MICROS, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_SIGNAL_TIME_EPOCH_MS, BIGINT, null, false, null, null));

                columns.add(new BiosColumnHandle(COLUMN_PARAM_DURATION_SECONDS, BIGINT,
                        biosConfig.getDefaultTimeRangeDeltaSeconds().toString(), false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_PARAM_DURATION_OFFSET_SECONDS, BIGINT,
                        "0 (from current time)", false, null, null));
                break;

            case SCHEMA_CONTEXTS_RAW:
                columns.add(new BiosColumnHandle(COLUMN_CONTEXT_TIMESTAMP, TIMESTAMP_MICROS, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_CONTEXT_TIME_EPOCH_MS, BIGINT, null, false, null, null));
                break;
        }

        return columns.build();
    }

    public List<AttributeConfig> getAttributeConfigs(final String schemaName,
                                                     final String tableName)
    {
        List<AttributeConfig> attributes = null;

        switch (schemaName) {
            case SCHEMA_SIGNALS:
            case SCHEMA_SIGNALS_RAW:
            case SCHEMA_SIGNALS_SKETCHES:
                final String underlyingSignalName = removeTableSuffix(tableName, schemaName);
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    if (!underlyingSignalName.equalsIgnoreCase(signalConfig.getName())) {
                        continue;
                    }
                    attributes = signalConfig.getAttributes();
                    break;
                }
                break;
            case SCHEMA_CONTEXTS:
            case SCHEMA_CONTEXTS_RAW:
            case SCHEMA_CONTEXTS_SKETCHES:
                ContextConfig contextConfig = getContextConfig(schemaName, tableName);
                attributes = contextConfig.getAttributes();
                break;
            default:
                throw new RuntimeException("This should never happen - gap in bios connector");
        }
        return attributes;
    }

    public ContextConfig getContextConfig(final String schemaName, final String tableName)
    {
        ContextConfig contextConfigToUse = null;
        final String underlyingContextName = removeTableSuffix(tableName, schemaName);
        for (ContextConfig contextConfig : tenantConfig.get().getContexts()) {
            if (!underlyingContextName.equalsIgnoreCase(contextConfig.getName())) {
                continue;
            }
            contextConfigToUse = contextConfig;
            break;
        }
        return contextConfigToUse;
    }

    public AttributeConfig getAttributeConfig(final String schemaName, final String tableName, final String attributeName)
    {
        List<AttributeConfig> attributes = getAttributeConfigs(schemaName, tableName);
        if (attributes == null) {
            throw new TableNotFoundException(new SchemaTableName(schemaName, tableName));
        }

        for (AttributeConfig attributeConfig : attributes) {
            if (attributeConfig.getName().equalsIgnoreCase(attributeName)) {
                return attributeConfig;
            }
        }
        throw new ColumnNotFoundException(new SchemaTableName(schemaName, tableName), attributeName);
    }

    public boolean isFeatureSupportedAggregate(String aggregate)
    {
        return featureAggregates.contains(aggregate.toLowerCase(Locale.getDefault()));
    }

    public static boolean isSketchFunction(String function)
    {
        if (numberTypeSketches.contains(function.toLowerCase(Locale.getDefault()))) {
            return true;
        }
        if (anyTypeSketches.contains(function.toLowerCase(Locale.getDefault()))) {
            return true;
        }
        if (function.toLowerCase(Locale.getDefault()).equals("count")) {
            return true;
        }

        return false;
    }

    public BiosConfig getBiosConfig()
    {
        return biosConfig;
    }

    private Session getSession()
    {
        return session.get();
    }
}
