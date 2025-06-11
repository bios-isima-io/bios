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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BiosTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private Long timeRangeStart;
    private Long timeRangeDelta;
    private Long windowSizeSeconds;
    private Long durationSeconds;
    private Long durationOffsetSeconds;
    private List<String> groupBy;
    private String where;

    @JsonCreator
    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta,
            @JsonProperty("windowSizeSeconds") Long windowSizeSeconds,
            @JsonProperty("durationSeconds") Long durationSeconds,
            @JsonProperty("durationOffsetSeconds") Long durationOffsetSeconds,
            @JsonProperty("groupBy") List<String> groupBy,
            @JsonProperty("where") String where)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
        this.windowSizeSeconds = windowSizeSeconds;
        this.durationSeconds = durationSeconds;
        this.durationOffsetSeconds = durationOffsetSeconds;
        this.groupBy = groupBy;
        this.where = where;
    }

    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Long getTimeRangeStart()
    {
        return timeRangeStart;
    }

    @JsonProperty
    public Long getTimeRangeDelta()
    {
        return timeRangeDelta;
    }

    @JsonProperty
    public Long getWindowSizeSeconds()
    {
        return windowSizeSeconds;
    }

    @JsonProperty
    public Long getDurationSeconds()
    {
        return durationSeconds;
    }

    @JsonProperty
    public Long getDurationOffsetSeconds()
    {
        return durationOffsetSeconds;
    }

    @JsonProperty
    public List<String> getGroupBy()
    {
        return groupBy;
    }

    @JsonProperty
    public String getWhere()
    {
        return where;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public BiosTableKind getTableKind()
    {
        return BiosTableKind.getTableKind(schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, timeRangeStart, timeRangeDelta,
                windowSizeSeconds, durationSeconds, durationOffsetSeconds,
                groupBy, where);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        BiosTableHandle other = (BiosTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.timeRangeStart, other.timeRangeStart) &&
                Objects.equals(this.timeRangeDelta, other.timeRangeDelta) &&
                Objects.equals(this.windowSizeSeconds, other.windowSizeSeconds) &&
                Objects.equals(this.durationSeconds, other.durationSeconds) &&
                Objects.equals(this.durationOffsetSeconds, other.durationOffsetSeconds) &&
                Objects.equals(this.groupBy, other.groupBy) &&
                Objects.equals(this.where, other.where);
    }

    @Override
    public String toString()
    {
        return toStringHelper("table")
                .add("", toSchemaTableName())
                .add("start", timeRangeStart)
                .add("delta", timeRangeDelta)
                .add("window", windowSizeSeconds)
                .add("duration", durationSeconds)
                .add("offset", durationOffsetSeconds)
                .add("groupBy", groupBy)
                .add("where", where)
                .omitNullValues()
                .toString();
    }
}
