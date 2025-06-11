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
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BiosQuery
{
    private final String schemaName;
    private final String tableName;
    private final Long timeRangeStart;
    private final Long timeRangeDelta;
    private final Long windowSizeSeconds;
    private final List<String> groupBy;
    private final String where;
    private List<String> attributes;
    private final List<String> aggregates;
    private final String contextKeyRange;
    private final List<Object> contextKeys;

    @JsonCreator
    public BiosQuery(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta,
            @JsonProperty("windowSizeSeconds") Long windowSizeSeconds,
            @JsonProperty("groupBy") List<String> groupBy,
            @JsonProperty("where") String where,
            @JsonProperty("attributes") List<String> attributes,
            @JsonProperty("aggregates") List<String> aggregates,
            @JsonProperty("contextKeyRange") String contextKeyRange,
            @JsonProperty("contextKeys") List<Object> contextKeys)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
        this.windowSizeSeconds = windowSizeSeconds;
        this.groupBy = groupBy;
        this.where = where;
        this.attributes = getNullIfEmptyList(attributes);
        this.aggregates = getNullIfEmptyList(aggregates);
        this.contextKeyRange = contextKeyRange;
        this.contextKeys = contextKeys;
    }

    private List<String> getNullIfEmptyList(List<String> list)
    {
        if (list == null) {
            return null;
        }
        if (list.isEmpty()) {
            return null;
        }
        return list;
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
    public List<String> getGroupBy()
    {
        return groupBy;
    }

    @JsonProperty
    public String getWhere()
    {
        return where;
    }

    @JsonProperty
    public List<String> getAttributes()
    {
        return attributes;
    }

    public void setAttributes(List<String> attributes)
    {
        this.attributes = attributes;
    }

    @JsonProperty
    public List<String> getAggregates()
    {
        return aggregates;
    }

    public List<String> getAttributesAndAggregates()
    {
        if (attributes == null) {
            return aggregates;
        }
        if (aggregates == null) {
            return attributes;
        }
        List<String> result = new ArrayList<>(attributes);
        result.addAll(aggregates);

        return result;
    }

    @JsonProperty
    public String getContextKeyRange()
    {
        return contextKeyRange;
    }

    @JsonProperty
    public List<Object> getContextKeys()
    {
        return contextKeys;
    }

    public BiosTableKind getTableKind()
    {
        return BiosTableKind.getTableKind(schemaName);
    }

    public String getUnderlyingTableName()
    {
        return BiosClient.removeTableSuffix(getTableName(), schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, timeRangeStart, timeRangeDelta,
                windowSizeSeconds, groupBy, where, attributes, aggregates, contextKeyRange, contextKeys);
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

        BiosQuery other = (BiosQuery) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.timeRangeStart, other.timeRangeStart) &&
                Objects.equals(this.timeRangeDelta, other.timeRangeDelta) &&
                Objects.equals(this.windowSizeSeconds, other.windowSizeSeconds) &&
                Objects.equals(this.groupBy, other.groupBy) &&
                Objects.equals(this.where, other.where) &&
                Objects.equals(this.attributes, other.attributes) &&
                Objects.equals(this.aggregates, other.aggregates) &&
                Objects.equals(this.contextKeyRange, other.contextKeyRange) &&
                Objects.equals(this.contextKeys, other.contextKeys);
    }

    @Override
    public String toString()
    {
        return toStringHelper("query")
                .add("", new SchemaTableName(schemaName, tableName))
                .add("start", timeRangeStart)
                .add("delta", timeRangeDelta)
                .add("window", windowSizeSeconds)
                .add("groupBy", groupBy)
                .add("where", where)
                .add("attributes", attributes)
                .add("aggregates", aggregates)
                .add("contextKeyRange", contextKeyRange)
                .add("contextKeys", contextKeys)
                .omitNullValues()
                .toString();
    }
}
