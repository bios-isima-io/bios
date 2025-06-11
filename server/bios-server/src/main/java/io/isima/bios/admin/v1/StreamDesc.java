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
package io.isima.bios.admin.v1;

import static io.isima.bios.admin.v1.StreamTag.CONTEXT_AUDIT;
import static io.isima.bios.admin.v1.StreamTag.FAC;

import io.isima.bios.admin.v1.StreamConversion.AttributeConversion;
import io.isima.bios.admin.v1.StreamConversion.ConversionType;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.ColumnDefinition;
import io.isima.bios.data.ColumnDefinitionsBuilder;
import io.isima.bios.data.StreamId;
import io.isima.bios.data.impl.feature.ContextAdjuster;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.ServerAttributeConfig;
import io.isima.bios.models.TimeInterval;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class StreamDesc extends StreamStoreDesc {

  /** Parent tenant descriptor. */
  @ToString.Exclude protected TenantDesc parent;

  protected StreamDesc parentStream;

  protected final Set<String> subStreamNames;

  protected StreamDesc prev;

  protected StreamConversion streamConversion;

  /** Compiled column definitions */
  private Map<String, ColumnDefinition> columnDefinitions;

  private List<ServerAttributeConfig> biosAttributes;
  private List<ServerAttributeConfig> allBiosAttributes;
  private Map<String, ServerAttributeConfig> biosAttributeMap;

  /** Primary key attributes, built in AdminImpl.initStreamConfig(). */
  private List<AttributeDesc> primaryKeyAttributes;

  /** Compiled preprocesses. */
  protected List<ProcessStage> preprocessStages;

  /** Compiled enrichments with contexts. */
  protected List<CompiledEnrichment> compiledEnrichments;

  /** Compiled context fetch adjuster. */
  protected ContextAdjuster contextAdjuster;

  /**
   * Organize data structures for sketches for optimal lookup on queries: 1. Attribute - to be able
   * to look up by attribute. 2. Sketch type - a given query metric function can usually only be
   * satisfied by a specific sketch type. So we can lookup by it. 3. Duration type - some queries
   * can be satisfied by multiple duration types, so we can have a choice of them and should not
   * need to know its value before lookup; so keep it last.
   */
  protected final AttributeMap sketches = new AttributeMap();

  /*
   * true: yes
   * false: no
   * null: unknown
   */
  protected Boolean isLatestVersion;

  /**
   * These flags are build on {@link #compile()}. They indicate that the signal is for client
   * metrics. The flags are meant to be utilized by the data service handler to determine if it runs
   * special incoming data cleansing for client metrics.
   */
  protected boolean isClientMetrics = false;

  protected boolean isAllClientMetrics = false;

  protected Set<StreamTag> tags = new HashSet<>();

  public StreamId getStreamId() {
    return new StreamId(parent.getName(), getName(), getVersion());
  }

  public static class AttributeMap extends HashMap<String, SketchTypeMap> {}

  public static class SketchTypeMap extends HashMap<DataSketchType, SketchDurationMap> {}

  public static class SketchDurationMap extends HashMap<DataSketchDuration, SketchStatus> {}

  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  @Getter
  @Setter
  @ToString
  public static class SketchStatus {
    private long doneSince = 0;
    private long doneUntil = 0;
    private double doneCoverage = 0;
  }

  /*
   * Following two properties are used by rollup stream. Rollup stream stores summary of an interval
   * regardless the horizon. If horizon is longer than interval, ExtractEngine would combine
   * multiple intervals on extraction.
   */
  TimeInterval rollupInterval;

  TimeInterval rollupHorizon;

  protected StreamDesc() {
    super();
    subStreamNames = new HashSet<>();
  }

  public StreamDesc(String name, Long version) {
    super(name, version, false);
    subStreamNames = new HashSet<>();
  }

  public StreamDesc(String name, Long version, boolean deleted) {
    super(name, version, deleted);
    subStreamNames = new HashSet<>();
  }

  /**
   * Constructor with stream name and type.
   *
   * @param name Stream name
   */
  public StreamDesc(String name, StreamType type) {
    super(name, type);
    subStreamNames = new HashSet<>();
  }

  public StreamDesc(StreamConfig src) {
    duplicate(src);
    subStreamNames = new HashSet<>();
  }

  public StreamDesc(StreamStoreDesc src) {
    duplicate(src);
    subStreamNames = new HashSet<>();
  }

  /**
   * Method to duplicate a StreamConfig object.
   *
   * <p>The purpose of the method is to decouple the object from later modification. It is necessary
   * for implementing unidirectional data flow.
   *
   * @return Duplicated object
   */
  @Override
  public StreamDesc duplicate() {
    StreamDesc dup = new StreamDesc();
    dup.duplicate(this);
    dup.parent = parent;
    dup.parentStream = parentStream;
    dup.subStreamNames.addAll(subStreamNames);
    if (prev != null) {
      dup.prev = prev.duplicate();
    }
    if (streamConversion != null) {
      dup.streamConversion = streamConversion.duplicate();
    }
    dup.preprocessStages = copyList(preprocessStages);
    dup.compiledEnrichments = copyList(compiledEnrichments);
    dup.contextAdjuster = contextAdjuster;
    dup.sketches.clear();
    for (final var entry : sketches.entrySet()) {
      final var sketchTypeMap = new SketchTypeMap();
      dup.sketches.put(entry.getKey(), sketchTypeMap);
      for (final var entry2 : entry.getValue().entrySet()) {
        final var sketchDurationMap = new SketchDurationMap();
        sketchTypeMap.put(entry2.getKey(), sketchDurationMap);
        for (final var entry3 : entry2.getValue().entrySet()) {
          final var sketchStatus =
              new SketchStatus(
                  entry3.getValue().doneSince,
                  entry3.getValue().doneUntil,
                  entry3.getValue().doneCoverage);
          sketchDurationMap.put(entry3.getKey(), sketchStatus);
        }
      }
    }
    dup.rollupInterval = rollupInterval;
    dup.rollupHorizon = rollupHorizon;
    dup.tags.addAll(tags);
    dup.primaryKeyAttributes = copyList(primaryKeyAttributes);
    return dup;
  }

  @Override
  public StreamDesc setVersion(Long version) {
    super.setVersion(version);
    return this;
  }

  @Override
  public StreamDesc setDeleted(boolean isDeleted) {
    this.deleted = isDeleted;
    return this;
  }

  @Override
  public StreamDesc addAttribute(AttributeDesc attr) {
    super.addAttribute(attr);
    return this;
  }

  public StreamDesc addSubStreamName(String name) {
    subStreamNames.add(name);
    return this;
  }

  @Override
  public Integer getFormatVersion() {
    if (formatVersion == null) {
      return parentStream != null ? parentStream.getFormatVersion() : FormatVersion.DEFAULT;
    } else {
      return formatVersion;
    }
  }

  /**
   * Sets the previous stream description to form stream modification history chain.
   *
   * <p>The method assumes the previous StreamDesc already has its modification history chain.
   *
   * @param prev previous version of StreamDesc object
   * @throws RuntimeException when linking the prev object causes history loop, i.e., some part of
   *     the prev chain is this object.
   */
  public void setPrev(StreamDesc prev) {
    for (StreamDesc current = prev; current != null; current = current.prev) {
      if (current == this) {
        throw new RuntimeException("Found a loop in a StreamDesc history list");
      }
    }
    this.prev = prev;
  }

  /**
   * Method to find a view descriptor registered in the stream.
   *
   * @param viewName View name
   * @return Found view descriptor or null
   */
  public ViewDesc findView(String viewName) {
    if (views == null) {
      return null;
    }
    for (ViewDesc view : views) {
      if (view.getName().equalsIgnoreCase(viewName)) {
        return view;
      }
    }
    return null;
  }

  /**
   * Method to find a rollup descriptor registered in the stream.
   *
   * @param name Rollup name. The name matches case insensitively.
   * @return Found rollup descriptor or null.
   */
  public Rollup findRollup(String name) {
    if (postprocesses == null) {
      return null;
    }
    for (PostprocessDesc pop : postprocesses) {
      if (pop.getRollups() == null) {
        continue;
      }
      for (Rollup rollup : pop.getRollups()) {
        if (rollup.getName().equalsIgnoreCase(name)) {
          return rollup;
        }
      }
    }
    return null;
  }

  public Rollup findRollupByView(String viewName) {
    if (postprocesses == null) {
      return null;
    }
    for (PostprocessDesc pop : postprocesses) {
      if (pop.getRollups() != null
          && pop.getView() != null
          && !pop.getRollups().isEmpty()
          && pop.getView().equalsIgnoreCase(viewName)) {
        return pop.getRollups().get(0);
      }
    }
    return null;
  }

  @Override
  public String getSchemaName() {
    return schemaName != null ? schemaName : name;
  }

  @Override
  public Long getSchemaVersion() {
    return schemaVersion != null ? schemaVersion : version;
  }

  /**
   * Answers if this is a main stream type.
   *
   * <p>Main streams are types configured by using admin config explicitly. They are so far signal
   * and context streams.
   *
   * @return true if main stream.
   */
  public boolean isMainType() {
    return type == StreamType.SIGNAL || type == StreamType.CONTEXT;
  }

  /**
   * Answers if this is an accessible stream type.
   *
   * <p>Accessible types are reachable via user SDK. They are so far signal, context, and metrics
   * streams.
   *
   * @return true if accessible type
   */
  public boolean isAccessibleType() {
    return type == StreamType.SIGNAL || type == StreamType.CONTEXT || type == StreamType.METRICS;
  }

  /**
   * Get the indexing interval.
   *
   * @return Shortest rollup interval in milliseconds if any rollup exists in the stream. If no
   *     rollup is set, the method returns -1.
   */
  public long getIndexingInterval() {
    long interval = -1;
    if (postprocesses != null) {
      for (PostprocessDesc pp : postprocesses) {
        if (pp.getRollups() != null) {
          for (Rollup rollup : pp.getRollups()) {
            final long current = rollup.getInterval().getValueInMillis();
            interval = interval == -1 || current < interval ? current : interval;
          }
        }
      }
    }
    if (interval >= 0) {
      return interval;
    }
    if (interval == -1 && views != null && !views.isEmpty()) {
      return TfosConfig.indexingDefaultIntervalSeconds() * 1000L;
    } else {
      return -1;
    }
  }

  /**
   * Returns the timestamp that points the origin of post processing.
   *
   * <p>The calculation formula is:
   *
   * <pre>
   * (long) (possible_oldest_timestamp / indexing_interval) * indexing_interval
   * </pre>
   *
   * @return Post processing origin timestamp in epoch milliseconds. If no view or post process is
   *     specified, the method returns null.
   */
  public long getPostProcessOrigin() {
    if (getType() == StreamType.VIEW) {
      return getOriginForView();
    } else {
      return getOriginDefault();
    }
  }

  private long getOriginDefault() {
    final long shortestInterval = getIndexingInterval();
    if (shortestInterval < 0) {
      return -1;
    }
    Long schemaVersion = getSchemaVersion();
    StreamDesc current = getPrev();
    while (current != null) {
      schemaVersion = current.getSchemaVersion();
      current = current.getPrev();
    }
    return schemaVersion / shortestInterval * shortestInterval;
  }

  private long getOriginForView() {
    final StreamDesc parentStream = getParentStream();
    final ViewDesc view = getViews().get(0);
    long schemaVersion = parentStream.getSchemaVersion();
    StreamDesc currentParent = parentStream.getPrev();
    while (currentParent != null) {
      final StreamConversion conv = currentParent.getStreamConversion();
      if (areAttributesCovered(conv, view)) {
        schemaVersion = currentParent.getSchemaVersion();
        currentParent = currentParent.getPrev();
      } else {
        currentParent = null;
      }
    }
    final long shortestInterval = parentStream.getIndexingInterval();
    return schemaVersion / shortestInterval * shortestInterval;
  }

  private boolean areAttributesCovered(StreamConversion conv, ViewDesc view) {
    for (String attribute : view.getAttributes()) {
      final AttributeConversion aconv = conv.getAttributeConversion(attribute);
      if (aconv == null || aconv.getConversionType() == ConversionType.ADD) {
        return false;
      }
    }
    for (String dimension : view.getGroupBy()) {
      final AttributeConversion aconv = conv.getAttributeConversion(dimension);
      if (aconv == null || aconv.getConversionType() == ConversionType.ADD) {
        return false;
      }
    }
    return true;
  }

  public void addSketchIfAbsent(
      String attributeName, DataSketchType sketchType, DataSketchDuration durationType) {
    final var sketchTypeMap = sketches.computeIfAbsent(attributeName, k -> new SketchTypeMap());
    final var sketchDurationMap =
        sketchTypeMap.computeIfAbsent(sketchType, k -> new SketchDurationMap());
    sketchDurationMap.computeIfAbsent(durationType, k -> new SketchStatus());
  }

  public void addTag(StreamTag tag) {
    tags.add(tag);
  }

  public StreamDesc duplicateWithoutIndeterminateFields() {
    final var out = this.duplicate();
    out.setInferredTagsForAdditionalAttributes(null);
    for (final var attribute : out.getAttributes()) {
      attribute.setInferredTags(null);
    }
    if (out.getAdditionalAttributes() != null) {
      for (final var attribute : out.getAdditionalAttributes()) {
        attribute.setInferredTags(null);
      }
    }
    return out;
  }

  /**
   * Checks if the stream is the latest undeleted version.
   *
   * <p>The isLatest flag maybe set 'unknown'. In that case, the method consider the stream is
   * active if undeleted.
   */
  public boolean isActive() {
    return isLatestVersion != Boolean.FALSE && !isDeleted();
  }

  public boolean isInternal() {
    return name.startsWith("_")
        || tags.stream().anyMatch((tag) -> Set.of(FAC, CONTEXT_AUDIT).contains(tag));
  }

  public List<ServerAttributeConfig> getBiosAttributes() {
    if (biosAttributes == null) {
      compile();
    }
    return biosAttributes;
  }

  public List<ServerAttributeConfig> getAllBiosAttributes() {
    if (allBiosAttributes == null) {
      compile();
    }
    return allBiosAttributes;
  }

  public ServerAttributeConfig getBiosAttribute(String attributeName) {
    if (biosAttributeMap == null) {
      compile();
    }
    return biosAttributeMap.get(attributeName.toLowerCase());
  }

  public void compile() {
    biosAttributes = new ArrayList<>();
    allBiosAttributes = new ArrayList<>();
    biosAttributeMap = new HashMap<>();
    attributes.forEach(
        (attr) -> {
          final var biosAttr = translateAttribute(attr, missingValuePolicy);
          biosAttributes.add(biosAttr);
          allBiosAttributes.add(biosAttr);
          biosAttributeMap.put(attr.getName().toLowerCase(), biosAttr);
        });
    if (additionalAttributes != null) {
      additionalAttributes.forEach(
          (attr) -> {
            final var biosAttr = translateAttribute(attr, missingValuePolicy);
            allBiosAttributes.add(biosAttr);
            biosAttributeMap.put(attr.getName().toLowerCase(), biosAttr);
          });
    }
    columnDefinitions = new ColumnDefinitionsBuilder().addAttributes(allBiosAttributes).build();

    if (type == StreamType.SIGNAL) {
      if (getParent() != null
          && BiosConstants.TENANT_SYSTEM.equalsIgnoreCase(getParent().getName())
          && BiosConstants.STREAM_ALL_CLIENT_METRICS.equalsIgnoreCase(getName())) {
        isClientMetrics = true;
        isAllClientMetrics = true;
      } else if (BiosConstants.STREAM_CLIENT_METRICS.equalsIgnoreCase(getName())) {
        isClientMetrics = true;
      }
    }
  }

  protected final ServerAttributeConfig translateAttribute(
      AttributeDesc tfosAttr, MissingAttributePolicyV1 globalMvp) {
    if (tfosAttr == null) {
      return null;
    }
    final var biosAttr = new ServerAttributeConfig();
    biosAttr.setName(tfosAttr.getName());
    switch (tfosAttr.getAttributeType()) {
      case STRING:
      case BOOLEAN:
      case LONG:
      case DOUBLE:
      case BLOB:
        biosAttr.setType(tfosAttr.getAttributeType().getBiosAttributeType());
        break;
      case ENUM:
        biosAttr.setType(io.isima.bios.models.AttributeType.STRING);
        if (tfosAttr.getEnum() != null) {
          biosAttr.setAllowedValues(
              tfosAttr.getEnum().stream()
                  .map(
                      entry ->
                          new AttributeValueGeneric(
                              entry, io.isima.bios.models.AttributeType.STRING))
                  .collect(Collectors.toList()));
        }
        break;
      default:
        // to have TFOS integration tests pass
        biosAttr.setType(io.isima.bios.models.AttributeType.UNSUPPORTED);
    }
    final var mvp = tfosAttr.getMissingValuePolicy();
    biosAttr.setMissingAttributePolicy(translateMvp(mvp != null ? mvp : globalMvp));

    // TODO(BIOS-4419): Fix this mess.
    // The problem here is that defaultValue for a blob attribute should come as byte[]
    // and it is sometimes string. internalDefaultValue is ByteBuffer consistently, but
    // you have to copy the content to retrieve byte array in it.
    final Object defaultValue = tfosAttr.getDefaultValue();
    if (defaultValue != null) {
      final AttributeValueGeneric attributeValue;
      if (tfosAttr.getAttributeType() == InternalAttributeType.BLOB) {
        final var internalDefaultValue = (ByteBuffer) tfosAttr.getInternalDefaultValue();
        final byte[] blobValue = new byte[internalDefaultValue.limit()];
        internalDefaultValue.get(blobValue);
        internalDefaultValue.flip();
        attributeValue = new AttributeValueGeneric(blobValue, biosAttr.getType());
      } else {
        attributeValue = new AttributeValueGeneric(defaultValue.toString(), biosAttr.getType());
      }
      biosAttr.setDefaultValue(attributeValue);
    }

    biosAttr.setTags(tfosAttr.getTags());

    biosAttr.compile();
    return biosAttr;
  }

  protected final io.isima.bios.models.MissingAttributePolicy translateMvp(
      MissingAttributePolicyV1 tfosPolicy) {
    if (tfosPolicy == null) {
      return null;
    }
    switch (tfosPolicy) {
      case STRICT:
        return io.isima.bios.models.MissingAttributePolicy.REJECT;
      case USE_DEFAULT:
        return io.isima.bios.models.MissingAttributePolicy.STORE_DEFAULT_VALUE;
      default:
        return null;
    }
  }

  // Object class method overrides //////////////////////////////////////

  @Override
  public String dump() {
    // TODO
    return "not yet implemented";
  }

  public StreamConfig asStreamConfig() {
    return new StreamConfig(this);
  }
}
