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

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.v1.AttributeProxy;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream store configuration descriptor.
 *
 * <p>This is a subclass of {@link StreamConfig} class. In addition to the super class, this class
 * has internal schema information. The class is used for storing stream configuration into database
 * on AdminInternal write operations.
 */
@Getter
@Setter
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@JsonSerialize(using = StreamStoreDescSerializer.class)
public class StreamStoreDesc extends StreamConfig {
  private static final Logger logger = LoggerFactory.getLogger(StreamStoreDesc.class);

  @Accessors(chain = true)
  protected Boolean deleted;

  /**
   * Table name for the stream. The value can be null. Data engine would use default table name
   * creation rule when the value is null.
   */
  protected String tableName;

  protected String schemaName;

  /**
   * Table version for the stream. The table version may be different from stream version In case,
   * e.g., only view and post-process has modified.
   */
  protected Long schemaVersion;

  /**
   * Name of previous version of stream config. This is set when a new stream descriptor is created
   * for modifyStream.
   */
  protected String prevName;

  /**
   * Version of previous version of stream config. This is set when a new stream descriptor is
   * created for modifyStream.
   */
  protected Long prevVersion;

  protected Long indexWindowLength;

  /** AdminInternal and DataEngine data format version. */
  protected Integer formatVersion = FormatVersion.DEFAULT;

  /** Names of streams that this stream refers to, i.e. is dependent on. */
  @Setter(AccessLevel.NONE)
  protected Set<String> referencedStreams;

  /**
   * An integer to represent this stream's name in a compact form, as opposed to the full string
   * form, which occupies many bytes. This is used in database tables to save space.
   */
  protected Integer streamNameProxy;

  /**
   * Proxy and related information for each attribute (both direct and additional attributes). While
   * ordering is not necessary, its convenient for debugging to see attributes serialized in
   * insertion order (which is the same as proxy number order), so using a LinkedHashMap instead of
   * a plain HashMap.
   */
  protected final Map<String, AttributeProxy> attributeProxyInfo = new LinkedHashMap<>();

  /**
   * The maximum allocated attribute proxy number for the whole stream, across versions. Keeping
   * track of this ensures we do not allocate the same proxy number to two different attributes even
   * if one is deleted before creating another.
   */
  protected Short maxAttributeProxy;

  /**
   * We need to store inferred tags in order to be able to use them after a server restart. Inferred
   * tags for main attributes (attributes directly part of this stream) get stored along with the
   * main attribute. For additional attributes (enriched attributes etc.) we need to store them
   * separately.
   */
  protected Map<String, AttributeTags> inferredTagsForAdditionalAttributes =
      new ConcurrentHashMap<>();

  public StreamStoreDesc() {
    super();
  }

  public StreamStoreDesc(String name) {
    super(name);
  }

  public StreamStoreDesc(String name, Long version, Boolean deleted) {
    super(name, version);
    this.deleted = deleted;
  }

  public StreamStoreDesc(String name, StreamType type) {
    super(name, type);
  }

  public StreamStoreDesc(StreamConfig src) {
    super.duplicate(src);
  }

  public short getAttributeProxy(String attributeName) {
    return attributeProxyInfo.get(attributeName.toLowerCase()).getProxy();
  }

  public String getAttributeNameForProxy(short attributeProxy) {
    for (final var entry : attributeProxyInfo.entrySet()) {
      if (entry.getValue().getProxy() == attributeProxy) {
        return entry.getKey();
      }
    }
    return null;
  }

  public long getAttributeBaseVersion(String attributeName) {
    return attributeProxyInfo.get(attributeName.toLowerCase()).getBaseVersion();
  }

  @Override
  public StreamStoreDesc duplicate() {
    StreamStoreDesc dup = new StreamStoreDesc();
    dup.duplicate(this);
    return dup;
  }

  protected StreamStoreDesc duplicate(StreamStoreDesc src) {
    super.duplicate(src);
    deleted = src.deleted;
    tableName = src.tableName;
    schemaName = src.schemaName;
    schemaVersion = src.schemaVersion;
    prevName = src.prevName;
    prevVersion = src.prevVersion;
    formatVersion = src.formatVersion;
    indexWindowLength = src.indexWindowLength;
    if (src.referencedStreams != null) {
      for (final var ref : src.referencedStreams) {
        addDependency(ref);
      }
    }
    copyProxyInformationFromExisting(src);
    inferredTagsForAdditionalAttributes.putAll(src.inferredTagsForAdditionalAttributes);

    return this;
  }

  public boolean isDeleted() {
    return deleted != null ? deleted : false;
  }

  public StreamStoreDesc setDeleted(boolean isDeleted) {
    this.deleted = isDeleted;
    return this;
  }

  public void addDependency(String refStream) {
    if (referencedStreams == null) {
      referencedStreams = new HashSet<>();
    }
    referencedStreams.add(refStream);
  }

  /**
   * Assign short integer proxies to each attribute that is new or updated, populate related
   * information (version and datatype), and delete stale proxies.
   *
   * @return Returns true if any proxy information was changed.
   */
  public boolean initAttributeProxyInfo() {
    boolean changesMade = false;

    if (maxAttributeProxy == null) {
      maxAttributeProxy = 0;
      changesMade = true;
      logger.debug("Initialized maxAttributeProxy to 0 for stream={}", this.name);
    }

    // If there are any assigned proxy numbers, assert that the max reflects it consistently.
    for (final var proxyInfo : attributeProxyInfo.values()) {
      assert (proxyInfo.getProxy() <= maxAttributeProxy);
    }

    final var newAttributeList = new ArrayList<>(attributes);
    if (additionalAttributes != null) {
      newAttributeList.addAll(additionalAttributes);
    }
    final var newAttributeNamesSet = new HashSet<String>();
    newAttributeList.forEach(
        attributeDesc -> newAttributeNamesSet.add(attributeDesc.getName().toLowerCase()));

    // If an attribute is deleted, it's proxy should not be reused again later even if a new
    // attribute with the same name is added. To ensure that, remove proxy entries for any deleted
    // attributes.
    var iterator = attributeProxyInfo.entrySet().iterator();
    while (iterator.hasNext()) {
      final var entry = iterator.next();
      if (!newAttributeNamesSet.contains(entry.getKey())) {
        iterator.remove();
        changesMade = true;
        logger.debug(
            "Deleted proxy={} for stream={}, attribute={}",
            entry.getValue(),
            this.name,
            entry.getKey());
      }
    }

    for (var attribute : newAttributeList) {
      final String canonicalAttributeName = attribute.getName().toLowerCase();
      if (!attributeProxyInfo.containsKey(canonicalAttributeName)) {

        // If there is a new attribute, create a new proxy number for it.
        maxAttributeProxy++;
        final AttributeProxy proxyInfo =
            new AttributeProxy(
                maxAttributeProxy, defaultIfNull(this.version, -1L), attribute.getAttributeType());
        // TODO: A bunch of whitebox tests do not set version before calling initStreamConfig
        // or addStream. Fixing all those tests will be a huge amount of work; instead use
        // a default version of -1 if the version is missing. In production, this should not
        // happen and it will be validated by proxy-specific tests.
        attributeProxyInfo.put(canonicalAttributeName, proxyInfo);
        changesMade = true;
        logger.debug(
            "Assigned proxy={} for stream={}, attribute={}",
            proxyInfo,
            this.name,
            attribute.getName());
      } else {
        final var oldProxyInfo = attributeProxyInfo.get(canonicalAttributeName);
        // If the attribute datatype changed, replace it's proxy number with a new one.
        if (oldProxyInfo.getType() != attribute.getAttributeType()) {

          maxAttributeProxy++;
          // If the old datatype is convertible to the new datatype, retain the old version
          // because we can still create data sketches for that old data.
          final long newBaseVersion;
          if (attribute.getAttributeType().isConvertibleFrom(oldProxyInfo.getType())) {
            newBaseVersion = oldProxyInfo.getBaseVersion();
          } else {
            newBaseVersion = defaultIfNull(this.version, -1L);
          }
          final AttributeProxy newProxyInfo =
              new AttributeProxy(maxAttributeProxy, newBaseVersion, attribute.getAttributeType());
          attributeProxyInfo.remove(canonicalAttributeName); // To order by proxy in Map.
          attributeProxyInfo.put(canonicalAttributeName, newProxyInfo);
          changesMade = true;
          logger.debug(
              "Replaced oldProxy={} with newProxy={} for stream={}, attribute={}",
              oldProxyInfo,
              newProxyInfo,
              this.name,
              attribute.getName());
          // assert (this.type != StreamType.CONTEXT); TODO(BIOS-2338)
        }
      }
    }
    return changesMade;
  }

  /**
   * Copies information related to this stream that is not provided by the user but is stored in the
   * DB (via StreamStoreDesc). This is needed when modifying a stream - the new StreamDesc object is
   * created with the new stream config, but the old StreamStoreDesc information gets left behind
   * and needs to be copied separately.
   */
  public void copyDbStoredInformationFromExisting(StreamStoreDesc existing) {
    assert (streamNameProxy == null);
    assert (attributeProxyInfo.size() == 0);
    assert (maxAttributeProxy == null);
    copyProxyInformationFromExisting(existing);
    copyInferredTagsFromExisting(existing);
  }

  private void copyProxyInformationFromExisting(StreamStoreDesc existing) {
    streamNameProxy = existing.streamNameProxy;
    attributeProxyInfo.putAll(existing.attributeProxyInfo);
    maxAttributeProxy = existing.maxAttributeProxy;
  }

  private void copyInferredTagsFromExisting(StreamStoreDesc existing) {
    for (final var attribute : attributes) {
      final var existingAttribute = existing.findAnyAttribute(attribute.getName());
      if (existingAttribute != null) {
        attribute.setInferredTags(existingAttribute.getInferredTags());
      }
    }
    if (additionalAttributes != null) {
      for (final var attribute : additionalAttributes) {
        final var existingAttribute = existing.findAnyAttribute(attribute.getName());
        if (existingAttribute != null) {
          attribute.setInferredTags(existingAttribute.getInferredTags());
        }
      }
    }
    inferredTagsForAdditionalAttributes = existing.inferredTagsForAdditionalAttributes;
  }

  @Override
  public boolean equals(Object target) {
    if (target == null) {
      return false;
    }
    if (!(target instanceof StreamStoreDesc)) {
      return false;
    }
    final StreamStoreDesc that = (StreamStoreDesc) target;
    if (!StreamComparators.equals(this, that)) {
      return false;
    }
    return memberEquals(tableName, that.tableName)
        && memberEquals(schemaName, that.schemaName)
        && memberEquals(schemaVersion, that.schemaVersion)
        && memberEquals(prevName, that.prevName)
        && memberEquals(prevVersion, that.prevVersion)
        && memberEquals(indexWindowLength, that.indexWindowLength)
        && memberEquals(streamNameProxy, that.streamNameProxy)
        && memberEquals(attributeProxyInfo, that.attributeProxyInfo)
        && memberEquals(maxAttributeProxy, that.maxAttributeProxy)
        && memberEquals(getMissingLookupPolicy(), that.getMissingLookupPolicy())
        && memberEquals(
            inferredTagsForAdditionalAttributes, that.inferredTagsForAdditionalAttributes);
  }

  @Override
  public int hashCode() {
    return super.hashCode()
        + Objects.hash(
            tableName,
            schemaName,
            schemaVersion,
            prevName,
            prevVersion,
            indexWindowLength,
            streamNameProxy,
            attributeProxyInfo,
            maxAttributeProxy,
            getMissingLookupPolicy(),
            inferredTagsForAdditionalAttributes);
  }
}
