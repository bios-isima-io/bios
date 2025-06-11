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
package io.isima.bios.models.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.IngestTimeLagEnrichmentConfig;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.v1.validators.ValidStreamConfig;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to be used for sending a stream configuration over network. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ValidStreamConfig
@JsonSerialize(using = StreamConfigSerializer.class)
@ToString
public class StreamConfig extends Duplicatable {

  private static final Logger logger = LoggerFactory.getLogger(StreamConfig.class);

  /** Stream name. */
  @NotNull
  @Size(min = 1, max = 40)
  @Pattern(regexp = ValidatorConstants.NAME_PATTERN)
  protected String name;

  /** Signal/Stream ID. This property is meant to be used for Report APIs. */
  protected String streamId;

  protected StreamType type = StreamType.SIGNAL;

  /** An optional description that can be set by the user. */
  @Getter @Setter protected String description;

  // TODO(TFOS-951): remove implicit config
  protected MissingAttributePolicyV1 missingValuePolicy = MissingAttributePolicyV1.STRICT;

  protected Long version;

  // Audit the context Signal
  protected Boolean auditEnabled = false;

  /** Event attributes. */
  @NotNull @Valid protected List<AttributeDesc> attributes;

  @Getter @Setter @Valid protected List<String> primaryKey;

  /** Additional event attributes. */
  protected List<AttributeDesc> additionalAttributes;

  @Valid protected List<ViewDesc> views;

  /** Preprocess configuration. */
  @Valid protected List<PreprocessDesc> preprocesses;

  @Valid @Getter @Setter protected List<IngestTimeLagEnrichmentConfig> ingestTimeLag;

  /** Postprocess configuration. */
  @Valid protected List<PostprocessDesc> postprocesses;

  /** Missing lookup policy for enrichments. */
  protected MissingAttributePolicyV1 missingLookupPolicy;

  /** Enrichment schema definitions. */
  protected List<EnrichmentConfigContext> enrichments;

  /** Export config name. */
  protected String exportDestinationId;

  /**
   * Time to live in milliseconds - automatically delete entries after TTL. Only supported for
   * contexts for now.
   */
  @Getter @Setter protected Long ttl;

  /** Features and indexes for contexts - similar to indexes for signal. */
  @Getter @Setter protected List<FeatureDesc> features;

  /** Default constructor. */
  public StreamConfig() {
    attributes = new ArrayList<>();
  }

  /**
   * Constructor with stream name.
   *
   * @param name Stream name
   */
  public StreamConfig(String name) {
    this.name = name;
    attributes = new ArrayList<>();
  }

  public StreamConfig(String name, Long version) {
    this(name);
    this.version = version;
  }

  /**
   * Constructor with stream name and type.
   *
   * @param name Stream name
   */
  public StreamConfig(String name, StreamType type) {
    this.name = name;
    this.type = type;
    attributes = new ArrayList<>();
  }

  public StreamConfig(StreamConfig streamConfig) {
    duplicate(streamConfig);
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
  public StreamConfig duplicate() {
    return new StreamConfig().duplicate(this);
  }

  protected StreamConfig duplicate(StreamConfig src) {
    name = src.name;
    description = src.description;
    streamId = src.streamId;
    type = src.type;
    auditEnabled = src.auditEnabled;
    missingValuePolicy = src.missingValuePolicy;
    version = src.version;
    attributes = copyList(src.attributes);
    additionalAttributes = copyList(src.additionalAttributes);
    views = copyList(src.views);
    preprocesses = copyList(src.preprocesses);
    ingestTimeLag = copyList(src.ingestTimeLag);
    postprocesses = copyList(src.postprocesses);
    missingLookupPolicy = src.missingLookupPolicy;
    enrichments = copyList(src.enrichments);
    exportDestinationId = src.exportDestinationId;
    ttl = src.ttl;
    features = copyList(src.features);
    if (src.primaryKey != null) {
      primaryKey = List.copyOf(src.primaryKey);
    }
    return this;
  }

  /**
   * Return name of the stream.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Set name of the stream.
   *
   * @param name the name to set
   */
  public StreamConfig setName(String name) {
    this.name = name;
    return this;
  }

  public StreamType getType() {
    return type;
  }

  public StreamConfig setType(StreamType type) {
    this.type = type;
    return this;
  }

  public String getSignalId() {
    return streamId;
  }

  public void setSignalId(String signalId) {
    this.streamId = signalId;
  }

  public MissingAttributePolicyV1 getMissingValuePolicy() {
    return missingValuePolicy;
  }

  public StreamConfig setMissingValuePolicy(MissingAttributePolicyV1 policy) {
    this.missingValuePolicy = policy;
    return this;
  }

  /**
   * Returns event attributes.
   *
   * @return the eventAttributes
   */
  public List<AttributeDesc> getAttributes() {
    return attributes;
  }

  /**
   * Sets event attributes.
   *
   * @param eventAttributes the eventAttributes to set
   */
  public void setAttributes(List<AttributeDesc> eventAttributes) {
    this.attributes = eventAttributes;
  }

  /**
   * Adds an event attribute.
   *
   * @param attributeDesc Event attribute to add.
   * @return self
   */
  public StreamConfig addAttribute(AttributeDesc attributeDesc) {
    if (attributes == null) {
      attributes = new ArrayList<>();
    }
    attributes.add(attributeDesc);
    return this;
  }

  /**
   * Returns additional event attributes.
   *
   * @return the additionalEventAttributes
   */
  public List<AttributeDesc> getAdditionalAttributes() {
    return additionalAttributes;
  }

  /**
   * Sets additional event attributes.
   *
   * @param additionalEventAttributes the additionalEventAttributes to set
   */
  public void setAdditionalAttributes(List<AttributeDesc> additionalEventAttributes) {
    this.additionalAttributes = additionalEventAttributes;
  }

  /**
   * Adds an additional event attribute.
   *
   * @param attributeDesc Additional event attribute to add.
   * @return self
   */
  public StreamConfig addAdditionalAttribute(AttributeDesc attributeDesc) {
    if (additionalAttributes == null) {
      additionalAttributes = new ArrayList<>();
    }
    additionalAttributes.add(attributeDesc);
    return this;
  }

  public AttributeDesc findAttribute(String name) {
    for (AttributeDesc attr : attributes) {
      if (attr.getName().equalsIgnoreCase(name)) {
        return attr;
      }
    }
    return null;
  }

  public AttributeDesc findAdditionalAttribute(String name) {
    if (additionalAttributes == null) {
      return null;
    }
    for (AttributeDesc attr : additionalAttributes) {
      if (attr.getName().equalsIgnoreCase(name)) {
        return attr;
      }
    }
    return null;
  }

  /**
   * Finds an attribute in any attribute group - direct attributes or additional attributes, by
   * matching the name in a case-insensitive manner.
   */
  public AttributeDesc findAnyAttribute(String name) {
    for (AttributeDesc attr : attributes) {
      if (attr.getName().equalsIgnoreCase(name)) {
        return attr;
      }
    }
    if (additionalAttributes == null) {
      return null;
    }
    for (AttributeDesc attr2 : additionalAttributes) {
      if (attr2.getName().equalsIgnoreCase(name)) {
        return attr2;
      }
    }
    return null;
  }

  public List<ViewDesc> getViews() {
    return views;
  }

  /** Returns a view with the given name, in a case-insensitive manner; null if not found. */
  public ViewDesc getView(String viewName) {
    for (final var view : views) {
      if (view.getName().equalsIgnoreCase(viewName)) {
        return view;
      }
    }
    return null;
  }

  public void setViews(List<ViewDesc> views) {
    this.views = views;
  }

  public void addView(ViewDesc view) {
    if (views == null) {
      views = new ArrayList<>();
    }
    views.add(view);
  }

  public void addFeature(FeatureDesc feature) {
    if (features == null) {
      features = new ArrayList<>();
    }
    features.add(feature);
  }

  /**
   * Returns preprocesses.
   *
   * @return the preprocesses
   */
  public List<PreprocessDesc> getPreprocesses() {
    return preprocesses;
  }

  /**
   * Sets preprocesses.
   *
   * @param preprocesses the preprocesses to set
   */
  public void setPreprocesses(List<PreprocessDesc> preprocesses) {
    this.preprocesses = preprocesses;
  }

  /**
   * Adds a pre-process description.
   *
   * @param preprocess Pre-process description to add.
   * @return self
   */
  public StreamConfig addPreprocess(PreprocessDesc preprocess) {
    if (preprocesses == null) {
      preprocesses = new ArrayList<>();
    }
    preprocesses.add(preprocess);
    return this;
  }

  /**
   * Returns postprocesses.
   *
   * @return the postprocesses
   */
  public List<PostprocessDesc> getPostprocesses() {
    return postprocesses;
  }

  /**
   * Sets postprocesses.
   *
   * @param postprocesses the postprocesses to set
   */
  public void setPostprocesses(final List<PostprocessDesc> postprocesses) {
    this.postprocesses = postprocesses;
  }

  /**
   * Adds a post-process description.
   *
   * @param postprocess Postprocess description to add.
   * @return self
   */
  public StreamConfig addPostprocess(PostprocessDesc postprocess) {
    if (postprocesses == null) {
      postprocesses = new ArrayList<>();
    }
    postprocesses.add(postprocess);
    return this;
  }

  public Long getVersion() {
    return version;
  }

  public StreamConfig setVersion(Long version) {
    this.version = version;
    return this;
  }

  @JsonProperty("globalLookupPolicy")
  public MissingAttributePolicyV1 getMissingLookupPolicy() {
    return missingLookupPolicy;
  }

  public void setMissingLookupPolicy(MissingAttributePolicyV1 policy) {
    this.missingLookupPolicy = policy;
  }

  public List<EnrichmentConfigContext> getContextEnrichments() {
    return enrichments;
  }

  public void setEnrichments(List<EnrichmentConfigContext> enrichments) {
    this.enrichments = enrichments;
  }

  public StreamConfig addEnrichment(EnrichmentConfigContext enrichment) {
    if (enrichments == null) {
      enrichments = new ArrayList<>();
    }
    enrichments.add(enrichment);
    return this;
  }

  public String getExportDestinationId() {
    return exportDestinationId;
  }

  public void setExportDestinationId(String exportDestinationName) {
    logger.debug("Signal {}, export config set to : {}", name, exportDestinationName);
    this.exportDestinationId = exportDestinationName;
  }

  public Boolean getAuditEnabled() {
    if (auditEnabled == null) {
      return false;
    }
    return auditEnabled;
  }

  public void setAuditEnabled(Boolean auditEnabled) {
    this.auditEnabled = auditEnabled;
  }

  public String dump() {
    // TODO
    return "not yet implemented";
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        type,
        version,
        missingValuePolicy,
        attributes,
        additionalAttributes,
        preprocesses,
        views,
        postprocesses,
        missingLookupPolicy,
        enrichments,
        exportDestinationId,
        auditEnabled,
        ttl,
        features);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    StreamConfig other = (StreamConfig) obj;
    if (!Objects.equals(name, other.name)) {
      return false;
    }
    if (!Objects.equals(streamId, other.streamId)) {
      return false;
    }
    if (!Objects.equals(description, other.description)) {
      return false;
    }
    if (type != other.type || missingValuePolicy != other.missingValuePolicy) {
      return false;
    }
    if (!Objects.equals(version, other.version)) {
      return false;
    }
    if (!Objects.equals(attributes, other.attributes)) {
      return false;
    }
    if (!Objects.equals(additionalAttributes, other.additionalAttributes)) {
      return false;
    }
    if (!Objects.equals(preprocesses, other.preprocesses)) {
      return false;
    }
    if (!Objects.equals(views, other.views)) {
      return false;
    }
    if (!Objects.equals(postprocesses, other.postprocesses)) {
      return false;
    }
    if (!Objects.equals(missingLookupPolicy, other.missingLookupPolicy)) {
      return false;
    }
    if (!Objects.equals(enrichments, other.enrichments)) {
      return false;
    }
    if (!Objects.equals(exportDestinationId, other.exportDestinationId)) {
      return false;
    }
    if (!Objects.equals(auditEnabled, other.auditEnabled)) {
      return false;
    }
    if (!Objects.equals(ttl, other.ttl)) {
      return false;
    }
    if (!Objects.equals(features, other.features)) {
      return false;
    }
    return true;
  }

  /**
   * Checks string config validity.
   *
   * <p>The method checks - if the object exists - if the stream config has a name TODO more items
   * will come
   *
   * @param streamConfig Stream configuration to validate
   * @return Whether if the given configuration is valid.
   */
  public static boolean validate(StreamConfig streamConfig) {
    if (streamConfig == null
        || streamConfig.getName() == null
        || streamConfig.getName().isEmpty()) {
      return false;
    }
    return true;
  }
}
