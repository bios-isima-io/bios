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
package io.isima.bios.models;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.StreamType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Builder class for generating a BIOS ContextConfig object. */
public class ContextConfigBuilder extends StreamConfigBuilder<ContextConfig, ContextConfigBuilder> {

  public static ContextConfigBuilder getBuilder(StreamDesc src) {
    Objects.requireNonNull(src, "src");
    if (src.getType() != StreamType.CONTEXT) {
      throw new IllegalArgumentException(
          "Source stream must be of context type, but given stream was " + src.getType());
    }
    return new ContextConfigBuilder(src);
  }

  private ContextConfigBuilder(StreamDesc src) {
    super(src);
  }

  @Override
  public ContextConfigBuilder least(boolean detail) {
    this.least = detail;
    return this;
  }

  @Override
  protected String streamType() {
    return "context";
  }

  @Override
  protected ContextConfig buildFromStreamDesc() throws InvalidRequestException {
    final var context = new ContextConfig();
    context.setName(streamDesc.getName());
    if (least) {
      return context;
    }
    context.setDescription(streamDesc.getDescription());
    context.setVersion(streamDesc.getVersion());
    context.setBiosVersion(streamDesc.getSchemaVersion());
    context.setMissingAttributePolicy(translateMvp(streamDesc.getMissingValuePolicy()));
    context.setAttributes(
        translateAttributes(streamDesc.getAttributes(), streamDesc.getMissingValuePolicy()));
    if (streamDesc.getPrimaryKey() != null) {
      context.setPrimaryKey(streamDesc.getPrimaryKey());
    } else {
      context.setPrimaryKey(List.of(streamDesc.getAttributes().get(0).getName()));
    }
    context.setExportDestinationId(streamDesc.getExportDestinationId());
    context.setMissingLookupPolicy(translateMlp(streamDesc.getMissingLookupPolicy()));
    context.setTtl(streamDesc.getTtl());
    context.setFeatures(toContextFeatures(streamDesc.getFeatures()));

    List<EnrichmentConfigContext> srcEnrichments = streamDesc.getContextEnrichments();
    List<EnrichmentConfigContext> destEnrichments;
    if (srcEnrichments == null) {
      destEnrichments = null;
    } else {
      destEnrichments = new ArrayList<EnrichmentConfigContext>();
      for (EnrichmentConfigContext entry : srcEnrichments) {
        destEnrichments.add((EnrichmentConfigContext) entry.duplicate());
      }
    }
    context.setEnrichments(destEnrichments);
    context.setAuditEnabled(streamDesc.getAuditEnabled());
    context.setIsInternal(streamDesc.isInternal());
    return context;
  }

  private List<ContextFeatureConfig> toContextFeatures(List<FeatureDesc> features) {
    if (features == null) {
      return null;
    }
    return features.stream()
        .map((featureDesc) -> featureDesc.toContextFeatureConfig())
        .collect(Collectors.toList());
  }
}
