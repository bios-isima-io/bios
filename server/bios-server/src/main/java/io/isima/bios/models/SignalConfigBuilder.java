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
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Builder class for generating a BIOS SignalConfig object. */
public class SignalConfigBuilder extends StreamConfigBuilder<SignalConfig, SignalConfigBuilder> {

  public static SignalConfigBuilder getBuilder(StreamDesc src) {
    Objects.requireNonNull(src, "src");
    if (src.getType() != StreamType.SIGNAL && src.getType() != StreamType.METRICS) {
      throw new IllegalArgumentException(
          "Source stream must be of signal type, but given stream was " + src.getType());
    }
    return new SignalConfigBuilder(src);
  }

  private SignalConfigBuilder(StreamDesc src) {
    super(src);
  }

  @Override
  protected String streamType() {
    return "signal";
  }

  @Override
  protected SignalConfig buildFromStreamDesc() throws InvalidRequestException {
    final var signal = new SignalConfig();
    signal.setName(streamDesc.getName());
    if (least) {
      return signal;
    }
    signal.setDescription(streamDesc.getDescription());
    signal.setVersion(streamDesc.getVersion());
    signal.setBiosVersion(streamDesc.getSchemaVersion());
    signal.setMissingAttributePolicy(translateMvp(streamDesc.getMissingValuePolicy()));
    signal.setAttributes(
        translateAttributes(streamDesc.getAttributes(), streamDesc.getMissingValuePolicy()));
    signal.setEnrich(
        translatePreprocesses(
            streamDesc.getPreprocesses(),
            streamDesc.getAdditionalAttributes(),
            streamDesc.getIngestTimeLag(),
            streamDesc.getMissingLookupPolicy()));
    signal.setPostStorageStage(
        translateViews(streamDesc.getViews(), streamDesc.getPostprocesses()));
    signal.setExportDestinationId(streamDesc.getExportDestinationId());
    signal.setIsInternal(streamDesc.isInternal());
    return signal;
  }

  private EnrichConfig translatePreprocesses(
      List<PreprocessDesc> preprocesses,
      List<AttributeDesc> additionalAttributes,
      List<IngestTimeLagEnrichmentConfig> dynamic,
      MissingAttributePolicyV1 missingLookupPolicy)
      throws InvalidRequestException {
    final var enrich = new EnrichConfig();
    enrich.setIngestTimeLag(dynamic);
    if (preprocesses == null) {
      return dynamic != null ? enrich : null;
    }
    enrich.setMissingLookupPolicy(translateMlp(missingLookupPolicy));
    final var enrichments = new ArrayList<EnrichmentConfigSignal>();
    enrich.setEnrichments(enrichments);
    for (var preprocess : preprocesses) {
      if (preprocess.getActions() == null || preprocess.getActions().isEmpty()) {
        continue;
      }
      var enrichment = new EnrichmentConfigSignal();
      enrichment.setName(preprocess.getName());
      var foreignKey = preprocess.getForeignKey();
      if (foreignKey == null) {
        foreignKey = List.of(preprocess.getCondition());
      }
      enrichment.setForeignKey(foreignKey);
      enrichment.setMissingLookupPolicy(translateMlp(preprocess.getMissingLookupPolicy()));
      // we should visit all actions strictly, but checking the first one is good enough since no
      // users set different contexts in a preprocess entry.
      enrichment.setContextName(preprocess.getActions().get(0).getContext());
      final var attrs = new ArrayList<EnrichmentAttribute>();
      for (var action : preprocess.getActions()) {
        var attr = new EnrichmentAttribute();
        attr.setAttributeName(action.getAttribute());
        attr.setAs(action.getAs());
        if (action.getDefaultValue() != null) {
          final String storedAttr = attr.getAs() != null ? attr.getAs() : attr.getAttributeName();
          AttributeDesc desc = null;
          for (var entry : additionalAttributes) {
            if (entry.getName().equalsIgnoreCase(storedAttr)) {
              desc = entry;
              break;
            }
          }
          if (desc != null) {
            final var temp = translateAttribute(desc, null);
            attr.setFillIn(temp.getDefaultValue());
          }
        }
        attrs.add(attr);
      }
      enrichment.setContextAttributes(attrs);
      enrichments.add(enrichment);
    }
    return enrich;
  }

  private PostStorageStageConfig translateViews(
      List<ViewDesc> views, List<PostprocessDesc> postprocesses) {
    if (views == null || postprocesses == null) {
      return null;
    }

    final var features = new LinkedHashMap<String, FeatureDesc>();
    for (var view : views) {
      FeatureDesc feature = view.toFeatureConfig();
      if (feature.getDimensions().isEmpty()) {
        feature.setDimensions(null);
      }
      features.put(feature.getName().toLowerCase(), feature);
    }

    for (var postprocess : postprocesses) {
      if (postprocess.getView() == null) {
        continue;
      }
      var feature = features.get(postprocess.getView().toLowerCase());
      if (feature == null) {
        continue;
      }
      if (postprocess.getRollups() == null) {
        continue;
      }
      for (var rollup : postprocess.getRollups()) {
        final var interval = rollup.getInterval();
        if (interval != null) {
          feature.setFeatureInterval(Long.valueOf(interval.getValueInMillis()));
          feature.setAlerts(rollup.getAlerts());
          break;
        }
      }
    }

    // We loop again to preserve the order of the original views
    final PostStorageStageConfig pssc = new PostStorageStageConfig();
    pssc.setFeatures(
        features.values().stream()
            .filter(
                entry ->
                    entry.getFeatureInterval() != null
                        || entry.getWriteTimeIndexing() == Boolean.TRUE)
            .map((entry) -> entry.toSignalFeatureConfig())
            .collect(Collectors.toList()));

    return pssc;
  }
}
