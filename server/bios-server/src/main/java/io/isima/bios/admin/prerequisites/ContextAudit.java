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
package io.isima.bios.admin.prerequisites;

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_OPERATION;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_PREFIX_PREV;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_FEATURE_INTERVAL;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_FEATURE_NAME;

import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentConfig;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.PostStorageStageConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalFeatureConfig;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ContextAudit {

  /**
   * Makes a configuration of audit signal for a context.
   *
   * @param contextConfig The original context config
   * @param auditSignalName Name of the audit signal
   * @return The audit signal configuration
   */
  public static SignalConfig makeAuditSignalConfig(
      ContextConfig contextConfig, String auditSignalName, SignalConfig existingAuditSignal)
      throws ConstraintViolationException {
    final var signalConfig = makeConfigFundamentalPart(contextConfig, auditSignalName);

    if (existingAuditSignal != null) {
      final var existingAttrs =
          existingAuditSignal.getAttributes().stream()
              .collect(
                  Collectors.toMap(
                      (attribute) -> attribute.getName().toLowerCase(), (attribute) -> attribute));

      final var unchangedAttributes = new HashSet<String>();

      for (var attribute : signalConfig.getAttributes()) {
        final var name = attribute.getName().toLowerCase();
        final var type = attribute.getType();
        if (existingAttrs.containsKey(name) && existingAttrs.get(name).getType() == type) {
          unchangedAttributes.add(name);
        } else {
          attribute.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
          if (attribute.getDefaultValue() == null) {
            final Object defaultValue;
            switch (type) {
              case STRING:
                defaultValue = "";
                break;
              case INTEGER:
                defaultValue = Long.valueOf(0);
                break;
              case DECIMAL:
                defaultValue = Double.valueOf(0.0);
                break;
              case BOOLEAN:
                defaultValue = Boolean.FALSE;
                break;
              case BLOB:
                defaultValue = ByteBuffer.allocate(0);
                break;
              default:
                throw new UnsupportedOperationException(
                    "Default value for type " + type + "not defined");
            }
            attribute.setDefaultValue(new AttributeValueGeneric(defaultValue, type));
          }
        }
      }

      // determine to keep or drop existing enrichments
      if (existingAuditSignal.getEnrich() != null) {
        final var oldEnrich = existingAuditSignal.getEnrich();
        final var newEnrich = new EnrichConfig();
        signalConfig.setEnrich(newEnrich);
        newEnrich.setIngestTimeLag(oldEnrich.getIngestTimeLag());
        newEnrich.setMissingLookupPolicy(oldEnrich.getMissingLookupPolicy());
        if (oldEnrich.getEnrichments() != null) {
          final var newEnrichments = new ArrayList<EnrichmentConfig>();
          for (var enrichment : oldEnrich.getEnrichments()) {
            if (enrichment.getForeignKey().stream()
                .allMatch((attr) -> unchangedAttributes.contains(attr.toLowerCase()))) {
              newEnrichments.add(enrichment);
              for (var contextAttribute : enrichment.getContextAttributes()) {
                final String enrichedAttribute =
                    contextAttribute.getAs() != null
                        ? contextAttribute.getAs()
                        : contextAttribute.getAttributeName();
                unchangedAttributes.add(enrichedAttribute.toLowerCase());
              }
            }
          }
          newEnrich.setEnrichments(
              oldEnrich.getEnrichments().stream()
                  .filter(
                      (enrichment) ->
                          enrichment.getForeignKey().stream()
                              .allMatch((attr) -> unchangedAttributes.contains(attr.toLowerCase())))
                  .collect(Collectors.toList()));
        }
      }

      // determine to keep or drop existing features
      if (existingAuditSignal.getPostStorageStage() != null
          && existingAuditSignal.getPostStorageStage().getFeatures() != null) {
        signalConfig.setPostStorageStage(new PostStorageStageConfig());
        signalConfig
            .getPostStorageStage()
            .setFeatures(
                existingAuditSignal.getPostStorageStage().getFeatures().stream()
                    .filter((feature) -> isFeatureValid(feature, unchangedAttributes))
                    .collect(Collectors.toList()));
      }
    } else {
      // Set the default feature
      final var feature = new SignalFeatureConfig();
      feature.setName(CONTEXT_AUDIT_FEATURE_NAME);
      feature.setDimensions(List.of(CONTEXT_AUDIT_ATTRIBUTE_OPERATION));
      feature.setAttributes(List.of());
      feature.setFeatureInterval(CONTEXT_AUDIT_FEATURE_INTERVAL);
      final var postStorageStage = new PostStorageStageConfig();
      postStorageStage.setFeatures(List.of(feature));
      signalConfig.setPostStorageStage(postStorageStage);
    }

    return signalConfig;
  }

  private static boolean isFeatureValid(
      SignalFeatureConfig feature, HashSet<String> unchangedAttributes) {
    final boolean dimensions =
        feature.getDimensions() == null
            || feature.getDimensions().stream()
                .allMatch((dimension) -> unchangedAttributes.contains(dimension.toLowerCase()));

    final boolean attributes =
        feature.getAttributes() == null
            || feature.getAttributes().stream()
                .allMatch((attribute) -> unchangedAttributes.contains(attribute.toLowerCase()));

    return dimensions && attributes;
  }

  public static SignalConfig makeConfigFundamentalPart(
      ContextConfig contextConfig, String auditSignalName) throws ConstraintViolationException {
    final var signalConfig = new SignalConfig(auditSignalName);
    signalConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);

    // Set attributes
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig(CONTEXT_AUDIT_ATTRIBUTE_OPERATION, AttributeType.STRING));
    if (contextConfig.getAttributes() == null || contextConfig.getAttributes().isEmpty()) {
      throw new ConstraintViolationException("Attributes are not set in the specified context");
    }
    for (var attribute : contextConfig.getAttributes()) {
      final var newAttr = new AttributeConfig(attribute);
      attributes.add(newAttr);
      final var attributeName = attribute.getName();
      final var prevName =
          CONTEXT_AUDIT_ATTRIBUTE_PREFIX_PREV
              + attributeName.substring(0, 1).toUpperCase()
              + attributeName.substring(1);
      final var prev = new AttributeConfig(attribute);
      prev.setName(prevName);
      attributes.add(prev);
    }

    signalConfig.setAttributes(attributes);

    return signalConfig;
  }
}
