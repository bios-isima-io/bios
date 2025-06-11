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
package io.isima.bios.admin;

import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextFeatureConfig;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.MaterializedAs;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalFeatureConfig;
import io.isima.bios.models.TimeInterval;
import io.isima.bios.models.TimeunitType;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StreamConfigTranslator {

  private static final Pattern metricPattern;

  static {
    metricPattern = Pattern.compile("^\\s*\\S+\\s*\\(\\s*(\\S*)\\s*\\)\\s*$");
  }

  public static StreamConfig toTfos(SignalConfig signalConfig)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {

    final String entityName = "SignalConfig";

    final StreamConfig tfosConfig =
        translateFundamentalPart(signalConfig, StreamType.SIGNAL, entityName, null);

    if (signalConfig.getEnrich() != null) {
      translateEnrichPart(tfosConfig, signalConfig, entityName);
    }

    if (signalConfig.getPostStorageStage() != null) {
      translatePostStorageStagePart(tfosConfig, signalConfig, entityName);
    }

    return tfosConfig;
  }

  public static StreamConfig toTfos(ContextConfig contextConfig)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {

    final String entityName = "ContextConfig";

    final var tfosContext =
        translateFundamentalPart(
            contextConfig,
            StreamType.CONTEXT,
            entityName,
            (tfosAttributes, biosAttributes) -> {
              // handle primary key
              final List<String> primaryKey = contextConfig.getPrimaryKey();
              try {
                requireBeingSet(primaryKey, "%s.primaryKey must be set", entityName);
                requireTrue(primaryKey.size() > 0, "%s.primaryKey must not be empty", entityName);
                for (String primaryKeyAttribute : primaryKey) {
                  final var biosKey = biosAttributes.get(primaryKeyAttribute.toLowerCase());
                  requireBeingSet(
                      biosKey,
                      "%s.primaryKey['%s']: Primary key does not match any of the attribute names",
                      entityName,
                      primaryKeyAttribute);
                }
              } catch (ConstraintViolationValidatorException e) {
                throw new RuntimeException(e);
              }
            });

    // Copy the parts unique to contexts.
    tfosContext.setPrimaryKey(contextConfig.getPrimaryKey());
    tfosContext.setMissingLookupPolicy(
        toTfos(
            contextConfig.getMissingLookupPolicy(), "%s.enrich.missingLookupPolicy", entityName));
    List<EnrichmentConfigContext> srcEnrichments = contextConfig.getEnrichments();
    List<EnrichmentConfigContext> destEnrichments;
    if (srcEnrichments == null) {
      destEnrichments = null;
    } else {
      destEnrichments = new ArrayList<>();
      for (EnrichmentConfigContext entry : srcEnrichments) {
        destEnrichments.add(entry.duplicate());
      }
    }
    tfosContext.setEnrichments(destEnrichments);
    //
    // copy the audit flags
    Boolean auditEnabled = contextConfig.getAuditEnabled();
    if (auditEnabled == null) {
      auditEnabled = false;
    }
    tfosContext.setAuditEnabled(auditEnabled);
    tfosContext.setTtl(contextConfig.getTtl());
    tfosContext.setFeatures(
        StreamConfigTranslator.contextFeatureToTfosFeatures(contextConfig.getFeatures()));

    return tfosContext;
  }

  private static List<FeatureDesc> signalFeatureToTfosFeatures(List<SignalFeatureConfig> features) {
    if (features == null) {
      return null;
    }
    return features.stream()
        .map((biosFeature) -> new FeatureDesc(biosFeature))
        .collect(Collectors.toList());
  }

  private static List<FeatureDesc> contextFeatureToTfosFeatures(
      List<ContextFeatureConfig> features) {
    if (features == null) {
      return null;
    }
    return features.stream()
        .map((biosFeature) -> new FeatureDesc(biosFeature))
        .collect(Collectors.toList());
  }

  private static MissingAttributePolicyV1 toTfos(
      io.isima.bios.models.MissingAttributePolicy biosPolicy, String format, Object... args)
      throws NotImplementedValidatorException {

    if (biosPolicy == null) {
      return null;
    }

    switch (biosPolicy) {
      case REJECT:
        return MissingAttributePolicyV1.STRICT;
      case STORE_DEFAULT_VALUE:
        return MissingAttributePolicyV1.USE_DEFAULT;
      default:
        throw makeNotImplementedError(
            format + ": Policy %s is not supported", args, biosPolicy.name());
    }
  }

  private static MissingAttributePolicyV1 toTfos(
      MissingLookupPolicy biosPolicy, String format, Object... args)
      throws NotImplementedValidatorException {
    if (biosPolicy == null) {
      return null;
    }

    switch (biosPolicy) {
      case REJECT:
        return MissingAttributePolicyV1.STRICT;
      case STORE_FILL_IN_VALUE:
        return MissingAttributePolicyV1.USE_DEFAULT;
      case FAIL_PARENT_LOOKUP:
        return MissingAttributePolicyV1.FAIL_PARENT_LOOKUP;
      default:
        throw makeNotImplementedError(
            format + ": Policy %s is not supported", args, biosPolicy.name());
    }
  }

  public static AttributeDesc toTfos(AttributeConfig biosAttr, String translatorContext)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {
    // TODO sanity check
    final var tfosAttr = new AttributeDesc(biosAttr.getName(), toTfos(biosAttr.getType()));

    // handle enum type
    if (biosAttr.getAllowedValues() != null) {
      if (tfosAttr.getAttributeType() != InternalAttributeType.STRING) {
        throw makeNotImplementedError(
            "%s: AllowedValue is supported only with String type", translatorContext);
      }
      tfosAttr.setAttributeType(InternalAttributeType.ENUM);
      tfosAttr.setEnum(new ArrayList<>());
      for (int i = 0; i < biosAttr.getAllowedValues().size(); ++i) {
        final var value = biosAttr.getAllowedValues().get(i);
        requireBeingSet(value, "%s.allowedValues[%d] must be set", translatorContext, i);
        tfosAttr
            .getEnum()
            .add(
                validateString(
                    value.asString(),
                    "%s.allowedValue[%d] must not be blank",
                    translatorContext,
                    i));
      }
    }

    // handle missingAttributePolicy
    if (biosAttr.getMissingAttributePolicy() != null) {
      tfosAttr.setMissingValuePolicy(
          toTfos(
              biosAttr.getMissingAttributePolicy(), translatorContext + ".missingAttributePolicy"));
    }

    // Handle rest of the members.
    if (biosAttr.getDefaultValue() != null) {
      tfosAttr.setDefaultValue(biosAttr.getDefaultValue().asObject());
    }

    tfosAttr.setTags(biosAttr.getTags());
    tfosAttr.setInferredTags(biosAttr.getInferredTags());

    return tfosAttr;
  }

  public static InternalAttributeType toTfos(io.isima.bios.models.AttributeType biosAttributeType)
      throws NotImplementedValidatorException {
    if (biosAttributeType == null) {
      return null;
    }
    switch (biosAttributeType) {
      case STRING:
        return InternalAttributeType.STRING;
      case INTEGER:
        return InternalAttributeType.LONG;
      case DECIMAL:
        return InternalAttributeType.DOUBLE;
      case BOOLEAN:
        return InternalAttributeType.BOOLEAN;
      case BLOB:
        return InternalAttributeType.BLOB;
      default:
        throw new NotImplementedValidatorException(
            "InternalAttributeType " + biosAttributeType.name() + ": unsupported");
    }
  }

  private static StreamConfig translateFundamentalPart(
      BiosStreamConfig biosConfig,
      StreamType streamType,
      String entityName,
      BiConsumer<List<AttributeDesc>, Map<String, AttributeConfig>> attributeHook)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {
    final var streamConfig = new StreamConfig(biosConfig.getName(), streamType);
    streamConfig.setDescription(biosConfig.getDescription());
    streamConfig.setVersion(biosConfig.getVersion());

    requireBeingSet(
        biosConfig.getMissingAttributePolicy(),
        "%s.missingAttributePolicy must be set",
        entityName);
    streamConfig.setMissingValuePolicy(
        toTfos(biosConfig.getMissingAttributePolicy(), "%s.missingAttributePolicy", entityName));

    final var biosAttributes = new LinkedHashMap<String, AttributeConfig>();
    requireBeingSet(biosConfig.getAttributes(), "%s.attributes must be set", entityName);
    for (int i = 0; i < biosConfig.getAttributes().size(); ++i) {
      final var attr = biosConfig.getAttributes().get(i);
      requireBeingSet(attr, "%s.attributes[%d] must be set", entityName, i);
      var name =
          validateString(
              attr.getName(), "%s.attributes[%d].attributeName must be set", entityName, i);
      requireBeingSet(attr.getType(), "%s.attributes[%d].type must be set", entityName, i);
      biosAttributes.put(name.toLowerCase(), attr);
    }

    final var tfosAttributes = new ArrayList<AttributeDesc>(biosAttributes.size());

    if (attributeHook != null) {
      try {
        attributeHook.accept(tfosAttributes, biosAttributes);
      } catch (RuntimeException e) {
        if (e.getCause() instanceof ConstraintViolationValidatorException) {
          throw (ConstraintViolationValidatorException) e.getCause();
        } else if (e.getCause() instanceof NotImplementedValidatorException) {
          throw (NotImplementedValidatorException) e.getCause();
        }
        throw e;
      }
    }

    // handle the rest of attributes
    for (var biosAttribute : biosAttributes.values()) {
      final String translatorContext =
          String.format("%s.attributes[%s]", entityName, biosAttribute.getName());
      tfosAttributes.add(toTfos(biosAttribute, translatorContext));
    }
    streamConfig.setAttributes(tfosAttributes);

    streamConfig.setExportDestinationId(biosConfig.getExportDestinationId());
    return streamConfig;
  }

  private static void translateEnrichPart(
      StreamConfig tfosConfig, SignalConfig signalConfig, String entityName)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {

    Objects.requireNonNull(tfosConfig);
    Objects.requireNonNull(signalConfig);
    Objects.requireNonNull(signalConfig.getEnrich());

    final var streamMlp = signalConfig.getEnrich().getMissingLookupPolicy();
    tfosConfig.setMissingLookupPolicy(
        toTfos(streamMlp, "%s.enrich.missingLookupPolicy", entityName));

    List<EnrichmentConfigSignal> enrichments = signalConfig.getEnrich().getEnrichments();
    if (enrichments == null) {
      enrichments = List.of();
    }

    for (int i = 0; i < enrichments.size(); ++i) {
      final var enrichment = enrichments.get(i);
      final String translatorContext = String.format("%s.enrich.enrichments[%d]", entityName, i);
      final var prp = new PreprocessDesc();
      tfosConfig.addPreprocess(prp);

      // name <- name
      prp.setName(validateString(enrichment.getName(), "%s.name must be set", translatorContext));

      // condition <- foreignKey
      final var foreignKey = enrichment.getForeignKey();
      requireBeingSet(foreignKey, "%s.foreignKey must be set", entityName);
      requireTrue(foreignKey.size() > 0, "%s.foreignKey must not be empty", entityName);
      for (int j = 0; j < foreignKey.size(); ++j) {
        validateString(foreignKey.get(j), "%s.foreignKey[%d] must be set", translatorContext, j);
      }
      prp.setForeignKey(foreignKey);
      /*
       New schema has missing lookup policy at enrich level which will be inherited by enrichment
       level if no mlp is defined at enrichment level.
       We have no such thing in old schema. So we need to set old schema mlp
       if its defined in enrichment level. If not defined in enrichment level,
       then we need to fetch it from parent enrich level.
      */
      final var biosMlp =
          enrichment.getMissingLookupPolicy() != null
              ? enrichment.getMissingLookupPolicy()
              : streamMlp;
      prp.setMissingLookupPolicy(toTfos(biosMlp, translatorContext));

      // fetch context name (used later in actions)
      final var contextName =
          validateString(
              enrichment.getContextName(), "%s.contextName must be set", translatorContext);

      // actions <- contextAttributes
      requireBeingSet(
          enrichment.getContextAttributes(), "%s.contextAttributes must be set", translatorContext);
      for (int j = 0; j < enrichment.getContextAttributes().size(); ++j) {
        final var contextAttr = enrichment.getContextAttributes().get(j);
        final String tContext2 = String.format("%s.contextAttributes[%d]", translatorContext, j);
        requireBeingSet(contextAttr, "%s must be set", tContext2);

        final var action = new ActionDesc();
        prp.addAction(action);

        action.setActionType(ActionType.MERGE);
        action.setAttribute(
            validateString(contextAttr.getAttributeName(), "%s.attributeName must be set"));
        action.setContext(contextName);
        action.setAs(contextAttr.getAs()); // no need to validate
        if (contextAttr.getFillIn() != null) {
          action.setDefaultValue(contextAttr.getFillIn().asObject());
        } else if (contextAttr.getFillInSerialized() != null) {
          action.setDefaultValue(contextAttr.getFillInSerialized());
        }
      }
    }

    // Set dynamic enrichment configurations
    tfosConfig.setIngestTimeLag(signalConfig.getEnrich().getIngestTimeLag());
  }

  private static void translatePostStorageStagePart(
      StreamConfig tfosConfig, SignalConfig signalConfig, String entityName)
      throws ConstraintViolationValidatorException {

    Objects.requireNonNull(tfosConfig);
    Objects.requireNonNull(signalConfig);
    Objects.requireNonNull(signalConfig.getPostStorageStage());

    if (signalConfig.getPostStorageStage().getFeatures() != null) {
      translateFeaturesPart(tfosConfig, signalConfig, entityName);
    }
  }

  private static void translateFeaturesPart(
      StreamConfig tfosConfig, SignalConfig signalConfig, String entityName)
      throws ConstraintViolationValidatorException {

    Objects.requireNonNull(tfosConfig);
    Objects.requireNonNull(signalConfig);
    Objects.requireNonNull(signalConfig.getPostStorageStage().getFeatures());

    for (int i = 0; i < signalConfig.getPostStorageStage().getFeatures().size(); ++i) {
      final var feature = signalConfig.getPostStorageStage().getFeatures().get(i);
      final String tcontext = String.format("%s.postStorageStage.features[%d]", entityName, i);
      requireBeingSet(feature, "%s must not be null", tcontext);
      final var featureName = validateString(feature.getName(), "%s.name must be set", tcontext);

      // A View and a PoP that has a Rollup is necessary for a feature
      final var view = new ViewDesc();
      view.setName(featureName);
      tfosConfig.addView(view);
      // TODO(Naoki): Do stricter sanity check
      if (feature.getDimensions() != null) {
        view.setGroupBy(feature.getDimensions());
      } else {
        view.setGroupBy(new ArrayList<>());
      }
      view.setAttributes(feature.getAttributes());
      view.setDataSketches(feature.getDataSketches());
      view.setFeatureAsContextName(feature.getFeatureAsContextName());
      view.setLastN(
          (feature.getDataSketches() != null
                  && feature.getDataSketches().contains(DataSketchType.LAST_N))
              || feature.getMaterializedAs() == MaterializedAs.LAST_N);
      view.setLastNItems(feature.getItems());
      view.setLastNTtl(feature.getTtl());
      view.setIndexTableEnabled(feature.getIndexed());
      view.setIndexType(feature.getIndexType());
      view.setTimeIndexInterval(feature.getTimeIndexInterval());
      view.setSnapshot(feature.getMaterializedAs() == MaterializedAs.ACCUMULATING_COUNT);
      view.setWriteTimeIndexing(feature.getIndexOnInsert());

      final var pop = new PostprocessDesc();
      final var rollup = new Rollup();
      pop.setRollups(List.of(rollup));
      pop.setView(featureName);
      tfosConfig.addPostprocess(pop);

      final var rollupName = AdminUtils.makeRollupStreamName(signalConfig.getName(), featureName);
      rollup.setName(rollupName);
      requireBeingSet(feature.getFeatureInterval(), "%s.featureInterval must be set", tcontext);
      final var interval =
          new TimeInterval((int) (long) (feature.getFeatureInterval()), TimeunitType.MILLISECOND);
      rollup.setInterval(interval);
      rollup.setHorizon(interval);
      rollup.setAlerts(feature.getAlerts());
    }
  }

  protected static List<String> retrieveMetricDomains(List<String> metrics, String tcontext)
      throws ConstraintViolationValidatorException {
    Objects.requireNonNull(metrics);

    final var domains = new LinkedHashMap<String, String>();

    for (int i = 0; i < metrics.size(); ++i) {
      final String tc2 = String.format("%s.metrics[%d]", tcontext, i);
      final var metric = validateString(metrics.get(i), "%s must be set properly", tc2);
      final var domain = captureDomain(metric);
      requireBeingSet(domain, "%s: Syntax error", tc2);
      if (!domain.isEmpty()) {
        domains.put(domain.toLowerCase(), domain);
      }
    }

    return new ArrayList<>(domains.values());
  }

  protected static String captureDomain(String metric) {
    Objects.requireNonNull(metric);
    final var m = metricPattern.matcher(metric);
    return m.find() ? m.group(1).trim() : null;
  }

  private static void requireBeingSet(Object object, String format, Object... args)
      throws ConstraintViolationValidatorException {
    if (object == null) {
      throw makeError(format, args);
    }
  }

  private static void requireTrue(boolean condition, String format, Object... args)
      throws ConstraintViolationValidatorException {
    if (!condition) {
      throw makeError(format, args);
    }
  }

  private static String validateString(String target, String format, Object... args)
      throws ConstraintViolationValidatorException {
    requireBeingSet(target, format, args);
    if (target.isBlank()) {
      throw makeError(format, args);
    }
    return target;
  }

  private static void unsupported(boolean unsupportedCondition, String format, Object... args)
      throws NotImplementedValidatorException {
    if (unsupportedCondition) {
      throw makeNotImplementedError(format, args);
    }
  }

  private static ConstraintViolationValidatorException makeError(String format, Object... args) {
    return new ConstraintViolationValidatorException(String.format(format, args));
  }

  private static NotImplementedValidatorException makeNotImplementedError(
      String format, Object... args) {
    return new NotImplementedValidatorException(String.format(format, args));
  }
}
