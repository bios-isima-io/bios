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

import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextFeatureConfig;
import io.isima.bios.models.DataSketchDurationHelper;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.FeatureConfigBase;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.IngestTimeLagEnrichmentConfig;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.PostStorageStageConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalFeatureConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.Unit;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Class to provide validation methods for the AdminInternal component. */
public class Validators {
  // Conflict and/or reference check categories
  private static final String CATEGORY_ATTRIBUTES = "attributes";
  private static final String CATEGORY_FEATURES = "features";
  private static final String CATEGORY_ENRICHMENTS = "enrichments";

  // property names
  private static final String TENANT_CONFIG = "tenantConfig";
  private static final String TENANT_NAME = "tenantName";

  private static final String SIGNAL_CONFIG = "signalConfig";
  private static final String SIGNAL_NAME = "signalName";
  private static final String ENRICH = "enrich";
  private static final String POST_STORAGE_STAGE = "postStorageStage";
  private static final String MISSING_ATTRIBUTE_POLICY = "missingAttributePolicy";
  private static final String ATTRIBUTES = "attributes";
  private static final String ATTRIBUTE_NAME = "attributeName";
  private static final String ALLOWED_VALUES = "allowedValues";
  private static final String TYPE = "type";
  private static final String DEFAULT = "default";
  private static final String ATTRIBUTE_MEMBER_TAGS = "tags";
  private static final String VERSION_ATTRIBUTE = "versionAttribute";
  private static final String ENRICHMENTS = "enrichments";
  private static final String INGEST_TIME_LAG = "ingestTimeLag";
  private static final String INGEST_TIME_LAG_NAME = "ingestTimeLagName";
  private static final String OPERATION = "operation";
  private static final String ATTRIBUTE = "attribute";
  private static final String ENRICHMENT_NAME = "enrichmentName";
  private static final String FOREIGN_KEY = "foreignKey";
  private static final String MISSING_LOOKUP_POLICY = "missingLookupPolicy";
  private static final String CONTEXT_ATTRIBUTES = "contextAttributes";
  private static final String AS = "as";
  private static final String FILL_IN = "fillIn";
  private static final String FEATURES = "features";
  private static final String FEATURE_NAME = "featureName";
  private static final String DIMENSIONS = "dimensions";
  private static final String DATA_SKETCHES = "dataSketches";
  private static final String FEATURE_INTERVAL = "featureInterval";
  private static final String ALERT_NAME = "alertName";
  private static final String CONDITION = "condition";
  private static final String WEBHOOK_URL = "webhookUrl";
  private static final String TTL = "ttl";
  private static final String AUDIT_ENABLED = "auditEnabled";

  private static final String CONTEXT_CONFIG = "contextConfig";
  private static final String CONTEXT_NAME = "contextName";
  private static final String PRIMARY_KEY = "primaryKey";

  // Regex patterns
  private static final Pattern NAME_PATTERN;
  private static final Pattern INTERNAL_NAME_PATTERN;

  private static final Pattern CONTEXT_NAME_PATTERN;
  private static final Pattern INTERNAL_CONTEXT_NAME_PATTERN;

  // Reserved Context Names
  private static final List<String> RESERVED_CTX_NAMES = Arrays.asList("_ip2geo", "_ipBlacklist");

  static {
    NAME_PATTERN = Pattern.compile(ValidatorConstants.NAME_PATTERN);
    INTERNAL_NAME_PATTERN = Pattern.compile(ValidatorConstants.INTERNAL_NAME_PATTERN);

    CONTEXT_NAME_PATTERN = Pattern.compile(ValidatorConstants.CONTEXT_NAME_PATTERN);
    INTERNAL_CONTEXT_NAME_PATTERN =
        Pattern.compile(ValidatorConstants.INTERNAL_CONTEXT_NAME_PATTERN);
  }

  /**
   * Run initial sanity check against a tenant config.
   *
   * @param tenantConfig Tenant config to check.
   */
  public void validateTenant(TenantConfig tenantConfig) throws ValidatorException {
    final var state = new ValidationState(TENANT_CONFIG);
    validateNameString(tenantConfig.getName(), state.next(TENANT_NAME));
  }

  public void validateSignal(String tenantName, SignalConfig signalConfig)
      throws ValidatorException {
    validateSignal(tenantName, signalConfig, Set.of());
  }

  public void validateSignal(
      String tenantName, SignalConfig signalConfig, Collection<String> internalAttributes)
      throws ValidatorException {
    final var state = new ValidationState(SIGNAL_CONFIG);
    state.putContext(TENANT_NAME, tenantName);
    validateNameString(signalConfig.getName(), state.next(SIGNAL_NAME), true);
    state.putContext(SIGNAL_NAME, signalConfig.getName());
    validateStreamConfig(signalConfig, state, internalAttributes);
    validateEnrich(signalConfig.getEnrich(), state.next(ENRICH));
    validatePostStorageStage(signalConfig.getPostStorageStage(), state.next(POST_STORAGE_STAGE));
  }

  public void validateContext(String tenantName, ContextConfig contextConfig)
      throws ValidatorException {
    final var state = new ValidationState(CONTEXT_CONFIG);
    state.putContext(TENANT_NAME, tenantName);
    validateNameString(contextConfig.getName(), state.next(CONTEXT_NAME), true);
    state.putContext(CONTEXT_NAME, contextConfig.getName());
    validateStreamConfig(contextConfig, state, Set.of());
    validateContextPrimaryKey(contextConfig.getPrimaryKey(), state);

    validateContextFeatures(contextConfig, state);

    if ((contextConfig.getTtl() != null) && (contextConfig.getTtl() <= 0)) {
      throw constraintViolation("TTL must be positive", state.next(TTL));
    }
  }

  private void validateContextPrimaryKey(List<String> primaryKey, ValidationState state)
      throws ValidatorException {
    final var statePrimaryKey = state.next(PRIMARY_KEY);
    requireNonEmptyList(primaryKey, statePrimaryKey, null);

    var names = state.getNames(CATEGORY_ATTRIBUTES);
    for (int i = 0; i < primaryKey.size(); ++i) {
      final var next = state.next(makeIndexedPath(PRIMARY_KEY, i));
      final var primaryKeyAttribute = primaryKey.get(i);
      requireNonNull(primaryKeyAttribute, next, null);

      final var canon = primaryKeyAttribute.toLowerCase();
      if (!names.contains(canon)) {
        throw constraintViolation("Primary key must exist in attributes", next);
      }
    }
  }

  private void validateContextFeatures(ContextConfig contextConfig, ValidationState state)
      throws ValidatorException {
    if (contextConfig.getFeatures() == null || contextConfig.getFeatures().size() == 0) {
      return;
    }

    if (contextConfig.getAuditEnabled() == null || !contextConfig.getAuditEnabled()) {
      throw constraintViolation(
          "context features/indexes cannot be enabled without audit", state.next(AUDIT_ENABLED));
    }

    final var features = contextConfig.getFeatures();
    for (int i = 0; i < features.size(); ++i) {
      var feature = features.get(i);
      validateContextFeature(contextConfig, feature, state.next(makeIndexedPath(FEATURES, i)));
    }
  }

  private void validateStreamConfig(
      BiosStreamConfig streamConfig, ValidationState state, Collection<String> internalAttributes)
      throws ValidatorException {
    requireNonNull(
        streamConfig.getMissingAttributePolicy(), state.next(MISSING_ATTRIBUTE_POLICY), null);

    requireNonEmptyList(
        streamConfig.getAttributes(), state.next(ATTRIBUTES), "Attributes must be set");
    for (int i = 0; i < streamConfig.getAttributes().size(); ++i) {
      final var attribute = streamConfig.getAttributes().get(i);
      validateAttribute(
          attribute,
          streamConfig.getMissingAttributePolicy(),
          internalAttributes,
          state.next(makeIndexedPath(ATTRIBUTES, i)));
    }
  }

  private void validateAttribute(
      AttributeConfig attribute,
      MissingAttributePolicy globalPolicy,
      Collection<String> internalAttributes,
      ValidationState state)
      throws ValidatorException {
    requireNonNull(attribute, state, "Attribute config entry must not be null");

    // check name
    final var attributeName = attribute.getName();
    if (attributeName == null || !internalAttributes.contains(attributeName.toLowerCase())) {
      validateNameString(attributeName, state.next(ATTRIBUTE_NAME));
    }
    state.putContext(ATTRIBUTE_NAME, attribute.getName());
    final var names = state.getNames(CATEGORY_ATTRIBUTES);
    String canon = attribute.getName().toLowerCase();
    if (names.contains(canon)) {
      throw constraintViolation("Attribute name conflict", state.next(ATTRIBUTE_NAME));
    }
    names.add(canon);

    // check type
    requireNonNull(attribute.getType(), state.next(TYPE), null);

    // check allowedValues
    validateAllowedValues(attribute.getAllowedValues(), attribute.getType(), state);

    // check default
    MissingAttributePolicy policy = attribute.getMissingAttributePolicy();
    if (policy == null) {
      policy = globalPolicy;
    }
    if (policy == MissingAttributePolicy.STORE_DEFAULT_VALUE
        && attribute.getDefaultValue() == null) {
      throw constraintViolation(
          "Default value must be set when missing attribute policy is StoreDefaultValue",
          state.next(DEFAULT));
    }

    validateTags(attribute.getTags(), attribute.getType(), state.next(ATTRIBUTE_MEMBER_TAGS));
  }

  private void validateTags(AttributeTags tags, AttributeType attributeType, ValidationState state)
      throws ValidatorException {
    if (tags == null) {
      return;
    }
    // Only numeric types can be of category Quantity.
    if ((tags.getCategory() == AttributeCategory.QUANTITY)
        && (attributeType != AttributeType.INTEGER)
        && (attributeType != AttributeType.DECIMAL)) {
      throw constraintViolation(
          "Only numeric types can be of category 'Quantity'", state.next("category"));
    }
    // Only quantity category can have most of the rest of the tags for now.
    if (tags.getCategory() != AttributeCategory.QUANTITY) {
      if ((tags.getKind() != null)
          || (tags.getOtherKindName() != null)
          || (tags.getUnit() != null)
          || (tags.getUnitDisplayName() != null)
          || (tags.getUnitDisplayPosition() != null)
          || (tags.getPositiveIndicator() != null)) {
        throw constraintViolation(
            "category must be 'Quantity' in order to specify any tags other than summaries",
            state.next("category"));
      }
    }

    // otherKindName should be provided iff kind is OTHER_KIND.
    if ((tags.getKind() != AttributeKind.OTHER_KIND) && (tags.getOtherKindName() != null)) {
      throw constraintViolation(
          "otherKindName can only be specified if 'kind' is 'OtherKind'", state.next("kind"));
    }

    // Unit (if provided) must match Kind (which must be specified).
    if (tags.getUnit() != null) {
      if (tags.getKind() == null) {
        throw constraintViolation(
            "unit can only be specified if 'kind' is specified", state.next("kind"));
      }
      if ((tags.getUnit().getKind() != tags.getKind()) && (tags.getUnit() != Unit.OTHER_UNIT)) {
        throw constraintViolation(
            String.format(
                "unit %s is not applicable to kind %s",
                tags.getUnit().stringify(), tags.getKind().stringify()),
            state.next("unit"));
      }
    }

    // Second summary cannot be specified if first summary is not specified.
    if ((tags.getFirstSummary() == null) && (tags.getSecondSummary() != null)) {
      throw constraintViolation(
          "secondSummary cannot be specified if firstSummary is not specified",
          state.next("secondSummary"));
    }

    // First and second summary cannot both be the same.
    if ((tags.getFirstSummary() != null) && (tags.getFirstSummary() == tags.getSecondSummary())) {
      throw constraintViolation(
          "secondSummary cannot be equal to firstSummary", state.next("secondSummary"));
    }
  }

  private void validateAllowedValues(
      List<AttributeValueGeneric> allowedValues, AttributeType attributeType, ValidationState state)
      throws ValidatorException {
    if (allowedValues == null) {
      return;
    }
    if (attributeType != AttributeType.STRING) {
      throw notImplemented(
          String.format(
              "Attribute type '%s' is not supported with allowedValues yet",
              attributeType.stringify()),
          state.next(ALLOWED_VALUES));
    }

    if (allowedValues.isEmpty()) {
      throw constraintViolation("The list may not be empty", state.next(ALLOWED_VALUES));
    }

    final var appeared = new HashSet<String>();
    for (int i = 0; i < allowedValues.size(); ++i) {
      final var value = allowedValues.get(i);
      final var currentState = state.next(makeIndexedPath(ALLOWED_VALUES, i));
      if (value == null) {
        throw invalidValue("The value must be set", currentState);
      }
      if (value.getType() != attributeType) {
        throw constraintViolation(
            String.format(
                "Allowed value type '%s' does not match attribute type '%s'",
                value.getType().stringify(), attributeType.stringify()),
            currentState);
      }
      final var stringValue = value.asString();
      if (stringValue == null || stringValue.isBlank()) {
        throw invalidValue("The value may not be null or blank", currentState);
      }
      if (appeared.contains(stringValue)) {
        throw constraintViolation(
            String.format("The value '%s' conflicts with another entry", stringValue),
            currentState);
      }
      appeared.add(stringValue);
    }
  }

  private void validateEnrich(EnrichConfig enrich, ValidationState state)
      throws ValidatorException {
    if (enrich == null) {
      return;
    }
    final var globalPolicy = enrich.getMissingLookupPolicy();
    final var enrichments = enrich.getEnrichments();
    if (enrichments != null) {
      for (int i = 0; i < enrichments.size(); ++i) {
        validateEnrichment(
            enrichments.get(i), globalPolicy, state.next(makeIndexedPath(ENRICHMENTS, i)));
      }
    }
    final var ingestTimeLag = enrich.getIngestTimeLag();
    if (ingestTimeLag != null) {
      for (int i = 0; i < ingestTimeLag.size(); ++i) {
        validateIngestTimeLagEnrichment(
            ingestTimeLag.get(i), state.next(makeIndexedPath(INGEST_TIME_LAG, i)));
      }
    }
  }

  private void validateEnrichment(
      EnrichmentConfigSignal enrichment, MissingLookupPolicy globalPolicy, ValidationState state)
      throws ValidatorException {
    requireNonNull(enrichment, state, "Enrichment config entry must not be null");

    // check enrichment name
    validateNameString(enrichment.getName(), state.next(ENRICHMENT_NAME));
    state.putContext(ENRICHMENT_NAME, enrichment.getName());

    // check enrich name
    final var names = state.getNames(CATEGORY_ENRICHMENTS);
    final var canon = enrichment.getName().toLowerCase();
    if (names.contains(canon)) {
      throw constraintViolation("Enrichment names conflict", state.next(ENRICHMENT_NAME));
    }
    names.add(canon);

    // check foreign key
    requireNonEmptyList(
        enrichment.getForeignKey(), state.next(FOREIGN_KEY), "Foreign key must be set");
    validateEnrichmentForeignKey(enrichment.getForeignKey(), state);

    // check missing lookup policy
    var policy = enrichment.getMissingLookupPolicy();
    if (policy == null) {
      policy = globalPolicy;
    }
    requireNonNull(
        policy,
        state.next(MISSING_LOOKUP_POLICY),
        "MissingLookupPolicy must be set in this enrichment or globally in enrich");

    // check context name, we don't refer to the context yet here
    validateContextNameString(enrichment.getContextName(), state.next(CONTEXT_NAME));
    state.putContext(CONTEXT_NAME, enrichment.getContextName());

    // check context attributes
    requireNonEmptyList(
        enrichment.getContextAttributes(),
        state.next(CONTEXT_ATTRIBUTES),
        "Context attributes must be set");
    for (int i = 0; i < enrichment.getContextAttributes().size(); ++i) {
      validateContextAttribute(
          enrichment.getContextAttributes().get(i),
          policy,
          state.next(makeIndexedPath(CONTEXT_ATTRIBUTES, i)));
    }
  }

  private void validateEnrichmentForeignKey(List<String> foreignKey, ValidationState state)
      throws ValidatorException {
    for (int i = 0; i < foreignKey.size(); ++i) {
      final var foreignKeyElement = foreignKey.get(i);
      final var nextState = state.next(makeIndexedPath(FOREIGN_KEY, i));
      if (foreignKeyElement == null || foreignKeyElement.isBlank()) {
        throw constraintViolation("Foreign key must be set", nextState);
      }
      // The foreign key must match one of the signal attributes
      final var canon = foreignKeyElement.toLowerCase();
      final var names = state.getNames(CATEGORY_ATTRIBUTES);
      if (!names.contains(canon)) {
        throw constraintViolation(
            String.format(
                "Foreign key '%s' does not match any of attribute names", foreignKeyElement),
            nextState);
      }
    }
  }

  private void validateContextAttribute(
      EnrichmentAttribute attribute, MissingLookupPolicy policy, ValidationState state)
      throws ValidatorException {
    requireNonNull(attribute, state, "Context attribute config entry must not be null");

    // check names
    validateNameString(attribute.getAttributeName(), state.next(ATTRIBUTE_NAME));
    state.putContext(ATTRIBUTE_NAME, attribute.getAttributeName());
    if (attribute.getAs() != null) {
      validateNameString(attribute.getAs(), state.next(AS));
      state.putContext(AS, attribute.getAs());
    }

    // check name conflict
    var attributeName = attribute.getAs();
    String property = AS;
    if (attributeName == null) {
      attributeName = attribute.getAttributeName();
      property = ATTRIBUTE_NAME;
    }
    final var canon = attributeName.toLowerCase();
    final var names = state.getNames(CATEGORY_ATTRIBUTES);
    if (names.contains(canon)) {
      throw constraintViolation(
          String.format("Context attribute name '%s' conflicts another attribute", attributeName),
          state.next(property));
    }
    names.add(canon);

    // check fill in value
    if (policy == MissingLookupPolicy.STORE_FILL_IN_VALUE
        && attribute.getFillIn() == null
        && attribute.getFillInSerialized() == null) {
      throw constraintViolation(
          "FillIn value must be set when missing lookup policy is StoreFillIn",
          state.next(FILL_IN));
    }
  }

  private void validateIngestTimeLagEnrichment(
      IngestTimeLagEnrichmentConfig entry, ValidationState state) throws ValidatorException {
    requireNonNull(entry, state, "IngestTimeLag config entry must not be null");

    validateNameString(entry.getName(), state.next(INGEST_TIME_LAG_NAME));
    state.putContext(INGEST_TIME_LAG_NAME, entry.getName());

    final var attribute = entry.getAttribute();
    validateNameString(attribute, state.next(ATTRIBUTE));

    // check if attribute name is available
    // We are not checking the attribute data type here. It will be done in the AdminInternal
    // component
    final var normalizedAttribute = attribute.toLowerCase();
    final var names = state.getNames(CATEGORY_ATTRIBUTES);
    if (!names.contains(normalizedAttribute)) {
      throw constraintViolation(
          String.format(
              "IngestTimeLag attribute name '%s' does not exist in signal attributes", attribute),
          state.next(ATTRIBUTE));
    }

    state.putContext(ATTRIBUTE, attribute);
    final var as = entry.getAs();
    validateNameString(as, state.next(AS));

    // check tags if exists
    final var tags = entry.getTags();
    if (tags != null) {
      if (tags.getCategory() != AttributeCategory.QUANTITY
          || tags.getKind() != AttributeKind.DURATION
          || (tags.getUnit() != Unit.MILLISECOND
              && tags.getUnit() != Unit.SECOND
              && tags.getUnit() != Unit.MINUTE
              && tags.getUnit() != Unit.HOUR
              && tags.getUnit() != Unit.DAY
              && tags.getUnit() != Unit.WEEK)) {
        throw constraintViolation(
            "Tags of an ingestTimeLag entry must be category=Quantity, kind=Duration, and "
                + "unit=Millisecond/Second/Minute/Hour/Day/Week",
            state.next(ATTRIBUTE_MEMBER_TAGS));
      }
    }

    // check output name conflict
    final var normalizedOutput = as.toLowerCase();
    if (names.contains(normalizedOutput)) {
      throw constraintViolation(
          String.format(
              "IngestTimeLag enrichment attribute name '%s' conflicts another attribute", as),
          state.next(AS));
    }
    names.add(normalizedOutput);

    // check fill-in value
    final var defaultValueString = entry.getFillInSerialized();
    if (defaultValueString != null) {
      try {
        final Double defaultValue = Double.parseDouble(defaultValueString);
        entry.setFillIn(new AttributeValueGeneric(defaultValue, AttributeType.DECIMAL));
      } catch (NumberFormatException e) {
        throw invalidValue(
            "Fill-in value " + defaultValueString + " is invalid", state.next(FILL_IN));
      }
    }
  }

  private void validatePostStorageStage(
      PostStorageStageConfig postStorageStage, ValidationState state) throws ValidatorException {
    if (postStorageStage == null
        || postStorageStage.getFeatures() == null
        || postStorageStage.getFeatures().isEmpty()) {
      return;
    }

    final var features = postStorageStage.getFeatures();
    for (int i = 0; i < features.size(); ++i) {
      validateSignalFeature(features.get(i), state.next(makeIndexedPath(FEATURES, i)));
    }
  }

  private void validateSignalFeature(SignalFeatureConfig feature, ValidationState state)
      throws ValidatorException {
    validateFeatureBase(feature, state);

    // Check data sketches.
    if (feature.getDataSketches() != null) {
      final var dataSketches = feature.getDataSketches();
      if ((feature.getAttributes() == null) || (feature.getAttributes().size() == 0)) {
        throw constraintViolation(
            "Data sketches require at least one attribute to be specified",
            state.next(DATA_SKETCHES));
      }
      final var uniqueDataSketches = new HashSet<>();
      for (final DataSketchType sketch : dataSketches) {
        if (uniqueDataSketches.contains(sketch)) {
          throw constraintViolation(
              String.format(
                  "Data sketches must not be repeated; {%s} was repeated", sketch.stringify()),
              state.next(DATA_SKETCHES));
        }
        uniqueDataSketches.add(sketch);
        if (sketch != DataSketchType.LAST_N
            && (feature.getDimensions() != null)
            && (feature.getDimensions().size() != 0)) {
          throw notImplemented(
              String.format(
                  "Data sketch %s with group-by dimensions are not supported yet",
                  sketch.stringify()),
              state.next(DATA_SKETCHES));
        }
      }
    }

    // check feature interval
    final Long featureInterval = feature.getFeatureInterval();
    if (feature.getIndexOnInsert() != Boolean.TRUE) {
      validateFeatureInterval(featureInterval, state.next(FEATURE_INTERVAL));
    }

    if (feature.getAlerts() != null) {
      for (int i = 0; i < feature.getAlerts().size(); ++i) {
        validateAlert(feature.getAlerts().get(i), state.next(makeIndexedPath("alerts", i)));
      }
    }
  }

  private void validateFeatureBase(FeatureConfigBase feature, ValidationState state)
      throws ValidatorException {
    requireNonNull(feature, state, "Feature config entry must not be null");

    // check feature name
    final var nextStateFeatureName = state.next(FEATURE_NAME);
    validateNameString(feature.getName(), nextStateFeatureName);
    state.putContext(FEATURE_NAME, feature.getName());

    // check name conflict
    final var canon = feature.getName().toLowerCase();
    final var names = state.getNames(CATEGORY_FEATURES);
    if (names.contains(canon)) {
      throw constraintViolation(
          "The name conflicts with another feature entry", nextStateFeatureName);
    }
    names.add(canon);

    // check dimensions
    final var attributeNames = state.getNames(CATEGORY_ATTRIBUTES);
    if (feature.getDimensions() != null) {
      for (int i = 0; i < feature.getDimensions().size(); ++i) {
        final var dimension = feature.getDimensions().get(i);
        final String path = makeIndexedPath(DIMENSIONS, i);
        if (dimension == null || dimension.isBlank()) {
          throw constraintViolation(
              "Entry in feature dimensions may not be null or blank", state.next(path));
        }
        final var canonDimension = dimension.toLowerCase();
        // dimension value must be found in the attribute names
        if (!attributeNames.contains(canonDimension)) {
          throw constraintViolation(
              String.format(
                  "'%s' in feature dimensions not found in the stream attribute names", dimension),
              state.next(path));
        }
      }
    }

    // check attributes
    if (feature.getAttributes() != null) {
      for (int i = 0; i < feature.getAttributes().size(); ++i) {
        final var attribute = feature.getAttributes().get(i);
        final String path = makeIndexedPath(ATTRIBUTES, i);
        if (attribute == null || attribute.isBlank()) {
          throw constraintViolation(
              "Entry in feature attributes may not be null or blank", state.next(path));
        }
        final var canonAttribute = attribute.toLowerCase();
        // entry must be found in the stream attribute names
        if (!attributeNames.contains(canonAttribute)) {
          throw constraintViolation(
              String.format(
                  "'%s' in feature attributes not found in the stream attribute names", attribute),
              state.next(path));
        }
      }
    }

    // check feature interval
    final Long featureInterval = feature.getFeatureInterval();
    if (feature.getIndexOnInsert() != Boolean.TRUE) {
      validateFeatureInterval(featureInterval, state.next(FEATURE_INTERVAL));
    }
  }

  private void validateContextFeature(
      ContextConfig contextConfig, ContextFeatureConfig feature, ValidationState state)
      throws ValidatorException {
    validateFeatureBase(feature, state);

    // Do context-specific feature validation.
    boolean indexed = false;
    if (feature.getAggregated() == Boolean.FALSE) {
      throw notImplemented("Coming soon. Currently context feature must be aggregated", state);
    }
    if (feature.getIndexed() == Boolean.TRUE) {
      indexed = true;
    }
    if (feature.getTimeIndexInterval() != null) {
      throw constraintViolation(
          "Property 'timeIndexInterval' must not be set for a context feature",
          state.next("timeIndexInterval"));
    }

    final var indexType = feature.getIndexType();
    if (indexed) {
      if (feature.getDimensions() == null || feature.getDimensions().isEmpty()) {
        throw constraintViolation("Property 'dimensions' must be set when indexed is true", state);
      }
      if (indexType == null) {
        throw constraintViolation("Property 'indexType' must be set when indexed is true", state);
      }
      if (indexType == IndexType.EXACT_MATCH) {
        final var dimensions =
            feature.getDimensions().stream()
                .map((dimension) -> dimension.toLowerCase())
                .collect(Collectors.toList());
        final var primaryKey =
            contextConfig.getPrimaryKey().stream()
                .map((pk) -> pk.toLowerCase())
                .collect(Collectors.toList());
        if (dimensions.equals(primaryKey)) {
          throw constraintViolation("ExactMatch index to primary key cannot be set", state);
        }
      }
    } else if (indexType != null) {
      throw constraintViolation("Index type specified but indexed is not set to true", state);
    }
  }

  private void validateFeatureInterval(Long featureInterval, ValidationState state)
      throws ValidatorException {
    requireNonNull(featureInterval, state, null);
    state.putContext(FEATURE_INTERVAL, featureInterval.toString());
    final String errorMessage = DataSketchDurationHelper.validateFeatureInterval(featureInterval);
    if (errorMessage != null) {
      throw invalidValue(errorMessage, state);
    }
  }

  private void validateAlert(AlertConfig alert, ValidationState state) throws ValidatorException {
    requireNonNull(alert, state, null);

    // check name
    validateNameString(alert.getName(), state.next(ALERT_NAME));
    state.putContext(ALERT_NAME, alert.getName());

    // check condition
    requireNonNull(alert.getCondition(), state.next(CONDITION), null);

    // check webhookUrl
    requireNonNull(alert.getWebhookUrl(), state.next(WEBHOOK_URL), null);
  }

  // Exception generators //////////////////////////////////////////////////////////////

  private InvalidValueValidatorException invalidValue(String message, ValidationState state) {
    return new InvalidValueValidatorException(buildMessage(message, state));
  }

  private ConstraintViolationValidatorException constraintViolation(
      String message, ValidationState state) {
    return new ConstraintViolationValidatorException(buildMessage(message, state));
  }

  private NotImplementedValidatorException notImplemented(String message, ValidationState state) {
    return new NotImplementedValidatorException(buildMessage(message, state));
  }

  private String buildMessage(String message, ValidationState state) {
    final var sb = new StringBuilder(state.getPaths()).append(": ").append(message);
    final var context = state.getContext();
    if (!context.isEmpty()) {
      sb.append("; ")
          .append(
              context.entrySet().stream()
                  .map((entry) -> entry.getKey() + "=" + entry.getValue())
                  .collect(Collectors.joining(", ")));
    }
    return sb.toString();
  }

  // Generic checkers ////////////////////////////////////////////////////////////////
  public static <T> boolean requires(
      T property,
      String propertyName,
      String forSpecificCondition,
      ValidationState state,
      List<ValidatorException> errors) {
    if (property == null) {
      if (propertyName != null) {
        state = state.next(propertyName);
      }
      final var sb = new StringBuilder(String.format("Property '%s' is required", propertyName));
      if (forSpecificCondition != null) {
        sb.append(" ").append(forSpecificCondition);
      }
      errors.add(new ConstraintViolationValidatorException(state.buildMessage(sb.toString())));
      return false;
    }
    return true;
  }

  // Utilities ///////////////////////////////////////////////////////////////////////
  private <T> void requireNonNull(T value, ValidationState state, String message)
      throws ConstraintViolationValidatorException {
    if (value == null) {
      final String errorMessage = message != null ? message : "The value must be set";
      throw constraintViolation(errorMessage, state);
    }
  }

  private <T> void requireNonEmptyList(List<T> values, ValidationState state, String message)
      throws ConstraintViolationValidatorException {
    if (values == null || values.isEmpty()) {
      final String errorMessage = message != null ? message : "The values must be set";
      throw constraintViolation(errorMessage, state);
    }
  }

  private String makeIndexedPath(String collectionName, int index) {
    Objects.requireNonNull(collectionName);
    return collectionName + "[" + index + "]";
  }

  public void validateNameString(String name, ValidationState state) throws ValidatorException {
    validateNameString(name, state, false);
  }

  public void validateNameString(String name, ValidationState state, boolean allowDoubleLength)
      throws ValidatorException {
    requireNonNull(name, state, null);
    if (name.isEmpty()) {
      throw invalidValue("The value must not be empty", state);
    }
    state.putContext(state.getCurrentPath(), name);
    int maxLength = ValidatorConstants.MAX_NAME_LENGTH;
    if (allowDoubleLength) {
      maxLength *= 2;
    }
    if (name.length() > maxLength) {
      throw invalidValue("Length of a name may not exceed " + maxLength, state);
    }
    if (allowInternalStreamName()) {
      checkNamePattern(INTERNAL_NAME_PATTERN, name, state);
    } else {
      checkNamePattern(NAME_PATTERN, name, state);
    }
  }

  private void validateContextNameString(String name, ValidationState state)
      throws ValidatorException {
    requireNonNull(name, state, null);
    if (name.isEmpty()) {
      throw invalidValue("The value must not be empty", state);
    }
    state.putContext(state.getCurrentPath(), name);
    final int allowedLength = ValidatorConstants.MAX_NAME_LENGTH * 3 + 1;
    if (name.length() > allowedLength) {
      throw invalidValue("Length of a name may not exceed " + allowedLength, state);
    }
    if (RESERVED_CTX_NAMES.contains(name)) {
      return;
    }
    if (allowInternalStreamName()) {
      checkNamePattern(INTERNAL_CONTEXT_NAME_PATTERN, name, state);
    } else {
      checkNamePattern(CONTEXT_NAME_PATTERN, name, state);
    }
  }

  private void checkNamePattern(Pattern pattern, String target, ValidationState state)
      throws InvalidValueValidatorException {
    final var matcher = pattern.matcher(target);
    // The target should have a match and the it should be equal to the full matching pattern.
    if (!matcher.find() || !matcher.group().equals(target)) {
      throw invalidValue(
          "Name string must start with an alphanumeric followed by alphanumerics or underscores",
          state);
    }
  }

  private boolean allowInternalStreamName() {
    final String prop = SharedProperties.get("prop.allowInternalStreamName");
    return Boolean.parseBoolean(prop);
  }
}
