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
package io.isima.bios.admin.v1.impl;

import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_OPERATION;
import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_TIMESTAMP;
import static io.isima.bios.feature.CountersConstants.OPERATION_VALUE_CHANGE;
import static io.isima.bios.feature.CountersConstants.OPERATION_VALUE_SET;

import io.isima.bios.admin.v1.FeatureAsContextInfo;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.StreamTag;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.exceptions.DirectedAcyclicGraphException;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.ViewDesc;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureValidator {
  private static final Logger logger = LoggerFactory.getLogger(FeatureValidator.class);

  public static void validateSignalFeature(
      TenantDesc tenantDesc, StreamDesc signalDesc, ViewDesc originalView)
      throws ConstraintViolationException {
    final Set<String> uniqueAttributes = new HashSet<>();
    FeatureDesc feature = originalView.toFeatureConfig();
    validateFeatureDimensions(tenantDesc, signalDesc, feature, uniqueAttributes, originalView);
    validateFeatureAttributes(tenantDesc, signalDesc, feature, uniqueAttributes, originalView);
    validateSketches(tenantDesc, signalDesc, feature);
    validateSpecialSignalFeatures(tenantDesc, signalDesc, feature);
  }

  public static void validateContextFeature(
      TenantDesc tenantDesc, StreamDesc contextDesc, FeatureDesc feature)
      throws ConstraintViolationException {
    Set<String> uniqueAttributes = new HashSet<>();
    checkSupportedFeaturesForContext(tenantDesc, contextDesc, feature);
    validateFeatureDimensions(tenantDesc, contextDesc, feature, uniqueAttributes, null);
    validateFeatureAttributes(tenantDesc, contextDesc, feature, uniqueAttributes, null);
  }

  private static void checkSupportedFeaturesForContext(
      TenantDesc tenantDesc, StreamDesc contextDesc, FeatureDesc feature)
      throws ConstraintViolationException {
    if (feature.getWriteTimeIndexing() == Boolean.TRUE) {
      StringBuilder sb =
          AdminImplUtils.createExceptionMessage(
                  "Write time indexing is not supported for a context", tenantDesc, contextDesc)
              .append(", feature=")
              .append(feature.getName());
      throw new ConstraintViolationException(sb.toString());
    }
    if (feature.getSnapshot() == Boolean.TRUE) {
      StringBuilder sb =
          AdminImplUtils.createExceptionMessage(
                  "Snapshot is not supported for a context", tenantDesc, contextDesc)
              .append(", feature=")
              .append(feature.getName());
      throw new ConstraintViolationException(sb.toString());
    }
    if (isLastN(feature)) {
      StringBuilder sb =
          AdminImplUtils.createExceptionMessage(
                  "LastN is not supported for a context", tenantDesc, contextDesc)
              .append(", feature=")
              .append(feature.getName());
      throw new ConstraintViolationException(sb.toString());
    }
  }

  private static boolean isLastN(FeatureDesc feature) {
    if (feature.getDataSketches() != null
        && feature.getDataSketches().contains(DataSketchType.LAST_N)) {
      return true;
    }
    return feature.getLastN() == Boolean.TRUE;
  }

  public static void validateFeatureDimensions(
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      FeatureDesc feature,
      Set<String> uniqueAttr,
      ViewDesc originalView)
      throws ConstraintViolationException {
    if (feature.getDimensions() == null) {
      StringBuilder sb =
          AdminImplUtils.createExceptionMessage(
                  "Feature property 'dimensions' is missing", tenantDesc, streamDesc)
              .append(", feature=")
              .append(feature.getName());
      throw new ConstraintViolationException(sb.toString());
    }
    if (feature.getWriteTimeIndexing() == Boolean.TRUE && feature.getDimensions().isEmpty()) {
      StringBuilder sb =
          AdminImplUtils.createExceptionMessage(
                  "Dimension may not be empty when 'indexOnInsert' is true", tenantDesc, streamDesc)
              .append(", feature=")
              .append(feature.getName());
      throw new ConstraintViolationException(sb.toString());
    }
    // We'll rebuild dimensions attribute names to preserve the original cases
    final List<String> dimensions = new ArrayList<>();
    for (String attr : feature.getDimensions()) {
      AdminImplUtils.sanityCheckStringItem(tenantDesc, streamDesc, attr, "feature.dimensions");
      final AttributeDesc attrDesc =
          AdminImplUtils.findAttributeReference(
              tenantDesc, streamDesc, attr, true, "feature.dimensions");
      dimensions.add(attrDesc.getName());
      if (!uniqueAttr.add(attr)) {
        StringBuilder sb =
            AdminImplUtils.createExceptionMessage(
                    "Feature properties 'dimensions' and 'attributes' may not have duplicates",
                    tenantDesc,
                    streamDesc)
                .append(", feature=")
                .append(feature.getName())
                .append(", attribute=")
                .append(attr);
        throw new ConstraintViolationException(sb.toString());
      }
    }
    feature.setDimensions(dimensions);
    if (originalView != null) {
      originalView.setGroupBy(dimensions);
    }
  }

  public static void validateFeatureAttributes(
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      FeatureDesc feature,
      Set<String> uniqueAttr,
      ViewDesc originalView)
      throws ConstraintViolationException {
    if (feature.getAttributes() == null) {
      StringBuilder sb =
          AdminImplUtils.createExceptionMessage(
                  "Feature property 'attributes' is missing", tenantDesc, streamDesc)
              .append(", feature=")
              .append(feature.getName());
      throw new ConstraintViolationException(sb.toString());
    }
    // We'll rebuild group attribute names to preserve the original cases
    final List<String> attributes = new ArrayList<>();
    for (String attr : feature.getAttributes()) {
      AdminImplUtils.sanityCheckStringItem(tenantDesc, streamDesc, attr, "feature.attributes'");
      final AttributeDesc attrDesc =
          AdminImplUtils.findAttributeReference(
              tenantDesc, streamDesc, attr, true, "feature.attributes");
      attributes.add(attrDesc.getName());
      if (!uniqueAttr.add(attr)) {
        StringBuilder sb =
            AdminImplUtils.createExceptionMessage(
                    "Feature properties 'dimensions' and 'attributes' may not have duplicates",
                    tenantDesc,
                    streamDesc)
                .append(", feature=")
                .append(feature.getName())
                .append(", attribute=")
                .append(attr);
        throw new ConstraintViolationException(sb.toString());
      }
    }
    feature.setAttributes(attributes);
    if (originalView != null) {
      originalView.setAttributes(attributes);
    }
  }

  public static void validateSketches(
      TenantDesc tenantDesc, StreamDesc signalDesc, FeatureDesc feature)
      throws ConstraintViolationException {
    final var genericDigestors = new LinkedHashSet<String>();
    final var dataSketchTypes =
        feature.getDataSketches() == null
            ? List.of(DataSketchType.MOMENTS)
            : feature.getDataSketches();
    for (final var sketchType : dataSketchTypes) {
      if (sketchType.isGenericDigestor()) {
        genericDigestors.add(sketchType.stringify());
      }
    }
    if (genericDigestors.size() > 1) {
      throw new ConstraintViolationException(
          AdminImplUtils.createExceptionMessage(
                  String.format(
                      "A feature can have only one generic digestor in data sketches, %s are found",
                      genericDigestors),
                  tenantDesc,
                  signalDesc)
              .append(", feature=")
              .append(feature.getName())
              .toString());
    }

    for (final var attributeName : feature.getAttributes()) {
      final var attributeType = signalDesc.findAnyAttribute(attributeName).getAttributeType();
      for (final var sketchType : dataSketchTypes) {
        if (sketchType == DataSketchType.LAST_N) {
          feature.setLastN(true);
        }
      }
    }
  }

  private static void validateLastN(
      TenantDesc tenantDesc, StreamDesc signalDesc, FeatureDesc feature)
      throws ConstraintViolationException {
    if (feature.getLastN() != Boolean.TRUE) {
      return;
    }
    final String message = validateLastNCore(tenantDesc, signalDesc, feature);
    if (message != null) {
      final var messageBuilder =
          AdminImplUtils.createExceptionMessage(message, tenantDesc, signalDesc)
              .append(", feature=")
              .append(feature.getName());
      if (feature.getFeatureAsContextName() != null) {
        messageBuilder.append(", featureAsContextName=").append(feature.getFeatureAsContextName());
      }
      throw new ConstraintViolationException(messageBuilder.toString());
    }
  }

  private static String validateLastNCore(
      TenantDesc tenantDesc, StreamDesc signalDesc, FeatureDesc feature) {
    final var contextName = feature.getEffectiveFeatureAsContextName(signalDesc.getName());
    if (StringUtils.isBlank(contextName)) {
      return "LastN: The context must be specified by featureAsContextName";
    }
    final var contextDesc = tenantDesc.getDependingStream(contextName, signalDesc);
    if (contextDesc == null) {
      return "LastN: The context specified by featureAsContextName is missing: " + contextName;
    }
    try {
      tenantDesc.addDependency(contextDesc, signalDesc);
    } catch (DirectedAcyclicGraphException e) {
      return String.format(
          "Cyclic dependency is detected, signal %s depends on context %s",
          contextName, signalDesc.getName());
    }
    contextDesc.getTags().add(StreamTag.FAC);
    final var featureName = signalDesc.getName() + "." + feature.getName();
    final Long items = feature.getLastNItems();
    if (items == null) {
      return "LastN: Feature attribute 'items' must be set";
    }
    if (items <= 0) {
      return "LastN: Feature attribute 'items' must be a positive integer";
    }
    final Long ttl = feature.getLastNTtl();
    if (ttl != null && ttl < 0) {
      return "LastN: Feature attribute 'ttl' may not be negative";
    }

    var info = tenantDesc.getFacInfoMap().get(contextName.toLowerCase());
    if (info == null) {
      info = new FeatureAsContextInfo(featureName, ttl);
      tenantDesc.getFacInfoMap().put(contextName.toLowerCase(), info);
    } else if (!info.getReferrer().equals(featureName)) {
      return String.format(
          "Two features %s and %s use the same context %s",
          info.getReferrer(), featureName, contextDesc.getName());
    }

    if (feature.getDimensions().isEmpty()) {
      return "Data sketch LastN requires a dimension";
    }
    if (feature.getDimensions().size() != contextDesc.getPrimaryKey().size()) {
      return "LastN: Dimensions and the context primary key"
          + " must have the same number of attributes";
    }
    for (int i = 0; i < feature.getDimensions().size(); ++i) {
      final var foreignKeyAttribute = signalDesc.findAttribute(feature.getDimensions().get(i));
      final var primaryKeyAttribute = contextDesc.getAttributes().get(i);
      if (foreignKeyAttribute.getAttributeType() != primaryKeyAttribute.getAttributeType()) {
        final var foreignType =
            foreignKeyAttribute.getAttributeType().getBiosAttributeType().stringify();
        final var remoteType =
            primaryKeyAttribute.getAttributeType().getBiosAttributeType().stringify();
        return String.format(
            "LastN: Foreign key '%s' of type %s does not match primary key '%s' of type %s",
            foreignKeyAttribute.getName(), foreignType, primaryKeyAttribute.getName(), remoteType);
      }
      if (!Objects.equals(foreignKeyAttribute.getEnum(), primaryKeyAttribute.getEnum())) {
        return String.format(
            "LastN: Allowed values mismatch between foreign key '%s.%s' and primary key '%s.%s'",
            signalDesc.getName(),
            foreignKeyAttribute.getName(),
            contextDesc.getName(),
            primaryKeyAttribute.getName());
      }
    }

    if (feature.getAttributes().isEmpty()) {
      return "Data sketch LastN requires an attribute";
    }
    if (feature.getAttributes().size() > 1) {
      return "Multiple attributes are not supported for data sketch LastN";
    }
    final var contextValueAttributes =
        contextDesc.getAttributes().stream()
            .filter((attribute) -> !contextDesc.getPrimaryKey().contains(attribute.getName()))
            .collect(Collectors.toList());
    if (contextValueAttributes.isEmpty()) {
      return "LastN: The value attribute must exist in the context";
    }
    if (contextValueAttributes.get(0).getAttributeType() != InternalAttributeType.STRING) {
      return String.format(
          "LastN: Context attribute '%s' must be of type String but is %s",
          contextValueAttributes.get(0).getName(),
          contextValueAttributes.get(0).getAttributeType().getBiosAttributeType().stringify());
    }
    if (contextValueAttributes.size() > 1) {
      return "LastN: Unnecessary attributes are in the context";
    }

    return null;
  }

  /**
   * Place validations of any special signal features in this method. The LastN feature should be
   * moved here, too, eventually.
   */
  private static void validateSpecialSignalFeatures(
      TenantDesc tenantDesc, StreamDesc signalDesc, FeatureDesc feature)
      throws ConstraintViolationException {
    validateLastN(tenantDesc, signalDesc, feature);
    validateSnapshot(tenantDesc, signalDesc, feature);
  }

  private static void validateSnapshot(
      TenantDesc tenantDesc, StreamDesc signalDesc, FeatureDesc feature)
      throws ConstraintViolationException {
    if (feature.getSnapshot() != Boolean.TRUE) {
      // not enabled
      return;
    }
    final var validatorContext = new StringBuilder();
    try {
      validateUpdatesSignal(signalDesc, feature, validatorContext);

      // FaC context must exist
      var contextName = feature.getEffectiveFeatureAsContextName(signalDesc.getName());
      if (StringUtils.isBlank(contextName)) {
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: The context must be specified "
                + "by featureAsContextName when snapshot is enabled");
      }
      final var contextDesc = tenantDesc.getDependingStream(contextName, signalDesc);
      if (contextDesc == null) {
        validatorContext.append(", featureAsContextName=").append(contextName);
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: The context specified by featureAsContextName is missing: "
                + contextName);
      }
      try {
        tenantDesc.addDependency(contextDesc, signalDesc);
      } catch (DirectedAcyclicGraphException e) {
        throw new ConstraintViolationException(
            String.format(
                "Cyclic dependency is detected, signal %s depends on context %s",
                contextName, signalDesc.getName()));
      }

      validateSnapshotsContext(signalDesc, contextDesc, feature, validatorContext);

    } catch (ConstraintViolationValidatorException e) {
      final var messageBuilder =
          AdminImplUtils.createExceptionMessage(e.getMessage(), tenantDesc, signalDesc)
              .append(", feature=")
              .append(feature.getName())
              .append(validatorContext);
      throw new ConstraintViolationException(messageBuilder.toString(), e);
    }
  }

  // Constraints for the updates signal attributes:
  //   - The order of dimensions must be the same with the top part of the signal attributes
  //   - There must be attribute "operation" of type String with allowed values "set" and "change".
  //   - The signal must not include unnecessary attributes (but doing enrichment is fine).
  public static void validateUpdatesSignal(
      StreamDesc signalDesc, FeatureDesc feature, StringBuilder validatorContext)
      throws ConstraintViolationValidatorException {
    validateCounterDimensions(signalDesc, feature, validatorContext);

    final var operationAttribute = signalDesc.findAttribute(ATTRIBUTE_OPERATION);
    if (operationAttribute == null) {
      validatorContext.append(", attribute=").append(ATTRIBUTE_OPERATION);
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: The signal must have attribute '%s'", ATTRIBUTE_OPERATION);
    }

    final var expectedAllowedValues = List.of(OPERATION_VALUE_CHANGE, OPERATION_VALUE_SET);
    final var allowedValues = operationAttribute.getEnum();
    if (allowedValues == null || !allowedValues.containsAll(expectedAllowedValues)) {
      validatorContext.append(", attribute=").append(ATTRIBUTE_OPERATION);
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: The attribute '%s' must be of type String with allowed values %s",
          ATTRIBUTE_OPERATION, expectedAllowedValues);
    }
  }

  // Constraint for dimensions of a counter updates signal:
  //   - The order of dimensions must be the same with the top part of the signal attributes
  private static void validateCounterDimensions(
      StreamDesc signalDesc, FeatureDesc feature, StringBuilder validatorContext)
      throws ConstraintViolationValidatorException {
    final var dimensions = feature.getDimensions();
    if (!dimensions.contains(ATTRIBUTE_OPERATION)) {
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: 'operation' must be in the dimensions");
    }
    if (dimensions.size() < 2) {
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: Dimension must include 'operation' and at least one attribute"
              + " for counter key");
    }
  }

  // Constraint for the snapshot context attributes:
  //   - The primary key must be of type String.
  //   - The snapshot ctx must have attributes of the same name and type with the feature
  //     dimensions.
  //   - The context must have attribute "timestamp" of type Integer.
  //   - The snapshot context must have corresponding attributes of name ${counterName}Snapshot.
  //   - Type of the all snapshot attributes must be Decimal.
  //   - The context must not include unnecessary attributes (but doing enrichment is fine).
  private static void validateSnapshotsContext(
      StreamDesc signalDesc,
      StreamDesc contextDesc,
      FeatureDesc feature,
      StringBuilder validatorContext)
      throws ConstraintViolationValidatorException {
    final var contextName = contextDesc.getName();

    final var primaryKeyNames =
        feature.getDimensions().stream()
            .filter((dimension) -> !ATTRIBUTE_OPERATION.equalsIgnoreCase(dimension))
            .collect(Collectors.toList());

    validateSnapshotsPrimaryKey(contextDesc, primaryKeyNames, validatorContext);

    validateSnapshotKeysInContext(signalDesc, contextDesc, primaryKeyNames, validatorContext);

    final var timestampAttribute = contextDesc.findAttribute(ATTRIBUTE_TIMESTAMP);
    if (timestampAttribute == null) {
      validatorContext.append(", featureAsContextName=").append(contextName);
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: Required attribute '%s' is missing in the context",
          ATTRIBUTE_TIMESTAMP);
    }
    if (timestampAttribute.getAttributeType() != InternalAttributeType.LONG) {
      validatorContext
          .append(", featureAsContextName=")
          .append(contextName)
          .append(", attribute=")
          .append(ATTRIBUTE_TIMESTAMP);
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: Attribute '%s' must be of type Integer", ATTRIBUTE_TIMESTAMP);
    }

    validateSnapshotAttributesInContext(signalDesc, contextDesc, feature, validatorContext);

    // (dimensions - 1) + attributes + timestamp(1)
    final int numNecessaryAttributes = primaryKeyNames.size() + feature.getAttributes().size() + 1;
    if (contextDesc.getAttributes().size() > numNecessaryAttributes) {
      final var necessaryAttributes = new HashSet<String>();
      necessaryAttributes.addAll(
          feature.getDimensions().stream()
              .map((name) -> name.toLowerCase())
              .collect(Collectors.toSet()));
      necessaryAttributes.addAll(
          feature.getAttributes().stream()
              .map((name) -> name.toLowerCase())
              .collect(Collectors.toSet()));
      necessaryAttributes.add(ATTRIBUTE_TIMESTAMP);
      final var unnecessaryAttributes =
          contextDesc.getAttributes().stream()
              .filter(
                  (attribute) -> !necessaryAttributes.contains(attribute.getName().toLowerCase()))
              .map((attributeDesc) -> attributeDesc.getName())
              .collect(Collectors.toList());
      validatorContext
          .append(", featureAsContextName=")
          .append(contextName)
          .append(", unnecessaryAttributes=")
          .append(unnecessaryAttributes);
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount: Unnecessary attributes are in the context");
    }
  }

  private static void validateSnapshotsPrimaryKey(
      StreamDesc contextDesc, List<String> expectedPrimaryKeyNames, StringBuilder validatorContext)
      throws ConstraintViolationValidatorException {
    final var contextName = contextDesc.getName();

    final var primaryKeyNames = contextDesc.getPrimaryKey();
    if (!primaryKeyNames.equals(expectedPrimaryKeyNames)) {
      validatorContext
          .append(", featureAsContextName=")
          .append(contextName)
          .append(", primaryKey=")
          .append(primaryKeyNames)
          .append(", dimensions(-operation)=")
          .append(expectedPrimaryKeyNames);
      throw new ConstraintViolationValidatorException(
          "AccumulatingCount:"
              + " Primary key of the context must be the same with the feature dimensions except %s",
          ATTRIBUTE_OPERATION);
    }
  }

  //   - The snapshot ctx must have attributes of the same name and type with the feature
  //     dimensions.
  private static void validateSnapshotKeysInContext(
      StreamDesc signalDesc,
      StreamDesc contextDesc,
      List<String> primaryKeyNames,
      StringBuilder validatorContext)
      throws ConstraintViolationValidatorException {
    final var contextName = contextDesc.getName();
    for (var primaryKeyName : primaryKeyNames) {
      final var signalAttribute = signalDesc.findAttribute(primaryKeyName);

      final var contextAttribute = contextDesc.findAttribute(primaryKeyName);
      if (contextAttribute == null) {
        validatorContext
            .append(", featureAsContextName=")
            .append(contextName)
            .append(", dimension=")
            .append(primaryKeyName);
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: Required attribute for a dimension is missing in the context");
      }

      final var dimensionTypeSignal = signalAttribute.getAttributeType().getBiosAttributeType();
      final var dimensionTypeContext = contextAttribute.getAttributeType().getBiosAttributeType();
      if (dimensionTypeSignal != dimensionTypeContext) {
        validatorContext
            .append(", featureAsContextName=")
            .append(contextName)
            .append(", attribute=")
            .append(primaryKeyName)
            .append(", type(signal)=")
            .append(dimensionTypeSignal.stringify())
            .append(", type(context)=")
            .append(dimensionTypeContext.stringify());
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: Dimension attribute type mismatch");
      }
      if (!Objects.equals(signalAttribute.getEnum(), contextAttribute.getEnum())) {
        validatorContext
            .append(", featureAsContextName=")
            .append(contextName)
            .append(", attribute=")
            .append(primaryKeyName)
            .append(", type(signal)=")
            .append(dimensionTypeSignal.stringify())
            .append(", type(context)=")
            .append(dimensionTypeContext.stringify());
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: Dimension attribute type mismatch");
      }
    }
  }

  // Constraint check for snapshot attributes of a counter snapshot context:
  //   - The snapshot context must have corresponding attributes of name ${counterName}Snapshot.
  //   - Type of the all snapshot attributes must be Decimal.
  private static void validateSnapshotAttributesInContext(
      StreamDesc signalDesc,
      StreamDesc contextDesc,
      FeatureDesc feature,
      StringBuilder validatorContext)
      throws ConstraintViolationValidatorException {

    final var contextName = contextDesc.getName();
    for (var attributeNameSignal : feature.getAttributes()) {
      final var signalAttribute = signalDesc.findAttribute(attributeNameSignal);

      final var attributeNameContext = attributeNameSignal;
      final var contextAttribute = contextDesc.findAttribute(attributeNameContext);
      if (contextAttribute == null) {
        validatorContext.append(", featureAsContextName=").append(contextName);
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: Required snapshot attribute '%s' is missing in the context",
            attributeNameContext, attributeNameSignal);
      }
      final var attributeTypeSignal = signalAttribute.getAttributeType().getBiosAttributeType();
      if (attributeTypeSignal != io.isima.bios.models.AttributeType.DECIMAL) {
        validatorContext
            .append(", attribute=")
            .append(attributeNameSignal)
            .append(", type=")
            .append(attributeTypeSignal.stringify());
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: An update value attribute of the signal must be of type Decimal");
      }
      final var attributeTypeContext = contextAttribute.getAttributeType().getBiosAttributeType();
      if (attributeTypeContext != io.isima.bios.models.AttributeType.DECIMAL) {
        validatorContext
            .append(", featureAsContextName=")
            .append(contextName)
            .append(", attribute=")
            .append(attributeNameContext)
            .append(", type=")
            .append(attributeTypeContext.stringify());
        throw new ConstraintViolationValidatorException(
            "AccumulatingCount: A snapshot value attribute of the context must be of type Decimal");
      }
    }
  }
}
