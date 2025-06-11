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
import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_SUFFIX_VALUE;
import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_TIMESTAMP;
import static io.isima.bios.feature.CountersConstants.OPERATION_VALUE_CHANGE;
import static io.isima.bios.feature.CountersConstants.OPERATION_VALUE_SET;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class AccumulatingCountFeatureValidatorTest {

  private static final String TENANT_NAME = "accumulatingCountValidatorTest";
  private static final String SIGNAL_NAME = "counterUpdates";
  private static final String CONTEXT_NAME = "counterSnapshots";

  private static final String FC_TYPE = "fcType";
  private static final String FC_ID = "fcId";
  private static final String ITEM_ID = "itemId";

  private static final String ON_HAND = "onHand";
  private static final String DEMAND = "demand";

  TenantDesc tenantDesc;
  private StreamDesc updatesSignal;
  private ViewDesc snapshotView;
  private StreamDesc snapshotsContext;

  @Before
  public void setUp() {
    tenantDesc = new TenantDesc(TENANT_NAME, System.currentTimeMillis(), false);
    updatesSignal = new StreamDesc(SIGNAL_NAME, System.currentTimeMillis());
    updatesSignal.setType(StreamType.SIGNAL);
    updatesSignal.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    updatesSignal.addAttribute(
        new AttributeDesc(FC_TYPE, InternalAttributeType.ENUM).setEnum(List.of("TypeA", "TypeB")));
    updatesSignal.addAttribute(new AttributeDesc(FC_ID, InternalAttributeType.LONG));
    updatesSignal.addAttribute(new AttributeDesc(ITEM_ID, InternalAttributeType.LONG));
    updatesSignal.addAttribute(
        new AttributeDesc(ATTRIBUTE_OPERATION, InternalAttributeType.ENUM)
            .setEnum(List.of(OPERATION_VALUE_CHANGE, OPERATION_VALUE_SET)));
    updatesSignal.addAttribute(
        new AttributeDesc(ON_HAND + ATTRIBUTE_SUFFIX_VALUE, InternalAttributeType.DOUBLE));
    updatesSignal.addAttribute(
        new AttributeDesc(DEMAND + ATTRIBUTE_SUFFIX_VALUE, InternalAttributeType.DOUBLE));

    snapshotView = new ViewDesc();
    snapshotView.setSnapshot(true);
    snapshotView.setName("inventoryCounter");
    snapshotView.setGroupBy(new ArrayList<>(List.of(FC_TYPE, FC_ID, ITEM_ID, ATTRIBUTE_OPERATION)));
    snapshotView.setAttributes(
        new ArrayList<>(
            List.of(ON_HAND + ATTRIBUTE_SUFFIX_VALUE, DEMAND + ATTRIBUTE_SUFFIX_VALUE)));
    snapshotView.setFeatureAsContextName(CONTEXT_NAME);
    snapshotView.setWriteTimeIndexing(Boolean.TRUE);

    updatesSignal.addView(snapshotView);

    snapshotsContext = new StreamDesc(CONTEXT_NAME, System.currentTimeMillis());
    snapshotsContext.setType(StreamType.CONTEXT);
    snapshotsContext.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    snapshotsContext.addAttribute(
        new AttributeDesc(FC_TYPE, InternalAttributeType.ENUM).setEnum(List.of("TypeA", "TypeB")));
    snapshotsContext.addAttribute(new AttributeDesc(FC_ID, InternalAttributeType.LONG));
    snapshotsContext.addAttribute(new AttributeDesc(ITEM_ID, InternalAttributeType.LONG));
    snapshotsContext.addAttribute(
        new AttributeDesc(ATTRIBUTE_TIMESTAMP, InternalAttributeType.LONG));
    snapshotsContext.addAttribute(
        new AttributeDesc(ON_HAND + ATTRIBUTE_SUFFIX_VALUE, InternalAttributeType.DOUBLE));
    snapshotsContext.addAttribute(
        new AttributeDesc(DEMAND + ATTRIBUTE_SUFFIX_VALUE, InternalAttributeType.DOUBLE));
    snapshotsContext.setPrimaryKey(List.of(FC_TYPE, FC_ID, ITEM_ID));

    tenantDesc.addStream(updatesSignal);
    tenantDesc.addStream(snapshotsContext);
  }

  @Test
  public void normalSnapshotsConfig() throws Exception {
    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void facMissing() {
    snapshotView.setFeatureAsContextName("noSuchContext");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "Constraint violation: AccumulatingCount:"
                + " The context specified by featureAsContextName is missing"));
  }

  @Test
  public void dimensionMissingInContext() {
    snapshotsContext.setAttributes(
        snapshotsContext.getAttributes().stream()
            .filter((attribute) -> !attribute.getName().equals("fcId"))
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: Required attribute for a dimension is missing in the context;"));
  }

  @Test
  public void dimensionTypeMismatch() {
    final var attribute = snapshotsContext.getAttributes().get(2);
    attribute.setAttributeType(InternalAttributeType.STRING);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: Dimension attribute type mismatch;"));
  }

  @Test
  public void dimensionEnumMismatch() {
    final var attribute = snapshotsContext.getAttributes().get(1);
    attribute.setEnum(List.of("hello", "world"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: Dimension attribute type mismatch;"));
  }

  @Test
  public void snapshotDimensionEnumHasWrongOrder() {
    final var attribute = snapshotsContext.getAttributes().get(1);
    attribute.setEnum(List.of("TypeB", "TypeA"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: Dimension attribute type mismatch;"));
  }

  @Test
  public void snapshotDimensionEnumHasLessValues() {
    final var attribute = snapshotsContext.getAttributes().get(1);
    attribute.setEnum(List.of("TypeA"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: Dimension attribute type mismatch;"));
  }

  @Test
  public void snapshotDimensionEnumHasMoreValues() {
    final var attribute = snapshotsContext.getAttributes().get(1);
    attribute.setEnum(List.of("TypeA", "TypeB", "TypeC"));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: Dimension attribute type mismatch;"));
  }

  @Test
  public void signalDimensionAttributesHasDifferentOrder() throws ConstraintViolationException {
    final var origAttributes = updatesSignal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.addAll(origAttributes.subList(3, origAttributes.size()));
    updatesSignal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void attributeOperationCanBeFirstInSignal() throws ConstraintViolationException {
    final var origAttributes = updatesSignal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.add(origAttributes.get(3));
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.addAll(origAttributes.subList(4, origAttributes.size()));
    updatesSignal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void attributeOperationCanBeLastInSignal() throws ConstraintViolationException {
    final var origAttributes = updatesSignal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.addAll(origAttributes.subList(4, origAttributes.size()));
    newAttributes.add(origAttributes.get(3));
    updatesSignal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void valueAttributesCanBeBeforeDimensions() throws ConstraintViolationException {
    final var origAttributes = updatesSignal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.addAll(origAttributes.subList(4, origAttributes.size()));
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.add(origAttributes.get(3));
    updatesSignal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void dimensionMissingOperation() {
    snapshotView.setGroupBy(List.of(FC_TYPE, FC_ID, ITEM_ID));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: 'operation' must be in the dimensions;"));
  }

  @Test
  public void dimensionOrderInvalidInContext() {
    final var attributes = new LinkedList<AttributeDesc>();

    snapshotsContext.setPrimaryKey(List.of(FC_ID, FC_TYPE, ITEM_ID));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: Primary key of the context must be the same with"
                + " the feature dimensions except operation;"));
  }

  @Test
  public void operationAttributeMissingInUpdatesSignal() {
    updatesSignal.setAttributes(
        updatesSignal.getAttributes().stream()
            .filter((attribute) -> !attribute.getName().equals(ATTRIBUTE_OPERATION))
            .collect(Collectors.toList()));
    final var view = updatesSignal.getViews().get(0);
    view.setGroupBy(
        view.getGroupBy().stream()
            .filter((dimension) -> !ATTRIBUTE_OPERATION.equals(dimension))
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: 'operation' must be in the dimensions;"));
  }

  @Test
  public void operationAttributeInvalidType() {
    updatesSignal.setAttributes(
        updatesSignal.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().equals(ATTRIBUTE_OPERATION)) {
                    return new AttributeDesc(ATTRIBUTE_OPERATION, InternalAttributeType.DOUBLE);
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: The attribute 'operation' must be of type String"
                + " with allowed values [change, set];"));
  }

  @Test
  public void operationAttributeMissingEnum() {
    updatesSignal.setAttributes(
        updatesSignal.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().equals(ATTRIBUTE_OPERATION)) {
                    attribute.setAttributeType(InternalAttributeType.STRING);
                    attribute.setEnum(null);
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: The attribute 'operation' must be of type String"
                + " with allowed values [change, set];"));
  }

  @Test
  public void operationAttributeWrongEnum() {
    updatesSignal.setAttributes(
        updatesSignal.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().equals(ATTRIBUTE_OPERATION)) {
                    attribute.setEnum(List.of("put", "update"));
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: The attribute"
                + " 'operation' must be of type String with allowed values [change, set];"));
  }

  @Test
  public void operationAttributeEnumOrderDoesNotMatter() throws ConstraintViolationException {
    updatesSignal.setAttributes(
        updatesSignal.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().equals(ATTRIBUTE_OPERATION)) {
                    attribute.setEnum(List.of(OPERATION_VALUE_SET, OPERATION_VALUE_CHANGE));
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void updatesSignalHasExtraAttributes() throws ConstraintViolationException {
    updatesSignal.addAttribute(new AttributeDesc("extra", InternalAttributeType.STRING));

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void snapshotFeatureHasWriteTimeIndexingDisabled() throws ConstraintViolationException {
    snapshotView.setWriteTimeIndexing(Boolean.FALSE);

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void snapshotFeatureHasWriteTimeIndexingNotSet() throws ConstraintViolationException {
    snapshotView.setWriteTimeIndexing(null);

    FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView);
  }

  @Test
  public void invalidValueType() {
    updatesSignal.setAttributes(
        updatesSignal.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().endsWith(ATTRIBUTE_SUFFIX_VALUE)) {
                    attribute.setAttributeType(InternalAttributeType.LONG);
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: An update value attribute of the signal must be of type Decimal;"));
  }

  @Test
  public void invalidSnapshotType() {
    snapshotsContext.setAttributes(
        snapshotsContext.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().endsWith(ATTRIBUTE_SUFFIX_VALUE)) {
                    attribute.setAttributeType(InternalAttributeType.LONG);
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: A snapshot value attribute of the context must be of type Decimal;"));
  }

  @Test
  public void missingPrimaryKeyInSnapshots() {
    final var origAttributes = snapshotsContext.getAttributes();
    snapshotsContext.setPrimaryKey(List.of(FC_TYPE, FC_ID));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: Primary key of"
                + " the context must be the same with the feature dimensions except operation;"));
  }

  @Test
  public void snapshotAttributeMissing() {
    snapshotsContext.setAttributes(
        snapshotsContext.getAttributes().stream()
            .filter((attribute) -> !attribute.getName().equals(DEMAND + ATTRIBUTE_SUFFIX_VALUE))
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: Required snapshot attribute 'demandValue'"
                + " is missing in the context;"));
  }

  @Test
  public void snapshotTimestampAttributeMissing() {
    snapshotsContext.setAttributes(
        snapshotsContext.getAttributes().stream()
            .filter((attribute) -> !attribute.getName().equals(ATTRIBUTE_TIMESTAMP))
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: Required attribute 'timestamp' is missing in the context;"));
  }

  @Test
  public void snapshotTimestampAttributeWrongType() {
    snapshotsContext.setAttributes(
        snapshotsContext.getAttributes().stream()
            .map(
                (attribute) -> {
                  if (attribute.getName().equals(ATTRIBUTE_TIMESTAMP)) {
                    attribute.setAttributeType(InternalAttributeType.DOUBLE);
                  }
                  return attribute;
                })
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString("AccumulatingCount: Attribute 'timestamp' must be of type Integer;"));
  }

  @Test
  public void snapshotsContextHasExtraAttributes() {
    snapshotsContext.addAttribute(new AttributeDesc("extra", InternalAttributeType.STRING));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, updatesSignal, snapshotView));
    assertThat(
        exception.getMessage(),
        containsString(
            "AccumulatingCount: Unnecessary attributes are in the context;"
                + " tenant=accumulatingCountValidatorTest, signal=counterUpdates,"
                + " feature=inventoryCounter, featureAsContextName=counterSnapshots,"
                + " unnecessaryAttributes=[extra]"));
  }
}
