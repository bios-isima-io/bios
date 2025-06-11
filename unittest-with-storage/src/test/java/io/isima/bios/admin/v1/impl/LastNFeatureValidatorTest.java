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

import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
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
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class LastNFeatureValidatorTest {

  private static final String TENANT_NAME = "lastNValidatorTest";
  private static final String SIGNAL_NAME = "lastNSignal";
  private static final String CONTEXT_NAME = "lastN_byProduct";

  private static final String REGION = "region";
  private static final String PRODUCT = "product";
  private static final String NAME = "name";
  private static final String QUANTITY = "quantity";
  private static final String PHONE = "phone";
  private static final String PHONE_NUMBERS = "phoneNumbers";

  TenantDesc tenantDesc;
  private StreamDesc signal;
  private ViewDesc view;
  private StreamDesc context;

  @Before
  public void setUp() {
    tenantDesc = new TenantDesc(TENANT_NAME, System.currentTimeMillis(), false);
    signal = new StreamDesc(SIGNAL_NAME, System.currentTimeMillis());
    signal.setType(StreamType.SIGNAL);
    signal.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    signal.addAttribute(new AttributeDesc(REGION, InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc(PRODUCT, InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc(NAME, InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc(QUANTITY, InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc(PHONE, InternalAttributeType.LONG));

    view = new ViewDesc();
    view.setLastN(true);
    view.setLastNItems(15L);
    view.setLastNTtl(3600000L);
    view.setName("byProduct");
    view.setGroupBy(new ArrayList<>(List.of(REGION, PRODUCT)));
    view.setAttributes(new ArrayList<>(List.of(PHONE)));
    view.setFeatureAsContextName(CONTEXT_NAME);

    signal.addView(view);

    context = new StreamDesc(CONTEXT_NAME, System.currentTimeMillis());
    context.setType(StreamType.CONTEXT);
    context.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    context.addAttribute(new AttributeDesc(REGION, InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc(PRODUCT, InternalAttributeType.LONG));
    context.addAttribute(new AttributeDesc(PHONE_NUMBERS, InternalAttributeType.STRING));
    context.setPrimaryKey(List.of(REGION, PRODUCT));

    tenantDesc.addStream(signal);
    tenantDesc.addStream(context);
  }

  @Test
  public void normalLastNConfig() throws Exception {
    FeatureValidator.validateSignalFeature(tenantDesc, signal, view);
  }

  @Test
  public void facUnset() {
    view.setFeatureAsContextName("");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString("LastN: The context must be specified by featureAsContextName"));
  }

  @Test
  public void facMissing() {
    view.setFeatureAsContextName("noSuchContext");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "Constraint violation: LastN:"
                + " The context specified by featureAsContextName is missing"));
  }

  @Test
  public void lastNItemUnset() {
    view.setLastNItems(null);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString("Constraint violation: LastN:" + " Feature attribute 'items' must be set"));
  }

  @Test
  public void lastNItemZero() {
    view.setLastNItems(0L);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "Constraint violation: LastN:"
                + " Feature attribute 'items' must be a positive integer"));
  }

  @Test
  public void lastNItemNegative() {
    view.setLastNItems(-1L);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "Constraint violation: LastN:"
                + " Feature attribute 'items' must be a positive integer"));
  }

  @Test
  public void lastNTtlNegative() {
    view.setLastNTtl(-1L);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "Constraint violation: LastN:" + " Feature attribute 'ttl' may not be negative"));
  }

  @Test
  public void dimensionMissingInContext() {
    context.setAttributes(
        context.getAttributes().stream()
            .filter((attribute) -> !attribute.getName().equals(REGION))
            .collect(Collectors.toList()));
    context.setPrimaryKey(List.of(PRODUCT));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "LastN: Dimensions and the context primary key must have the same number of attributes;"));
  }

  @Test
  public void dimensionTypeMismatch() {
    final var attribute = context.getAttributes().get(1);
    attribute.setAttributeType(InternalAttributeType.STRING);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "LastN: Foreign key 'product' of type Integer does not match"
                + " primary key 'product' of type String;"));
  }

  @Test
  public void dimensionEnum() throws Exception {
    signal
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "EMEA", "APAC")));

    context
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "EMEA", "APAC")));
    FeatureValidator.validateSignalFeature(tenantDesc, signal, view);
  }

  @Test
  public void dimensionEnumMismatch() {
    signal
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "EMEA", "APAC")));

    context
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("WRONG", "EMEA", "APAC")));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "LastN: Allowed values mismatch between"
                + " foreign key 'lastNSignal.region' and primary key 'lastN_byProduct.region';"));
  }

  @Test
  public void lastNDimensionEnumHasWrongOrder() {
    signal
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "EMEA", "APAC")));

    context
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("EMEA", "Americas", "APAC")));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "LastN: Allowed values mismatch between"
                + " foreign key 'lastNSignal.region' and primary key 'lastN_byProduct.region';"));
  }

  @Test
  public void lastNDimensionEnumHasLessValues() {
    signal
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "EMEA", "APAC")));

    context
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "APAC")));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "LastN: Allowed values mismatch between"
                + " foreign key 'lastNSignal.region' and primary key 'lastN_byProduct.region';"));
  }

  @Test
  public void lastNDimensionEnumHasMoreValues() {
    signal
        .getAttributes()
        .set(0, new AttributeDesc(REGION, ENUM).setEnum(List.of("Americas", "EMEA", "APAC")));

    context
        .getAttributes()
        .set(
            0,
            new AttributeDesc(REGION, ENUM)
                .setEnum(List.of("Americas", "EMEA", "APAC", "Arctica")));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString(
            "LastN: Allowed values mismatch between"
                + " foreign key 'lastNSignal.region' and primary key 'lastN_byProduct.region';"));
  }

  @Test
  public void signalDimensionAttributesHasDifferentOrder() throws ConstraintViolationException {
    final var origAttributes = signal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.addAll(origAttributes.subList(3, origAttributes.size()));
    signal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, signal, view);
  }

  @Test
  public void valueAttributeCanBeFirstInSignal() throws ConstraintViolationException {
    final var origAttributes = signal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.add(origAttributes.get(3));
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.addAll(origAttributes.subList(4, origAttributes.size()));
    signal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, signal, view);
  }

  @Test
  public void valueAttributeCanBeLastInSignal() throws ConstraintViolationException {
    final var origAttributes = signal.getAttributes();
    final var newAttributes = new ArrayList<AttributeDesc>();
    newAttributes.add(origAttributes.get(2));
    newAttributes.add(origAttributes.get(1));
    newAttributes.add(origAttributes.get(0));
    newAttributes.addAll(origAttributes.subList(4, origAttributes.size()));
    newAttributes.add(origAttributes.get(3));
    signal.setAttributes(newAttributes);

    FeatureValidator.validateSignalFeature(tenantDesc, signal, view);
  }

  @Test
  public void invalidLastNType() {
    context.getAttributes().get(2).setAttributeType(InternalAttributeType.LONG);

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString("Context attribute 'phoneNumbers' must be of type String but is Integer;"));
  }

  @Test
  public void lastNLastNAttributeMissing() {
    context.setAttributes(
        context.getAttributes().stream()
            .filter((attribute) -> !attribute.getName().equals(PHONE_NUMBERS))
            .collect(Collectors.toList()));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString("LastN: The value attribute must exist in the context;"));
  }

  @Test
  public void lastNsContextHasExtraAttributes() {
    context.addAttribute(new AttributeDesc("extra", InternalAttributeType.STRING));

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> FeatureValidator.validateSignalFeature(tenantDesc, signal, view));
    assertThat(
        exception.getMessage(),
        containsString("LastN: Unnecessary attributes are in the context;"));
  }
}
