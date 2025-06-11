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

import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeSummary;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.PositiveIndicator;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.Unit;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SignalValidatorFundamentalTest {
  private static Validators validators;

  private SignalConfig simpleSignal;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    validators = new Validators();
  }

  @Before
  public void setup() throws Exception {
    simpleSignal = new SignalConfig();
    simpleSignal.setName("simpleSignal");
    simpleSignal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("first", AttributeType.STRING));
    simpleSignal.setAttributes(attributes);
  }

  @Test
  public void testSimpleSignal() throws Exception {
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void signalWithNameWithLength80() throws Exception {
    simpleSignal.setName(
        "abcdeABCDE0123456789_1234567890123456789abcdeABCDE0123456789_1234567890123456789");
    validators.validateSignal("testTenant", simpleSignal);
  }

  // Regression BB-1247
  @Test
  public void signalWithNameWithLength81() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.signalName: Length of a name may not exceed 80;"
            + " tenantName=testTenant, signalName="
            + "abcdeABCDE0123456789_1234567890123456789xabcdeABCDE0123456789_1234567890123456789");

    simpleSignal.setName(
        "abcdeABCDE0123456789_1234567890123456789xabcdeABCDE0123456789_1234567890123456789");
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testSignalNameWithUnderscore() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.signalName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, signalName=_requests");
    simpleSignal.setName("_requests");
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testSignalNameWithInvalidCharacters() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.signalName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, signalName=^this_is_invalid");

    simpleSignal.setName("^this_is_invalid");
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testNoMissingAttributePolicy() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.missingAttributePolicy: The value must be set;"
            + " tenantName=testTenant, signalName=simpleSignal");

    simpleSignal.setMissingAttributePolicy(null);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testNoAttributes() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("signalConfig.attributes: Attributes must be set;");

    simpleSignal.setAttributes(null);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testNoAttributeEntry() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("signalConfig.attributes[1]: Attribute config entry must not be null;");

    simpleSignal.getAttributes().add(null);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testNoAttributeName() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].attributeName: The value must be set;"
            + " tenantName=testTenant, signalName=simpleSignal");

    final var attribute = new AttributeConfig();
    attribute.setType(AttributeType.INTEGER);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testNoAttributeType() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].type: The value must be set;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=abc");

    final var attribute = new AttributeConfig();
    attribute.setName("abc");
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testInvalidAttributeName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].attributeName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=hello.world");

    final var attribute = new AttributeConfig();
    attribute.setName("hello.world");
    attribute.setType(AttributeType.INTEGER);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  // Regression BB-1248
  @Test
  public void testAttributeNameConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].attributeName:"
            + " Attribute name conflict;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=first");

    final var attribute = new AttributeConfig();
    attribute.setName("first");
    attribute.setType(AttributeType.INTEGER);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeDefaultValue() throws Exception {
    final var attribute = new AttributeConfig();
    attribute.setName("second");
    attribute.setType(AttributeType.INTEGER);
    attribute.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
    attribute.setDefaultValue(new AttributeValueGeneric(-1, AttributeType.INTEGER));
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeMissingDefaultValue() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[0].default:"
            + " Default value must be set when missing attribute policy is StoreDefaultValue;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=first");

    final var attribute = new AttributeConfig();
    attribute.setName("second");
    attribute.setType(AttributeType.INTEGER);
    attribute.setDefaultValue(new AttributeValueGeneric(-1, AttributeType.INTEGER));
    simpleSignal.getAttributes().add(attribute);
    simpleSignal.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testStringEnum() throws Exception {
    simpleSignal
        .getAttributes()
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric("ONE", AttributeType.STRING),
                new AttributeValueGeneric("TWO", AttributeType.STRING),
                new AttributeValueGeneric("THREE", AttributeType.STRING)));
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testEnumEntryConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[0].allowedValues[3]:"
            + " The value 'TWO' conflicts with another entry;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=first");

    simpleSignal
        .getAttributes()
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric("ONE", AttributeType.STRING),
                new AttributeValueGeneric("TWO", AttributeType.STRING),
                new AttributeValueGeneric("THREE", AttributeType.STRING),
                new AttributeValueGeneric("TWO", AttributeType.STRING)));
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testEnumTypeMismatch() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[0].allowedValues[0]:"
            + " Allowed value type 'Integer' does not match attribute type 'String';"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=first");

    simpleSignal
        .getAttributes()
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric(1L, AttributeType.INTEGER),
                new AttributeValueGeneric(2L, AttributeType.INTEGER),
                new AttributeValueGeneric(3L, AttributeType.INTEGER)));
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testUnsupportedEnumType() throws Exception {
    thrown.expect(NotImplementedValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].allowedValues:"
            + " Attribute type 'Integer' is not supported with allowedValues yet;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    attribute.setAllowedValues(
        List.of(
            new AttributeValueGeneric(1L, AttributeType.INTEGER),
            new AttributeValueGeneric(2L, AttributeType.INTEGER),
            new AttributeValueGeneric(3L, AttributeType.INTEGER)));
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testInvalidCategory() throws Exception {
    // String cannot be a Quantity.
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.category:"
            + " Only numeric types can be of category 'Quantity';"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.STRING);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testInvalidPositiveIndicator() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.category:"
            + " category must be 'Quantity' in order to specify any tags other than summaries;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.DIMENSION);
    tags.setPositiveIndicator(PositiveIndicator.LOW);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeOtherKindName() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.kind:"
            + " otherKindName can only be specified if 'kind' is 'OtherKind';"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setKind(AttributeKind.DISTANCE);
    tags.setOtherKindName("Hello");
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeUnitAndKind1() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.kind:"
            + " unit can only be specified if 'kind' is specified;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setUnit(Unit.KILOGRAM);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeUnitAndKind2() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.unit:"
            + " unit Kilogram is not applicable to kind Distance;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setKind(AttributeKind.DISTANCE);
    tags.setUnit(Unit.KILOGRAM);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeSecondSummary1() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.secondSummary:"
            + " secondSummary cannot be specified if firstSummary is not specified;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setSecondSummary(AttributeSummary.P1);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeSecondSummary2() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.attributes[1].tags.secondSummary:"
            + " secondSummary cannot be equal to firstSummary;"
            + " tenantName=testTenant, signalName=simpleSignal, attributeName=second");

    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setFirstSummary(AttributeSummary.MAX);
    tags.setSecondSummary(AttributeSummary.MAX);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeTagsPositive1() throws Exception {
    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setKind(AttributeKind.DISTANCE);
    tags.setUnit(Unit.KILOMETRE);
    tags.setFirstSummary(AttributeSummary.MEDIAN);
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeTagsPositive2() throws Exception {
    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setKind(AttributeKind.DISTANCE);
    tags.setUnit(Unit.OTHER_UNIT);
    tags.setFirstSummary(AttributeSummary.MEDIAN);
    tags.setSecondSummary(AttributeSummary.SUM);
    tags.setUnitDisplayName("My Custom Unit");
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }

  @Test
  public void testAttributeTagsPositive3() throws Exception {
    final var attribute = new AttributeConfig("second", AttributeType.INTEGER);
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setKind(AttributeKind.OTHER_KIND);
    tags.setOtherKindName("My Custom Kind");
    tags.setUnit(Unit.OTHER_UNIT);
    tags.setUnitDisplayName("My Custom Unit");
    attribute.setTags(tags);
    simpleSignal.getAttributes().add(attribute);
    validators.validateSignal("testTenant", simpleSignal);
  }
}
