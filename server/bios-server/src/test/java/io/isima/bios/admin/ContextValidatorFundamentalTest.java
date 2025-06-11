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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextFeatureConfig;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class ContextValidatorFundamentalTest {
  private static ObjectMapper mapper;
  private static Validators validators;

  private ContextConfig simpleContext;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    mapper = BiosObjectMapperProvider.get();
    validators = new Validators();
  }

  @Before
  public void setup() throws Exception {
    simpleContext = new ContextConfig();
    simpleContext.setName("simpleContext");
    simpleContext.setAuditEnabled(true);
    simpleContext.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("first", AttributeType.STRING));
    attributes.add(new AttributeConfig("second", AttributeType.INTEGER));
    simpleContext.setAttributes(attributes);
    simpleContext.setPrimaryKey(List.of("first"));
  }

  @Test
  public void testSimpleContext() throws Exception {
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testPrimaryKeyOnlyContextSingleDimension() throws Exception {
    simpleContext.setAttributes(List.of(simpleContext.getAttributes().get(0)));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testPrimaryKeyOnlyContextMultiDimension() throws Exception {
    simpleContext.setAttributes(List.of(simpleContext.getAttributes().get(0)));
    simpleContext.setPrimaryKey(
        simpleContext.getAttributes().stream()
            .map((attr) -> attr.getName())
            .collect(Collectors.toList()));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testSwitchingPrimaryKey() throws Exception {
    simpleContext.setPrimaryKey(List.of("second"));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testMissingPrimaryKey() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.primaryKey:"
            + " The values must be set;"
            + " tenantName=testTenant, contextName=simpleContext");

    simpleContext.setPrimaryKey(null);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testEmptyPrimaryKey() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.primaryKey:"
            + " The values must be set;"
            + " tenantName=testTenant, contextName=simpleContext");

    simpleContext.setPrimaryKey(List.of());
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testTwoDimensionalPrimaryKey() throws Exception {
    simpleContext.getAttributes().add(new AttributeConfig("third", AttributeType.INTEGER));
    simpleContext.setPrimaryKey(List.of("first", "second"));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testTwoDimensionalPrimaryKeyReverseOrder() throws Exception {
    simpleContext.getAttributes().add(new AttributeConfig("third", AttributeType.INTEGER));
    simpleContext.setPrimaryKey(List.of("third", "second"));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testInvalidPrimaryKey() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.primaryKey[0]:"
            + " Primary key must exist in attributes;"
            + " tenantName=testTenant, contextName=simpleContext");

    simpleContext.setPrimaryKey(List.of("mountainLion"));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void contextWithNameWithLength80() throws Exception {
    simpleContext.setName(
        "abcdeABCDE0123456789_1234567890123456789abcdeABCDE0123456789_1234567890123456789");
    validators.validateContext("testTenant", simpleContext);
  }

  // Regression BB-1247
  @Test
  public void contextWithNameWithLength81() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "contextConfig.contextName: Length of a name may not exceed 80;"
            + " tenantName=testTenant, contextName="
            + "abcdeABCDE0123456789_1234567890123456789xabcdeABCDE0123456789_1234567890123456789");

    simpleContext.setName(
        "abcdeABCDE0123456789_1234567890123456789xabcdeABCDE0123456789_1234567890123456789");
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testContextNameWithUnderscore() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "contextConfig.contextName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, contextName=_requests");
    simpleContext.setName("_requests");
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testContextNameWithInvalidCharacters() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "contextConfig.contextName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, contextName=$this_is_invalid");

    simpleContext.setName("$this_is_invalid");
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testNoMissingAttributePolicy() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.missingAttributePolicy: The value must be set;"
            + " tenantName=testTenant, contextName=simpleContext");

    simpleContext.setMissingAttributePolicy(null);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testNoAttributes() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("contextConfig.attributes: Attributes must be set;");

    simpleContext.setAttributes(null);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testNoAttributeEntry() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("contextConfig.attributes[2]: Attribute config entry must not be null;");

    simpleContext.getAttributes().add(null);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testNoAttributeName() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.attributes[2].attributeName: The value must be set;"
            + " tenantName=testTenant, contextName=simpleContext");

    final var attribute = new AttributeConfig();
    attribute.setType(AttributeType.INTEGER);
    simpleContext.getAttributes().add(attribute);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testNoAttributeType() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.attributes[2].type: The value must be set;"
            + " tenantName=testTenant, contextName=simpleContext, attributeName=abc");

    final var attribute = new AttributeConfig();
    attribute.setName("abc");
    simpleContext.getAttributes().add(attribute);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testInvalidAttributeName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "contextConfig.attributes[2].attributeName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, contextName=simpleContext, attributeName=hello.world");

    final var attribute = new AttributeConfig();
    attribute.setName("hello.world");
    attribute.setType(AttributeType.INTEGER);
    simpleContext.getAttributes().add(attribute);
    validators.validateContext("testTenant", simpleContext);
  }

  // Regression BB-1248
  @Test
  public void testAttributeNameConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.attributes[2].attributeName:"
            + " Attribute name conflict;"
            + " tenantName=testTenant, contextName=simpleContext, attributeName=first");

    final var attribute = new AttributeConfig();
    attribute.setName("first");
    attribute.setType(AttributeType.INTEGER);
    simpleContext.getAttributes().add(attribute);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testAttributeDefaultValue() throws Exception {
    final var attribute = new AttributeConfig();
    attribute.setName("third");
    attribute.setType(AttributeType.INTEGER);
    attribute.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
    attribute.setDefaultValue(new AttributeValueGeneric(-1, AttributeType.INTEGER));
    simpleContext.getAttributes().add(attribute);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testAttributeMissingDefaultValue() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.attributes[0].default:"
            + " Default value must be set when missing attribute policy is StoreDefaultValue;"
            + " tenantName=testTenant, contextName=simpleContext, attributeName=first");

    final var attribute = new AttributeConfig();
    attribute.setName("second");
    attribute.setType(AttributeType.INTEGER);
    attribute.setDefaultValue(new AttributeValueGeneric(-1, AttributeType.INTEGER));
    simpleContext.getAttributes().add(attribute);
    simpleContext.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testInvalidTtl1() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("contextConfig.ttl: TTL must be positive");

    simpleContext.setTtl(0L);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testInvalidTtl2() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("contextConfig.ttl: TTL must be positive");

    simpleContext.setTtl(-1000L);
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testValidContextFeature() throws Exception {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setAggregated(true);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testValidContextFeatureAndIndex() throws Exception {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setAggregated(true);
    feature.setIndexed(true);
    feature.setIndexType(IndexType.RANGE_QUERY);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testValidIndex() throws Exception {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("second"));
    feature.setAttributes(List.of("first"));
    feature.setIndexed(true);
    feature.setIndexType(IndexType.EXACT_MATCH);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testIndexMissingDimensions() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setAttributes(List.of("second"));
    feature.setIndexed(true);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0]: Property 'dimensions' must be set when indexed is true;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testIndexEmptyDimensions() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of());
    feature.setAttributes(List.of("second"));
    feature.setIndexed(true);
    feature.setIndexType(IndexType.EXACT_MATCH);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0]: Property 'dimensions' must be set when indexed is true;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testIndexMissingIndexType() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setIndexed(true);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0]: Property 'indexType' must be set when indexed is true;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testIndexMissingFeatureInterval() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setIndexed(true);
    feature.setIndexType(IndexType.RANGE_QUERY);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0].featureInterval: The value must be set;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testIndexExactMatchIndexToPrimaryKey() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setIndexed(true);
    feature.setIndexType(IndexType.EXACT_MATCH);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0]: ExactMatch index to primary key cannot be set;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testIndexIndexTypeSetButIndexNotEnabled() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setIndexed(false);
    feature.setIndexType(IndexType.RANGE_QUERY);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0]: Index type specified but indexed is not set to true;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testFeatureWithTimeIndexInterval() {
    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setAggregated(true);
    feature.setIndexed(true);
    feature.setIndexType(IndexType.RANGE_QUERY);
    feature.setFeatureInterval(30000L);
    feature.setTimeIndexInterval(300000L);
    simpleContext.setFeatures(List.of(feature));
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> validators.validateContext("testTenant", simpleContext));
    assertThat(
        exception.getMessage(),
        is(
            "contextConfig.features[0].timeIndexInterval:"
                + " Property 'timeIndexInterval' must not be set for a context feature;"
                + " tenantName=testTenant, contextName=simpleContext, featureName=feature1"));
  }

  @Test
  public void testInvalidFeature() throws Exception {
    // TODO: Once we implement allowing features without aggregated but only indexed, the exception
    // thrown will need to change.
    // thrown.expect(ConstraintViolationValidatorException.class);
    // thrown.expectMessage("contextConfig.features[0]:"
    //     + " Context feature must set at least one of aggregated or indexed");
    thrown.expect(NotImplementedValidatorException.class);

    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setFeatureInterval(30000L);
    feature.setAggregated(false);
    simpleContext.setFeatures(List.of(feature));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testInvalidFeature2() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "contextConfig.features[0]:" + " Index type specified but indexed is not set to true");

    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setAggregated(true);
    feature.setIndexType(IndexType.EXACT_MATCH);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void testInvalidIndex() throws Exception {
    // TODO: Once we implement allowing features without aggregated but only indexed, the exception
    // thrown will need to change.
    // thrown.expect(ConstraintViolationValidatorException.class);
    // thrown.expectMessage("contextConfig.features[0]:"
    //     + " Data sketches specified but aggregated is not set to true");
    thrown.expect(NotImplementedValidatorException.class);

    ContextFeatureConfig feature = new ContextFeatureConfig();
    feature.setName("feature1");
    feature.setDimensions(List.of("first"));
    feature.setAttributes(List.of("second"));
    feature.setIndexed(true);
    feature.setAggregated(false);
    feature.setFeatureInterval(30000L);
    simpleContext.setFeatures(List.of(feature));
    validators.validateContext("testTenant", simpleContext);
  }

  @Test
  public void regression1() throws Exception {
    final String src =
        "{"
            + "  'contextName': 'storeContext',"
            + "  'missingAttributePolicy': 'StoreDefaultValue',"
            + "  'attributes': ["
            + "    {'attributeName': 'storeId', 'type': 'Integer',"
            + "     'missingAttributePolicy': 'Reject'},"
            + "    {'attributeName': 'zipCode', 'type': 'Integer',"
            + "     'default': -1},"
            + "    {'attributeName': 'address', 'type': 'String',"
            + "     'default': 'n/a'}"
            + "  ],"
            + "  'primaryKey': ['storeId']"
            + "}";
    final var contextConfig = mapper.readValue(src.replace("'", "\""), ContextConfig.class);
    validators.validateContext("regression1", contextConfig);
  }
}
