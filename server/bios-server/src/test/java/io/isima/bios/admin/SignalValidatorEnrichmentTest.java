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

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.IngestTimeLagEnrichmentConfig;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.Unit;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SignalValidatorEnrichmentTest {
  private static Validators validators;

  private SignalConfig simpleEnrichment;
  private EnrichmentConfigSignal enrichment;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    validators = new Validators();
  }

  @Before
  public void setup() throws Exception {
    simpleEnrichment = new SignalConfig();
    simpleEnrichment.setName("simpleEnrichment");
    simpleEnrichment.setMissingAttributePolicy(MissingAttributePolicy.STORE_DEFAULT_VALUE);
    final var attributes = new ArrayList<AttributeConfig>();
    final var first = new AttributeConfig("first", AttributeType.STRING);
    first.setDefaultValue(new AttributeValueGeneric("n/a", AttributeType.STRING));
    final var second = new AttributeConfig("second", AttributeType.INTEGER);
    second.setDefaultValue(new AttributeValueGeneric(-1L, AttributeType.INTEGER));
    final var third = new AttributeConfig("third", AttributeType.DECIMAL);
    third.setDefaultValue(new AttributeValueGeneric(0.0, AttributeType.DECIMAL));
    final var eventTime = new AttributeConfig("eventTime", AttributeType.INTEGER);
    eventTime.setDefaultValue(new AttributeValueGeneric(0L, AttributeType.INTEGER));
    attributes.add(first);
    attributes.add(second);
    attributes.add(third);
    attributes.add(eventTime);
    simpleEnrichment.setAttributes(attributes);
    simpleEnrichment.setEnrich(new EnrichConfig());
    enrichment = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment));
    enrichment.setName("enrichmentOne");
    enrichment.setForeignKey(List.of("second"));
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment.setContextName("theContext");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux");
    final var tags = new AttributeTags();
    tags.setCategory(AttributeCategory.QUANTITY);
    tags.setKind(AttributeKind.DURATION);
    tags.setUnit(Unit.MILLISECOND);
    simpleEnrichment
        .getEnrich()
        .setIngestTimeLag(
            List.of(
                new IngestTimeLagEnrichmentConfig(
                    "lagSinceEventTime", "eventTime", "eventTimeLag", tags, "0", null)));
  }

  @Test
  public void testSimpleEnrichment() throws Exception {
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testTwoEnrichments() throws Exception {
    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  // Enrichment name //////////////////////////////////////////////////////

  @Test
  public void testMissingEnrichmentName() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[0].enrichmentName:"
            + " The value must be set;"
            + " tenantName=testTenant, signalName=simpleEnrichment");

    enrichment.setName(null);
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testBlankEnrichmentName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(startsWith("signalConfig.enrich.enrichments[0].enrichmentName:"));

    enrichment.setName(" ");
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testEnrichmentNameConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].enrichmentName:"
            + " Enrichment names conflict;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentOne");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));
    enrichment2.setName("enrichmentOne");
    enrichment2.setForeignKey(List.of("second"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  // Foreign key //////////////////////////////////////////////////////

  @Test
  public void testMissingForeignKey() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].foreignKey:"
            + " Foreign key must be set;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));
    enrichment2.setName("enrichmentTwo");
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void tesEmptyForeignKey() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].foreignKey:"
            + " Foreign key must be set;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));
    enrichment2.setName("enrichmentTwo");
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setForeignKey(List.of());
    enrichment2.setContextName("theContext");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testBlankForeignKey() throws Exception {

    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].foreignKey[0]:"
            + " Foreign key must be set;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));
    enrichment2.setName("enrichmentTwo");
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setForeignKey(List.of(""));
    enrichment2.setContextName("theContext");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testForeignKeyNotFoundInAttributes() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].foreignKey[0]:"
            + " Foreign key 'noSuchKey' does not match any of attribute names;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));
    enrichment2.setName("enrichmentTwo");
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext");
    enrichment2.setForeignKey(List.of("noSuchKey"));
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  // Missing lookup policy //////////////////////////////////////////////////////

  @Test
  public void testMissingLookupPolicyFillIn() throws Exception {
    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");
    contextAttribute.setFillIn(new AttributeValueGeneric(3.14, AttributeType.DECIMAL));

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testMissingLookupPolicyNotSet() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].missingLookupPolicy:"
            + " MissingLookupPolicy must be set in this enrichment or globally in enrich;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");
    contextAttribute.setFillIn(new AttributeValueGeneric(3.14, AttributeType.DECIMAL));

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testGlobalMissingLookupPolicy() throws Exception {
    simpleEnrichment.getEnrich().setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux2");
    contextAttribute.setFillIn(new AttributeValueGeneric(3.14, AttributeType.DECIMAL));

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  // Context name //////////////////////////////////////////////////////

  @Test
  public void testContextNameMissing() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[0].contextName:"
            + " The value must be set;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentOne");

    enrichment.setContextName(null);
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testInvalidContextName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[0].contextName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentOne,"
            + " contextName=Hello.World");

    enrichment.setContextName("Hello.World");
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testContextNameWithColon() throws Exception {
    enrichment.setContextName("Hello:World");
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  // Context attribute //////////////////////////////////////////////////////
  @Test
  public void testContextAttributeAs() throws Exception {
    enrichment.getContextAttributes().get(0).setAs("aabbcc");
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testInvalidContextAttributeName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[0].contextAttributes[0].attributeName:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentOne,"
            + " contextName=theContext, attributeName=$snake_snail");

    enrichment.getContextAttributes().get(0).setAttributeName("$snake_snail");
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testInvalidContextOutputName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[0].contextAttributes[0].as:"
            + " Name string must start with an alphanumeric followed by alphanumerics or underscores;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentOne,"
            + " contextName=theContext, attributeName=aux, as=##!@#$");

    enrichment.getContextAttributes().get(0).setAs("##!@#$");
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testContextAttributeConflictWithAttr() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].contextAttributes[0].attributeName:"
            + " Context attribute name 'third' conflicts another attribute;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo,"
            + " contextName=theContext2, attributeName=third");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("third");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testContextOutputConflictWithAttr() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].contextAttributes[0].as:"
            + " Context attribute name 'third' conflicts another attribute;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo,"
            + " contextName=theContext2, attributeName=aux, as=third");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));
    simpleEnrichment.getEnrich().setIngestTimeLag(null);

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux");
    contextAttribute.setAs("third");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testContextAttributeConflictWithAnotherEnrichment() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].contextAttributes[0].attributeName:"
            + " Context attribute name 'aux' conflicts another attribute;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo,"
            + " contextName=theContext2, attributeName=aux");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("aux");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testContextOutputConflictWithAnotherEnrichment() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].contextAttributes[0].as:"
            + " Context attribute name 'aux' conflicts another attribute;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo,"
            + " contextName=theContext2, attributeName=xxxnnn, as=aux");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment.getContextAttributes().get(0).setAttributeName("abcdefg");
    enrichment.getContextAttributes().get(0).setAs("aux");

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    enrichment2.setContextAttributes(List.of(contextAttribute));
    contextAttribute.setAttributeName("xxxnnn");
    contextAttribute.setAs("aux");

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testContextOutputConflictWithSelfEarlier() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[1].contextAttributes[1].attributeName:"
            + " Context attribute name 'aux2' conflicts another attribute;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentTwo,"
            + " contextName=theContext2, attributeName=aux2");

    final var enrichment2 = new EnrichmentConfigSignal();
    simpleEnrichment.getEnrich().setEnrichments(List.of(enrichment, enrichment2));

    enrichment.getContextAttributes().get(0).setAttributeName("abcdefg");
    enrichment.getContextAttributes().get(0).setAs("aux");

    enrichment2.setName("enrichmentTwo");
    enrichment2.setForeignKey(List.of("third"));
    enrichment2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment2.setContextName("theContext2");
    final var contextAttribute = new EnrichmentAttribute();
    contextAttribute.setAttributeName("aux2");
    final var contextAttribute2 = new EnrichmentAttribute();
    contextAttribute2.setAttributeName("aux2");
    enrichment2.setContextAttributes(List.of(contextAttribute, contextAttribute2));

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testMissingFillInValue() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.enrich.enrichments[0].contextAttributes[0].fillIn:"
            + " FillIn value must be set when missing lookup policy is StoreFillIn;"
            + " tenantName=testTenant, signalName=simpleEnrichment, enrichmentName=enrichmentOne,"
            + " contextName=theContext, attributeName=aux");

    enrichment.setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);
    validators.validateSignal("testTenant", simpleEnrichment);
  }

  // IngestTimeLag enrichment //////////////////////////////////////////////
  @Test
  public void testOnlyIngestTimeLagEnrichment() throws Exception {
    simpleEnrichment.getEnrich().setEnrichments(null);

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testMissingIngestTimeLagEnrichmentName() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).setName(null);

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testMissingIngestTimeLagEnrichmentAttribute() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).setAttribute(null);

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testIngestTimeLagEnrichmentMissingOutput() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).setAs(null);

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testIngestTimeLagEnrichmentInputAttributeMismatch() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).setAttribute("noSuch");

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testIngestTimeLagEnrichmentOutputNameConflict() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).setAs("aux");

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testIngestTimeLagEnrichmentTagsAreOptional() throws Exception {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).setTags(null);

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testIngestTimeLagEnrichmentUnixSecondUnit() throws Exception {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).getTags().setUnit(Unit.SECOND);

    validators.validateSignal("testTenant", simpleEnrichment);
  }

  @Test
  public void testIngestTimeLagEnrichmentInvalidCategory() {
    simpleEnrichment
        .getEnrich()
        .getIngestTimeLag()
        .get(0)
        .getTags()
        .setCategory(AttributeCategory.DIMENSION);

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testIngestTimeLagEnrichmentInvalidKind() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).getTags().setKind(AttributeKind.MONEY);

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }

  @Test
  public void testIngestTimeLagEnrichmentInvalidUnit() {
    simpleEnrichment.getEnrich().getIngestTimeLag().get(0).getTags().setUnit(Unit.MONTH);

    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> validators.validateSignal("testTenant", simpleEnrichment));
  }
}
