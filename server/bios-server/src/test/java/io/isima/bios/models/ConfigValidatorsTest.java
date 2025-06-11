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

import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.DATE;
import static io.isima.bios.models.v1.InternalAttributeType.DOUBLE;
import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.INET;
import static io.isima.bios.models.v1.InternalAttributeType.NUMBER;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;
import static io.isima.bios.models.v1.InternalAttributeType.TIMESTAMP;
import static io.isima.bios.models.v1.InternalAttributeType.UUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigValidatorsTest {
  private static Validator validator;

  @BeforeClass
  public static void setUpClass() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Test
  public void testContextConfig() throws Exception {
    final var src =
        "{"
            // + "    \"contextName\": SIMPLE_CONTEXT_NAME," +
            + "    \"missingAttributePolicy\": \"Reject\","
            + "    \"attributes\": ["
            + "        {\"attributeName\": \"the_key\", \"type\": \"integer\"},"
            + "        {\"attributeName\": \"the_value\", \"type\": \"string\"}"
            + "    ],"
            + "    \"primaryKey\": [\"the_key\"]"
            + "}";
    final var config = BiosObjectMapperProvider.get().readValue(src, ContextConfig.class);
    final var violations = validator.validate(config);
    assertFalse(violations.isEmpty());
    for (var violation : violations) {
      final var path = violation.getPropertyPath();
      final var message = violation.getMessage();
      System.out.println(String.format("Property '%s' %s", path, message));
    }
  }

  @Test
  public void testAttributeDesc() {
    AttributeDesc desc = new AttributeDesc();

    // invalid: name and attributeType are null
    Set<ConstraintViolation<AttributeDesc>> constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: name is null
    desc.setAttributeType(NUMBER);
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: attributeType is null
    desc.setName("hello");
    desc.setAttributeType(null);
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // valid: set attributeType
    desc.setAttributeType(STRING);
    constraintViolations = validator.validate(desc);
    assertTrue(constraintViolations.isEmpty());

    // invalid: set empty name
    desc.setName("");
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: set reserved name "eventId"
    desc.setName("eventId");
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: set reserved name "ingestTimestamp"
    desc.setName("ingestTimestamp");
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: too long name
    desc.setName("a1234567890123456789012345678901234567890");
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());
  }

  @Test
  public void testEnumAttributeDesc() {
    AttributeDesc desc = new AttributeDesc("car_make", ENUM);

    // invalid: enum entries are missing
    Set<ConstraintViolation<AttributeDesc>> constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: enum entries is empty
    desc.setEnum(Arrays.asList(new String[] {}));
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // valid: has non-empty list of enum entries
    desc.setEnum(Arrays.asList(new String[] {"Toyota", "Suzuki", "Ford", "Tata"}));
    constraintViolations = validator.validate(desc);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: default value is not on the enum entry list
    desc.setDefaultValue("Nissan");
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: default value is on the enum entry list but cases are different
    desc.setDefaultValue("SUZUKI");
    constraintViolations = validator.validate(desc);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // valid: default value is on the enum entry list and cases match
    desc.setDefaultValue("Suzuki");
    constraintViolations = validator.validate(desc);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());
  }

  @Test
  public void testStreamConfig() {
    StreamConfig streamConfig = new StreamConfig("myStream");

    // valid: simplest
    Set<ConstraintViolation<StreamConfig>> constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    // valid: has valid event attributes
    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    // valid: has valid additional event attributes
    streamConfig.setAdditionalAttributes(new ArrayList<>());
    streamConfig.getAdditionalAttributes().add(new AttributeDesc("three", DOUBLE));
    streamConfig.getAdditionalAttributes().add(new AttributeDesc("four", BOOLEAN));
    constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: null name
    streamConfig.setName(null);
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: empty name
    streamConfig.setName("");
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: too long name
    streamConfig.setName("a1234567890123456789012345678901234567890");
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has duplicate attributes between eventAttributes and additionalEventAttributes
    streamConfig.setName("validName");
    streamConfig.getAttributes().add(new AttributeDesc("three", INET));
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has duplicate attributes in eventAttributes
    streamConfig.getAttributes().add(new AttributeDesc("ONE", DATE));
    streamConfig.setAdditionalAttributes(null);
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has duplicate attributes in additionalEventAttributes
    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    streamConfig.setAdditionalAttributes(new ArrayList<>());
    streamConfig.getAdditionalAttributes().add(new AttributeDesc("three", DOUBLE));
    streamConfig.getAdditionalAttributes().add(new AttributeDesc("four", BOOLEAN));
    streamConfig.getAdditionalAttributes().add(new AttributeDesc("FOUR", TIMESTAMP));
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has duplicate attributes in preprocess actions.
    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    streamConfig.setAdditionalAttributes(null);
    PreprocessDesc desc = new PreprocessDesc();
    desc.setName("preprocess");
    ActionDesc action = new ActionDesc();
    action.setAttribute("one");
    desc.addAction(action);
    streamConfig.addPreprocess(desc);
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has invalid attribute
    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    streamConfig.getAttributes().add(new AttributeDesc("eventId", UUID));
    streamConfig.setAdditionalAttributes(null);
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has attribute with reserved name
    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    streamConfig.getAttributes().add(new AttributeDesc("EVENTID", UUID));
    streamConfig.setAdditionalAttributes(null);
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // invalid: has attribute with reserved name
    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    streamConfig.setAdditionalAttributes(new ArrayList<>());
    streamConfig.getAdditionalAttributes().add(new AttributeDesc("ingesttimestamp", UUID));
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());
  }

  @Test
  public void testContextStream() {
    // invalid: context stream must have at least two attributes
    StreamConfig streamConfig = new StreamConfig("myStream");
    streamConfig.setType(StreamType.CONTEXT);
    Set<ConstraintViolation<StreamConfig>> constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    streamConfig.setAttributes(new ArrayList<>());
    streamConfig.getAttributes().add(new AttributeDesc("one", STRING));
    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());

    // valid: two attributes
    streamConfig.getAttributes().add(new AttributeDesc("two", NUMBER));
    constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());
  }

  @Test
  public void testStreamConfigMultiPreprocessor() {
    // invalid: stream config with valid preprocess
    StreamConfig streamConfig = new StreamConfig("streamConfig");
    Set<ConstraintViolation<StreamConfig>> constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    AttributeDesc attrString = new AttributeDesc("str", STRING);
    AttributeDesc attrDouble = new AttributeDesc("double", DOUBLE);
    AttributeDesc attrBoolean = new AttributeDesc("bool", BOOLEAN);
    List<AttributeDesc> configAttrList = new ArrayList<>();
    configAttrList.add(attrString);
    configAttrList.add(attrDouble);
    configAttrList.add(attrBoolean);

    Set<ConstraintViolation<List<AttributeDesc>>> aatrConstraintViolations =
        validator.validate(configAttrList);
    assertTrue(aatrConstraintViolations.toString(), aatrConstraintViolations.isEmpty());

    ActionDesc actionDescOne = new ActionDesc();
    actionDescOne.setActionType(ActionType.MERGE);
    actionDescOne.setAttribute("str");
    actionDescOne.setAs("attrOne");
    actionDescOne.setContext("ctx");
    actionDescOne.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);

    ActionDesc actionDescTwo = new ActionDesc();
    actionDescTwo.setActionType(ActionType.MERGE);
    actionDescTwo.setAttribute("str");
    actionDescTwo.setContext("ctx");
    actionDescTwo.setAs("attrTwo");
    actionDescTwo.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);

    PreprocessDesc preprocess = new PreprocessDesc("preprocess");
    preprocess.addAction(actionDescOne);
    preprocess.addAction(actionDescTwo);
    preprocess.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocess.setCondition("cnd");

    List<PreprocessDesc> configPreprocesses = new ArrayList<>();
    configPreprocesses.add(preprocess);
    streamConfig.setPreprocesses(configPreprocesses);

    constraintViolations = validator.validate(streamConfig);
    System.err.println(constraintViolations.toString());
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());
  }

  @Test
  public void testStreamConfigMultiPreprocessorDuplicateAsAttribute() {
    // invalid: stream config with duplicate action 'as' attribute
    StreamConfig streamConfig = new StreamConfig("streamConfig");
    Set<ConstraintViolation<StreamConfig>> constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    AttributeDesc attrString = new AttributeDesc("str", STRING);
    AttributeDesc attrDouble = new AttributeDesc("double", DOUBLE);
    AttributeDesc attrBoolean = new AttributeDesc("bool", BOOLEAN);
    List<AttributeDesc> configAttrList = new ArrayList<>();
    configAttrList.add(attrString);
    configAttrList.add(attrDouble);
    configAttrList.add(attrBoolean);

    ActionDesc actionDescOne = new ActionDesc();
    actionDescOne.setActionType(ActionType.MERGE);
    actionDescOne.setAttribute("str");
    actionDescOne.setAs("attrOne");
    actionDescOne.setContext("ctx");
    actionDescOne.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);

    ActionDesc actionDescTwo = new ActionDesc();
    actionDescTwo.setActionType(ActionType.MERGE);
    actionDescTwo.setAttribute("str");
    actionDescTwo.setContext("ctx");
    actionDescTwo.setAs("attrOne");
    actionDescTwo.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);

    PreprocessDesc preprocess = new PreprocessDesc("preprocess");
    preprocess.addAction(actionDescOne);
    preprocess.addAction(actionDescTwo);
    preprocess.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocess.setCondition("cond");

    List<PreprocessDesc> configPreprocesses = new ArrayList<>();
    configPreprocesses.add(preprocess);
    streamConfig.setPreprocesses(configPreprocesses);

    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());
  }

  @Test
  public void testStreamConfigInvalidView() {
    // invalid: stream config with duplicate action 'as' attribute
    StreamConfig streamConfig = new StreamConfig("streamConfig");
    Set<ConstraintViolation<StreamConfig>> constraintViolations = validator.validate(streamConfig);
    assertTrue(constraintViolations.toString(), constraintViolations.isEmpty());

    AttributeDesc attrString = new AttributeDesc("str", STRING);
    AttributeDesc attrDouble = new AttributeDesc("double", DOUBLE);
    AttributeDesc attrBoolean = new AttributeDesc("bool", BOOLEAN);
    List<AttributeDesc> configAttrList = new ArrayList<>();
    configAttrList.add(attrString);
    configAttrList.add(attrDouble);
    configAttrList.add(attrBoolean);

    ActionDesc actionDescOne = new ActionDesc();
    actionDescOne.setActionType(ActionType.MERGE);
    actionDescOne.setAttribute("str");
    actionDescOne.setAs("attrOne");
    actionDescOne.setContext("ctx");
    actionDescOne.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);

    ActionDesc actionDescTwo = new ActionDesc();
    actionDescTwo.setActionType(ActionType.MERGE);
    actionDescTwo.setAttribute("str");
    actionDescTwo.setContext("ctx");
    actionDescTwo.setAs("attrOne");
    actionDescTwo.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);

    PreprocessDesc preprocess = new PreprocessDesc("preprocess");
    preprocess.addAction(actionDescOne);
    preprocess.addAction(actionDescTwo);
    preprocess.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocess.setCondition("cond");

    List<PreprocessDesc> configPreprocesses = new ArrayList<>();
    configPreprocesses.add(preprocess);
    streamConfig.setPreprocesses(configPreprocesses);

    constraintViolations = validator.validate(streamConfig);
    assertFalse(constraintViolations.toString(), constraintViolations.isEmpty());
  }
}
