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
package io.isima.bios.preprocess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.ServiceException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.Event;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateEventTest {
  private static StreamConfig streamConfig;
  private static StreamConfig streamConfigMultiTypes;
  private static StreamConfig streamConfigTimestamp;

  @BeforeClass
  public static void setupClass() {
    streamConfig = new StreamConfig("abc");
    streamConfig.setAttributes(new ArrayList<>());

    AttributeDesc desc = new AttributeDesc("hello", InternalAttributeType.STRING);
    streamConfig.getAttributes().add(desc);

    desc = new AttributeDesc("foo", InternalAttributeType.STRING);
    streamConfig.getAttributes().add(desc);

    desc = new AttributeDesc("ratio", InternalAttributeType.STRING);
    streamConfig.getAttributes().add(desc);

    streamConfigMultiTypes = new StreamConfig("abc");
    streamConfigMultiTypes.setAttributes(new ArrayList<>());
    desc = new AttributeDesc("int", InternalAttributeType.INT);
    streamConfigMultiTypes.getAttributes().add(desc);
    desc = new AttributeDesc("number", InternalAttributeType.NUMBER);
    streamConfigMultiTypes.getAttributes().add(desc);
    desc = new AttributeDesc("string", InternalAttributeType.STRING);
    streamConfigMultiTypes.getAttributes().add(desc);
    desc = new AttributeDesc("text", InternalAttributeType.STRING);
    streamConfigMultiTypes.getAttributes().add(desc);
    desc = new AttributeDesc("uuid", InternalAttributeType.UUID);
    streamConfigMultiTypes.getAttributes().add(desc);

    streamConfigTimestamp = new StreamConfig("abc");
    streamConfigTimestamp.setAttributes(new ArrayList<>());
    desc = new AttributeDesc("timestamp", InternalAttributeType.TIMESTAMP);
    streamConfigTimestamp.getAttributes().add(desc);
  }

  @Test
  public void testParserNormal() throws ServiceException, TfosException {
    // basic
    IngestRequest input =
        PreprocessTestUtils.makeInput("world,bar,119:220", streamConfig.getVersion());
    Event event = Utils.createSignalEvent(input, streamConfig, false);

    assertEquals(input.getEventId(), event.getEventId());
    assertNotNull(event.getIngestTimestamp());
    assertEquals("world", event.getAttributes().get("hello"));

    assertEquals("bar", event.getAttributes().get("foo"));

    assertEquals("119:220", event.getAttributes().get("ratio"));

    // try creating another event
    input =
        PreprocessTestUtils.makeInput(
            "hello:elasticflash,ratio:21:9,foo:baz", streamConfig.getVersion());
    event = Utils.createSignalEvent(input, streamConfig, true);

    assertEquals(input.getEventId(), event.getEventId());
    assertNotNull(event.getIngestTimestamp());
    assertEquals("elasticflash", event.getAttributes().get("hello"));

    assertEquals("baz", event.getAttributes().get("foo"));

    assertEquals("21:9", event.getAttributes().get("ratio"));

    // test escaping
    input =
        PreprocessTestUtils.makeInput(
            "\"hello:elasticflash, inc.\",\"ratio:21,9,3\",foo:baz", streamConfig.getVersion());
    event = Utils.createSignalEvent(input, streamConfig, true);

    assertEquals(input.getEventId(), event.getEventId());
    assertNotNull(event.getIngestTimestamp());
    assertEquals("elasticflash, inc.", event.getAttributes().get("hello"));

    assertEquals("baz", event.getAttributes().get("foo"));

    assertEquals("21,9,3", event.getAttributes().get("ratio"));

    // test escaping #2
    input =
        PreprocessTestUtils.makeInput(
            "\"elastic\\flash, inc.\",baz,\"21,9,3\"", streamConfig.getVersion());
    event = Utils.createSignalEvent(input, streamConfig, false);

    assertEquals(input.getEventId(), event.getEventId());
    assertNotNull(event.getIngestTimestamp());
    assertEquals("elastic\\flash, inc.", event.getAttributes().get("hello"));

    assertEquals("baz", event.getAttributes().get("foo"));

    assertEquals("21,9,3", event.getAttributes().get("ratio"));
  }

  @Test(expected = TfosException.class)
  public void unknownKey() throws ServiceException, TfosException {
    Utils.createSignalEvent(
        PreprocessTestUtils.makeInput(
            "hello:elasticflash,ratio:21:9,bar:baz", streamConfig.getVersion()),
        streamConfig,
        true);
  }

  @Test(expected = TfosException.class)
  public void keyMissing() throws ServiceException, TfosException {
    Utils.createSignalEvent(
        PreprocessTestUtils.makeInput(
            "hello:elasticflash,ratio:21:9,baz", streamConfig.getVersion()),
        streamConfig,
        true);
  }

  @Test(expected = TfosException.class)
  public void tooManyValues() throws ServiceException, TfosException {
    Utils.createSignalEvent(
        PreprocessTestUtils.makeInput("elasticflash,abc,def,ghi", streamConfig.getVersion()),
        streamConfig,
        false);
  }

  @Test
  public void testValueTypes() throws ServiceException, TfosException {
    Event event =
        Utils.createSignalEvent(
            PreprocessTestUtils.makeInput(
                "123,987654321,abc,hello world,9da82911-1a4d-4d04-990d-c2ac58392051",
                streamConfigMultiTypes.getVersion()),
            streamConfigMultiTypes,
            false);

    assertEquals(Integer.valueOf(123), event.getAttributes().get("int"));
    assertTrue(event.getAttributes().get("int") instanceof Integer);

    assertEquals(BigInteger.valueOf(987654321L), event.getAttributes().get("number"));
    assertTrue(event.getAttributes().get("number") instanceof BigInteger);

    assertEquals("abc", event.getAttributes().get("string"));
    assertTrue(event.getAttributes().get("string") instanceof String);

    assertEquals("hello world", event.getAttributes().get("text"));
    assertTrue(event.getAttributes().get("text") instanceof String);

    assertEquals(
        UUID.fromString("9da82911-1a4d-4d04-990d-c2ac58392051"), event.getAttributes().get("uuid"));
    assertTrue(event.getAttributes().get("uuid") instanceof UUID);
  }

  @Test
  public void twoAttributes() throws ServiceException, TfosException {
    StreamConfig stream = new StreamConfig("increase_attributes");
    stream.addAttribute(new AttributeDesc("one", InternalAttributeType.STRING));
    stream.addAttribute(new AttributeDesc("two", InternalAttributeType.INT));

    IngestRequest input = PreprocessTestUtils.makeInput("ichi,2", stream.getVersion());
    Event event = Utils.createSignalEvent(input, stream, false);
    assertEquals("ichi", event.getAttributes().get("one"));
    assertEquals(2, event.getAttributes().get("two"));
  }

  @Test
  public void testTimestampValue() throws ServiceException, TfosException {
    Event event =
        Utils.createSignalEvent(
            PreprocessTestUtils.makeInput(
                "2018-03-08T21:30:27.339Z", streamConfigTimestamp.getVersion()),
            streamConfigTimestamp,
            false);
    System.out.println(event.getAttributes().get("timestamp"));
  }

  @Test(expected = InvalidValueSyntaxException.class)
  public void testInvalidInt() throws ServiceException, TfosException {
    Utils.createSignalEvent(
        PreprocessTestUtils.makeInput(
            "xxx,987654321,abc,hello world,9da82911-1a4d-4d04-990d-c2ac58392051",
            streamConfigMultiTypes.getVersion()),
        streamConfigMultiTypes,
        false);
  }

  @Test(expected = InvalidValueSyntaxException.class)
  public void testInvalidLong() throws ServiceException, TfosException {
    Utils.createSignalEvent(
        PreprocessTestUtils.makeInput(
            "123,beef,abc,hello world,9da82911-1a4d-4d04-990d-c2ac58392051",
            streamConfigMultiTypes.getVersion()),
        streamConfigMultiTypes,
        false);
  }

  @Test(expected = InvalidValueSyntaxException.class)
  public void testInvalidUuid() throws ServiceException, TfosException {
    Utils.createSignalEvent(
        PreprocessTestUtils.makeInput(
            "123,987,abc,hello world,dead-beef-cafe-beef", streamConfigMultiTypes.getVersion()),
        streamConfigMultiTypes,
        false);
  }

  @Test
  public void convertStringTest() throws TfosException {
    final AttributeDesc desc = new AttributeDesc("stringAttr", InternalAttributeType.STRING);
    assertEquals("abc", Attributes.convertValue("abc", desc));
    assertEquals("", Attributes.convertValue("", desc));
  }

  @Test
  public void convertBooleanTest() throws TfosException {
    final AttributeDesc desc = new AttributeDesc("booleanAttr", InternalAttributeType.BOOLEAN);
    assertEquals(true, Attributes.convertValue("true", desc));
    try {
      Attributes.convertValue("", desc);
      fail("exception must happen");
    } catch (InvalidValueSyntaxException e) {
      assertTrue(e.getMessage().contains("Boolean value may not be null or empty"));
    }
    desc.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    desc.setInternalDefaultValue(Boolean.TRUE);

    assertEquals(true, Attributes.convertValue("", desc));
  }

  @Test
  public void convertDateTest() throws TfosException {
    AttributeDesc desc = new AttributeDesc("dateAttr", InternalAttributeType.DATE);
    Object converted = Attributes.convertValue("2018-03-08", desc);
    assertEquals("2018-03-08", converted.toString());

    converted = Attributes.convertValue("12345", desc);
    assertEquals(12345, ((LocalDate) converted).toEpochDay());
  }

  @Test
  public void convertTimestampTest() throws TfosException {
    AttributeDesc desc = new AttributeDesc("timestampAttr", InternalAttributeType.TIMESTAMP);
    Object converted = Attributes.convertValue("2018-03-08T21:30:27.339-0000", desc);
    assertEquals(1520544627339L, ((Date) converted).getTime());
    converted = Attributes.convertValue("2018-03-08T21:30:27.339Z", desc);
    assertEquals(1520544627339L, ((Date) converted).getTime());

    converted = Attributes.convertValue("12345", desc);
    assertEquals(12345L, ((Date) converted).getTime());
  }

  @Test
  public void convertUuidTest() throws TfosException {
    AttributeDesc desc = new AttributeDesc("uuidAttr", InternalAttributeType.UUID);
    Object converted = Attributes.convertValue("4de755b4-acec-46e7-a493-e01215e2db22", desc);
    assertEquals(UUID.fromString("4de755b4-acec-46e7-a493-e01215e2db22"), converted);
  }

  @Test(expected = InvalidValueSyntaxException.class)
  public void convertInvalidUuidTest() throws TfosException {
    AttributeDesc desc = new AttributeDesc("uuidAttr", InternalAttributeType.UUID);
    Attributes.convertValue("4de755b4-acec-46e7-a493-e01215e2db2", desc);
  }

  @Test
  public void convertEnumTest() throws TfosException {
    final AttributeDesc desc = new AttributeDesc("car_make", InternalAttributeType.ENUM);
    desc.setEnum(Arrays.asList(new String[] {"Toyota", "Suzuki", "Ford", "Tata"}));
    assertEquals(2, Attributes.convertValue("Ford", desc));

    // specified value must be one of enum entries
    try {
      Attributes.convertValue("Bianchi", desc);
      fail("exception must happen");
    } catch (TfosException e) {
      final String message = e.getMessage();
      assertEquals(
          "Invalid enum: Specified name is not in the list of enum entries;"
              + " attribute=car_make, type=Enum, value=Bianchi",
          message);
    }

    // entry match is case sensitive
    try {
      Attributes.convertValue("SUZUKI", desc);
      fail("exception must happen");
    } catch (TfosException e) {
      assertEquals(
          "Invalid enum: Specified name is not in the list of enum entries;"
              + " attribute=car_make, type=Enum, value=SUZUKI",
          e.getMessage());
    }
  }
}
