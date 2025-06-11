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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.net.InetAddresses;
import io.isima.bios.admin.v1.TableDesc;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.mapper.JsonMappingExceptionMapper;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonConvertTest {
  private static ObjectMapper mapper;
  private static JsonMappingExceptionMapper exceptionMapper;

  public static class LocalDateSerializer extends StdSerializer<LocalDate> {

    /** generated version uid. */
    private static final long serialVersionUID = -7933038046345741652L;

    public LocalDateSerializer() {
      this(null);
    }

    public LocalDateSerializer(Class<LocalDate> t) {
      super(t);
    }

    @Override
    public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeString(value.toString());
    }
  }

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
    exceptionMapper = new JsonMappingExceptionMapper();
  }

  @Test
  public void testDeserializeAttributeDesc()
      throws JsonParseException, JsonMappingException, IOException {
    final String src = "{\"name\": \"ip\",\"type\": \"INET\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("ip", desc.getName());
    assertEquals(InternalAttributeType.INET, desc.getAttributeType());
    assertNull(desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescValidEnum()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', 'Chinese', 'Mongolian'], 'defaultValue': 'Chinese'}";
    final AttributeDesc desc = mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
    assertEquals("cousin", desc.getName());
    assertEquals(InternalAttributeType.ENUM, desc.getAttributeType());
    assertArrayEquals(new String[] {"Mexican", "Chinese", "Mongolian"}, desc.getEnum().toArray());
    assertEquals("Chinese", desc.getDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescIntEnum()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', 98739, 'Mongolian'], 'defaultValue': 'Chinese'}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum entry name must be a string;"
              + " attribute=cousin, enum_index=1, value=98739",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescNullEnum()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', null, 'Mongolian'], 'defaultValue': 'Chinese'}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum entry name must be a string;"
              + " attribute=cousin, enum_index=1, value=null",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescBooleanEnum()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', true, 'Mongolian'], 'defaultValue': 'Chinese'}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum entry name must be a string;"
              + " attribute=cousin, enum_index=1, value=true",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescEmptyEnum()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', '', 'Mongolian'], 'defaultValue': 'Mongolian'}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum entry name may not be empty;"
              + " attribute=cousin, enum_index=1",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescBlankEnum()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', ' ', 'Mongolian'], 'defaultValue': 'Mongolian'}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum entry name may not be empty;"
              + " attribute=cousin, enum_index=1",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescNonExistingEnumDefault()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', 'Chinese', 'Mongolian'], 'defaultValue': 'Turkish'}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum default value must match one of entries;"
              + " attribute=cousin, defaultValue=Turkish",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescEmptyEnumDefault()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', 'Chinese', 'Mongolian'], 'defaultValue': ''}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum default value must match one of entries;"
              + " attribute=cousin, defaultValue=",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeAttributeDescIntegerEnumDefault()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'name': 'cousin', 'type': 'ENUM',"
            + " 'enum': ['Mexican', 'Chinese', 'Mongolian', '123'], 'defaultValue': 123}";
    try {
      mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
      fail("exception must happen");
    } catch (JsonMappingException e) {
      assertEquals(
          ": Invalid value syntax: Enum default value must be a string;"
              + " attribute=cousin, defaultValue=123",
          applyExceptionMapper(e));
    }
  }

  @Test
  public void testDeserializeEnumAttributeDesc()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"cousine\", \"type\": \"enum\","
            + " \"enum\": [\"American\", \"Chinese\", \"Indian\", \"Japanese\", \"Mexican\"],"
            + " \"defaultValue\": \"American\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("cousine", desc.getName());
    assertEquals(InternalAttributeType.ENUM, desc.getAttributeType());
    assertEquals("American", desc.getDefaultValue());
    assertNotNull(desc.getEnum());
    assertEquals(5, desc.getEnum().size());
    assertEquals("American", desc.getEnum().get(0));
    assertEquals("Chinese", desc.getEnum().get(1));
    assertEquals("Indian", desc.getEnum().get(2));
    assertEquals("Japanese", desc.getEnum().get(3));
    assertEquals("Mexican", desc.getEnum().get(4));
  }

  @Test
  public void testDeserializeAttributeDescValidBlob()
      throws JsonParseException, JsonMappingException, IOException {

    final String blobPart =
        Base64.getEncoder()
            .encodeToString(new byte[] {(byte) 0xba, (byte) 0x5e, (byte) 0xba, (byte) 0x11});
    final String src =
        String.format("{'name': 'thumnail', 'type': 'BLOB', 'defaultValue': '%s'}", blobPart);
    final AttributeDesc desc = mapper.readValue(src.replaceAll("'", "\""), AttributeDesc.class);
    assertEquals("thumnail", desc.getName());
    assertEquals(InternalAttributeType.BLOB, desc.getAttributeType());
    assertTrue(desc.getInternalDefaultValue() instanceof ByteBuffer);
    final ByteBuffer buf = (ByteBuffer) desc.getInternalDefaultValue();
    assertEquals(4, buf.remaining());
    assertEquals(0xba5eba11, buf.getInt());

    final String out = mapper.writeValueAsString(desc);
    assertTrue(out.contains(blobPart));
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultString()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"string_attribute\",\"type\": \"string\", \"defaultValue\": 123}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("string_attribute", desc.getName());
    assertEquals(InternalAttributeType.STRING, desc.getAttributeType());
    assertEquals("123", desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultBoolean()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"BooleanAttribute\",\"type\": \"boolean\", \"defaultValue\": true}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("BooleanAttribute", desc.getName());
    assertEquals(InternalAttributeType.BOOLEAN, desc.getAttributeType());
    assertEquals(Boolean.TRUE, desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultInt()
      throws JsonParseException, JsonMappingException, IOException {
    final String src = "{\"name\": \"integer\",\"type\": \"INT\", \"defaultValue\": 123}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("integer", desc.getName());
    assertEquals(InternalAttributeType.INT, desc.getAttributeType());
    assertEquals(123, desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultNegativeInt()
      throws JsonParseException, JsonMappingException, IOException {
    final String src = "{\"name\": \"integer\",\"type\": \"INT\", \"defaultValue\": -123}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("integer", desc.getName());
    assertEquals(InternalAttributeType.INT, desc.getAttributeType());
    assertEquals(-123, desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultIntAsString()
      throws JsonParseException, JsonMappingException, IOException {
    final String src = "{\"name\": \"integer\",\"type\": \"INT\", \"defaultValue\": \"123\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("integer", desc.getName());
    assertEquals(InternalAttributeType.INT, desc.getAttributeType());
    assertEquals(123, desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultNegativeIntAsString()
      throws JsonParseException, JsonMappingException, IOException {
    final String src = "{\"name\": \"integer\",\"type\": \"INT\", \"defaultValue\": \"-123\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("integer", desc.getName());
    assertEquals(InternalAttributeType.INT, desc.getAttributeType());
    assertEquals(-123, desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultNumber()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"bigNumber\",\"type\": \"NUMBER\","
            + " \"defaultValue\": 18446744073709551615000000}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("bigNumber", desc.getName());
    assertEquals(InternalAttributeType.NUMBER, desc.getAttributeType());
    assertEquals(new BigInteger("18446744073709551615000000"), desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultInet()
      throws JsonParseException, JsonMappingException, IOException {
    final String src = "{\"name\": \"ip\",\"type\": \"INET\", \"defaultValue\": \"123.45.67.89\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("ip", desc.getName());
    assertEquals(InternalAttributeType.INET, desc.getAttributeType());
    assertEquals(InetAddresses.forString("123.45.67.89"), desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultDate()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"birthday\",\"type\": \"Date\", \"defaultValue\": \"2001-02-10\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("birthday", desc.getName());
    assertEquals(InternalAttributeType.DATE, desc.getAttributeType());
    assertEquals(LocalDate.of(2001, 2, 10), desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultTimestamp()
      throws JsonParseException, JsonMappingException, IOException, InvalidValueSyntaxException {
    final String src =
        "{\"name\": \"birthday\",\"type\": \"timestamp\","
            + " \"defaultValue\": \"2018-09-10T13:21:45.567Z\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("birthday", desc.getName());
    assertEquals(InternalAttributeType.TIMESTAMP, desc.getAttributeType());
    assertEquals(
        InternalAttributeType.TIMESTAMP.parse("2018-09-10T13:21:45.567Z"),
        desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithDefaultUuid()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"id\",\"type\": \"uuid\","
            + " \"defaultValue\": \"45bc81a2-b779-11e8-96f8-529269fb1459\"}";
    final AttributeDesc desc = mapper.readValue(src, AttributeDesc.class);
    assertEquals("id", desc.getName());
    assertEquals(InternalAttributeType.UUID, desc.getAttributeType());
    assertEquals(
        UUID.fromString("45bc81a2-b779-11e8-96f8-529269fb1459"), desc.getInternalDefaultValue());
  }

  @Test
  public void testDeserializeAttributeDescWithInvalidDefault()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{\"name\": \"sequence\",\"type\": \"INT\", \"defaultValue\": \"123.45.67.89\"}";
    try {
      mapper.readValue(src, AttributeDesc.class);
      fail("exception must occur");
    } catch (JsonMappingException e) {
      System.out.println(e.getMessage());
      assertTrue(
          e.getMessage(),
          e.getMessage().contains("Invalid value syntax:" + " Input must be a numeric string"));
    }
  }

  @Test
  public void testDeserializeAttributeDescWithEmptyDefault()
      throws JsonParseException, IOException {
    final String src = "{\"name\": \"date\",\"type\": \"DATE\", \"defaultValue\": \"\"}";
    try {
      mapper.readValue(src, AttributeDesc.class);
      fail("exception must occur");
    } catch (JsonMappingException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains("Invalid value syntax: Date value may not be null or empty"));
    }
  }

  @Test
  public void testSerializeAttributeDesc() throws JsonProcessingException {
    AttributeDesc desc = new AttributeDesc();
    desc.setName("Leonardo Da Vinci");
    desc.setAttributeType(InternalAttributeType.NUMBER);
    desc.setDefaultValue(10);
    desc.setMinValue(1);
    String out = mapper.writeValueAsString(desc);
    String expected =
        "{\"name\":\"Leonardo Da Vinci\",\"type\":\"number\",\"defaultValue\":10,\"minValue\":1}";
    assertEquals(expected, out);
  }

  @Test
  public void testSerializeAttributeDescDate() throws JsonProcessingException {
    AttributeDesc desc = new AttributeDesc();
    desc.setName("birthday");
    desc.setAttributeType(InternalAttributeType.DATE);
    desc.setDefaultValue(LocalDate.of(1970, 1, 1));
    // desc.setMinValue(1);
    String out = mapper.writeValueAsString(desc);
    String expected = "{\"name\":\"birthday\",\"type\":\"date\",\"defaultValue\":\"1970-01-01\"}";
    assertEquals(expected, out);
  }

  @Test
  public void testSerializeAttributeDescEnum() throws JsonProcessingException {
    AttributeDesc desc = new AttributeDesc();
    desc.setName("priority");
    desc.setAttributeType(InternalAttributeType.ENUM);
    desc.setEnum(Arrays.asList(new String[] {"low", "medium", "high"}));
    desc.setDefaultValue("medium");
    String out = mapper.writeValueAsString(desc);
    String expected =
        "{\"name\":\"priority\",\"type\":\"enum\",\"enum\":[\"low\",\"medium\",\"high\"],"
            + "\"defaultValue\":\"medium\"}";
    assertEquals(expected, out);
  }

  @Test
  public void testTableDesc() throws JsonParseException, JsonMappingException, IOException {
    // test deserialization
    String src =
        "{\"name\": \"geo_lookup\",\"load\": true,\"save\": true,"
            + "\"keys\": [{\"name\": \"ip\",\"type\": \"string\"}],"
            + "\"values\": [{\"name\": \"country\",\"type\": \"string\"},"
            + "{\"name\": \"state\",\"type\": \"string\"},{\"name\": \"city\",\"type\": \"string\"}]}";
    TableDesc desc = mapper.readValue(src, TableDesc.class);
    assertEquals("geo_lookup", desc.getName());
    assertEquals(true, desc.isLoad());
    assertEquals(true, desc.isSave());
    assertEquals(1, desc.getKeys().length);
    assertEquals("ip", desc.getKeys()[0].getName());
    assertEquals(InternalAttributeType.STRING, desc.getKeys()[0].getAttributeType());
    assertEquals(3, desc.getValues().length);
    assertEquals("country", desc.getValues()[0].getName());

    // test serialization
    assertEquals(
        "{\"name\":\"geo_lookup\",\"load\":true,\"save\":true,"
            + "\"keys\":[{\"name\":\"ip\",\"type\":\"string\"}],"
            + "\"values\":[{\"name\":\"country\",\"type\":\"string\"},"
            + "{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"}]}",
        mapper.writeValueAsString(desc));
  }

  @Test
  public void testIndexDesc() throws JsonParseException, JsonMappingException, IOException {
    // one-column index
    String src = "{\"_comment\": \"table name: remoteaccess_by_ip\",\"attributes\": [\"ip\"]}";
    IndexDesc desc = mapper.readValue(src, IndexDesc.class);
    assertNull(desc.getName());
    assertEquals(1, desc.getAttributes().size());
    assertEquals("ip", desc.getAttributes().get(0).getName());

    // empty index entry
    src = "{}";
    desc = mapper.readValue(src, IndexDesc.class);
    assertNull(desc.getAttributes());

    // three-column index
    src =
        "{\"_comment\": \"table name: remoteaccess_by_country_state_city\","
            + "\"attributes\": [\"country\",\"state\",\"city\"]}";
    desc = mapper.readValue(src, IndexDesc.class);
    assertEquals(3, desc.getAttributes().size());
    assertEquals("country", desc.getAttributes().get(0).getName());
    assertEquals("state", desc.getAttributes().get(1).getName());
    assertEquals("city", desc.getAttributes().get(2).getName());

    // test serialization
    desc.setName("autogenerated-name");
    desc.getAttributes().get(0).setAttributeType(InternalAttributeType.STRING);
    String out = mapper.writeValueAsString(desc);
    assertEquals("{\"attributes\":[\"country\",\"state\",\"city\"]}", out);
  }

  @Test
  public void testPreprocessesDesc() throws JsonParseException, JsonMappingException, IOException {
    String src =
        "{\"_comment\": \"add_geolocation_info\",\"condition\": \"ip = geo_lookup.ip\","
            + "\"actions\": [{\"actionType\": \"merge\",\"attribute\": \"country\"},"
            + "{\"actionType\": \"merge\",\"attribute\": \"state\"},"
            + "{\"actionType\": \"merge\",\"attribute\": \"city\"}]}";
    PreprocessDesc desc = mapper.readValue(src, PreprocessDesc.class);
    assertEquals("ip = geo_lookup.ip", desc.getCondition());
    assertEquals(3, desc.getActions().size());
    assertEquals("country", desc.getActions().get(0).getAttribute());
    assertEquals(ActionType.MERGE, desc.getActions().get(0).getActionType());
  }

  @Test
  public void testEvent() throws JsonParseException, JsonMappingException, IOException {
    // deserialize without attributes
    String src =
        "{\"eventId\":\"4de755b4-acec-46e7-a493-e01215e2db28\",\"ingestTimestamp\":1520544627339}";
    Event event = mapper.readValue(src, EventJson.class);
    assertEquals(UUID.fromString("4de755b4-acec-46e7-a493-e01215e2db28"), event.getEventId());
    assertEquals(new Date(1520544627339L), event.getIngestTimestamp());
    assertNotNull(event.getAttributes());
    assertTrue(event.getAttributes().isEmpty());

    // deserialize with attributes
    src =
        "{\"eventId\":\"4de755b4-acec-46e7-a493-e01215e2db28\","
            + "\"ingestTimestamp\":\"2018-03-08T21:30:27.339Z\","
            + "\"hello\":\"world\","
            + "\"abc\":123,"
            + "\"blank\":null}";
    event = mapper.readValue(src, EventJson.class);
    assertEquals(1520544627339L, event.getIngestTimestamp().getTime());
    assertEquals(3, event.getAttributes().size());
    assertEquals("world", event.getAttributes().get("hello"));
    assertEquals(123, event.getAttributes().get("abc"));

    // serialize
    String out = mapper.writeValueAsString(event);
    String expected =
        "{\"eventId\":\"4de755b4-acec-46e7-a493-e01215e2db28\","
            + "\"ingestTimestamp\":1520544627339,"
            + "\"hello\":\"world\","
            + "\"abc\":123,"
            + "\"blank\":null}";
    assertEquals(expected, out);
  }

  @Test
  public void testStreamConfig() throws JsonParseException, JsonMappingException, IOException {
    String src = "{\"name\":\"streamtest\"}";
    StreamConfig streamConfig = mapper.readValue(src, StreamConfig.class);
    assertEquals(StreamType.SIGNAL, streamConfig.getType());

    src = "{\"name\":\"streamtest\",\"type\":\"context\"}";
    streamConfig = mapper.readValue(src, StreamConfig.class);
    assertEquals(StreamType.CONTEXT, streamConfig.getType());

    src =
        "{\"name\":\"streamtest\","
            + "\"eventAttributes\":[{\"name\":\"fundamental\",\"type\":\"string\"}],"
            + "\"additionalEventAttributes\":[{\"name\":\"secondary\",\"type\":\"string\"}]}";
    streamConfig = mapper.readValue(src, StreamConfig.class);
    assertNotNull(streamConfig.getAttributes());
    assertNull(streamConfig.getAdditionalAttributes());
  }

  @Test
  public void testView() throws JsonParseException, JsonMappingException, IOException {
    String src = "{\"function\": \"group\", \"by\": \"group_key\"}";
    View view = mapper.readValue(src, View.class);
    assertEquals(ViewFunction.GROUP, view.getFunction());
    assertEquals("group_key", view.getBy());
  }

  @Test
  public void operation() throws JsonParseException, JsonMappingException, IOException {
    operationSub("add", Operation.ADD);
    operationSub("Add", Operation.ADD);
    operationSub("remove", Operation.REMOVE);
    operationSub("Remove", Operation.REMOVE);

    String src = "\"invalid\"";
    try {
      mapper.readValue(src, Operation.class);
      fail("exception is expected");
    } catch (JsonMappingException e) {
      // do nothing here
    }
  }

  private void operationSub(String src, Operation expectedOp)
      throws JsonParseException, JsonMappingException, IOException {
    String completeSrc = String.format("\"%s\"", src);
    Operation operation = mapper.readValue(completeSrc, Operation.class);
    assertEquals(expectedOp, operation);
    String out = mapper.writeValueAsString(operation);
    assertEquals(completeSrc.toLowerCase(), out);
  }

  @Test
  public void updateEndpointsRequest()
      throws JsonParseException, JsonMappingException, IOException {
    String src =
        "{"
            + "\"operation\": \"add\", "
            + "\"endpoint\": \"http://172.18.0.11\", "
            + "\"nodeType\": \"signal\"}";
    UpdateEndpointsRequest request = mapper.readValue(src, UpdateEndpointsRequest.class);
    assertNotNull(request);
    assertEquals("http://172.18.0.11", request.getEndpoint());
    assertEquals(Operation.ADD, request.getOperation());
    assertEquals(NodeType.SIGNAL, request.getNodeType());

    String out = mapper.writeValueAsString(request);
    String expected =
        "{\"operation\":\"add\",\"endpoint\":\"http://172.18.0.11\",\"nodeType\":\"signal\"}";
    assertEquals(expected, out);
  }

  @Test
  public void updateEndpointsRequestBackwardCompatibility()
      throws JsonParseException, JsonMappingException, IOException {
    String src = "{\"operation\": \"add\", \"endpoint\": \"http://172.18.0.11\"}";
    UpdateEndpointsRequest request = mapper.readValue(src, UpdateEndpointsRequest.class);
    assertNotNull(request);
    assertEquals("http://172.18.0.11", request.getEndpoint());
    assertEquals(Operation.ADD, request.getOperation());
    assertEquals(NodeType.SIGNAL, request.getNodeType());

    String out = mapper.writeValueAsString(request);
    String expected =
        "{\"operation\":\"add\",\"endpoint\":\"http://172.18.0.11\",\"nodeType\":\"signal\"}";
    assertEquals(expected, out);
  }

  @Test
  public void upstreamConfigJsoNConversionTest()
      throws JsonParseException, JsonMappingException, IOException {
    String src =
        "{"
            + "  \"upstreams\": ["
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-signal-1\","
            + "        \"bios-signal-2\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"CONTEXT_WRITE\""
            + "          }"
            + "        ]"
            + "      }"
            + "    },"
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-analysis-1\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"UNDERFAILURE\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"NEVER\","
            + "            \"operationType\": \"EXTRACT\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"ADMIN_READ\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"ADMIN_WRITE\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"SUMMARIZE\""
            + "          }"
            + "        ]"
            + "      }"
            + "    },"
            + "    {"
            + "      \"hostSet\": ["
            + "        \"bios-rollup-1\""
            + "      ],"
            + "      \"operationSet\": {"
            + "        \"operation\": ["
            + "          {"
            + "            \"operationPolicy\": \"UNDERFAILURE\","
            + "            \"operationType\": \"INGEST\""
            + "          },"
            + "          {"
            + "            \"operationPolicy\": \"ALWAYS\","
            + "            \"operationType\": \"EXTRACT\""
            + "          }"
            + "        ]"
            + "      }"
            + "    }"
            + "  ],"
            + "  \"lbPolicy\": \"ROUND_ROBIN\","
            + "  \"version\": 0,"
            + "  \"epoch\": 0"
            + "}";
    UpstreamConfig upstreamConfig = mapper.readValue(src, UpstreamConfig.class);
    assertNotNull(upstreamConfig);
    assertNotNull(upstreamConfig.getUpstreams());
    assertNotNull(upstreamConfig.getUpstreams().get(0));
    Upstream upstream = upstreamConfig.getUpstreams().get(0);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertEquals(upstream.getHostSet()[0], "bios-signal-1");
    assertEquals(upstream.getHostSet()[1], "bios-signal-2");
    OperationSet opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());

    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(opSet.getOperation().get(1));
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.CONTEXT_WRITE);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(upstreamConfig.getUpstreams().get(1));
    upstream = upstreamConfig.getUpstreams().get(1);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertEquals(upstream.getHostSet()[0], "bios-analysis-1");
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());

    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.UNDERFAILURE);

    assertNotNull(opSet.getOperation().get(1));
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.EXTRACT);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.NEVER);

    assertNotNull(opSet.getOperation().get(2));
    assertEquals(opSet.getOperation().get(2).getOperationType(), OperationType.ADMIN_READ);
    assertEquals(opSet.getOperation().get(2).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(opSet.getOperation().get(3));
    assertEquals(opSet.getOperation().get(3).getOperationType(), OperationType.ADMIN_WRITE);
    assertEquals(opSet.getOperation().get(3).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(opSet.getOperation().get(4));
    assertEquals(opSet.getOperation().get(4).getOperationType(), OperationType.SUMMARIZE);
    assertEquals(opSet.getOperation().get(4).getOperationPolicy(), OperationPolicy.ALWAYS);

    assertNotNull(upstreamConfig.getUpstreams().get(2));
    upstream = upstreamConfig.getUpstreams().get(2);
    assertNotNull(upstream.getHostSet());
    assertNotNull(upstream.getOperationSet());
    assertEquals(upstream.getHostSet()[0], "bios-rollup-1");
    opSet = upstream.getOperationSet();
    assertNotNull(opSet.getOperation());

    assertNotNull(opSet.getOperation().get(0));
    assertEquals(opSet.getOperation().get(0).getOperationType(), OperationType.INGEST);
    assertEquals(opSet.getOperation().get(0).getOperationPolicy(), OperationPolicy.UNDERFAILURE);

    assertNotNull(opSet.getOperation().get(1));
    assertEquals(opSet.getOperation().get(1).getOperationType(), OperationType.EXTRACT);
    assertEquals(opSet.getOperation().get(1).getOperationPolicy(), OperationPolicy.ALWAYS);

    String out = mapper.writeValueAsString(upstreamConfig);
    assertEquals(src.replaceAll("\\s", ""), out);
  }

  private Object applyExceptionMapper(JsonMappingException e) {
    Response resp = exceptionMapper.toResponse(e);
    return ((ErrorResponsePayload) resp.getEntity()).getMessage();
  }
}
