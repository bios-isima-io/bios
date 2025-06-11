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
package io.isima.bios.dto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextEntryRecord;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextDataDtosTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testMultiContextEntriesSpec() throws Exception {
    final String src =
        toJson(
            "{"
                + "'primaryKeys':[['one',1],['two',2],['three',3]],"
                + "'contentRepresentation':'UNTYPED'"
                + "}");

    final var decoded = mapper.readValue(src, MultiContextEntriesSpec.class);
    assertNotNull(decoded);
    assertEquals(ContentRepresentation.UNTYPED, decoded.getContentRepresentation());
    assertEquals(3, decoded.getPrimaryKeys().size());
    assertEquals(Arrays.asList("one", Long.valueOf(1)), decoded.getPrimaryKeys().get(0));
    assertEquals(Arrays.asList("two", Long.valueOf(2)), decoded.getPrimaryKeys().get(1));
    assertEquals(Arrays.asList("three", Long.valueOf(3)), decoded.getPrimaryKeys().get(2));
    assertNull(decoded.getAttributes());

    final String expectedRestoration =
        toJson(
            "{"
                + "'contentRepresentation':'UNTYPED',"
                + "'primaryKeys':[['one',1],['two',2],['three',3]]"
                + "}");
    assertEquals(expectedRestoration, mapper.writeValueAsString(decoded));
  }

  @Test
  public void testMultiContextEntriesSpecWithAttributes() throws Exception {
    final String src =
        toJson(
            "{"
                + "'primaryKeys':[['eine',1],['zwei',2]],"
                + "'contentRepresentation':'UNTYPED',"
                + "'attributes':['pk']"
                + "}");

    final var decoded = mapper.readValue(src, MultiContextEntriesSpec.class);
    assertNotNull(decoded);
    assertEquals(ContentRepresentation.UNTYPED, decoded.getContentRepresentation());
    assertEquals(2, decoded.getPrimaryKeys().size());
    assertEquals(Arrays.asList("eine", 1L), decoded.getPrimaryKeys().get(0));
    assertEquals(Arrays.asList("zwei", 2L), decoded.getPrimaryKeys().get(1));

    final String expectedRestoration =
        toJson(
            "{"
                + "'contentRepresentation':'UNTYPED',"
                + "'primaryKeys':[['eine',1],['zwei',2]],"
                + "'attributes':['pk']"
                + "}");

    assertEquals(expectedRestoration, mapper.writeValueAsString(decoded));
  }

  @Test
  public void testGetContextEntriesResponse() throws Exception {
    final var response = new GetContextEntriesResponse();
    response.setContentRepresentation(ContentRepresentation.UNTYPED);
    response.setPrimaryKey(Arrays.asList("first", "second"));
    response.setDefinitions(
        Arrays.asList(
            aconf("first", AttributeType.STRING),
            aconf("second", AttributeType.INTEGER),
            aconf("latitude", AttributeType.DECIMAL),
            aconf("longtitude", AttributeType.DECIMAL)));
    final var record0 = new ContextEntryRecord();
    final var uuid0 = UUID.fromString("1a40e3d0-327b-4af5-9165-b0cad796414d");
    final Long timestamp = 1593034231486L;
    record0.setTimestamp(timestamp);
    record0.setAttributes(
        Arrays.asList("one", Long.valueOf(1), Double.valueOf(45.67), Double.valueOf(-123.45)));
    final var record1 = new ContextEntryRecord();
    final var uuid1 = UUID.fromString("a12f84db-ab4b-48a6-b0f1-3b74237328f0");
    record1.setTimestamp(timestamp);
    record1.setAttributes(
        Arrays.asList("two", Long.valueOf(2), Double.valueOf(-12.34), Double.valueOf(76.54)));
    response.setEntries(Arrays.asList(record0, record1));

    final String payload = mapper.writeValueAsString(response);

    assertEquals(
        toJson(
            "{"
                + "'contentRepresentation':'UNTYPED',"
                + "'primaryKey':['first','second'],"
                + "'definitions':["
                + "{'attributeName':'first','type':'String'},"
                + "{'attributeName':'second','type':'Integer'},"
                + "{'attributeName':'latitude','type':'Decimal'},"
                + "{'attributeName':'longtitude','type':'Decimal'}],"
                + "'entries':["
                + "{'timestamp':1593034231486,"
                + "'attributes':['one',1,45.67,-123.45]},"
                + "{'timestamp':1593034231486,"
                + "'attributes':['two',2,-12.34,76.54]}]}"),
        payload);

    var decoded = mapper.readValue(payload, GetContextEntriesResponse.class);
    assertEquals(response.getContentRepresentation(), decoded.getContentRepresentation());
    assertEquals(response.getPrimaryKey(), response.getPrimaryKey());
    assertEquals(response.getDefinitions().size(), decoded.getDefinitions().size());
    assertEquals(response.getEntries().size(), decoded.getEntries().size());
    var decoded0 = decoded.getEntries().get(0);
    assertEquals(record0.getTimestamp(), decoded0.getTimestamp());
    assertEquals(record0.getAttributes(), decoded0.getAttributes());
  }

  @Test
  public void testPutContextEntriesRequest() throws Exception {
    final var request = new PutContextEntriesRequest();
    request.setContentRepresentation(ContentRepresentation.CSV);
    request.setEntries(Arrays.asList("one,1,0.123", "two,2,-1.324", "three,3,10.192"));

    final String payload = mapper.writeValueAsString(request);
    assertEquals(
        toJson(
            "{'contentRepresentation':'CSV',"
                + "'entries':['one,1,0.123','two,2,-1.324','three,3,10.192']}"),
        payload);

    final var decoded = mapper.readValue(payload, PutContextEntriesRequest.class);
    assertEquals(request.getContentRepresentation(), decoded.getContentRepresentation());
    assertEquals(request.getEntries(), decoded.getEntries());
  }

  @Test
  public void testUpdateContextEntryRequest() throws Exception {
    final var request = new UpdateContextEntryRequest();
    request.setContentRepresentation(ContentRepresentation.UNTYPED);
    request.setPrimaryKey(Arrays.asList("one", Long.valueOf(1), Double.valueOf(0.123)));
    request.setAttributes(new ArrayList<>());
    final var attr1 = new AttributeSpec();
    attr1.setName("productName");
    attr1.setValue("Brake Pad");
    request.getAttributes().add(attr1);
    final var attr2 = new AttributeSpec();
    attr1.setName("price");
    attr1.setValue(Double.valueOf(6.5));
    request.getAttributes().add(attr2);

    final String payload = mapper.writeValueAsString(request);
    assertEquals(
        toJson(
            "{'contentRepresentation':'UNTYPED',"
                + "'primaryKey':['one',1,0.123],"
                + "'attributes':["
                + "{'name':'price','value':6.5},"
                + "{'name':null,'value':null}]}"),
        payload);

    final var decoded = mapper.readValue(payload, UpdateContextEntryRequest.class);
    assertEquals(request.getContentRepresentation(), decoded.getContentRepresentation());
    assertEquals(request.getPrimaryKey(), decoded.getPrimaryKey());
    assertEquals(request.getAttributes().size(), decoded.getAttributes().size());
    assertEquals(
        request.getAttributes().get(0).getName(), decoded.getAttributes().get(0).getName());
    assertEquals(
        request.getAttributes().get(0).getValue(), decoded.getAttributes().get(0).getValue());
  }

  @Test
  public void testReplaceContextAttributesRequest() throws Exception {
    final var request = new ReplaceContextAttributesRequest();
    request.setContentRepresentation(ContentRepresentation.UNTYPED);
    request.setAttributeName("productId");
    request.setOldValue(Long.valueOf(123));
    request.setNewValue(Long.valueOf(456));

    final String payload = mapper.writeValueAsString(request);
    assertEquals(
        toJson(
            "{'contentRepresentation':'UNTYPED',"
                + "'attributeName':'productId',"
                + "'oldValue':123,"
                + "'newValue':456}"),
        payload);

    final var decoded = mapper.readValue(payload, ReplaceContextAttributesRequest.class);
    assertEquals(request.getContentRepresentation(), decoded.getContentRepresentation());
    assertEquals(request.getAttributeName(), decoded.getAttributeName());
    assertEquals(request.getOldValue(), decoded.getOldValue());
    assertEquals(request.getNewValue(), decoded.getNewValue());
  }

  private String toJson(String src) {
    return src.replace("'", "\"");
  }

  private AttributeConfig aconf(String name, AttributeType type) {
    final var config = new AttributeConfig();
    config.setName(name);
    config.setType(type);
    return config;
  }
}
