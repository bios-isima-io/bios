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
package io.isima.bios.proto;

import static io.isima.bios.models.proto.DataProto.AttributeType.BLOB;
import static io.isima.bios.models.proto.DataProto.AttributeType.BOOLEAN;
import static io.isima.bios.models.proto.DataProto.AttributeType.DECIMAL;
import static io.isima.bios.models.proto.DataProto.AttributeType.INTEGER;
import static io.isima.bios.models.proto.DataProto.AttributeType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import io.isima.bios.codec.proto.RecordProtobufReader;
import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.exceptions.InvalidDataRetrievalException;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.models.proto.DataProto.AttributeType;
import io.isima.bios.models.proto.DataProto.ColumnDefinition;
import io.isima.bios.models.proto.DataProto.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

public class RecordProtobufReaderTest {
  private static final byte[] BLOB_DATA = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};

  @Test
  public void testSimple() {
    RecordProtobufReader record = buildTestRecord();

    assertThat(record.getAttribute("long").asLong(), is(100L));
    assertThat(record.getAttribute("long").asDouble(), is(100.0));
    assertThat(record.getAttribute("double").asDouble(), is(10.0));
    assertThat(record.getAttribute("double").asLong(), is(10L));
    assertThat(record.getAttribute("double").asString(), is("10.0"));
    assertThat(record.getAttribute("string").asString(), is("hello"));
    assertThat(record.getAttribute("bool").asBoolean(), is(true));
    assertThat(record.getAttribute("blob").asByteArray(), is(BLOB_DATA));
  }

  @Test
  public void testAttributes() {
    RecordProtobufReader record = buildTestRecord();

    for (var rec : record.attributes()) {
      switch (rec.name()) {
        case "long":
          assertThat(rec.asLong(), is(100L));
          assertThat(rec.asDouble(), is(100.0));
          assertThat(rec.asString(), is("100"));
          break;
        case "double":
          assertThat(rec.asDouble(), is(10.0));
          assertThat(rec.asLong(), is(10L));
          assertThat(rec.asString(), is("10.0"));
          break;
        case "string":
          assertThat(rec.asString(), is("hello"));
          break;
        case "bool":
          assertThat(rec.asBoolean(), is(true));
          break;
        case "blob":
          assertThat(rec.asByteArray(), is(BLOB_DATA));
          break;
      }
    }
  }

  @Test
  public void testInvalidConversions() {
    RecordProtobufReader record = buildTestRecord();

    try {
      record.getAttribute("string").asLong();
      fail("Given string is not convertible to long");
    } catch (InvalidDataRetrievalException e) {
      assertThat(e.getMessage(), containsString("Invalid"));
    }

    try {
      record.getAttribute("long").asByteArray();
      fail("Given long is not convertible to byte array");
    } catch (InvalidDataRetrievalException e) {
      assertThat(e.getMessage(), containsString("Invalid"));
    }

    try {
      record.getAttribute("bool").asDouble();
      fail("Given bool is not convertible to double");
    } catch (InvalidDataRetrievalException e) {
      assertThat(e.getMessage(), containsString("Invalid"));
    }
  }

  private static final String[] names = {"long", "double", "string", "bool", "blob"};
  private static final AttributeType[] types = {INTEGER, DECIMAL, STRING, BOOLEAN, BLOB};

  private RecordProtobufReader buildTestRecord() {
    List<ColumnDefinition> definitionList = new ArrayList<>();
    ColumnDefinition.Builder definitionBuilder = ColumnDefinition.newBuilder();
    int idx = 0;
    for (var name : names) {
      definitionList.add(
          definitionBuilder.setName(name).setIndexInValueArray(0).setType(types[idx]).build());
      idx++;
    }

    Record.Builder builder = Record.newBuilder();
    DataProto.Record record =
        builder
            .setEventId(UuidMessageConverter.toProtoUuid(UUID.randomUUID()))
            .setTimestamp(System.currentTimeMillis())
            .addLongValues(100L)
            .addDoubleValues(10.0)
            .addStringValues("hello")
            .addBooleanValues(true)
            .addBlobValues(ByteString.copyFrom(BLOB_DATA))
            .build();

    return new RecordProtobufReader(record, definitionList);
  }
}
