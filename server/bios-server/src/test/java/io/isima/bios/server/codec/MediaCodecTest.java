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
package io.isima.bios.server.codec;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.proto.DataProto.ContentRepresentation;
import io.isima.bios.models.proto.DataProto.InsertRequest;
import io.isima.bios.models.proto.DataProto.Record;
import io.netty.buffer.Unpooled;
import java.nio.charset.Charset;
import javax.ws.rs.core.Response.Status;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MediaCodecTest {

  private static Charset utf8;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utf8 = Charset.forName("UTF-8");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testJson() throws Exception {
    final var codec = new JsonPayloadCodec();

    final var src = new TenantConfig("testTenant");
    final var payload = Unpooled.buffer();

    // encoding test
    codec.encode(src, payload);
    final var json = payload.toString(0, payload.readableBytes(), utf8);
    assertThat(json, is("{'tenantName':'testTenant'}".replace("'", "\"")));

    // decoding test
    final var restored = codec.decode(payload, TenantConfig.class);
    assertThat(restored.getName(), is(src.getName()));
  }

  @Test
  public void testDecodeMalformedJson() throws Exception {
    final var codec = new JsonPayloadCodec();

    final var json = "{'malformed'}";
    final var payload = Unpooled.buffer();
    payload.writeCharSequence(json, utf8);

    // decoding test
    try {
      codec.decode(payload, TenantConfig.class);
      fail("exception must happen");
    } catch (InvalidRequestException e) {
      assertThat(e.getStatus(), is(Status.BAD_REQUEST));
    }
  }

  @Test
  public void testProtobufCodec() throws Exception {
    final var codec = new ProtobufPayloadCodec();

    final var input = "a,long,time,ago,in,a,galaxy";

    // make a content
    final var record = Record.newBuilder().addStringValues(input).build();
    final var request =
        InsertRequest.newBuilder()
            .setContentRep(ContentRepresentation.CSV)
            .setRecord(record)
            .build();

    // make a payload
    final var payload = Unpooled.buffer();

    // encoding test
    codec.encode(request, payload);
    assertThat(payload.readableBytes(), greaterThan(0));

    // decoding test
    final InsertRequest restored = codec.decode(payload, InsertRequest.class);
    assertNotNull(restored);
    assertThat(restored.getContentRep(), is(ContentRepresentation.CSV));
    assertThat(restored.getRecord().getStringValuesCount(), is(1));
    assertThat(restored.getRecord().getStringValues(0), is(input));
  }

  @Test
  public void testProtobufUnsupportedTypeEncode() throws Exception {
    final var codec = new ProtobufPayloadCodec();
    final var payload = Unpooled.buffer();

    assertThrows(IllegalArgumentException.class, () -> codec.encode(Integer.valueOf(123), payload));
  }

  @Test
  public void testProtobufUnsupportedTypeDecode() throws Exception {
    final var codec = new ProtobufPayloadCodec();

    final var input = "a,long,time,ago,in,a,galaxy";

    // make an encoded payload
    final var record = Record.newBuilder().addStringValues(input).build();
    final var request =
        InsertRequest.newBuilder()
            .setContentRep(ContentRepresentation.CSV)
            .setRecord(record)
            .build();
    final var payload = Unpooled.buffer();
    codec.encode(request, payload);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(payload, TenantConfig.class));
  }

  @Test
  public void testProtobufDecodeMulformedPayload() throws Exception {
    final var codec = new ProtobufPayloadCodec();

    // Make a wrong payload
    final var payload = Unpooled.buffer();
    {
      final var badCodec = new TextPayloadCodec();
      badCodec.encode("Hello world", payload);
    }

    try {
      codec.decode(payload, InsertRequest.class);
      fail("exception must happen");
    } catch (InvalidRequestException e) {
      assertThat(e.getStatus(), is(Status.BAD_REQUEST));
      assertThat(
          e.getMessage(),
          is(
              "Invalid request: Malformed protobuf payload:"
                  + " Protocol message end-group tag did not match expected tag."));
    }
  }

  @Test
  public void testTextPlain() throws Exception {
    final var codec = new TextPayloadCodec();

    final var src = "<html><body>Hello World</body></html>";
    final var payload = Unpooled.buffer();

    // encoding test
    codec.encode(src, payload);
    assertEquals(src, payload.toString(0, payload.readableBytes(), utf8));

    // decoding test
    final var restored = codec.decode(payload, String.class);
    assertEquals(src, restored);
  }

  @Test
  public void testTextPlainIllegalDecoding() throws Exception {
    final var codec = new TextPayloadCodec();

    final var src = "<html><body>Hello World</body></html>";
    final var payload = Unpooled.buffer();

    // encoding test
    codec.encode(src, payload);

    // decoding test
    assertThrows(IllegalArgumentException.class, () -> codec.decode(payload, Integer.class));
  }
}
