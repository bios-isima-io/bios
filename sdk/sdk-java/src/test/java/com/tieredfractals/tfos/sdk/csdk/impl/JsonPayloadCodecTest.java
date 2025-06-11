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
package com.tieredfractals.tfos.sdk.csdk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.models.Credentials;
import io.isima.bios.sdk.csdk.codec.JsonPayloadCodec;
import io.isima.bios.sdk.csdk.codec.PayloadCodec;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.nio.ByteBuffer;
import org.junit.Test;

public class JsonPayloadCodecTest {

  @Test
  public void test() throws BiosClientException {
    Credentials credentials = new Credentials();
    credentials.setEmail("myuser@mytenant.example.com");
    credentials.setPassword("mypassword");

    // encoding test
    PayloadCodec codec = new JsonPayloadCodec(BiosObjectMapperProvider.get());
    ByteBuffer serialized = codec.encode(credentials);
    assertTrue(serialized.isDirect());
    byte[] content = new byte[serialized.remaining()];
    serialized.get(content);
    String out = new String(content);
    assertEquals("{\"email\":\"myuser@mytenant.example.com\",\"password\":\"mypassword\"}", out);

    // decoding test
    serialized.flip();
    Credentials decoded = codec.decode(serialized, Credentials.class);
    assertEquals("myuser@mytenant.example.com", decoded.getEmail());
    assertEquals("mypassword", decoded.getPassword());
  }

  @Test
  public void testDecodingEmptyStream() throws BiosClientException {
    PayloadCodec codec = new JsonPayloadCodec(BiosObjectMapperProvider.get());
    ByteBuffer input = ByteBuffer.allocateDirect(0);
    assertNull(codec.decode(input, String.class));
  }
}
