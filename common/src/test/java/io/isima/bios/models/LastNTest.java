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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class LastNTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testSerializer() throws Exception {
    final LastN lastN = new LastN(List.of(new LastNItem(123L, 456L), new LastNItem(456L, 987L)));
    final var out = objectMapper.writeValueAsString(lastN);
    assertEquals("{'c':[{'t':123,'v':456},{'t':456,'v':987}]}".replace("'", "\""), out);
  }

  @Test
  public void testDeserializer() throws Exception {
    final LastN lastN = new LastN(List.of(new LastNItem(123L, 456L), new LastNItem(456L, 987L)));
    final String src = "{'c':[{'t':123,'v':456},{'t':456,'v':987}]}".replace("'", "\"");
    final var decoded = objectMapper.readValue(src, LastN.class);
    assertEquals(2, decoded.getCollection().size());
    assertEquals(Long.valueOf(123), decoded.getCollection().get(0).getTimestamp());
    assertEquals(Long.valueOf(456), decoded.getCollection().get(0).getValue());
    assertEquals(Long.valueOf(456), decoded.getCollection().get(1).getTimestamp());
    assertEquals(Long.valueOf(987), decoded.getCollection().get(1).getValue());
  }
}
