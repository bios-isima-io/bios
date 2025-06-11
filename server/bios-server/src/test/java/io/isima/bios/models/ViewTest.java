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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.v1.Group;
import java.io.IOException;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewTest {

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = new ObjectMapper();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testSingleDimension() throws IOException {
    final View view = new Group("groupBy");

    final String serialized = mapper.writeValueAsString(view);

    final View decoded = mapper.readValue(serialized, View.class);

    assertEquals(ViewFunction.GROUP, decoded.getFunction());
    assertEquals(Arrays.asList("groupBy"), decoded.getDimensions());
    assertNull(decoded.getBy());
  }

  @Test
  public void testMultiDimension() throws IOException {
    final View view = new Group("Hello", "World");

    final String serialized = mapper.writeValueAsString(view);

    final View decoded = mapper.readValue(serialized, View.class);

    assertEquals(ViewFunction.GROUP, decoded.getFunction());
    assertNotNull(decoded.getDimensions());
    assertEquals(2, decoded.getDimensions().size());
    assertEquals("Hello", decoded.getDimensions().get(0));
    assertEquals("World", decoded.getDimensions().get(1));
    assertNull(decoded.getBy());
  }
}
