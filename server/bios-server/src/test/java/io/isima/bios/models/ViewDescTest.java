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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewDescTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testDeserialization() throws JsonParseException, JsonMappingException, IOException {
    final String input =
        "{"
            + "  'name': 'view_name',"
            + "  'groupBy': ['section', 'subsection'],"
            + "  'attributes': ['price', 'quantity']"
            + "}";
    final ViewDesc desc = mapper.readValue(input.replaceAll("'", "\""), ViewDesc.class);
    assertEquals("view_name", desc.getName());
    assertEquals(Arrays.asList("section", "subsection"), desc.getGroupBy());
    assertEquals(Arrays.asList("price", "quantity"), desc.getAttributes());
  }

  @Test
  public void testEquals() throws Exception {
    final String src =
        "{"
            + "  'name': 'view_name',"
            + "  'groupBy': ['section', 'subsection'],"
            + "  'attributes': ['price', 'quantity']"
            + "}";
    final ViewDesc desc1 = mapper.readValue(src.replaceAll("'", "\""), ViewDesc.class);
    final ViewDesc desc2 = mapper.readValue(src.replaceAll("'", "\""), ViewDesc.class);
    assertTrue(desc1.equals(desc2));
    assertTrue(desc1.equals(desc1));
    {
      final ViewDesc desc3 = desc1.duplicate();
      assertTrue(desc1.equals(desc3));
      desc3.setName("modified_name");
      assertFalse(desc1.equals(desc3));
    }
    {
      final ViewDesc desc3 = desc1.duplicate();
      desc3.setGroupBy(Arrays.asList("chapter", "subchapter"));
      assertFalse(desc1.equals(desc3));
    }
    {
      final ViewDesc desc3 = desc1.duplicate();
      desc3.setGroupBy(null);
      assertFalse(desc1.equals(desc3));
      assertFalse(desc3.equals(desc1));
    }
    {
      final ViewDesc desc3 = desc1.duplicate();
      desc3.setAttributes(Arrays.asList("temperature", "humidity"));
      assertFalse(desc1.equals(desc3));
    }
    {
      final ViewDesc desc3 = desc1.duplicate();
      desc3.setAttributes(null);
      assertFalse(desc1.equals(desc3));
      assertFalse(desc3.equals(desc1));
    }
  }
}
