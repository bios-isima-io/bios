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
package io.isima.bios.models.v1;

import static org.junit.Assert.assertEquals;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.control.MockAdmin;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamConfigTest {

  private static AdminInternal tenantManager;

  // private static StreamConfig streamConfig;

  @BeforeClass
  public static void setUpClass() throws FileReadException {
    tenantManager = new MockAdmin();
  }

  @Test
  public void testGetEventAttributes() throws NoSuchStreamException, NoSuchTenantException {

    StreamConfig streamConfig = tenantManager.getStream("elasticflash", "hello");
    assertEquals("hello", streamConfig.getName());

    List<AttributeDesc> eventAttributes = streamConfig.getAttributes();
    assertEquals("first", eventAttributes.get(0).getName());
    assertEquals(InternalAttributeType.STRING, eventAttributes.get(0).getAttributeType());
    assertEquals("first", eventAttributes.get(0).getDefaultValue());

    assertEquals("second", eventAttributes.get(1).getName());
    assertEquals(InternalAttributeType.STRING, eventAttributes.get(1).getAttributeType());
    assertEquals("second", eventAttributes.get(1).getDefaultValue());
  }

  @Test
  public void testGetAdditionalEventAttributes()
      throws NoSuchStreamException, NoSuchTenantException {

    StreamConfig streamConfig = tenantManager.getStream("elasticflash", "hello");

    List<AttributeDesc> additionalEventAttributes = streamConfig.getAdditionalAttributes();
    assertEquals("city", additionalEventAttributes.get(0).getName());
    assertEquals(InternalAttributeType.STRING, additionalEventAttributes.get(0).getAttributeType());
    assertEquals("Fremont", additionalEventAttributes.get(0).getDefaultValue());

    assertEquals("state", additionalEventAttributes.get(1).getName());
    assertEquals(InternalAttributeType.STRING, additionalEventAttributes.get(1).getAttributeType());
    assertEquals("California", additionalEventAttributes.get(1).getDefaultValue());

    assertEquals("country", additionalEventAttributes.get(2).getName());
    assertEquals(InternalAttributeType.STRING, additionalEventAttributes.get(2).getAttributeType());
    assertEquals("US", additionalEventAttributes.get(2).getDefaultValue());
  }
}
