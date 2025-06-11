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
package io.isima.bios.sdk.csdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CsdkDirectTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testLoading() {
    CSdkDirect csdk = new CSdkDirect();
    assertTrue(csdk.isInitialized());
  }

  @Test
  public void testBufferExchange() {
    CSdkDirect csdk = new CSdkDirect();
    ByteBuffer buffer = csdk.allocateBuffer(256);
    assertNotNull(buffer);
    assertEquals(256, buffer.capacity());

    csdk.writeHello(buffer);
    assertEquals(11, buffer.limit());
    byte[] data = new byte[buffer.limit()];
    buffer.get(data);
    assertEquals("hello world", new String(data));

    csdk.releasePayload(buffer);
  }
}
