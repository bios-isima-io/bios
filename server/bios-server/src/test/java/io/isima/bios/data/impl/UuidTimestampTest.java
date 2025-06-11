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
package io.isima.bios.data.impl;

import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.uuid.Generators;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UuidTimestampTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void test() throws InterruptedException {
    long prev = UUIDs.unixTimestamp(Generators.timeBasedGenerator().generate());
    for (int i = 0; i < 1000; ++i) {
      Thread.sleep(1, 200000);
      final UUID uuid = Generators.timeBasedGenerator().generate();
      final long current = UUIDs.unixTimestamp(uuid);
      assertTrue(current > prev);
      prev = current;
    }
  }
}
