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
package io.isima.bios.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.uuid.Generators;
import java.util.UUID;
import org.junit.Test;

/**
 * BIOS generic utils class test.
 *
 * <p>The class to test is in the common package. It's unnatural that the test case is in the server
 * module, but we use Cassandra Driver 3 library as the provider of "right answers" of UUID
 * timestamp retrieval.
 */
public class UtilsTest {

  @Test
  public void testUuidTimestamp() {
    for (int i = 0; i < 20; ++i) {
      UUID sourceUuid = Generators.timeBasedGenerator().generate();
      long ourTimestamp = Utils.uuidV1TimestampInMillis(sourceUuid);
      long reference = UUIDs.unixTimestamp(sourceUuid);
      assertEquals(reference, ourTimestamp);
      long ourTimestampInMicros = Utils.uuidV1TimestampInMicros(sourceUuid);
      assertEquals(reference, ourTimestampInMicros / 1000);
    }
  }

  @Test
  public void testGettingNodeName() {
    final String nodeName = Utils.getNodeName();
    System.out.println(nodeName);
    assertNotNull(nodeName);
    assertFalse(nodeName.isEmpty());
  }
}
