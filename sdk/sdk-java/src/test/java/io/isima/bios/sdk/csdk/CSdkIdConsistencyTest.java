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

import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test cases to verify identifier consistency between wrapper and C-SDK.
 *
 * <p>Identifiers to be tested are ones such as operation ID, error code, etc.
 */
public class CSdkIdConsistencyTest {

  CSdkDirect csdk = new CSdkDirect();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  /**
   * Names of enum CSdkOperationId entries and those of CSDK operation ID in the native space must
   * match. This test verifies that Java and native space keep the same names for operation IDs.
   */
  @Test
  public void testOpIds() {
    for (CSdkOperationId id : CSdkOperationId.values()) {
      testOpId(id);
    }
  }

  private void testOpId(CSdkOperationId id) {
    String opName = csdk.getOperationName(id.value());
    assertEquals(id.name(), opName);
  }

  @Test
  public void testStatusCodes() throws BiosClientException {
    final List<BiosClientError> errors = csdk.listStatusCodes();
    for (BiosClientError error : errors) {
      String statusName = csdk.getStatusName(error.getErrorNumber());
      assertEquals(error.name(), statusName);
    }
  }
}
