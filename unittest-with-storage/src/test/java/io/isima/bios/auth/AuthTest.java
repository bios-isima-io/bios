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
package io.isima.bios.auth;

import static org.junit.Assert.assertEquals;

import io.isima.bios.auth.v1.AuthV1;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.UserContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthTest {
  private static AuthV1 authV1;
  private static Auth auth;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(AuthTest.class);
    authV1 = BiosModules.getAuthV1();
    auth = BiosModules.getAuth();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Test
  public void tokenCompatibilityTest() {
    var userContext = new UserContext();
    userContext.setTenant("isima");
    userContext.setSubject("subject");
    final long currentTime = System.currentTimeMillis();
    final long expirationTime = currentTime + 3600000;
    var tfosToken = authV1.createToken(currentTime, expirationTime, userContext);
    var biosToken = auth.createToken(currentTime, expirationTime, userContext);
    assertEquals(tfosToken, biosToken);
  }
}
