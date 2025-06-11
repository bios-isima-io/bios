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
import static org.junit.Assert.fail;

import io.isima.bios.auth.v1.AuthV1;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AuthTokenValidationTest {
  private static AuthV1 authV1;
  private static Auth auth;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(AuthTokenValidationTest.class);
    authV1 = BiosModules.getAuthV1();
    auth = BiosModules.getAuth();
  }

  @AfterClass
  public static void tearDownClass() {
    Bios2TestModules.shutdown();
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // Regular user
          {"regular user, unknown tenant", "isima", "subject", "", null, null, true},
          {"regular user, matching tenant", "isima", "subject", "isima", null, null, true},
          {
            "regular user, unmatching tenant", "isima", "subject", "elasticflash", null, null, false
          },
          {
            "regular user, matching tenant, matching permissions",
            "isima",
            "subject",
            "isima",
            Arrays.asList(Permission.ADMIN),
            Arrays.asList(Permission.ADMIN),
            true
          },
          {
            "regular user, matching tenant, unmatching permissions",
            "isima",
            "subject",
            "isima",
            Arrays.asList(Permission.INGEST),
            Arrays.asList(Permission.ADMIN),
            false
          },
          // System admin
          {"system admin, unknown tenant", "/", "subject", "", null, null, true},
          {"system admin, system tenant", "_system", "subject", "_system", null, null, true},
          // TODO(BB-944): Disabled for a known issue
          /*
           * {"system admin, regular tenant",
           * "/", "subject", "elasticflash", null, null, false},
           */
          {
            "system admin, system tenant, matching permissions",
            "_system",
            "subject",
            "_system",
            Arrays.asList(Permission.SUPERADMIN),
            Arrays.asList(Permission.SUPERADMIN),
            true
          },
          // TODO(BB-944): Disabled for a known issue
          /*
           * {"system admin, system tenant, admin operation",
           * "/", "subject", "_system", Arrays.asList(Permission.SUPERADMIN),
           * Arrays.asList(Permission.ADMIN), true},
           */
          {
            "system admin, matching tenant, matching permissions",
            "/",
            "subject",
            "isima",
            Arrays.asList(Permission.SUPERADMIN),
            Arrays.asList(Permission.ADMIN),
            false
          },
        });
  }

  @Parameter public String description;

  @Parameter(1)
  public String tenantName;

  @Parameter(2)
  public String subject;

  @Parameter(3)
  public String testTenantName;

  @Parameter(4)
  public List<Permission> userPermissions;

  @Parameter(5)
  public List<Permission> requiredPermissions;

  @Parameter(6)
  public boolean acceptanceExcpected;

  @Test
  public void test() throws Exception {
    var userContext = new UserContext();
    userContext.setTenant(tenantName);
    userContext.setSubject(subject);
    if (userPermissions != null) {
      userContext.setPermissions(
          userPermissions.stream().map(perm -> perm.getId()).collect(Collectors.toList()));
    }
    final long currentTime = System.currentTimeMillis();
    final long expirationTime = currentTime + 3600000;
    var biosToken = auth.createToken(currentTime, expirationTime, userContext);
    try {
      var retrievedContext =
          auth.validateSessionToken(
              new SessionToken(biosToken, false), testTenantName, requiredPermissions);
      if (acceptanceExcpected) {
        assertEquals(description, userContext.getTenant(), retrievedContext.getTenant());
        assertEquals(description, userContext.getSubject(), retrievedContext.getSubject());
      } else {
        fail("rejection expected: " + description);
      }
    } catch (UserAccessControlException e) {
      if (acceptanceExcpected) {
        fail("acceptance expected: " + description);
      }
    }
  }
}
