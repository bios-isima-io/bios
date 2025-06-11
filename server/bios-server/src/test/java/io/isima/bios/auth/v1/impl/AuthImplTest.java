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
package io.isima.bios.auth.v1.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.auth.v1.AuthV1;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.AuthError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.AppType;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthImplTest {

  private static AuthV1 auth;
  protected static Executor executor;

  @BeforeClass
  public static void setUpClass() {
    System.setProperty(TfosConfig.AUTH_TOKEN_SECRET, "U53-R3A11Y-L0NG-K3Y-H3R3");
    auth = new AuthV1Impl();
    executor =
        Executors.newCachedThreadPool(
            ExecutorManager.makeThreadFactory("test-service", Thread.NORM_PRIORITY));
  }

  @Test
  public void testAuthToken() throws Exception {
    long currentTime = System.currentTimeMillis();
    long expiryTime = currentTime + 10000;
    Long userId = 100L;
    Long orgId = 10L;
    String subject = "admin@isima.io";
    String tenant = "authTestTenant";
    List<Integer> permissions = Arrays.asList(Permission.ADMIN.getId());

    UserContext userContext1 =
        new UserContext(userId, orgId, subject, tenant, permissions, "test", AppType.UNKNOWN);
    String validToken = auth.createToken(currentTime, expiryTime, userContext1);

    assertNotNull(validToken);

    // test valid token
    UserContext userContext2 =
        auth.parseToken(validToken, false, new GenericExecutionState("ParseToken", executor)).get();

    assertNotNull(userContext2);
    assertEquals(userId, userContext2.getUserId());
    assertEquals(orgId, userContext2.getOrgId());
    assertEquals(subject, userContext2.getSubject());
    assertEquals(tenant, userContext2.getTenant());
    assertEquals(permissions, userContext2.getPermissionIds());

    try {
      // test invalid token
      auth.parseToken("a342516g65738290", false, new GenericExecutionState("ParseToken", executor))
          .get();
      fail("token parser failed - invalid token parsed as valid");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof TfosException);
      assertEquals("Invalid token", e.getCause().getMessage());
    }
  }

  @Test
  public void testExpiredAuthToken() {
    final String token =
        "ZXlKaGJHY2lPaUpJVXpVeE1pSjkuZXlKemRXSWlPaUpoWkdsMGVXRXVhMkZ5WVc1a2FXdGhj"
            + "a0J3YUdGeWJXVmhjM2t1YVc0aUxDSnBZWFFpT2pFMk1EQXdPVFl4T0Rrc0ltNWlaaUk2TV"
            + "RZd01EQTVOakU0T1N3aVpYaHdJam94TmpBd01UTXlNVGc1TENKMFpXNWhiblFpT2lKd2FH"
            + "RnliV1ZoYzNraUxDSnpZMjl3WlNJNklpOTBaVzVoYm5SekwzQm9ZWEp0WldGemVTOGlMQ0"
            + "oxYzJWeUlqb3hOVGcxTmpJNU56VXlNelV3TENKdmNtY2lPakUwT1RVME5Ua3hNRGsxTmpr"
            + "ek1ESXNJbkJsY20waU9pSTBMRFlpZlEueGl2Wmp1UmpqWDJkMTlYRTJ5Vi1acnBKaFlUOW"
            + "hrMnhCd195dUU0UHBBR2xpem5xUnc1Y0RycDNyMXNNR0VzUGtpYVJxTThNQmNkanNPYndLVFE5Wmc=";
    try {
      auth.parseToken(token, false, new GenericExecutionState("ParseToken", executor)).get();
      fail("exception is expected");
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      assertTrue(cause instanceof TfosException);
      assertEquals(
          AuthError.SESSION_EXPIRED.getErrorCode(), ((TfosException) cause).getErrorCode());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSignupToken() {
    long currentTime = System.currentTimeMillis();
    long expiryTime = currentTime + 10000;
    String email = "abc@xyz.com";
    String signupToken = auth.createSignupToken(currentTime, expiryTime, null, email);
    assertNotNull(signupToken);

    try {
      String data = auth.parseSignupToken(signupToken).getSubject();

      assertNotNull(data);
      assertEquals(email, data);
    } catch (TfosException e) {
      fail("token parsing failed");
    }
  }

  @Test
  public void testTokenValidation() {
    long currentTime = System.currentTimeMillis();
    long expiryTime = currentTime + 10000;
    Long userId = 100L;
    Long orgId = 10L;
    String subject = "admin@isima.io";
    String tenant = "authTestTenant";

    // user have single permission
    List<Integer> userPermissions = Arrays.asList(Permission.BI_REPORT.getId());

    UserContext userContext1 =
        new UserContext(
            userId,
            orgId,
            subject,
            tenant,
            userPermissions,
            "tokenValidationTest",
            AppType.REALTIME);
    String authToken = auth.createToken(currentTime, expiryTime, userContext1);

    assertNotNull(authToken);

    boolean result = validateTokenInternal(authToken, "", Permission.BI_REPORT);
    assertTrue(result);

    result = validateTokenInternal(authToken, "", Permission.INGEST);
    assertFalse(result);

    result = validateTokenInternal(authToken, "", Permission.EXTRACT);
    assertFalse(result);

    result =
        validateTokenInternal(authToken, "", Arrays.asList(Permission.BI_REPORT, Permission.ADMIN));
    assertTrue(result);

    result =
        validateTokenInternal(
            authToken, "", Arrays.asList(Permission.BI_REPORT, Permission.DEVELOPER));
    assertTrue(result);

    result =
        validateTokenInternal(authToken, "", Arrays.asList(Permission.INGEST, Permission.ADMIN));
    assertFalse(result);

    result =
        validateTokenInternal(
            authToken, "", Arrays.asList(Permission.INGEST, Permission.DEVELOPER));
    assertFalse(result);

    result =
        validateTokenInternal(authToken, "", Arrays.asList(Permission.DEVELOPER, Permission.ADMIN));
    assertFalse(result);

    result = validateTokenInternal(authToken, "", Arrays.asList());
    assertTrue(result);

    // user have multiple permission
    userPermissions = Arrays.asList(Permission.BI_REPORT.getId(), Permission.INGEST.getId());

    userContext1 =
        new UserContext(
            userId, orgId, subject, tenant, userPermissions, "biOSTest", AppType.UNKNOWN);
    authToken = auth.createToken(currentTime, expiryTime, userContext1);

    assertNotNull(authToken);

    result = validateTokenInternal(authToken, "", Permission.BI_REPORT);
    assertTrue(result);

    result = validateTokenInternal(authToken, "", Permission.INGEST);
    assertTrue(result);

    result = validateTokenInternal(authToken, "", Permission.EXTRACT);
    assertFalse(result);

    result =
        validateTokenInternal(authToken, "", Arrays.asList(Permission.BI_REPORT, Permission.ADMIN));
    assertTrue(result);

    result =
        validateTokenInternal(
            authToken, "", Arrays.asList(Permission.BI_REPORT, Permission.DEVELOPER));
    assertTrue(result);

    result =
        validateTokenInternal(authToken, "", Arrays.asList(Permission.INGEST, Permission.ADMIN));
    assertTrue(result);

    result =
        validateTokenInternal(
            authToken, "", Arrays.asList(Permission.INGEST, Permission.DEVELOPER));
    assertTrue(result);

    result =
        validateTokenInternal(authToken, "", Arrays.asList(Permission.DEVELOPER, Permission.ADMIN));
    assertFalse(result);

    result = validateTokenInternal(authToken, "", Arrays.asList());
    assertTrue(result);
  }

  private boolean validateTokenInternal(String authToken, String scope, Permission permission) {
    try {
      auth.validateToken(authToken, false, scope, permission);
    } catch (UserAccessControlException e) {
      return false;
    }
    return true;
  }

  private boolean validateTokenInternal(
      String authToken, String scope, List<Permission> permissions) {
    try {
      auth.validateToken(authToken, false, scope, permissions);
    } catch (UserAccessControlException e) {
      return false;
    }
    return true;
  }
}
