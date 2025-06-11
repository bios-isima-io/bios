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
package io.isima.bios.auth.v1;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.AuthenticationException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.LoginResponse;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.Credentials;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.user.TfosUserManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthTest {

  private static AuthV1 auth;
  private static TfosUserManager userManager;

  List<TenantDesc> configs = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.setProperty(
        TfosConfig.INITIAL_USERS_FILE, "../server/src/test/resources/initial_test_users.json");
    Bios2TestModules.startModulesWithoutMaintenance(AuthTest.class);
    auth = BiosModules.getAuthV1();
    userManager = (TfosUserManager) BiosModules.getUserManager();
  }

  @AfterClass
  public static void tearDownClass() {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() {
    configs.clear();
  }

  @After
  public void tearDown() throws NoSuchTenantException, ApplicationException {
    for (TenantDesc tenantDesc : configs) {
      userManager.deleteTenant(tenantDesc, RequestPhase.INITIAL);
    }
  }

  @Test
  public void test()
      throws ApplicationException, InterruptedException, ExecutionException, TfosException {
    String tenantName1 = "authTestLonger";
    String scope1 = String.format(AuthV1.SCOPE_TENANT, tenantName1);
    TenantDesc tenantConfig = new TenantDesc(tenantName1, System.currentTimeMillis(), false);
    configs.add(tenantConfig);

    final var state = new GenericExecutionState("test", Executors.newSingleThreadExecutor());

    Credentials credSAdmin = new Credentials("superadmin", "superadmin");
    LoginResponse resp = auth.login(credSAdmin, 0, null, state).get();
    String token = resp.getToken();
    auth.validateToken(token, false, AuthV1.SCOPE_SYSTEM, Permission.SUPERADMIN);

    Credentials credAdmin = new Credentials("admin@" + tenantName1, "admin", scope1);
    auth.login(credAdmin, 0, null, state);

    try {
      auth.login(null, 0, null, state).get();
      fail("exception is expected");
    } catch (ExecutionException e) {
      // expected cause
      assertTrue(e.getCause() instanceof AuthenticationException);
    }

    userManager.createTenant(tenantConfig, RequestPhase.INITIAL);
    LoginResponse respAdmin = auth.login(credAdmin, 0, null, state).get();
    String tokenAdmin = respAdmin.getToken();
    auth.validateToken(tokenAdmin, false, scope1, Permission.ADMIN);

    String tenantName2 = "authTest";
    String scope2 = String.format(AuthV1.SCOPE_TENANT, tenantName2);
    try {
      auth.validateToken(tokenAdmin, false, scope2, Permission.ADMIN);
      fail("UserAccessControlException is expected be thrown");
    } catch (UserAccessControlException e) {
      // expected
    }

    // login is not possible after deleting the tenant
    userManager.deleteTenant(tenantConfig, RequestPhase.INITIAL);
    configs.clear();

    auth.login(credAdmin, 0, null, state);
  }

  @Test
  public void testRenewSessionToken()
      throws InterruptedException, ExecutionException, TfosException {
    Credentials credentials = new Credentials("superadmin", "superadmin");
    final var state = new GenericExecutionState("test", Executors.newSingleThreadExecutor());
    LoginResponse loginResponse = auth.login(credentials, 0, null, state).get();
    assertNotNull(loginResponse);

    LoginResponse renewResponse =
        (LoginResponse)
            auth.renewSessionToken(BiosConstants.TOKEN_SEPARATOR + loginResponse.getToken(), 0L);
    assertNotNull(renewResponse);
  }

  @Test
  public void testRenewSessionTokenBadToken()
      throws ApplicationException, InterruptedException, ExecutionException, TfosException {
    Credentials credentials = new Credentials("superadmin", "superadmin");
    final var state = new GenericExecutionState("test", Executors.newSingleThreadExecutor());
    LoginResponse loginResponse = auth.login(credentials, 0, null, state).get();
    assertNotNull(loginResponse);

    try {
      LoginResponse renewResponse =
          (LoginResponse) auth.renewSessionToken(loginResponse.getToken(), 0L);
    } catch (AuthenticationException ae) {
      assertTrue(ae instanceof AuthenticationException);
    }
  }
}
