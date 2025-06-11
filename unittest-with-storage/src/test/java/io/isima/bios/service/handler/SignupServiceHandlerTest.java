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
package io.isima.bios.service.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.isima.bios.common.BiosConstants;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.dto.SignupRequest;
import io.isima.bios.errors.SignupError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.MemberStatus;
import io.isima.bios.models.Role;
import io.isima.bios.models.UserConfig;
import java.util.List;
import java.util.concurrent.Executors;
import javax.ws.rs.core.Response.Status;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignupServiceHandlerTest {
  private static final Logger logger = LoggerFactory.getLogger(SignupServiceHandlerTest.class);

  private static SignupServiceHandler handler;
  private static final String validEmail = "demo@isima.io";
  private static final String existingTenantName = "tieredfractals";
  private static final String existingUserEmail = "ingest@tieredfractals.com";
  private static final ExecutionState state =
      new GenericExecutionState("test", Executors.newSingleThreadExecutor());

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    logger.info("setUpBeforeClass() started");
    // Bios2TestModules.setProperty("io.isima.bios.mail.disabled", "true");
    Bios2TestModules.startModulesWithoutMaintenance(SignupServiceHandlerTest.class);

    // partially mocked sign up service handler.
    handler = BiosModules.getSignUpServiceHandler();

    TestUtils.upsertContextEntries(
        BiosModules.getDataServiceHandler(),
        BiosModules.getHttpClientManager(),
        BiosConstants.TENANT_SYSTEM,
        BiosConstants.STREAM_EMAIL_DOMAINS,
        List.of("yahoo.com,Reject"));

    AdminTestUtils.deleteTenantIgnoreError(BiosModules.getAdminInternal(), existingTenantName);
    AdminTestUtils.addTenant(BiosModules.getAdminInternal(), existingTenantName);
    final var user = new UserConfig();
    user.setEmail(existingUserEmail);
    user.setTenantName(existingTenantName);
    user.setStatus(MemberStatus.ACTIVE);
    user.setRoles(List.of(Role.INGEST));
    user.setPassword("stringPassword");
    DataUtils.wait(BiosModules.getUserManager().createUserAsync(user, state), "createUser");
    logger.info("setUpBeforeClass() done");
  }

  @Test
  public void testInitiateSignupEmailMissing() {
    logger.info("testInitiateSignupEmailMissing() started");
    final var signupInfo = new SignupRequest();
    final var e =
        assertThrows(
            TfosException.class,
            () -> DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup"));
    assertEquals(Status.BAD_REQUEST, e.getStatus());
    assertEquals(SignupError.INVALID_EMAIL_ADDRESS.getStatus(), e.getStatus());
    logger.info("testInitiateSignupEmailMissing() done");
  }

  @Test
  public void testInitiateSignupInvalidEmailAddress() {
    logger.info("testInitiateSignupInvalidEmailAddress() started");
    final var signupInfo = new SignupRequest("dummy");
    final var e =
        assertThrows(
            TfosException.class,
            () -> DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup"));
    assertEquals(Status.BAD_REQUEST, e.getStatus());
    assertEquals(SignupError.INVALID_EMAIL_ADDRESS.getStatus(), e.getStatus());
    logger.info("testInitiateSignupInvalidEmailAddress() done");
  }

  @Test
  public void testInitiateSignupProhibitedEmailDomain() {
    logger.info("testInitiateSignupProhibitedEmailDomain() started");
    final var signupInfo = new SignupRequest("dummy@yahoo.com");
    final var e =
        assertThrows(
            TfosException.class,
            () -> DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup"));
    assertEquals(Status.BAD_REQUEST, e.getStatus());
    assertEquals(SignupError.INVALID_EMAIL_DOMAIN.getStatus(), e.getStatus());
    logger.info("testInitiateSignupProhibitedEmailDomain() done");
  }

  @Test
  public void testInitiateSignupProhibitedEmailDomainWithSubdomain() {
    logger.info("testInitiateSignupProhibitedEmailDomainWithSubdomain() started");
    final var signupInfo = new SignupRequest("dummy@subdomain.yahoo.com");
    final var e =
        assertThrows(
            TfosException.class,
            () -> DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup"));
    assertEquals(Status.BAD_REQUEST, e.getStatus());
    assertEquals(SignupError.INVALID_EMAIL_DOMAIN.getStatus(), e.getStatus());
    logger.info("testInitiateSignupProhibitedEmailDomainWithSubdomain() done");
  }

  @Test
  public void testInitiateSignupWithValidUser() throws Exception {
    logger.info("testInitiateSignupWithValidUser() started");
    final var signupInfo = new SignupRequest(validEmail);
    DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup");
    logger.info("testInitiateSignupWithValidUser() done");
  }

  @Test
  public void testInitiateSignupWithExistingUser() throws Exception {
    logger.info("testInitiateSignupWithExistingUser() started");
    final var signupInfo = new SignupRequest(existingUserEmail);
    DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup");
    logger.info("testInitiateSignupWithExistingUser() done");
  }

  @Test
  public void testInitiateSignupToWrongDomainWithExistingUser() {
    logger.info("testInitiateSignupToWrongDomainWithExistingUser() started");
    final var signupInfo = new SignupRequest(existingUserEmail);
    signupInfo.setDomain("example.com");
    signupInfo.setService("Analytics");
    signupInfo.setSource("Wordpress");
    final var e =
        assertThrows(
            TfosException.class,
            () -> DataUtils.wait(handler.initiateSignup(signupInfo, state), "signup"));
    assertEquals(Status.FORBIDDEN, e.getStatus());
    assertEquals(SignupError.INVALID_DOMAIN.getStatus(), e.getStatus());
    logger.info("testInitiateSignupToWrongDomainWithExistingUser() done");
  }
}
