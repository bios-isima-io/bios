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
package io.isima.bios.qa.quick;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.models.TenantConfig;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import org.junit.BeforeClass;
import org.junit.Test;

public class BiosSessionGetTenantConfigIT {
  private static final String TENANT_NAME = "biosClientE2eTest";

  private static String host;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Test
  public void testIngestUserAccess() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    try (final Session session = starter.connect()) {
      session.getTenant(TENANT_NAME, false, false);
    } catch (BiosClientException e) {
      assertThat(e.getCode(), is(BiosClientError.FORBIDDEN));
      assertThat(e.getMessage(), containsString("Permission denied"));
    }
  }

  @Test
  public void testExtractUserAccess() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("extract@" + TENANT_NAME).password("extract");
    try (final Session session = starter.connect()) {
      session.getTenant(TENANT_NAME, false, false);
    } catch (BiosClientException e) {
      assertThat(e.getCode(), is(BiosClientError.FORBIDDEN));
      assertThat(e.getMessage(), containsString("Permission denied"));
    }
  }

  @Test
  public void testAdminUserAccess() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {
      TenantConfig tenantConfig = session.getTenant(TENANT_NAME, true, true);
      assertEquals(tenantConfig.getName(), TENANT_NAME);
    }
  }
}
