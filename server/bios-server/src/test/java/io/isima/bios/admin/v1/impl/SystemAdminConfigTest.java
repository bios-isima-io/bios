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
package io.isima.bios.admin.v1.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.models.v1.TenantConfig;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SystemAdminConfigTest {
  private SystemAdminConfig config;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws FileReadException {
    config = new SystemAdminConfig();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void test() {
    final Collection<TenantConfig> tenants = config.getTenantConfigList();
    assertNotNull(tenants);
    assertTrue(tenants.size() >= 1);
  }
}
