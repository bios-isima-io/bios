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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;

import com.datastax.driver.core.TableMetadata;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Checks default TTLs of data tables. */
public class CassandraTablesTtlTest {

  private static AdminInternal admin;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection cassandraConnection;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String contextName;

  /** Sets up all; starts BIOS modules. */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(CassandraTablesTtlTest.class);
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    cassandraConnection = BiosModules.getCassandraConnection();
    tenantName = "tableTtlCheck";
    contextName = "testContext";

    tenantConfig = new TenantConfig(tenantName);
    long timestamp = System.currentTimeMillis();
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (TfosException e) {
      // ignore
    }
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final var context = new StreamConfig(contextName, StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.STRING));
    context.setVersion(++timestamp);

    admin.addStream(tenantName, context, RequestPhase.INITIAL);
    admin.addStream(tenantName, context, RequestPhase.FINAL);
  }

  /** Tears down all; shuts down BIOS modules. */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      long timestamp = System.currentTimeMillis();
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (TfosException e) {
      // ignore
    }
    Bios2TestModules.shutdown();
  }

  @Test
  public void test() throws Exception {
    final var tenantDesc = admin.getTenant(tenantName);
    final var cassTenant = dataEngine.getCassTenant(tenantDesc);
    final var keyspaceName = cassTenant.getKeyspaceName();

    // check signal table TTL
    final var signalDesc = tenantDesc.getStream("_usage");
    final var signalCassStream = dataEngine.getCassStream(signalDesc);
    final var signalTableName = signalCassStream.getTableName();
    assertThat(getTtl(keyspaceName, signalTableName), is(TfosConfig.signalDefaultTimeToLive()));

    // check sketches table TTL
    assertThat(
        getTtl(keyspaceName, "sketch_summary"), is(TfosConfig.featureRecordsDefaultTimeToLive()));

    // check rollup table TTL
    final var rollupCassStream =
        dataEngine.getCassStream(tenantDesc.getStream("_usage.rollup.byAllDimensions"));
    assertNotNull(rollupCassStream);
    assertThat(
        getTtl(keyspaceName, rollupCassStream.getTableName()),
        is(TfosConfig.featureRecordsDefaultTimeToLive()));

    // check context table TTL
    final var contextDesc = tenantDesc.getStream(contextName);
    final var contextCassStream = dataEngine.getCassStream(contextDesc);
    final var contextTableName = contextCassStream.getTableName();
    assertThat(getTtl(keyspaceName, contextTableName), is(0));
  }

  private static int getTtl(String keyspace, String table) throws Exception {
    return getTableMetadata(keyspace, table).getOptions().getDefaultTimeToLive();
  }

  private static TableMetadata getTableMetadata(String keyspace, String table) throws Exception {
    final var keyspaceMetadata =
        cassandraConnection.getCluster().getMetadata().getKeyspace(keyspace);
    if (keyspaceMetadata == null) {
      throw new Exception("keyspace " + keyspace + " not found");
    }
    return keyspaceMetadata.getTable(table);
  }
}
