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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.TableMetadata;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.CassTenant;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataEngineTest {
  private static DataEngineImpl dataEngine;
  private static CassandraConnection conn;
  private static AdminImpl admin;

  private List<TenantDesc> testTenants = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(DataEngineTest.class);
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    conn = BiosModules.getCassandraConnection();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    testTenants.clear();
  }

  @After
  public void tearDown() throws Exception {
    for (TenantDesc tenant : testTenants) {
      try {
        dataEngine.deleteTenantInternal(tenant, RequestPhase.INITIAL);
        dataEngine.deleteTenantInternal(tenant, RequestPhase.FINAL);
      } catch (Exception e) {
        // ignore error
      }
    }
  }

  @Test
  public void createTenant() throws ApplicationException, NoSuchTenantException {
    String tenantName = "aaaaa";
    long timestamp = System.currentTimeMillis();
    TenantDesc tenantDesc = new TenantDesc(tenantName, timestamp, false);
    testTenants.add(tenantDesc);
    String signal = "signal";
    String context = "context";
    StreamDesc signalStream = new StreamDesc(signal, timestamp);
    signalStream.addAttribute(new AttributeDesc("first", InternalAttributeType.STRING));
    signalStream.setVersion(timestamp);
    tenantDesc.addStream(signalStream);
    StreamDesc contextStream = new StreamDesc(context, timestamp);
    contextStream.setVersion(timestamp);
    contextStream.setType(StreamType.CONTEXT);
    contextStream.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    contextStream.addAttribute(new AttributeDesc("value", InternalAttributeType.STRING));
    contextStream.setPrimaryKey(List.of("key"));
    tenantDesc.addStream(contextStream);

    dataEngine.createTenant(tenantDesc, RequestPhase.INITIAL);

    CassTenant cassTenant = dataEngine.getCassTenant(tenantDesc);
    assertNotNull(cassTenant);

    CassStream cassStream = cassTenant.getCassStream(signalStream);
    assertNotNull(cassStream);
    String signalTableName = cassStream.getTableName();

    String keyspace = cassTenant.getKeyspaceName();
    Cluster cluster = conn.getCluster();
    TableMetadata metadata = cluster.getMetadata().getKeyspace(keyspace).getTable(signalTableName);
    assertNotNull(metadata);

    cassStream = cassTenant.getCassStream(contextStream);
    assertNotNull(cassStream);
    String contextTableName = cassStream.getTableName();
    metadata = cluster.getMetadata().getKeyspace(keyspace).getTable(contextTableName);
    assertNotNull(metadata);

    dataEngine.deleteTenantInternal(tenantDesc, RequestPhase.INITIAL);

    assertNull(dataEngine.getCassTenant(tenantDesc));
    assertNull(cluster.getMetadata().getKeyspace(keyspace).getTable(signalTableName));
    assertNull(cluster.getMetadata().getKeyspace(keyspace).getTable(contextTableName));

    BiosModules.getCassandraConnection().dropKeyspace(keyspace);
  }

  @Test
  public void createStream() throws ApplicationException, NoSuchTenantException {
    String tenantName = "create_stream_test";
    long timestamp = System.currentTimeMillis();
    TenantDesc tenantConfig = new TenantDesc(tenantName, timestamp, false);
    testTenants.add(tenantConfig);
    String context = "context";
    StreamDesc contextStream = new StreamDesc(context, timestamp);
    contextStream.setType(StreamType.CONTEXT);
    contextStream.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    contextStream.addAttribute(new AttributeDesc("value", InternalAttributeType.STRING));
    contextStream.setVersion(timestamp);
    contextStream.setPrimaryKey(List.of("key"));
    tenantConfig.addStream(contextStream);

    dataEngine.createTenant(tenantConfig, RequestPhase.INITIAL);

    String signal = "signal";
    StreamDesc signalStream = new StreamDesc(signal, timestamp);
    signalStream.addAttribute(new AttributeDesc("first", InternalAttributeType.STRING));
    signalStream.setVersion(timestamp);
    signalStream.setParent(tenantConfig);

    dataEngine.createStream(tenantName, signalStream, RequestPhase.INITIAL);

    CassTenant cassTenant = dataEngine.getCassTenant(tenantConfig);
    assertNotNull(cassTenant);

    CassStream cassStream = cassTenant.getCassStream(signalStream);
    assertNotNull(cassStream);
    String signalTableName = cassStream.getTableName();

    String keyspace = cassTenant.getKeyspaceName();
    Cluster cluster = conn.getCluster();
    TableMetadata metadata = cluster.getMetadata().getKeyspace(keyspace).getTable(signalTableName);
    assertNotNull(metadata);

    cassStream = cassTenant.getCassStream(contextStream);
    assertNotNull(cassStream);
    String contextTableName = cassStream.getTableName();
    metadata = cluster.getMetadata().getKeyspace(keyspace).getTable(contextTableName);
    assertNotNull(metadata);

    BiosModules.getCassandraConnection().dropKeyspace(keyspace);
  }
}
