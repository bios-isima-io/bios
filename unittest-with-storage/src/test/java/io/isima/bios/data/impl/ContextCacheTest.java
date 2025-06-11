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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.ContextMetricsCounter;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextCacheTest {

  private static long timestamp;
  private static AdminInternal admin;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection conn;
  private static SharedProperties sharedProperties;
  private static DataServiceHandler dataServiceHandler;
  private static HttpClientManager clientManager;

  private String previousCacheSize;

  // Tenants used for tests; This list is used for DB cleanup after test.
  private final List<TenantDesc> testTenants = new ArrayList<>();

  // Async process executor
  ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(true, ContextCacheTest.class, Map.of());
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    timestamp = System.currentTimeMillis();
    conn = BiosModules.getCassandraConnection();
    sharedProperties = BiosModules.getSharedProperties();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    clientManager = BiosModules.getHttpClientManager();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    sharedProperties.deleteProperty(DataEngineImpl.PROP_CACHE_ONLY_CONTEXTS);
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    testTenants.clear();
  }

  @After
  public void tearDown() throws Exception {
    for (TenantDesc tenantDesc : testTenants) {
      final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
      try {
        admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantDesc.getVersion());
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
      try {
        admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantDesc.getVersion());
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
    }
  }

  @Test
  public void testBasic() throws Exception {

    final String origCacheOnlyContexts =
        sharedProperties.get(DataEngineImpl.PROP_CACHE_ONLY_CONTEXTS);
    final String origCacheMaxNumEntries =
        sharedProperties.get(ContextCassStream.PROPERTY_CONTEXT_CACHE_SIZE);
    try {
      String tenantName = "contextCacheTestTenant";
      final String contextName = "context";

      // narrow maximum number of cache entries
      sharedProperties.setProperty(
          ContextCassStream.PROPERTY_CONTEXT_CACHE_SIZE, tenantName + "." + contextName + ":30000");

      final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
      metrics.fullReset();

      // build the tenant
      System.out.println("Creating tenant " + tenantName);
      TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
      testTenants.add(tenantDesc);

      final String keyName = "thePrimaryKey";
      final String valueName = "tenTimes";
      StreamDesc context = new StreamDesc(contextName, timestamp);
      context.setType(StreamType.CONTEXT);
      context.addAttribute(new AttributeDesc(keyName, InternalAttributeType.LONG));
      context.addAttribute(new AttributeDesc(valueName, InternalAttributeType.LONG));
      tenantDesc.addStream(context);

      // Add the tenant to AdminInternal
      final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
      admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

      System.out.println("Done creating tenant " + tenantName);

      context = admin.getStream(tenantName, contextName);
      assertNotNull(context);

      final int numContextEntries = 50000;

      // add context entries
      System.out.println("Populating " + numContextEntries + " context entries");
      final var entries = new ArrayList<Event>();
      for (int i = 0; i < numContextEntries; ++i) {
        final var entry = new EventJson();
        entry.set(keyName, Long.valueOf(i));
        entry.set(valueName, Long.valueOf(i * 10));
        entries.add(entry);
        if (entries.size() == 5000) {
          System.out.println("  putting 5000 entries (" + (i + 1) + ")");
          TestUtils.upsertContextEntries(
              dataServiceHandler, clientManager, admin, tenantName, contextName, entries);
          entries.clear();
          System.out.println("  done");
        }
      }
      System.out.println("Done populating " + numContextEntries + " context entries");

      System.out.println("Deleting entries");
      // delete context entries
      final var keys = new ArrayList<List<Object>>();
      for (int i = 0; i < numContextEntries / 2; ++i) {
        keys.add(List.of(Long.valueOf(i * 2 + 1)));
        if (keys.size() == 5000) {
          System.out.println("  deleting 5000 entries (" + (i + 1) + ")");
          TestUtils.deleteContextEntries(
              dataServiceHandler, clientManager, tenantName, contextName, keys);
          keys.clear();
          System.out.println("  done");
        }
      }
      System.out.println("Done deleting entries");

      // Turn on cache-only mode
      sharedProperties.setProperty(
          DataEngineImpl.PROP_CACHE_ONLY_CONTEXTS, tenantName + "." + contextName);

      final var contextCassStream = (ContextCassStream) dataEngine.getCassStream(context);

      final int maxTrials = 120;
      boolean cacheOnlyModeEnabled = false;
      System.out.println("Waiting for the context being entering cache only mode");
      for (int i = 0; i < maxTrials; ++i) {
        if (contextCassStream.getCacheMode() == ContextCassStream.CacheMode.CACHE_ONLY) {
          cacheOnlyModeEnabled = true;
          break;
        }
        Thread.sleep(1000);
      }
      assertTrue(cacheOnlyModeEnabled);
      System.out.println("Context went into cache only mode");

      // get context entries
      System.out.println("Getting context entries (1st)");
      verifyContextEntries(dataServiceHandler, context, valueName, numContextEntries);

      // reloading
      System.out.println("Reloading modules");
      final var modules2 = TestModules.reload(ContextCacheTest.class + "rl", conn);
      DataEngineImpl reloadedDataEngine = modules2.getDataEngine();
      AdminInternal admin2 = modules2.getTfosAdmin();
      final var context2 = admin2.getStream(tenantName, contextName);

      cacheOnlyModeEnabled = false;
      System.out.println("Waiting for the reloaded context being entering cache only mode");
      for (int i = 0; i < maxTrials; ++i) {
        if (contextCassStream.getCacheMode() == ContextCassStream.CacheMode.CACHE_ONLY) {
          cacheOnlyModeEnabled = true;
          break;
        }
        Thread.sleep(1000);
      }
      assertTrue(cacheOnlyModeEnabled);
      System.out.println("Reloaded context went into cache only mode");

      verifyContextEntries(
          modules2.getDataServiceHandler(), context2, valueName, numContextEntries);

    } finally {
      sharedProperties.setProperty(
          DataEngineImpl.PROP_CACHE_ONLY_CONTEXTS,
          origCacheOnlyContexts != null ? origCacheOnlyContexts : "");
      sharedProperties.setProperty(
          ContextCassStream.PROPERTY_CONTEXT_CACHE_SIZE,
          origCacheMaxNumEntries != null ? origCacheMaxNumEntries : "");
    }
  }

  private void verifyContextEntries(
      DataServiceHandler handler, StreamDesc context, String valueName, int numContextEntries)
      throws Exception {
    final var tenantName = context.getParent().getName();
    final var contextName = context.getName();
    final List<List<Object>> keys = new ArrayList<>();
    for (int i = 0; i < numContextEntries; ++i) {
      keys.add(List.of(Long.valueOf(i)));
      if (keys.size() == 5000) {
        final var retrieved = TestUtils.getContextEntries(handler, tenantName, contextName, keys);
        assertEquals(2500, retrieved.size());
        for (int j = 0; j < 2500; ++j) {
          final var event = retrieved.get(j);
          assertNotNull(event);
          assertEquals(((Long) keys.get(j * 2).get(0)) * 10, event.get(valueName));
        }
      }
    }
  }
}
