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
package io.isima.bios.data.impl.storage;

import static io.isima.bios.data.impl.models.MaintenanceMode.ENABLED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.data.impl.maintenance.TaskSlots;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextCleanupTest {

  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static long timestamp;
  private static AdminInternal admin;
  private static DataEngineImpl dataEngine;
  private static TaskSlots taskSlots;

  // Async process executor
  ExecutorService service = Executors.newSingleThreadExecutor();

  private static final Logger logger = LoggerFactory.getLogger(ContextCleanupTest.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.setProperty(TfosConfig.ROLLUP_INTERVAL_SECONDS, "1");
    Bios2TestModules.setProperty(TfosConfig.CONTEXT_MAINTENANCE_CLEANUP_MARGIN, "100");
    Bios2TestModules.startModules(
        false,
        ContextCleanupTest.class,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "1000",
            ContextCassStream.PROPERTY_CONTEXT_MAINTENANCE_MODE,
            "ENABLED",
            DataEngineImpl.PROP_CONTEXT_MAINTENANCE_INTERVAL,
            "0",
            ContextCassStream.PROPERTY_CONTEXT_MAINTENANCE_BATCH_SIZE,
            "1000"));
    Bios2TestModules.startModulesWithoutMaintenance(ContextCleanupTest.class + " 2");

    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    timestamp = System.currentTimeMillis() - 50;

    tenantName = ContextCleanupTest.class.getSimpleName();
    final TenantDesc tenantDesc = new TenantDesc(tenantName, timestamp, false);

    tenantConfig = tenantDesc.toTenantConfig();
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp - 50);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp - 50);
    } catch (TfosException e) {
      // ignore
    }
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    taskSlots = new TaskSlots(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    timestamp = System.currentTimeMillis();
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (TfosException e) {
      // ignore
    }
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testBatchExecutions() throws Exception {
    // Add the test context
    final String contextName = "batch_execution";
    final String keyName = "index";
    final String valueName = "value";
    {
      final StreamDesc inputContext = new StreamDesc(contextName, ++timestamp);
      inputContext.setType(StreamType.CONTEXT);
      inputContext.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
      inputContext.addAttribute(new AttributeDesc(valueName, InternalAttributeType.INT));
      inputContext.setTtl(60000L);
      admin.addStream(tenantName, inputContext, RequestPhase.INITIAL);
      admin.addStream(tenantName, inputContext, RequestPhase.FINAL);
    }
    final StreamDesc contextDesc = admin.getStream(tenantName, contextName);
    assertNotNull(contextDesc);
    final ContextCassStream cassContext = (ContextCassStream) dataEngine.getCassStream(contextDesc);
    assertNotNull(cassContext);
    // populate initial entries
    final int numKeys = 1000;
    final int entriesPerKey = 4;
    populateContextEntries(numKeys, entriesPerKey, contextDesc);
    timestamp += 100;
    // Try cleaning with 1024 entries for each
    final int batchSize = 256;
    {
      final long first =
          cassContext
              .clearOldEntries(
                  Long.MIN_VALUE,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertTrue(Long.toString(first), first > Long.MIN_VALUE);
      assertTrue(Long.toString(first), first < Long.MAX_VALUE);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, batchSize, false);
      final long second =
          cassContext
              .clearOldEntries(
                  first,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertTrue(Long.toString(second), second > first);
      assertTrue(Long.toString(second), second < Long.MAX_VALUE);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, batchSize * 2, false);
      final long third =
          cassContext
              .clearOldEntries(
                  second,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertTrue(Long.toString(third), third > second);
      assertTrue(Long.toString(third), third < Long.MAX_VALUE);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, batchSize * 3, false);
      final long fourth =
          cassContext
              .clearOldEntries(
                  third,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertEquals(Long.MIN_VALUE, fourth);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, numKeys, false);
    }
    // wait until ttl and try batch cleanups again
    {
      /*
      final List<List<Object>> keys = new ArrayList<>();
      for (int i = 0; i < numKeys; ++i) {
        keys.add(List.of(Integer.toString(i)));
      }
      TestUtils.deleteContextEntries(dataServiceHandler, clientManager, tenantName, contextName,
          keys);
          */
      timestamp += 120000;
      final long first =
          cassContext
              .clearOldEntries(
                  Long.MIN_VALUE,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertTrue(Long.toString(first), first > Long.MIN_VALUE);
      assertTrue(Long.toString(first), first < Long.MAX_VALUE);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, batchSize, true);
      final long second =
          cassContext
              .clearOldEntries(
                  first,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertTrue(Long.toString(second), second > first);
      assertTrue(Long.toString(second), second < Long.MAX_VALUE);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, batchSize * 2, true);
      final long third =
          cassContext
              .clearOldEntries(
                  second,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertTrue(Long.toString(third), third > second);
      assertTrue(Long.toString(third), third < Long.MAX_VALUE);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, batchSize * 3, true);
      final long fourth =
          cassContext
              .clearOldEntries(
                  third,
                  timestamp,
                  batchSize,
                  ENABLED,
                  new GenericExecutionState("test", dataEngine.getExecutor()))
              .getNextToken();
      assertEquals(Long.MIN_VALUE, fourth);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, numKeys, true);
    }
  }

  @Test
  public void testMaintenanceMain() throws Exception {
    // Add the test context
    final String contextName = "maintenance_core_test";
    final String keyName = "index";
    final String valueName = "value";
    {
      timestamp = System.currentTimeMillis();
      final StreamDesc inputContext = new StreamDesc(contextName, timestamp);
      inputContext.setType(StreamType.CONTEXT);
      inputContext.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
      inputContext.addAttribute(new AttributeDesc(valueName, InternalAttributeType.INT));
      inputContext.setTtl(60000L);
      admin.addStream(tenantName, inputContext, RequestPhase.INITIAL);
      admin.addStream(tenantName, inputContext, RequestPhase.FINAL);
    }
    final StreamDesc contextDesc = admin.getStream(tenantName, contextName);
    assertNotNull(contextDesc);
    final ContextCassStream cassContext = (ContextCassStream) dataEngine.getCassStream(contextDesc);
    assertNotNull(cassContext);
    // populate initial entries
    final int numKeys = 700;
    final int entriesPerKey = 4;
    populateContextEntries(numKeys, entriesPerKey, contextDesc);
    assertNull(cassContext.getProperty(cassContext.getPropertyNextToken()));
    assertNull(cassContext.getProperty(cassContext.getPropertyLastTimestamp()));
    timestamp += 1000;
    final long maintenanceInterval = 60000;
    final long margin = 100;
    final int limit = 256;
    assertEquals(Long.MIN_VALUE, cassContext.getNextMaintenanceToken());
    // Try the first maintenance run
    final long revisitInterval =
        SharedProperties.getLong(
            ContextCassStream.PROPERTY_CONTEXT_MAINTENANCE_REVISIT_INTERVAL, 24 * 60 * 60);
    cassContext.maintenanceMain(
        revisitInterval,
        timestamp,
        margin,
        limit,
        ENABLED,
        new GenericExecutionState("test", dataEngine.getExecutor()),
        dataEngine.getSketchStore());
    final long initialTimestamp =
        Long.parseLong(cassContext.getProperty(cassContext.getPropertyLastTimestamp()));
    assertThat(initialTimestamp, greaterThanOrEqualTo(timestamp));
    final long firstNextToken = cassContext.getNextMaintenanceToken();
    assertTrue(firstNextToken > Long.MIN_VALUE);
    // Proceed the timestamp by maintenanceInterval, the maintenance should be ready to go.
    timestamp += maintenanceInterval;
    cassContext.maintenanceMain(
        revisitInterval,
        timestamp,
        margin,
        limit,
        ENABLED,
        new GenericExecutionState("test", dataEngine.getExecutor()),
        dataEngine.getSketchStore());
    final long secondTimestamp =
        Long.parseLong(cassContext.getProperty(cassContext.getPropertyLastTimestamp()));
    assertThat(secondTimestamp, greaterThan(initialTimestamp));
    assertThat(secondTimestamp, greaterThanOrEqualTo(timestamp));
    final long secondNextToken = cassContext.getNextMaintenanceToken();
    assertTrue(secondNextToken > firstNextToken);
    // Try again, this should complete maintenance of entire range.
    timestamp += maintenanceInterval;
    cassContext.maintenanceMain(
        revisitInterval,
        timestamp,
        margin,
        limit,
        ENABLED,
        new GenericExecutionState("test", dataEngine.getExecutor()),
        dataEngine.getSketchStore());
    assertEquals(Long.MIN_VALUE, cassContext.getNextMaintenanceToken());
  }

  @Test
  public void testScheduledMaintenances() throws Exception {
    // Add the test context
    final String contextName = "maintenance_test";
    final String keyName = "index";
    final String valueName = "value";
    {
      timestamp = System.currentTimeMillis();
      final StreamDesc inputContext = new StreamDesc(contextName, timestamp);
      inputContext.setType(StreamType.CONTEXT);
      inputContext.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
      inputContext.addAttribute(new AttributeDesc(valueName, InternalAttributeType.INT));
      admin.addStream(tenantName, inputContext, RequestPhase.INITIAL);
      admin.addStream(tenantName, inputContext, RequestPhase.FINAL);
    }

    final StreamDesc contextDesc = admin.getStream(tenantName, contextName);
    assertNotNull(contextDesc);
    final ContextCassStream cassContext = (ContextCassStream) dataEngine.getCassStream(contextDesc);
    assertNotNull(cassContext);

    // populate initial entries
    final int numKeys = 1000;
    final int entriesPerKey = 4;
    populateContextEntries(numKeys, entriesPerKey, contextDesc);

    // run maintenance
    dataEngine.maintainInternal(taskSlots);
    try {
      // maintenance would be done within 10 seconds
      Thread.sleep(10000);
      verifySegmentalCleanup(cassContext, valueName, numKeys, entriesPerKey, numKeys, false);
    } catch (InterruptedException e) {
      logger.info("ContextCleanupTest thread interrupted");
    }
  }

  // utilities ////////////////////////////////////////

  /**
   * Utility to populate context entries.
   *
   * <p>This method has limitation that number of context attributes must be 2, and they have to be
   * of type string, int, long, number, or double.
   *
   * @param numKeys Number of keys to populate.
   * @param entriesPerKey Number of entries to populate for each key.
   * @param contextDesc The target context descriptor.
   * @return The last put timestamp.
   * @throws TfosException When the operation fails
   * @throws ApplicationException When an unexpected error happens.
   */
  private long populateContextEntries(int numKeys, int entriesPerKey, StreamDesc contextDesc)
      throws TfosException, ApplicationException, ExecutionException, InterruptedException {
    final String keyName = contextDesc.getAttributes().get(0).getName();
    final String valueName = contextDesc.getAttributes().get(1).getName();
    final int numEntries = numKeys * entriesPerKey;
    final List<Event> entries = new ArrayList<>();
    long putDoneTime = 0;
    for (int i = 0; i < numEntries; ++i) {
      final Event entry = new EventJson();
      entry.getAttributes().put(keyName, Integer.toString(i % numKeys));
      entry.getAttributes().put(valueName, Integer.valueOf(i));
      entries.add(entry);
      if ((i + 1) % numKeys == 0) {
        System.out.println(
            "writing " + numKeys + " entries (" + (i / numKeys * numKeys) + "-" + i + ")");

        // final TestContextExecutionState ces = createInitial(contextDesc, ++timestamp);
        // dataEngine.putContextEntries(ces, entries);
        // dataEngine.putContextEntries(ces.switchToFinal(), entries);
        final var state =
            TestUtils.makeUpsertState(
                admin, tenantName, contextDesc.getName(), entries, ++timestamp);
        dataEngine
            .putContextEntriesAsync(state)
            .thenCompose(
                (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
            .toCompletableFuture()
            .get();

        putDoneTime = timestamp;
        entries.clear();
      }
    }
    return putDoneTime;
  }

  /**
   * Utility to verify the context table cleanup.
   *
   * <p>The method checks all primary keys and counts number of cleaned up keys. The method also
   * checks the values in the table to verify the remaining entries are the latest ones.
   *
   * <p>The method assumes that the context entries were populated using the method {@link
   * #populateContextEntries}.
   *
   * @param contextCassStream Target context CassStream.
   * @param valueName Attribute name of the value.
   * @param numKeys Number of primary keys to check.
   * @param entriesPerKey Number of entries per key that were populated.
   * @param expectedCleanedKeys Expected number of keys that are cleaned up.
   * @param isDeletionCheck Flag to indicate whether the method does deletion check.
   * @throws ApplicationException When an unexpected error happens.
   */
  private void verifySegmentalCleanup(
      ContextCassStream contextCassStream,
      String valueName,
      int numKeys,
      int entriesPerKey,
      int expectedCleanedKeys,
      boolean isDeletionCheck)
      throws ApplicationException {

    final String table = contextCassStream.keyspaceName + "." + contextCassStream.tableName;
    final List<String> keyColumns = contextCassStream.getKeyColumns();
    final String valueColumn = CassandraConstants.PREFIX_EVENTS_COLUMN + valueName.toLowerCase();
    final String where =
        keyColumns.stream().map((column) -> column + "= ?").collect(Collectors.joining(" AND "));
    final String query = String.format("SELECT * FROM %s WHERE %s", table, where);
    final int numEntries = numKeys * entriesPerKey;

    int cleanedKeys = 0;
    for (int i = 0; i < numKeys; ++i) {
      final String key = Integer.toString(i);
      final ResultSet results = contextCassStream.getSession().execute(query, key);
      List<Row> rows = results.all();
      if (!isDeletionCheck) {
        assertFalse(rows.isEmpty());
        final int expected = (numEntries / numKeys - 1) * numKeys + i;
        assertEquals(expected, rows.get(0).getInt(valueColumn));
      } else if (rows.size() == 0) {
        ++cleanedKeys;
      }
    }
    if (isDeletionCheck) {
      assertEquals(expectedCleanedKeys, cleanedKeys);
    }
  }
}
