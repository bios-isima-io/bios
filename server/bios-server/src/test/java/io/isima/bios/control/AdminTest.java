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
package io.isima.bios.control;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.AttributeModAllowance;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdminTest {

  private static AdminInternal tenantManager;
  private static TenantConfig tenantForStreamTest;
  private static long timestamp;

  @BeforeClass
  public static void setUpClass() throws FileReadException {
    tenantManager = new AdminImpl(null, new MetricsStreamProvider());
    tenantForStreamTest = new TenantConfig();
    tenantForStreamTest.setName("tenantForStreamAdd");
    timestamp = System.currentTimeMillis();
  }

  /**
   * TearDown: Clears all tenants.
   *
   * @throws NoSuchTenantException should not happen
   * @throws ConstraintViolationException
   */
  @After
  public void tearDown()
      throws NoSuchTenantException, ApplicationException, ConstraintViolationException {
    List<String> tenants = tenantManager.getAllTenants();
    for (String tenant : tenants) {
      tenantManager.removeTenant(new TenantConfig(tenant), RequestPhase.INITIAL, ++timestamp);
    }
  }

  @Test
  public void testTenantImmutability() throws TfosException, ApplicationException {
    TenantConfig tenantConfig = new TenantConfig("first_generation");

    tenantManager.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    assertEquals("first_generation", tenantManager.getTenant("first_generation").getName());
    tenantConfig.setName("second_generation");
    assertEquals("first_generation", tenantManager.getTenant("first_generation").getName());

    assertTrue(tenantConfig.getStreams().isEmpty());
    StreamConfig streamConfig = new StreamConfig("simple_stream");
    tenantConfig.addStream(streamConfig);
    assertTrue(tenantManager.getTenant("first_generation").getStreams().isEmpty());

    tenantManager.addStream("first_generation", streamConfig, RequestPhase.FINAL, ++timestamp);
    tenantConfig
        .getStreams()
        .get(0)
        .addAdditionalAttribute(new AttributeDesc("added", InternalAttributeType.STRING));
    assertEquals(
        null,
        tenantManager.getStream("first_generation", "simple_stream").getAdditionalAttributes());
  }

  /** Worker class used for testing concurrent addTenant operations. */
  private class TenantWorker implements Runnable {
    private final int offset;
    private final int depth;

    public TenantWorker(int depth, int offset) {
      this.depth = depth;
      this.offset = offset;
    }

    @Override
    public void run() {
      for (int i = 0; i < depth; ++i) {
        int index = offset + i;
        TenantConfig tenantConfig = new TenantConfig("tenant" + index);
        // System.out.println(tenantConfig);
        try {
          tenantManager.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
        } catch (TfosException | ApplicationException e) {
          // shouldn't happen
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testTenantAdd() {
    final int numThreads = 10;
    final int tenantsPerThread = 1000;

    // create tenant writer threads
    List<Thread> threads = new ArrayList<>();
    for (int ithread = 0; ithread < numThreads; ++ithread) {
      threads.add(new Thread(new TenantWorker(tenantsPerThread, tenantsPerThread * ithread)));
    }

    // run threads
    threads.forEach(thread -> thread.start());

    // wait for all tasks done
    threads.forEach(
        thread -> {
          // thread.start();
          try {
            thread.join();
          } catch (InterruptedException e) {
            // finish anyway
            e.printStackTrace();
          }
        });

    // verify
    for (int i = 0; i < tenantsPerThread * numThreads; ++i) {
      String tenant = "tenant" + i;
      try {
        TenantDesc conf = tenantManager.getTenant(tenant);
        assertNotEquals(null, conf);
      } catch (NoSuchTenantException e) {
        fail(e.toString());
      }
    }
  }

  /** Worker class used for testing concurrent addStream operations. */
  private class StreamAddWorker implements Runnable {
    private final int offset;
    private final int depth;

    public StreamAddWorker(int depth, int offset) {
      this.depth = depth;
      this.offset = offset;
    }

    @Override
    public void run() {
      for (int i = 0; i < depth; ++i) {
        int index = offset + i;
        StreamConfig streamConfig = new StreamConfig("stream" + index);
        try {
          tenantManager.addStream(
              tenantForStreamTest.getName(), streamConfig, RequestPhase.FINAL, ++timestamp);
        } catch (TfosException | ApplicationException e) {
          fail(e.toString());
        }
      }
    }
  }

  private class StreamReadWorker implements Runnable {
    private final int size;
    private int failureCount;

    public StreamReadWorker(int size) {
      this.size = size;
      failureCount = 0;
    }

    public int getFailureCount() {
      return failureCount;
    }

    @Override
    public void run() {
      for (int i = 0; i < size; ++i) {
        try {
          StreamConfig streamConfig =
              tenantManager.getStream(tenantForStreamTest.getName(), "reference");
          if (streamConfig == null) {
            ++failureCount;
          }
        } catch (Throwable e) {
          System.out.println(e);
          ++failureCount;
        }
      }
    }
  }

  @Test
  public void testStreamAdd() throws TfosException, ApplicationException {
    tenantManager.addTenant(tenantForStreamTest, RequestPhase.FINAL, ++timestamp);
    // add a stream config to be referred by the reader thread.
    StreamConfig reference = new StreamConfig("reference");
    tenantManager.addStream(
        tenantForStreamTest.getName(), reference, RequestPhase.FINAL, ++timestamp);

    // Create writer threads
    final int numThreads = 10;
    final int streamPerThread = 100;
    List<Thread> threads = new ArrayList<>();
    for (int ithread = 0; ithread < numThreads; ++ithread) {
      threads.add(new Thread(new StreamAddWorker(streamPerThread, streamPerThread * ithread)));
    }
    final int numReads = 10000;
    StreamReadWorker readerWorker = new StreamReadWorker(numReads);
    Thread readerThread = new Thread(readerWorker);

    // execute time!
    threads.forEach(thread -> thread.start());
    readerThread.start();
    threads.forEach(
        thread -> {
          try {
            thread.join();
          } catch (InterruptedException e) {
            // finish anyway
            e.printStackTrace();
          }
        });
    try {
      readerThread.join();
    } catch (InterruptedException e1) {
      // continue anyway
      e1.printStackTrace();
    }

    // verify
    assertEquals(0, readerWorker.getFailureCount());
    for (int i = 0; i < streamPerThread * numThreads; ++i) {
      String stream = "stream" + i;
      try {
        StreamConfig conf;
        conf = tenantManager.getStream(tenantForStreamTest.getName(), stream);
        assertNotEquals(null, conf);
      } catch (NoSuchTenantException | NoSuchStreamException e) {
        fail(e.toString());
      }
    }
  }

  @Test
  public void testStreamAliasMappingWhenCreatingStream()
      throws TfosException, ApplicationException {
    final String tenantName = "test_tenant";
    final String streamName = "order_signal";
    final String streamAlias = "order";

    TenantConfig tenantConfig = new TenantConfig(tenantName);

    tenantManager.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    TenantDesc tenantDesc = tenantManager.getTenant(tenantName);
    assertEquals(tenantName, tenantDesc.getName());
    assertTrue(tenantDesc.getStreams().isEmpty());

    StreamConfig streamConfig = new StreamConfig(streamName);
    tenantManager.addStream(tenantName, streamConfig, RequestPhase.FINAL, ++timestamp);

    tenantDesc = tenantManager.getTenant(tenantName);
    assertEquals(1, tenantDesc.getStreams().size());

    assertEquals(streamName, tenantManager.resolveSignalName(tenantName, streamAlias));
  }

  @Test
  public void testStreamAliasMappingWhenUpdatingStream()
      throws TfosException, ApplicationException {
    final String tenantName = "test_tenant";
    final String streamName = "order_signal";
    final String streamAlias = "order";

    TenantConfig tenantConfig = new TenantConfig(tenantName);

    tenantManager.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    tenantManager.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    TenantDesc tenantDesc = tenantManager.getTenant(tenantName);
    assertEquals(tenantName, tenantDesc.getName());
    assertTrue(tenantDesc.getStreams().isEmpty());

    StreamConfig streamConfig = new StreamConfig(streamName, ++timestamp);
    tenantManager.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
    tenantManager.addStream(tenantName, streamConfig, RequestPhase.FINAL);

    streamConfig.addAttribute(
        new AttributeDesc("user_type", InternalAttributeType.STRING).setDefaultValue("NB"));
    streamConfig.setVersion(++timestamp);

    tenantManager.modifyStream(
        tenantName,
        streamName,
        streamConfig,
        RequestPhase.INITIAL,
        AttributeModAllowance.CONVERTIBLES_ONLY,
        Set.of());
    tenantManager.modifyStream(
        tenantName,
        streamName,
        streamConfig,
        RequestPhase.FINAL,
        AttributeModAllowance.CONVERTIBLES_ONLY,
        Set.of());

    tenantDesc = tenantManager.getTenant(tenantName);
    assertEquals(1, tenantDesc.getStreams().size());

    assertEquals(streamName, tenantManager.resolveSignalName(tenantName, streamAlias));
  }
}
