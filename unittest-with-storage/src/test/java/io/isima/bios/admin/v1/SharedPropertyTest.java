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
package io.isima.bios.admin.v1;

import static io.isima.bios.common.SharedProperties.SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SharedPropertyTest {

  private static SharedProperties properties;

  private final List<String> keys = new ArrayList<>();

  @BeforeClass
  public static void setUpClass() {
    Bios2TestModules.startModules(false, SharedPropertyTest.class, Map.of());
    properties = BiosModules.getSharedProperties();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() {
    keys.clear();
  }

  @After
  public void tearDown() {
    for (String key : keys) {
      properties.deleteProperty(key);
    }
    SharedProperties.clearCache();
  }

  @Test
  public void testBasic() throws ApplicationException {
    String key = "hello";
    keys.add(key);
    properties.setProperty(key, "world");
    assertEquals("world", properties.getProperty(key));
  }

  @Test
  public void testCaseSensitivity() throws ApplicationException {
    String key1 = "thisismykey";
    String key2 = "thisIsMyKey";
    keys.add(key1);
    keys.add(key2);
    properties.setProperty(key1, "lower case");
    properties.setProperty(key2, "mixed case");
    assertEquals("lower case", properties.getProperty(key1));
    assertEquals("mixed case", properties.getProperty(key2));
  }

  @Test
  public void testCachedGetProperty() throws ApplicationException {
    String key = "hello";
    String nonexistentKey = "nonexistentKey";
    String nonexistentKey2 = "number";
    long cacheExpiry = 100L;
    keys.add(SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY);
    properties.setProperty(SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY, Long.toString(cacheExpiry));
    SharedProperties.clearCache();
    keys.add(key);
    properties.setProperty(key, "world");
    assertEquals("world", properties.getProperty(key));
    assertEquals("world", properties.getPropertyCached(key));
    properties.setProperty(key, "another world!");
    assertEquals("world", properties.getPropertyCached(key));

    assertNull(properties.getPropertyCached(nonexistentKey));
    keys.add(nonexistentKey);
    properties.setProperty(nonexistentKey, "Now it exists!");
    assertNull(properties.getPropertyCached(nonexistentKey));
    assertEquals("default1", properties.getPropertyCached(nonexistentKey, "default1"));

    assertNull(properties.getPropertyCached(nonexistentKey2));
    keys.add(nonexistentKey2);
    properties.setProperty(nonexistentKey2, "1234567890123456");
    assertNull(properties.getPropertyCached(nonexistentKey2));
    assertEquals((Long) 123L, properties.getPropertyCached(nonexistentKey2, 123L));

    TestUtils.sleepAtLeast(cacheExpiry);
    assertEquals("another world!", properties.getPropertyCached(key));
    assertEquals("Now it exists!", properties.getPropertyCached(nonexistentKey));
    assertEquals("Now it exists!", properties.getPropertyCached(nonexistentKey, "default2"));
    assertEquals(1234567890123456L, Long.parseLong(properties.getPropertyCached(nonexistentKey2)));
    assertEquals((Long) 1234567890123456L, properties.getPropertyCached(nonexistentKey2, 5L));
  }

  @Test
  public void testOverwrite() throws ApplicationException {
    String key = "testOverwrite";
    keys.add(key);
    assertNull(properties.getProperty(key));
    properties.setProperty(key, "Ek");
    assertEquals("Ek", properties.getProperty(key));
    properties.setProperty(key, "Do");
    assertEquals("Do", properties.getProperty(key));
    properties.setProperty(key, null);
    assertNull(properties.getProperty(key));
  }

  @Test
  public void testSafeAdd() throws ApplicationException {
    String key = "safeadd";
    keys.add(key);
    String result = properties.safeAddProperty(key, "one");
    assertNull(result);
    result = properties.safeAddProperty(key, "two");
    assertEquals("one", result);
    result = properties.getProperty(key);
    assertEquals("one", result);
  }

  @Test
  public void testSafeUpdate() throws ApplicationException {
    String key = "safeupdate";
    keys.add(key);
    String result = properties.safeUpdateProperty(key, null, "one");
    assertNull(result);
    assertEquals("one", properties.getProperty(key));
    assertNull(properties.safeUpdateProperty(key, "one", "two"));
    assertEquals("two", properties.safeUpdateProperty(key, "one", "three"));
    assertEquals("two", properties.getProperty(key));
  }

  @Test
  public void testSimpleAppend() throws ApplicationException {
    String key = "simpleappend";
    keys.add(key);
    properties.appendPropertyIfAbsent(key, "one", ",", true);
    properties.appendPropertyIfAbsent(key, "two", ",", true);
    properties.appendPropertyIfAbsent(key, "three", ",", true);
    properties.appendPropertyIfAbsent(key, "four", ",", true);
    properties.appendPropertyIfAbsent(key, "five", ",", true);
    String out = properties.getProperty(key);
    assertEquals("one,two,three,four,five", out);
    properties.removeProperty(key, "one", ",", true);
    assertEquals("two,three,four,five", properties.getProperty(key));
    properties.removeProperty(key, "five", ",", true);
    assertEquals("two,three,four", properties.getProperty(key));
    properties.removeProperty(key, "three", ",", true);
    assertEquals("two,four", properties.getProperty(key));
    properties.removeProperty(key, "x", ",", true);
    assertEquals("two,four", properties.getProperty(key));
    properties.removeProperty(key, "two", ",", true);
    assertEquals("four", properties.getProperty(key));
    properties.removeProperty(key, "four", ",", true);
    assertEquals("", properties.getProperty(key));

    properties.appendPropertyIfAbsent(key, "one", ",", true);
    properties.appendPropertyIfAbsent(key, "two", ",", true);
    properties.appendPropertyIfAbsent(key, "three", ",", true);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.appendPropertyIfAbsent(key, "three", ",", true);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.appendPropertyIfAbsent(key, "two", ",", true);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.appendPropertyIfAbsent(key, "one", ",", true);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.appendPropertyIfAbsent(key, "ONE", ",", true);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.appendPropertyIfAbsent(key, "ONE", ",", false);
    assertEquals("one,two,three,ONE", properties.getProperty(key));

    properties.removeProperty(key, "ONE", ",", false);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.removeProperty(key, "three", ",", false);
    assertEquals("one,two", properties.getProperty(key));
    properties.removeProperty(key, "two", ",", false);
    assertEquals("one", properties.getProperty(key));
    properties.removeProperty(key, "ONE", ",", false);
    assertEquals("one", properties.getProperty(key));
    properties.removeProperty(key, "one", ",", false);
    assertEquals("", properties.getProperty(key));
    properties.removeProperty(key, "zero", ",", false);
    assertEquals("", properties.getProperty(key));

    properties.appendPropertyIfAbsent(key, "one", ",", true);
    properties.appendPropertyIfAbsent(key, "two", ",", true);
    properties.appendPropertyIfAbsent(key, "three", ",", true);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.removeProperties(key, new String[] {"One", "THREE"}, ",", false);
    assertEquals("one,two,three", properties.getProperty(key));
    properties.removeProperties(key, new String[] {"one", "three"}, ",", false);
    assertEquals("two", properties.getProperty(key));
  }

  @Test
  public void testParallelAppend() throws InterruptedException {
    String key = "parallel";
    keys.add(key);
    List<Worker> workers = new ArrayList<>();
    List<Thread> threads = new ArrayList<>();
    final int jobSize = 10;
    final int numThreads = 10;

    // parallel append
    for (int ithread = 0; ithread < numThreads; ++ithread) {
      workers.add(new AppendWorker(properties, ithread * jobSize, jobSize));
      threads.add(new Thread(workers.get(ithread)));
      threads.get(ithread).start();
    }
    for (int i = 0; i < numThreads; ++i) {
      threads.get(i).join();
      assertEquals(0, workers.get(i).getFailureCount());
    }
    String values = properties.getProperty(key);
    assertNotNull(values);
    Set<String> entries = new HashSet<>();
    for (String entry : values.split(",")) {
      assertTrue(entries.add(entry));
    }
    for (int i = 0; i < numThreads * jobSize; ++i) {
      assertTrue(entries.contains(Integer.toString(i)));
    }

    // parallel remove
    workers.clear();
    threads.clear();
    for (int ithread = 0; ithread < numThreads; ++ithread) {
      workers.add(new RemoveWorker(properties, ithread * jobSize, jobSize));
      threads.add(new Thread(workers.get(ithread)));
      threads.get(ithread).start();
    }
    for (int i = 0; i < numThreads; ++i) {
      threads.get(i).join();
      assertEquals(0, workers.get(i).getFailureCount());
    }
    values = properties.getProperty(key);
    assertNotNull(values);
    assertEquals("", values);
  }

  private abstract static class Worker implements Runnable {
    protected static final String key = "parallel";
    protected static final String delimiter = ",";

    protected final SharedProperties properties;
    protected final int start;
    protected final int numEntries;
    protected int failureCount;

    public Worker(SharedProperties properties, int start, int numEntries) {
      this.properties = properties;
      this.start = start;
      this.numEntries = numEntries;
      failureCount = 0;
    }

    public int getFailureCount() {
      return failureCount;
    }
  }

  private static class AppendWorker extends Worker {
    public AppendWorker(SharedProperties properties, int start, int numEntries) {
      super(properties, start, numEntries);
    }

    @Override
    public void run() {
      for (int i = start; i < start + numEntries; ++i) {
        try {
          properties.appendPropertyIfAbsent(key, Integer.toString(i), delimiter, true);
        } catch (ApplicationException e) {
          ++failureCount;
        }
      }
    }
  }

  private static class RemoveWorker extends Worker {
    public RemoveWorker(SharedProperties properties, int start, int numEntries) {
      super(properties, start, numEntries);
    }

    @Override
    public void run() {
      List<Integer> indexes = new ArrayList<>();
      for (int i = start; i < start + numEntries; ++i) {
        indexes.add(i);
      }
      Collections.shuffle(indexes);
      for (Integer index : indexes) {
        try {
          properties.removeProperty(key, index.toString(), delimiter, true);
        } catch (ApplicationException e) {
          ++failureCount;
        }
      }
    }
  }
}
