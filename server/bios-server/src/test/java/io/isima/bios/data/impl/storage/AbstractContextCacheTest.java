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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.uuid.Generators;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.utils.Utils;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public abstract class AbstractContextCacheTest {

  protected static class CacheBeater extends Thread {
    private final ContextCache cache1;
    private final ContextCache cache2;
    private AtomicLong timestamp;
    private final int iteration;
    private final Integer id;
    private final Random random;
    private static final int KEY_RANGE = 32768;

    public CacheBeater(
        ContextCache cache1, ContextCache cache2, AtomicLong timestamp, int iteration, int id) {
      if (id % 2 == 0) {
        this.cache1 = cache1;
        this.cache2 = cache2;
      } else {
        this.cache1 = cache2;
        this.cache2 = cache1;
      }
      this.timestamp = timestamp;
      this.iteration = iteration;
      this.id = id;
      random = new Random();
    }

    @Override
    public void run() {
      for (int i = 0; i < iteration; ++i) {
        final List<Object> key = List.of(Integer.valueOf(random.nextInt(KEY_RANGE)));
        final int dice = random.nextInt(1024);
        long now = timestamp.getAndIncrement();
        if (dice > 16) {
          final Event event1 = new EventJson();
          event1.setIngestTimestamp(new Date(now));
          event1.getAttributes().put("key", id);
          cache1.put(key, event1, event1.getIngestTimestamp().getTime());
          final Event event2 = new EventJson();
          event2.setIngestTimestamp(new Date(now));
          event2.getAttributes().put("key", id);
          cache2.put(key, event2, event2.getIngestTimestamp().getTime());
        } else {
          cache1.delete(key, now);
          cache2.delete(key, now);
        }
      }
    }
  }

  protected abstract void verifyImplementationSpecifics(ContextCache c);

  protected abstract ContextCache createCache(int size);

  protected void capacityCheck(ContextCache cache, int size) {
    assertEquals(size, cache.capacity());
  }

  @Test
  public void testBasic() {
    ContextCache cache = createCache(256);

    capacityCheck(cache, 256);
    final long timestamp = getTimestamp();

    // put entries
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // get entries
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }
    assertNull(cache.get(List.of(Integer.valueOf(512))));

    // update entries
    for (int i = 0; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp + 10));
      event.getAttributes().put("key", Integer.valueOf(key * 10));
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // verify updates
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      if (i % 2 == 0) {
        assertEquals(key * 10, event.getAttributes().get("key"));
      } else {
        assertEquals(key, event.getAttributes().get("key"));
      }
    }

    // updates with older timestamp would have no effects
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp + 5));
      event.getAttributes().put("key", Integer.valueOf(key * 20));
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      if (i % 2 == 0) {
        assertEquals(key * 10, event.getAttributes().get("key"));
      } else {
        assertEquals(key * 20, event.getAttributes().get("key"));
      }
    }

    // test deletion
    for (int i = 0; i < 256; ++i) {
      final Integer key = Integer.valueOf(i);
      cache.delete(List.of(key), timestamp + 7);
    }
    // adding entries with older timestamp after deletion would have no effects
    for (int i = 128; i < 256; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      long offset = (i % 2 == 0) ? 15 : 6;
      event.setIngestTimestamp(new Date(timestamp + offset));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }
    // verify deletion
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      if (i % 2 == 0) {
        assertNotNull("i=" + i, event);
        assertEquals(key * 10, event.getAttributes().get("key"));
      } else {
        assertNull(event);
      }
    }
    for (int i = 128; i < 256; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      if (i % 2 == 0) {
        // successfully overwritten
        assertNotNull(event);
        assertEquals(key, event.getAttributes().get("key"));
      } else {
        // put timestamp was older than deleted entry
        assertNull(event);
      }
    }
    assertEquals(256, cache.count());

    verifyImplementationSpecifics(cache);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPrecondition() {
    createCache(256).get(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPutPreconditionKey() {
    final Event event = new EventJson();
    event.setIngestTimestamp(new Date(getTimestamp()));
    createCache(256).put(null, event, event.getIngestTimestamp().getTime());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPutPreconditionEvent() {
    createCache(256).put(List.of("hello"), null, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeletePrecondition() {
    createCache(256).delete(null, 1234);
  }

  private long getTimestamp() {
    return System.currentTimeMillis() - DefaultContextCache.STALE_TIME_WINDOW - 100;
  }

  @Test
  public void testSimpleOverflow() {
    final long timestamp = getTimestamp();

    ContextCache cache = createCache(64);

    // put entries
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // verify cache
    assertEquals(64, cache.count());
    for (int i = 64; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }
    verifyImplementationSpecifics(cache);
  }

  @Test
  public void testOverflowAfterGet() {
    final long timestamp = getTimestamp();

    ContextCache cache = createCache(128);

    // put entries
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // get even entries
    assertEquals(128, cache.count());
    for (int i = 0; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }

    // put another 64 entries
    for (int i = 128; i < 192; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // check the initial even entries remains and added entries are available
    assertEquals(128, cache.count());
    for (int i = 0; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }
    for (int i = 128; i < 192; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }

    verifyImplementationSpecifics(cache);
  }

  @Test
  public void testOverflowAfterUpdate() {
    final long timestamp = getTimestamp();

    ContextCache cache = createCache(128);

    // put entries
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // update even entries
    assertEquals(128, cache.count());
    for (int i = 0; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp + 10));
      event.getAttributes().put("key", key * 10);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // put another 64 entries
    for (int i = 128; i < 192; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // check the updated even entries remains and added entries are available
    assertEquals(128, cache.count());
    for (int i = 0; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key * 10, event.getAttributes().get("key"));
    }
    for (int i = 128; i < 192; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }

    verifyImplementationSpecifics(cache);
  }

  @Test
  public void testOverflowAfterDelete() {
    final long timestamp = getTimestamp();

    ContextCache cache = createCache(128);

    // put entries
    for (int i = 0; i < 128; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // delete even entries
    assertEquals(128, cache.count());
    for (int i = 0; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      cache.delete(List.of(key), timestamp + 1);
    }

    // put another 64 entries
    for (int i = 128; i < 192; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = new EventJson();
      event.setIngestTimestamp(new Date(timestamp));
      event.getAttributes().put("key", key);
      cache.put(List.of(key), event, event.getIngestTimestamp().getTime());
    }

    // verify that odd entries are flushed
    // (unfortunately, they are older than the deleted entries)
    assertEquals(128, cache.count());
    for (int i = 1; i < 128; i += 2) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNull(String.valueOf(i), event);
    }
    for (int i = 128; i < 192; ++i) {
      final Integer key = Integer.valueOf(i);
      final Event event = cache.get(List.of(key));
      assertNotNull(event);
      assertEquals(key, event.getAttributes().get("key"));
    }

    verifyImplementationSpecifics(cache);
  }

  @Test
  public void testExtractCacheItemAndDeleteAttributes() {
    ContextCache cache = createCache(128);
    final var event = new EventJson();
    event.setEventId(Generators.timeBasedGenerator().generate());
    event.setIngestTimestamp(new Date(Utils.uuidV1TimestampInMillis(event.getEventId())));
    event.set("key", 111L);
    event.set("value", 222L);
    cache.put(List.of(111L), event, event.getIngestTimestamp().getTime());
    final var cachedEvent = cache.get(List.of(111L));
    assertNotNull(cachedEvent);
    assertEquals(111L, cachedEvent.get("key"));
    assertEquals(222L, cachedEvent.get("value"));

    cachedEvent.getAttributes().remove("value");

    final var cachedEvent2 = cache.get(List.of(111L));
    assertNotNull(cachedEvent2);
    assertEquals(111L, cachedEvent2.get("key"));
    assertEquals(222L, cachedEvent2.get("value"));
  }

  @Test
  public void multiThreadTest() throws InterruptedException {
    final ContextCache cache1 = createCache(8192);
    final ContextCache cache2 = createCache(8192);
    final AtomicLong timestamp = new AtomicLong(System.currentTimeMillis());
    final int parallelism = 16;
    CacheBeater[] beaters = new CacheBeater[parallelism];
    for (int outer = 0; outer < 100; ++outer) {
      for (int i = 0; i < parallelism; ++i) {
        beaters[i] = new CacheBeater(cache1, cache2, timestamp, 1024, i);
        beaters[i].start();
      }
      for (CacheBeater beater : beaters) {
        beater.join();
      }
    }
    verifyImplementationSpecifics(cache1);
    verifyImplementationSpecifics(cache2);
    // ConcurrentLinkedHashMapContextCache does not evict entries precisely
    if (!(cache1 instanceof ConcurrentLinkedHashMapContextCache)) {
      assertEquals(cache1.count(), cache2.count());
    }
    int count1 = 0;
    int count2 = 0;
    int count3 = 0;
    for (Object key : cache1.getKeys()) {
      Event value1 = cache1.get(List.of(key));
      Event value2 = cache2.get(List.of(key));
      count1 += (value1 != null) ? 1 : 0;
      count2 += (value2 != null) ? 1 : 0;
      if (value1 != null && value2 != null) {
        ++count3;
        assertEquals(value1.getAttributes().get("key"), value2.getAttributes().get("key"));
      }
    }
    System.out.println(
        "cache1.count()="
            + cache1.count()
            + ", count1="
            + count1
            + ", count2="
            + count2
            + ", count3="
            + count3);
  }
}
