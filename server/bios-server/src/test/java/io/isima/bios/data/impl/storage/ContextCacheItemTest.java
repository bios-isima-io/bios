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
import static org.junit.Assert.assertSame;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import org.junit.Test;

public class ContextCacheItemTest {

  @Test
  public void testBasic() {
    final Event event = new EventJson();
    ContextCacheItem item = new ContextCacheItem("hello", event, 12345);
    assertNotNull(item);
    assertEquals("hello", item.getKey());
    assertSame(event, item.getValue());
    assertEquals(12345, item.getVersion());
    assertNull(item.prev);
    assertNull(item.next);
  }

  @Test
  public void tesChainOperations() {
    final ContextCacheItem head = new ContextCacheItem("head", null, 0);
    final ContextCacheItem tail = new ContextCacheItem("tail", null, 0);
    head.next = tail;
    tail.prev = tail;
    final ContextCacheItem first = new ContextCacheItem("first", null, 0);

    head.insertAfter(first);
    assertSame(first, head.next);
    assertSame(head, first.prev);
    assertSame(tail, first.next);
    assertSame(first, tail.prev);

    final ContextCacheItem second = new ContextCacheItem("second", null, 0);
    first.insertAfter(second);
    assertSame(head, first.prev);
    assertSame(second, first.next);
    assertSame(first, second.prev);
    assertSame(tail, second.next);
    assertSame(second, tail.prev);

    final ContextCacheItem third = new ContextCacheItem("third", null, 0);
    second.insertAfter(third);

    second.removeFromChain();
    assertSame(third, first.next);
    assertSame(head, first.prev);
    assertSame(first, third.prev);
    assertSame(tail, third.next);
  }
}
