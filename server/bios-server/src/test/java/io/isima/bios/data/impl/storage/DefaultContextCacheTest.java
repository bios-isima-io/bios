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

public class DefaultContextCacheTest extends AbstractContextCacheTest {

  @Override
  protected ContextCache createCache(int size) {
    return new DefaultContextCache(size);
  }

  @Override
  protected void verifyImplementationSpecifics(ContextCache c) {
    DefaultContextCache cache = (DefaultContextCache) c;
    int count = 0;
    for (ContextCacheItem item = cache.head.next; item != cache.tail; item = item.next) {
      ++count;
      assertNotNull(cache.cache.get(item.getKey()));
    }
    assertEquals(cache.count(), count);
  }
}
