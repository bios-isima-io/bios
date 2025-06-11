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

public class SimpleContextCacheTest extends AbstractContextCacheTest {

  @Override
  protected ContextCache createCache(int size) {
    return new SimpleContextCache();
  }

  @Override
  protected void verifyImplementationSpecifics(ContextCache c) {}

  @Override
  public void testOverflowAfterGet() {
    // SimpleContextCache cannot test this because this implementation does not limit size
  }

  @Override
  public void testOverflowAfterDelete() {
    // SimpleContextCache cannot test this because this implementation does not limit size
  }

  @Override
  public void testOverflowAfterUpdate() {
    // SimpleContextCache cannot test this because this implementation does not limit size
  }

  @Override
  public void testSimpleOverflow() {
    // SimpleContextCache cannot test this because this implementation does not limit size
  }

  @Override
  protected void capacityCheck(ContextCache cache, int size) {
    // capacity check is not possible since this implementation does not limit size
  }
}
