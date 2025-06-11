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
package io.isima.bios.data.impl.maintenance;

import static org.junit.Assert.assertEquals;

import io.isima.bios.models.LastNItem;
import java.util.List;
import org.junit.Test;

public class DataEngineMaintenanceTest {

  @Test
  public void testMergeNewItemSimple() {
    final var collection = List.of(new LastNItem(10L, "one"), new LastNItem(20L, "two"));
    final var newItem = new LastNItem(30L, "three");
    final var newCollection = LastNCollector.mergeNewItem(collection, newItem, 5, 10);
    assertEquals(3, newCollection.size());
    assertEquals(new LastNItem(10L, "one"), newCollection.get(0));
    assertEquals(new LastNItem(20L, "two"), newCollection.get(1));
    assertEquals(new LastNItem(30L, "three"), newCollection.get(2));
  }

  @Test
  public void testMergeNewItemInsert() {
    final var collection =
        List.of(new LastNItem(10L, "one"), new LastNItem(20L, "two"), new LastNItem(30L, "three"));
    final var newItem = new LastNItem(25L, "twentyFive");
    final var newCollection = LastNCollector.mergeNewItem(collection, newItem, 5, 10);
    assertEquals(4, newCollection.size());
    assertEquals(new LastNItem(10L, "one"), newCollection.get(0));
    assertEquals(new LastNItem(20L, "two"), newCollection.get(1));
    assertEquals(new LastNItem(25L, "twentyFive"), newCollection.get(2));
    assertEquals(new LastNItem(30L, "three"), newCollection.get(3));
  }

  @Test
  public void testMergeNewItemConflict() {
    final var collection =
        List.of(new LastNItem(10L, "one"), new LastNItem(20L, "two"), new LastNItem(30L, "three"));
    final var newItem = new LastNItem(20L, "two");
    final var newCollection = LastNCollector.mergeNewItem(collection, newItem, 5, 10);
    assertEquals(3, newCollection.size());
    assertEquals(new LastNItem(10L, "one"), newCollection.get(0));
    assertEquals(new LastNItem(20L, "two"), newCollection.get(1));
    assertEquals(new LastNItem(30L, "three"), newCollection.get(2));
  }

  @Test
  public void testMergeNewItemTimestampConflict() {
    final var collection =
        List.of(new LastNItem(10L, "one"), new LastNItem(20L, "two"), new LastNItem(30L, "three"));
    final var newItem = new LastNItem(20L, "two.one");
    final var newCollection = LastNCollector.mergeNewItem(collection, newItem, 5, 10);
    assertEquals(4, newCollection.size());
    assertEquals(new LastNItem(10L, "one"), newCollection.get(0));
    assertEquals(new LastNItem(20L, "two.one"), newCollection.get(1));
    assertEquals(new LastNItem(20L, "two"), newCollection.get(2));
    assertEquals(new LastNItem(30L, "three"), newCollection.get(3));
  }

  @Test
  public void testMergeNewItemTimestampTimeLimit() {
    final var collection =
        List.of(new LastNItem(10L, "one"), new LastNItem(20L, "two"), new LastNItem(30L, "three"));
    final var newItem = new LastNItem(40L, "four");
    final var newCollection = LastNCollector.mergeNewItem(collection, newItem, 15, 10);
    assertEquals(3, newCollection.size());
    assertEquals(new LastNItem(20L, "two"), newCollection.get(0));
    assertEquals(new LastNItem(30L, "three"), newCollection.get(1));
    assertEquals(new LastNItem(40L, "four"), newCollection.get(2));
  }

  @Test
  public void testMergeNewItemSizeLimit() {
    final var collection =
        List.of(
            new LastNItem(10L, "one"),
            new LastNItem(20L, "two"),
            new LastNItem(30L, "three"),
            new LastNItem(40L, "four"),
            new LastNItem(50L, "five"));
    final var newItem = new LastNItem(60L, "six");
    final var newCollection = LastNCollector.mergeNewItem(collection, newItem, 5, 5);
    assertEquals(5, newCollection.size());
    assertEquals(new LastNItem(20L, "two"), newCollection.get(0));
    assertEquals(new LastNItem(30L, "three"), newCollection.get(1));
    assertEquals(new LastNItem(40L, "four"), newCollection.get(2));
    assertEquals(new LastNItem(50L, "five"), newCollection.get(3));
    assertEquals(new LastNItem(60L, "six"), newCollection.get(4));
  }
}
