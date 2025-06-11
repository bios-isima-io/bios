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
package io.isima.bios.admin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceAllocatorTest {

  private static ResourceAllocator resourceAllocator;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(ResourceAllocatorTest.class);
    resourceAllocator = BiosModules.getResourceAllocator();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    resourceAllocator.clearAllocation(ResourceType.TEST);
    resourceAllocator.clearAllocation(ResourceType.TEST2);
  }

  @Test
  public void testResourceAssignment() throws Exception {
    final var start = new Date();
    resourceAllocator.assignResource(ResourceType.TEST, "1000", "one thousand");
    resourceAllocator.assignResource(ResourceType.TEST, "1001", "one thousand one");
    resourceAllocator.assignResource(ResourceType.TEST, "1010", "one thousand ten");

    final var allocated = resourceAllocator.checkResource(ResourceType.TEST, "1000");
    assertThat(allocated.getResourceType(), is(ResourceType.TEST));
    assertThat(allocated.getResource(), is("1000"));
    assertThat(allocated.getResourceUser(), is("one thousand"));
    assertFalse(allocated.getUpdatedAt().before(start));

    assertNull(resourceAllocator.checkResource(ResourceType.TEST, "2001"));

    assertThrows(
        AlreadyExistsException.class,
        () -> resourceAllocator.assignResource(ResourceType.TEST, "1000", "trying again"));
    resourceAllocator.assignResource(ResourceType.TEST2, "1000", "different type");

    final int assigned =
        resourceAllocator.allocateIntegerResource(ResourceType.TEST, 1005, "one thousand five");
    assertThat(assigned, is(1005));
    final int assinged2 =
        resourceAllocator.allocateIntegerResource(
            ResourceType.TEST, 1005, "one thousand five again");
    assertThat(assinged2, is(1011));
  }

  @Test
  public void testResourceReleasing() throws Exception {
    resourceAllocator.assignResource(ResourceType.TEST, "1000", "first");
    resourceAllocator.releaseResource(ResourceType.TEST, "1000", 0);
    resourceAllocator.assignResource(ResourceType.TEST, "1000", "second");
    resourceAllocator.releaseResource(ResourceType.TEST, "1000", 10);
    assertThrows(
        AlreadyExistsException.class,
        () -> resourceAllocator.assignResource(ResourceType.TEST, "1000", "second"));
    Thread.sleep(15000);
    resourceAllocator.assignResource(ResourceType.TEST, "1000", "second");
  }

  @Test
  public void integerAllocatorTest() throws Exception {
    final var inputQueue = new ConcurrentLinkedDeque<Integer[]>();
    final int totalResources = 256;
    for (int i = 0; i < totalResources; ++i) {
      inputQueue.add(new Integer[1]);
    }

    final var outputQueue = new ConcurrentLinkedDeque<Integer[]>();
    final var errors = new ConcurrentLinkedDeque<String>();
    final var threads = new ArrayList<Thread>();
    for (int i = 0; i < 2; ++i) {
      final int index = i;
      final var thread =
          new Thread() {
            @Override
            public void run() {
              int count = 0;
              while (true) {
                final var holder = inputQueue.poll();
                if (holder == null) {
                  return;
                }
                try {
                  ++count;
                  holder[0] =
                      resourceAllocator.allocateIntegerResource(
                          ResourceType.TEST, 9001, "whatever");
                  outputQueue.add(holder);
                } catch (ApplicationException e) {
                  e.printStackTrace();
                  errors.add(
                      String.format("thread=%d, countInThread=%d, error=%s", index, count, e));
                }
              }
            }
          };
      threads.add(thread);
    }

    threads.forEach((thread) -> thread.start());
    threads.forEach(
        (thread) -> {
          try {
            thread.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
    if (!errors.isEmpty()) {
      throw new Exception(String.format("Error(s) encountered during the test: %s", errors));
    }
    assertThat(outputQueue.size(), is(totalResources));
    final var taken = new HashSet<Integer>();
    for (var item : outputQueue) {
      var value = item[0];
      assertThat(value, greaterThanOrEqualTo(9001));
      assertThat(value, lessThan(9001 + totalResources));
      assertFalse(taken.contains(value));
      taken.add(value);
    }
  }
}
