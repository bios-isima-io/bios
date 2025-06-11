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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class ContextCassStreamTest {

  @Test
  public void cacheLimitationTest() {

    final int defaultLimitation = 524288;
    final String conf1 = "test.testContext1:10000, " + "test2.testContext2:20000," + "* : 3000";
    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test", "testContext1", "node1"),
        is(10000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test2", "testContext2", "host10"),
        is(20000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test", "testContext3", "host10"),
        is(3000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "testx", "testContext2", "host10"),
        is(3000));
  }

  @Test
  public void cacheLimitationTestWithHostSpec() {

    final int defaultLimitation = 524288;
    final String conf1 =
        "test.testContext1:10000/host10/host20,"
            + "test.testContext1:5000,"
            + "test2.testContext2:20000/host40/host20,"
            + "*:3000";
    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test", "testContext1", "node1"),
        is(5000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test", "testContext1", "host10"),
        is(10000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test2", "testContext2", "host40"),
        is(20000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test2", "testContext2", "host50"),
        is(3000));

    assertThat(
        ContextCassStream.determineContextCacheLimit(conf1, "test", "testContext3", "host10"),
        is(3000));
  }
}
