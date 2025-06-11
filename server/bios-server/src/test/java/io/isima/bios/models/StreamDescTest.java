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
package io.isima.bios.models;

import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.StreamDesc;
import org.junit.Test;

public class StreamDescTest {

  @Test
  public void testHistoryChainLoopDetection0() {
    final StreamDesc one = new StreamDesc("one", System.currentTimeMillis());
    try {
      one.setPrev(one);
      fail("exception must occur");
    } catch (RuntimeException e) {
      // expected
    }
  }

  @Test
  public void testHistoryChainLoopDetection() {
    final StreamDesc two = new StreamDesc("two", System.currentTimeMillis());
    final StreamDesc one = new StreamDesc("one", System.currentTimeMillis());
    one.setPrev(two);
    try {
      two.setPrev(one);
      fail("exception must occur");
    } catch (RuntimeException e) {
      // expected
    }
  }

  @Test
  public void testHistoryChainLoopDetection2() {
    final StreamDesc one = new StreamDesc("one", System.currentTimeMillis());
    final StreamDesc three = new StreamDesc("three", System.currentTimeMillis());
    final StreamDesc two = new StreamDesc("two", System.currentTimeMillis());
    two.setPrev(three);
    three.setPrev(one);
    try {
      one.setPrev(two);
      fail("exception must occur");
    } catch (RuntimeException e) {
      // expected
    }
  }
}
