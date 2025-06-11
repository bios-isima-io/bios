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
package io.isima.bios.server;

import static org.junit.Assert.assertEquals;

import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Http2FrameComparatorTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  /**
   * The purpose of this test is to understand the difference of performance between two approaches
   * to check HTTP2 frame types.
   *
   * <dl>
   *   <dt>Approach 1: Compare {@link Http2Frame.name()} values
   *   <dd>These are defined as constsnts in default implementations, then the <code>compare</code>
   *       method compares references, which is lightweight than string content comparison.
   *   <dt>Approach 2: Use <code>instanceof</code> operator
   *   <dd>This used to be slow in old version of Java, but it has been improved in later versions.
   * </dl>
   *
   * <p>Results is that<br>
   * The execution time changes depending on number of iterations, compare / instanceof comparisons
   * stay in the same range, such as 122 ns / 272 ns (500 iterations), 130 ns / 67 ns (1,000
   * iterations), 94/65 (10,000 iteration), 79/66 (100,000 iterations), 36 ns / 45 ns (1,000,000
   * iterations). The observations are:
   *
   * <ul>
   *   <li>Average execution time gets shorter when number of iterations increases
   *   <li>The <code>instanceof</code> operator is faster with fewer iterations
   *   <li>The <code>equals()</code> method gets faster as number of iterations grows
   * </ul>
   *
   * <p>which means the instanceof operator gives more predictable execution time. Also verified
   * that instanceof operator runs some hidden byte code or native method (it's a part of language)
   * while the <code>equals</code> operator consists of a Java code lines. The instanceof operator
   * should give more stable execution time potentially.
   *
   * <p>Based on the results above, the class {@link Http2FrameHandler} would use the <code>
   * instanceof</code> operator to distinguish incoming frame types.
   */
  @Test
  public void test() {
    final var headers = new DefaultHttp2Headers();
    final Http2Frame reference = new DefaultHttp2HeadersFrame(headers);
    final String referenceName = reference.name();
    final int count = 100000;
    long elapsedEquals = 0;
    long elapsedInstanceof = 0;
    boolean warmingup = true;
    for (int i = 0; i < count; ++i) {
      boolean isEven = i % 2 == 0;
      final Http2Frame frame;
      if (isEven) {
        frame = new DefaultHttp2HeadersFrame(headers);
      } else {
        frame = new DefaultHttp2DataFrame(false);
      }

      final long time0 = System.nanoTime();
      boolean equals = referenceName.equals(frame.name());
      final long time2 = System.nanoTime();
      if (!warmingup) {
        elapsedEquals += time2 - time0;
      }

      final long time1 = System.nanoTime();
      boolean isInstance = frame instanceof Http2HeadersFrame;
      final long time3 = System.nanoTime();
      if (!warmingup) {
        elapsedInstanceof += time3 - time1;
      }

      assertEquals(isEven, equals);
      assertEquals(isEven, isInstance);
      warmingup = false;
    }
    System.out.println("equals     : elapsed = " + elapsedEquals / count + " ns");
    System.out.println("instanceof : elapsed = " + elapsedInstanceof / count + " ns");
  }
}
