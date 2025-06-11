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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class BiosVersionTest {
  @Test
  public void testNullVersion() {
    BiosVersion v = new BiosVersion(null);
    assertFalse(v.isSnapshotVersion());
    assertThat(v.toString(), is("0.0.0"));
  }

  @Test
  public void testEmptyVersion() {
    BiosVersion v = new BiosVersion("");
    assertFalse(v.isSnapshotVersion());
    assertThat(v.toString(), is("0.0.0"));
  }

  @Test
  public void testSnapshotVersion() {
    BiosVersion v = new BiosVersion("1000.900.340-SNAPSHOT");
    assertTrue(v.isSnapshotVersion());
    assertThat(v.toString(), is("1000.900.340-SNAPSHOT"));
  }

  @Test
  public void testHotFixVersion() {
    BiosVersion v1 = new BiosVersion("0.9.58-SNAPSHOT-2021040907-75ada34");
    BiosVersion v2 = new BiosVersion("0.9.58-SNAPSHOT-2021040908-0101101");
    BiosVersion v3 = new BiosVersion("0.9.58-SNAPSHOT-2021040907-75ada34");
    assertThat(v1.getMajor(), is(0));
    assertThat(v1.getMinor(), is(9));
    assertThat(v1.getBuild(), is(58));
    assertThat(v1.getSnapshot(), is("SNAPSHOT-2021040907-75ada34"));
    assertTrue(v1.isSnapshotVersion());
    assertThat(v1.toString(), is("0.9.58-SNAPSHOT-2021040907-75ada34"));
    assertThat(v1.compareTo(v2), is(-1));
    assertThat(v2.compareTo(v1), is(1));
    assertThat(v1.compareTo(v3), is(0));
    assertNotEquals(v1, v2);
    assertEquals(v1, v3);
    assertNotEquals(v1.hashCode(), v2.hashCode());
    assertEquals(v1.hashCode(), v3.hashCode());
  }

  @Test
  public void testVersionCompareGreaterOrLesser() {
    BiosVersion v1 = new BiosVersion(null);
    BiosVersion v2 = new BiosVersion("0.8.31");
    BiosVersion v3 = new BiosVersion("0.008.31.00569");
    BiosVersion v4 = new BiosVersion("0.09.31");
    BiosVersion v5 = new BiosVersion("1.009.31");

    assertThat(v1.compareTo(v2), is(-1));
    assertThat(v1.compareTo(v3), is(-1));
    assertThat(v1.compareTo(v4), is(-1));
    assertThat(v1.compareTo(v5), is(-1));

    assertThat(v2.compareTo(v1), is(1));
    assertThat(v2.compareTo(v3), is(-1));
    assertThat(v2.compareTo(v4), is(-1));
    assertThat(v2.compareTo(v5), is(-1));

    assertThat(v3.compareTo(v1), is(1));
    assertThat(v3.compareTo(v2), is(1));
    assertThat(v3.compareTo(v4), is(-1));
    assertThat(v3.compareTo(v5), is(-1));

    assertThat(v4.compareTo(v1), is(1));
    assertThat(v4.compareTo(v2), is(1));
    assertThat(v4.compareTo(v3), is(1));
    assertThat(v4.compareTo(v5), is(-1));

    assertThat(v5.compareTo(v1), is(1));
    assertThat(v5.compareTo(v2), is(1));
    assertThat(v5.compareTo(v3), is(1));
    assertThat(v5.compareTo(v4), is(1));

    assertNotEquals(v1, v2);
    assertNotEquals(v1, v3);
    assertNotEquals(v1, v4);
    assertNotEquals(v1, v5);
  }

  @Test
  public void testVersionCompareEqual() {
    BiosVersion v1 = new BiosVersion("0.1.34-SNAPSHOT");
    BiosVersion v2 = new BiosVersion("0.1.34");
    BiosVersion v3 = new BiosVersion("0.1.34");
    assertThat(v1.compareTo(v2), is(-1));
    assertThat(v2.compareTo(v1), is(1));
    assertThat(v2.compareTo(v3), is(0));
    assertNotEquals(v1, v2);
    assertNotEquals(v1, v3);
    assertEquals(v2, v3);
    assertNotEquals(v1.hashCode(), v2.hashCode());
    assertNotEquals(v1.hashCode(), v3.hashCode());
    assertEquals(v2.hashCode(), v3.hashCode());
  }

  @Test
  public void testVersionCompareEqualForFuture() {
    BiosVersion v1 = new BiosVersion("1000.1.34-SNAPSHOT");
    BiosVersion v2 = new BiosVersion("1000.01.34");
    BiosVersion v3 = new BiosVersion("1000.0001.34");
    assertThat(v1.compareTo(v2), is(-1));
    assertThat(v2.compareTo(v1), is(1));
    assertThat(v1.compareTo(v3), is(-1));
    assertNotEquals(v1, v2);
    assertNotEquals(v1, v3);
    assertEquals(v2, v3);
  }

  @Test
  public void testIllegalVersion() {
    try {
      new BiosVersion("0.7.34.56.78.77.79");
      fail("Should not parse illegal version string");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("0.7"));
    }
  }

  @Test
  public void testIllegalDigitInVersion() {
    try {
      new BiosVersion("0.7.34s");
      fail("Should not parse illegal version string");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("0.7"));
    }
  }
}
