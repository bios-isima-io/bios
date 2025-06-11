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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.ViewDesc;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class EqualsTestV1 {
  @Test
  public void testViewEqualsHashcodeContract() {
    final View view1 = new Group("groupBy");
    final View view2 = new Group("groupBy");
    final View view3 = new Group("groupBy1");
    objectsToMap(view1, view2, view3);
  }

  @Test
  public void testAttributeDescEqualsHashcodeContract() {
    final AttributeDesc desc1 = new AttributeDesc("test1", InternalAttributeType.LONG);
    final AttributeDesc desc2 = new AttributeDesc("test1", InternalAttributeType.LONG);
    final AttributeDesc desc3 = new AttributeDesc("test1", InternalAttributeType.INT);
    objectsToMap(desc1, desc2, desc3);
  }

  @Test
  public void testAggregateHashcodeContract() {
    final Aggregate agg1 = new Sum("one");
    final Aggregate agg2 = new Sum("one");
    final Aggregate agg3 = new Sum("two");
    objectsToMap(agg1, agg2, agg3);
  }

  @Test
  public void testRollupEqualsHashcodeContract() {
    final Rollup r1 = new Rollup();
    r1.setName("x1");
    r1.setSchemaName("s1");
    r1.setSchemaVersion(1L);
    final Rollup r2 = new Rollup();
    r2.setName("x1");
    r2.setSchemaName("s1");
    r2.setSchemaVersion(1L);
    final Rollup r3 = new Rollup();
    r3.setName("x2");
    r3.setSchemaName("s1");
    r3.setSchemaVersion(1L);
    objectsToMap(r1, r2, r3);
  }

  @Test
  public void testTimeIntervalEqualsHashcodeContract() {
    final TimeInterval i1 = new TimeInterval(10, TimeunitType.MINUTE);
    final TimeInterval i2 = new TimeInterval(10, TimeunitType.MINUTE);
    final TimeInterval i3 = new TimeInterval(10, TimeunitType.SECOND);
    objectsToMap(i1, i2, i3);
  }

  @Test
  public void testViewDescEqualsHashcodeContract() {
    final ViewDesc v1 = new ViewDesc();
    v1.setName("s1");
    v1.setAttributes(Arrays.asList("a1", "a2"));
    final ViewDesc v2 = new ViewDesc();
    v2.setName("s1");
    v2.setAttributes(Arrays.asList("a1", "a2"));
    final ViewDesc v3 = new ViewDesc();
    v3.setName("s1");
    v3.setAttributes(Arrays.asList("a1", "a3"));
    objectsToMap(v1, v2, v3);
  }

  private <T> void objectsToMap(T o1, T o2, T o3) {
    assertEquals(o1, o2);
    assertNotEquals(o1, o3);
    Map<T, Integer> testMap = new HashMap<>();
    testMap.put(o1, 1);
    testMap.put(o2, 2);
    assertThat(testMap.size(), is(1));
    assertThat(testMap.get(o1), is(2));
    assertThat(testMap.get(o2), is(2));
    testMap.put(o3, 3);
    assertThat(testMap.size(), is(2));
    assertThat(testMap.get(o1), is(2));
    assertThat(testMap.get(o3), is(3));
  }
}
