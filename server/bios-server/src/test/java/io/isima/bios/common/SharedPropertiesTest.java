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
package io.isima.bios.common;

import static io.isima.bios.common.SharedProperties.SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.isima.bios.TestUtils;
import io.isima.bios.exceptions.ApplicationException;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SharedPropertiesTest {

  private final List<String> keys = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    keys.clear();
  }

  @After
  public void tearDown() throws Exception {
    for (String key : keys) {
      SharedProperties.getInstance().deleteProperty(key);
    }
    SharedProperties.clearCache();
  }

  @Test
  public void testCachedGetProperty() throws ApplicationException {
    final String key = "hello";
    final String nonexistentKey = "nonexistentKey";
    final String nonexistentKey2 = "number";
    final String nonexistentKey3 = "bool";
    long cacheExpiry = 100L;
    keys.add(SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY);
    SharedProperties.getInstance()
        .setProperty(SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY, Long.toString(cacheExpiry));
    SharedProperties.clearCache();
    keys.add(key);
    SharedProperties.getInstance().setProperty(key, "world");
    assertEquals("world", SharedProperties.get(key));
    var beforeCachedGet = System.currentTimeMillis();
    assertEquals("world", SharedProperties.getCached(key));
    SharedProperties.getInstance().setProperty(key, "another world!");
    var expectedToBeCachedValue = SharedProperties.getCached(key);
    var getAfterSet = System.currentTimeMillis();
    if ((getAfterSet - beforeCachedGet) < cacheExpiry) {
      assertEquals("world", expectedToBeCachedValue);
    }

    beforeCachedGet = System.currentTimeMillis();
    assertNull(SharedProperties.getCached(nonexistentKey));
    keys.add(nonexistentKey);
    SharedProperties.getInstance().setProperty(nonexistentKey, "Now it exists!");
    expectedToBeCachedValue = SharedProperties.getCached(nonexistentKey);
    var expectedDefaultValue = SharedProperties.getCached(nonexistentKey, "default1");
    getAfterSet = System.currentTimeMillis();
    if ((getAfterSet - beforeCachedGet) < cacheExpiry) {
      assertNull(expectedToBeCachedValue);
      assertEquals("default1", expectedDefaultValue);
    }

    beforeCachedGet = System.currentTimeMillis();
    assertNull(SharedProperties.getCached(nonexistentKey2));
    keys.add(nonexistentKey2);
    SharedProperties.getInstance().setProperty(nonexistentKey2, "1234567890123456");
    expectedToBeCachedValue = SharedProperties.getCached(nonexistentKey2);
    Long expectedDefaultValueLong = SharedProperties.getCached(nonexistentKey2, 123L);
    getAfterSet = System.currentTimeMillis();
    if ((getAfterSet - beforeCachedGet) < cacheExpiry) {
      assertNull(expectedToBeCachedValue);
      assertEquals(123L, (long) expectedDefaultValueLong);
    }

    beforeCachedGet = System.currentTimeMillis();
    assertNull(SharedProperties.getCached(nonexistentKey3));
    keys.add(nonexistentKey3);
    SharedProperties.getInstance().setProperty(nonexistentKey3, "true");
    expectedToBeCachedValue = SharedProperties.getCached(nonexistentKey3);
    Boolean expectedDefaultValueBoolean = SharedProperties.getCached(nonexistentKey3, Boolean.TRUE);
    getAfterSet = System.currentTimeMillis();
    if ((getAfterSet - beforeCachedGet) < cacheExpiry) {
      assertNull(expectedToBeCachedValue);
      assertEquals(Boolean.TRUE, expectedDefaultValueBoolean);
    }

    TestUtils.sleepAtLeast(cacheExpiry);
    assertEquals("another world!", SharedProperties.getCached(key));
    assertEquals("Now it exists!", SharedProperties.getCached(nonexistentKey));
    assertEquals("Now it exists!", SharedProperties.getCached(nonexistentKey, "default2"));
    assertEquals(1234567890123456L, Long.parseLong(SharedProperties.getCached(nonexistentKey2)));
    assertEquals((Long) 1234567890123456L, SharedProperties.getCached(nonexistentKey2, 5L));
    assertEquals((Long) 1234567890123456L, SharedProperties.getCachedLong(nonexistentKey2));
    assertEquals(Boolean.TRUE, Boolean.parseBoolean(SharedProperties.getCached(nonexistentKey3)));
    assertEquals(Boolean.TRUE, SharedProperties.getCached(nonexistentKey3, Boolean.FALSE));
    assertEquals(Boolean.TRUE, SharedProperties.getCachedBoolean(nonexistentKey3));
  }

  @Test
  public void testNonStringValues() throws ApplicationException {
    final String propInteger = "integer";
    final String propLong = "long";
    final String propCachedLong = "clong";
    final String propCachedBoolean = "boolean";

    // test default values for null entries
    SharedProperties.clearCache();
    assertThat(SharedProperties.getInteger(propInteger, -1), is(-1));
    assertThat(SharedProperties.getLong(propLong, -1L), is(-1L));
    assertThat(SharedProperties.getCachedLong(propCachedLong), is(nullValue()));
    assertThat(SharedProperties.getCachedBoolean(propCachedBoolean), is(nullValue()));

    // test set values
    SharedProperties.getInstance().setProperty(propInteger, "111");
    SharedProperties.getInstance().setProperty(propLong, "222");
    SharedProperties.getInstance().setProperty(propCachedLong, "333");
    SharedProperties.getInstance().setProperty(propCachedBoolean, "true");

    SharedProperties.clearCache();
    assertThat(SharedProperties.getInteger(propInteger, -1), is(111));
    assertThat(SharedProperties.getLong(propLong, -1L), is(222L));
    assertThat(SharedProperties.getCachedLong(propCachedLong), is(333L));
    assertThat(SharedProperties.getCachedBoolean(propCachedBoolean), is(true));

    // test default values for blank entries
    SharedProperties.getInstance().setProperty(propInteger, "");
    SharedProperties.getInstance().setProperty(propLong, " ");
    SharedProperties.getInstance().setProperty(propCachedLong, "");
    SharedProperties.getInstance().setProperty(propCachedBoolean, " ");

    SharedProperties.clearCache();
    assertThat(SharedProperties.getInteger(propInteger, -1), is(-1));
    assertThat(SharedProperties.getLong(propLong, -1L), is(-1L));
    assertThat(SharedProperties.getCachedLong(propCachedLong), is(nullValue()));
    assertThat(SharedProperties.getCachedBoolean(propCachedBoolean), is(nullValue()));
  }
}
