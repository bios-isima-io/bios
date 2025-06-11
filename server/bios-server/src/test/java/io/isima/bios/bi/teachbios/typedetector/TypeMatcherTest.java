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
package io.isima.bios.bi.teachbios.typedetector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TypeMatcherTest {

  private TypeMatcher typeMatcher;

  @Before
  public void setUp() {
    typeMatcher = new TypeCastBasedTypeDetector();
  }

  @Test
  public void testAttemptToMatchIntegerTypeIsSuccess() {
    String value = "12";
    boolean result = typeMatcher.tryParseAsDouble(value);
    Assert.assertTrue(result);
  }

  @Test
  public void testAttemptToMatchNonIntegerTypeIsFailure() {
    String value = "kjsnfkn";
    boolean result = typeMatcher.tryParseAsInteger(value);
    Assert.assertFalse(result);
  }

  @Test
  public void testAttemptToMatchFloatTypeIsSuccess() {
    String value = "12.34";
    boolean result = typeMatcher.tryParseAsDouble(value);
    Assert.assertTrue(result);
  }

  @Test
  public void testAttemptToMatchNonFloatTypeIsFailure() {
    String value = "jsdkjd";
    boolean result = typeMatcher.tryParseAsDouble(value);
    Assert.assertFalse(result);
  }
}
