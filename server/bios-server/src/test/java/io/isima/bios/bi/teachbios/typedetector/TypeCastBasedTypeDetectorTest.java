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

import io.isima.bios.models.v1.InternalAttributeType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TypeCastBasedTypeDetectorTest {

  private TypeCastBasedTypeDetector typeCastBasedTypeDetector;

  @Before
  public void setUp() {
    typeCastBasedTypeDetector = new TypeCastBasedTypeDetector();
  }

  @Test
  public void testIntegerStreamIsCorrectlyDetected() {
    List<String> valueList = new ArrayList<>(Arrays.asList("1", "2", "3", "4"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.INT, detectedType);
  }

  @Test
  public void testIntegerStreamWithMissingValuesIsCorrectlyDetected() {
    List<String> valueList = new ArrayList<>(Arrays.asList("1", "2", "", null, "3"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.INT, detectedType);
  }

  @Test
  public void testIntegerStreamWithOneNonIntegerTypeIsDetectedAsString() {
    List<String> valueList = new ArrayList<>(Arrays.asList("1", "2", "abc", "3"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertNotEquals(InternalAttributeType.NUMBER, detectedType);
    Assert.assertEquals(InternalAttributeType.STRING, detectedType);
  }

  @Test
  public void testDoubleStreamIsCorrectlyDetected() {
    List<String> valueList = new ArrayList<>(Arrays.asList("12.32", "43.2"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.DOUBLE, detectedType);
  }

  @Test
  public void testDoubleStreamWithMissingValuesIsCorrectlyDetected() {
    List<String> valueList = new ArrayList<>(Arrays.asList("1.23", "43.21", "", "56.7"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.DOUBLE, detectedType);
  }

  @Test
  public void testStreamWithArbitraryValuesIsDetectedAsString() {
    List<String> valueList = new ArrayList<>(Arrays.asList("jnsjf", "2", "3.4", "aesf", "", null));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.STRING, detectedType);
  }

  @Test
  public void testBoolean() {
    List<String> valueList = new ArrayList<>(Arrays.asList("true", "false", "TRUE", "FALSE"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.BOOLEAN, detectedType);
  }

  @Test
  public void testBooleanIncludingNull() {
    List<String> valueList = new ArrayList<>(Arrays.asList("true", "false", null, "TRUE", "FALSE"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.BOOLEAN, detectedType);
  }

  @Test
  public void testBooleanIncludingEmpty() {
    List<String> valueList = new ArrayList<>(Arrays.asList("true", "false", "", "TRUE", "FALSE"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.BOOLEAN, detectedType);
  }

  @Test
  public void testBooleanIncludingInteger() {
    List<String> valueList = new ArrayList<>(Arrays.asList("true", "false", "123", "TRUE"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.STRING, detectedType);
  }

  @Test
  public void testBooleanIncludingDouble() {
    List<String> valueList = new ArrayList<>(Arrays.asList("true", "false", "123.45", "TRUE"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.STRING, detectedType);
  }

  @Test
  public void testBooleanIncludingString() {
    List<String> valueList = new ArrayList<>(Arrays.asList("true", "false", "yes", "TRUE"));
    InternalAttributeType detectedType = typeCastBasedTypeDetector.inferDataType(valueList);
    Assert.assertEquals(InternalAttributeType.STRING, detectedType);
  }
}
