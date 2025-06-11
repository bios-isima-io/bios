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
package io.isima.bios.dto;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class EventAttributeReplaceInfoValidatorTest {
  private static Validator validator;

  @BeforeClass
  public static void setUpClass() {
    validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Parameter(0)
  public String attributeName;

  @Parameter(1)
  public String oldValue;

  @Parameter(2)
  public String newValue;

  @Parameter(3)
  public Boolean expected;

  /**
   * Provides test parameters.
   *
   * @return collection of (attributeName, oldValue, newValue, expected validation result)
   */
  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"attrName", "value_old", "value_new", Boolean.TRUE},
          {"attrName", "value_old", null, Boolean.FALSE},
          {"attrName", null, "value_new", Boolean.FALSE},
          {null, "value_old", "value_new", Boolean.FALSE},
        });
  }

  @Test
  public void testEventAttributeReplaceInfo() {
    final EventAttributeReplaceInfo info = new EventAttributeReplaceInfo();
    info.setAttributeName(attributeName);
    info.setOldValue(oldValue);
    info.setNewValue(newValue);

    final Set<ConstraintViolation<EventAttributeReplaceInfo>> violations = validator.validate(info);
    assertEquals(violations.toString(), expected, violations.isEmpty());
  }
}
