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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.TfosObjectMapperProvider;
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
public class UpdateContextEntryValidatorTest {
  private static Validator validator;
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    validator = Validation.buildDefaultValidatorFactory().getValidator();
    mapper = TfosObjectMapperProvider.get();
  }

  @Parameter(0)
  public String src;

  @Parameter(1)
  public Boolean expected;

  /**
   * Provides test parameters.
   *
   * @return Collection of (Source JSON for an UpdateContextEntry object, expected validation
   *     result)
   */
  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"{'key':'the_key','attributes':{'one':1,'two':'2'}}", Boolean.TRUE},
          {"{'key':'the_key'}", Boolean.FALSE},
          {"{'attributes':{'one':1,'two':'2'}}", Boolean.FALSE},
        });
  }

  @Test
  public void testEventAttributeReplaceInfo() throws Exception {
    UpdateContextEntry obj = mapper.readValue(src.replace("'", "\""), UpdateContextEntry.class);

    final Set<ConstraintViolation<UpdateContextEntry>> violations = validator.validate(obj);
    assertEquals(violations.toString(), expected, violations.isEmpty());
  }
}
