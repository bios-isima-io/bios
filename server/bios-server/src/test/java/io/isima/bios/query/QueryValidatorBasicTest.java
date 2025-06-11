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
package io.isima.bios.query;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.v1.StreamConfig;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Test;

/** Various query validator test cases. */
public class QueryValidatorBasicTest extends QueryBaseTest {

  @Test
  public void testSimpleValidation() throws ValidatorException {
    StreamConfig config = buildStreamConfig(1234L, 2, 2);
    SelectStatement query = buildQuery(2, 2, 0, false).queries().get(0);

    QueryValidator validator = new TfosQueryValidator(config, null);
    validator.validate(query, 0, null);
  }

  @Test
  public void testErrorValidation() {
    StreamConfig config = buildStreamConfig(1234L, 1, 2);
    SelectStatement query = buildQuery(2, 2, 0, false).queries().get(0);

    QueryValidator validator = new TfosQueryValidator(config, null);
    try {
      validator.validate(query, 0, null);
      fail("Query validation must fail");
    } catch (ValidatorException e) {
      MatcherAssert.assertThat(e.getMessage(), containsString(ATTRIBUTE_NAME_PREFIX + 1));
    }
  }
}
