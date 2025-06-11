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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExtractObjectValidatorsTest {
  private static Validator validator;

  @BeforeClass
  public static void setUpClass() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Test
  public void testExtractRequest() {
    ExtractRequest request = new ExtractRequest();

    // attributes may not include empty names
    request.setAttributes(new ArrayList<>());
    request.getAttributes().add("");
    Set<ConstraintViolation<ExtractRequest>> constraintViolations = validator.validate(request);
    assertFalse(constraintViolations.isEmpty());

    // attributes may not have duplicate names
    request.setAttributes(new ArrayList<>());
    request.getAttributes().add("twin");
    request.getAttributes().add("twin");
    constraintViolations = validator.validate(request);
    assertFalse(constraintViolations.isEmpty());

    // this is valid one
    request.setAttributes(new ArrayList<>());
    request.getAttributes().add("bob");
    request.getAttributes().add("alice");
    constraintViolations = validator.validate(request);
    assertTrue(constraintViolations.isEmpty());
  }
}
