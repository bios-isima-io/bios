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
package io.isima.bios.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SignalValidatorExamplesTest {
  private static ObjectMapper mapper;
  private static Validators validators;

  private SignalConfig exampleSignal;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    mapper = BiosObjectMapperProvider.get();
    validators = new Validators();
  }

  @Before
  public void setup() throws Exception {
    exampleSignal =
        mapper.readValue(
            SignalValidatorExamplesTest.class.getResource("/bios-signal-example-1.json"),
            SignalConfig.class);
  }

  @Test
  public void testFullConfiguration() throws Exception {
    validators.validateSignal("testTenant", exampleSignal);
  }
}
