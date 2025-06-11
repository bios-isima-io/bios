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
package io.isima.bios.upgrade.common;

import static org.hamcrest.CoreMatchers.is;

import io.isima.bios.common.BiosConstants;
import io.isima.bios.errors.exception.FileReadException;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Test;

public class UpgradeConfigLoaderTest {

  @Test
  public void testConfigLoader() throws FileReadException {
    final var config = UpgradeConfigLoader.load();
    MatcherAssert.assertThat(config.size(), Matchers.greaterThanOrEqualTo(1));
    MatcherAssert.assertThat(
        config.get(0).getUpgradeCommands().get(0).getTenantName(), is(BiosConstants.TENANT_SYSTEM));
  }
}
