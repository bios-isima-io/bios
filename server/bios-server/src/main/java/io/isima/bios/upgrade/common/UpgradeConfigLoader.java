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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.upgrade.models.UpgradeVersionConfig;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class UpgradeConfigLoader {
  private static final String UPGRADE_CMD_FILE = "upgrade.json";
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  public static List<UpgradeVersionConfig> load() throws FileReadException {
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(UPGRADE_CMD_FILE)) {
      UpgradeVersionConfig[] configs = mapper.readValue(inputStream, UpgradeVersionConfig[].class);
      return Arrays.asList(configs);
    } catch (IOException e) {
      throw new FileReadException("Failed to Upgrade config", e);
    }
  }
}
