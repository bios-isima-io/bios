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
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestUtils {
  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  public static SignalConfig getSignalConfig(String jsonFile) throws Exception {
    final String streamJson = new String(Files.readAllBytes(Paths.get(jsonFile)));
    final SignalConfig conf = mapper.readValue(streamJson, SignalConfig.class);
    return conf;
  }

  public static ContextConfig getContextConfig(String jsonFile) throws Exception {
    final String streamJson = new String(Files.readAllBytes(Paths.get(jsonFile)));
    final ContextConfig conf = mapper.readValue(streamJson, ContextConfig.class);
    return conf;
  }
}
