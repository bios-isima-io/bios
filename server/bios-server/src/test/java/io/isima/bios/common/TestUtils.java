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
package io.isima.bios.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;

public class TestUtils {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  public static final String SIMPLE_ALL_TYPES_SIGNAL = "signalAllTypes.json";

  public static SignalConfig loadSignalJson(String resourceFileName)
      throws JsonParseException, JsonMappingException, IOException {
    final var signal =
        mapper.readValue(TestUtils.class.getResource("/" + resourceFileName), SignalConfig.class);
    return signal;
  }

  public static StreamDesc loadStreamJson(String resourceFileName)
      throws JsonParseException, JsonMappingException, IOException {
    final var signal =
        mapper.readValue(TestUtils.class.getResource("/" + resourceFileName), StreamDesc.class);
    signal.compile();
    return signal;
  }
}
