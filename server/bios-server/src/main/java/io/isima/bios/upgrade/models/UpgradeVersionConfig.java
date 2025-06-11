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
package io.isima.bios.upgrade.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpgradeVersionConfig {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  public UpgradeVersionConfig() {}

  /** Target Version. */
  @JsonProperty("targetVersion")
  private String targetVersion;

  /** Upgrade Commands. */
  @JsonProperty("upgradeCommands")
  private List<UpgradeCommandConfig> upgradeCommands;

  public String getTargetVersion() {
    return targetVersion;
  }

  public List<UpgradeCommandConfig> getUpgradeCommands() {
    return upgradeCommands;
  }

  @Override
  public String toString() {
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "";
    }
  }
}
