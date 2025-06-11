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
package io.isima.bios.data.service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exception.ConfigException;
import io.isima.bios.load.model.MappedSignalValue;
import io.isima.bios.load.model.StreamConfig;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.load.utils.LoadPattern;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
  private final static Logger logger = LoggerFactory.getLogger(Config.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final Map<String, StreamConfig> streamConfigMap =
          new HashMap<String, StreamConfig>();

  public Config(final String filePath)
          throws JsonParseException, JsonMappingException, IOException {
    parseStreamJson(filePath);
  }

  /**
   * Method to parse load stream json to list of stream confing
   *
   * @param configDirectory
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws IOException
   */
  private void parseStreamJson(final String configDirectory)
          throws JsonParseException, JsonMappingException, IOException {
    if (null == configDirectory || configDirectory.trim().isEmpty()) {
      throw new ConfigException("Invalid stream config directory");
    }
    File streamConfigDirectory = new File(configDirectory);
    if (!streamConfigDirectory.exists()) {
      throw new ConfigException(
              String.format("Stream config directory : %s does not exists", configDirectory));
    }
    if (!streamConfigDirectory.isDirectory()) {
      throw new ConfigException(
              String.format("Stream config : %s must be a directory", configDirectory));
    }
    FilenameFilter jsonFilefilter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".json");
      }
    };
    File[] configFiles = streamConfigDirectory.listFiles(jsonFilefilter);
    if (null == configFiles || configFiles.length == 0) {
      throw new ConfigException(
              String.format("No json config files found in %s ", configDirectory));
    }

    for (File jsonFile : configFiles) {
      try {
        StreamConfig config = mapper.readValue(jsonFile, StreamConfig.class);
        streamConfigMap.put(config.getName(), config);
        int keySize = ApplicationUtils.getLoadPattern().equals(LoadPattern.CONSTANT) ? 100000
                : Integer.valueOf(config.getPeakRequestSize() / 3);
        MappedSignalValue.initStreamKeySizeMap(config.getName(), keySize);
      } catch (Exception ex) {
        logger.error(ex.getMessage(), ex);
        throw new ConfigException(String.format("Failed to paarse %s", jsonFile.getName()),ex);
      }
    }
  }


  /**
   * Method to get stream config for given stream name
   *
   * @param streamName
   * @return StreamConfig
   */
  public StreamConfig getStream(final String streamName) {
    return streamConfigMap.get(streamName);
  }


  /**
   * Method to get all stream names which is present in stream JSON
   *
   * @return Set<String>
   */
  public Set<String> getStreamNames() {
    return streamConfigMap.keySet();
  }

}
