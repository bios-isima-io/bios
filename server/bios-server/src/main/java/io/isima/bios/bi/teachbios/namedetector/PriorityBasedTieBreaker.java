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
package io.isima.bios.bi.teachbios.namedetector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriorityBasedTieBreaker implements NameMatchTieBreaker {

  private static final Logger logger = LoggerFactory.getLogger(PriorityBasedTieBreaker.class);

  private static final String JSON_FILE = "keyword_priorities.json";

  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  private final Map<String, Integer> priorityMap = new HashMap<>();

  public PriorityBasedTieBreaker() throws FileReadException {
    logger.info("Attempting to load keyword priorities from file keyword_priorities.json");
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(JSON_FILE)) {
      String[] keywords = mapper.readValue(inputStream, String[].class);
      for (int i = keywords.length - 1; i >= 0; i--) {
        priorityMap.put(keywords[i], keywords.length - i);
      }
    } catch (IOException e) {
      logger.error("Failed to load keyword priorities file keyword_priorities.json", e);
      throw new FileReadException(
          "Failed to load keyword priorities file keyword_priorities.json", e);
    }
  }

  public int compareMatchPriority(String first, String second) {
    return priorityMap.get(first) - priorityMap.get(second);
  }
}
