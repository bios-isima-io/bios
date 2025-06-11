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
import io.isima.bios.models.KeywordRegexes;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegexBasedNameDetector implements NameDetector {

  private static final Logger logger = LoggerFactory.getLogger(RegexBasedNameDetector.class);

  private static final String JSON_FILE = "regexstore.json";

  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  private final List<KeywordRegexes> keywordRegexesStore;

  private final Map<String, List<Pattern>> keywordPatterns;

  private final NameMatchTieBreaker nameMatchTieBreaker;

  public RegexBasedNameDetector(NameMatchTieBreaker nameMatchTieBreaker) throws FileReadException {

    this.nameMatchTieBreaker = nameMatchTieBreaker;
    logger.info("Attempting to load keyword regexes from file regexstore.json");
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(JSON_FILE)) {
      KeywordRegexes[] configs = mapper.readValue(inputStream, KeywordRegexes[].class);
      keywordRegexesStore = Arrays.asList(configs);
      keywordPatterns = new HashMap<>();
      for (KeywordRegexes keywordRegexes : keywordRegexesStore) {
        List<Pattern> patternList = new ArrayList<>();
        for (String pattern : keywordRegexes.getPatterns()) {
          patternList.add(Pattern.compile(pattern));
        }
        keywordPatterns.put(keywordRegexes.getName(), patternList);
      }
    } catch (IOException e) {
      logger.error("Failed to load keyword regexes file regexstore.json", e);
      throw new FileReadException("Failed to load keyword regexes file regexstore.json", e);
    }
  }

  public String inferColumnName(List<String> columnValues, String domain) {
    List<String> nonEmptyColumnValues =
        columnValues.stream().filter(value -> !value.isEmpty()).collect(Collectors.toList());
    Map<String, Integer> matchCountMap = new HashMap<>();
    for (String columnValue : nonEmptyColumnValues) {
      for (String keyword : keywordPatterns.keySet()) {
        if (isAMatch(columnValue, keywordPatterns.get(keyword))) {
          matchCountMap.put(keyword, matchCountMap.getOrDefault(keyword, 0) + 1);
        }
      }
    }

    if (matchCountMap.size() > 0) {
      Map.Entry<String, Integer> maxMatchEntry =
          Collections.max(
              matchCountMap.entrySet(),
              new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(
                    final Map.Entry<String, Integer> o1, final Map.Entry<String, Integer> o2) {
                  if (o1.getValue().equals(o2.getValue())) {
                    return nameMatchTieBreaker.compareMatchPriority(o1.getKey(), o2.getKey());
                  } else {
                    return o1.getValue() - o2.getValue();
                  }
                }
              });
      /*
      Only return the match if match percentage exceeds threshold of 70%
       */
      if ((maxMatchEntry.getValue() * 100) / nonEmptyColumnValues.size() > 70) {
        return maxMatchEntry.getKey();
      }
    }
    return "NA";
  }

  private boolean isAMatch(String columnValue, List<Pattern> patternList) {
    for (Pattern pattern : patternList) {
      if (pattern.matcher(columnValue).matches()) {
        return true;
      }
    }
    return false;
  }
}
