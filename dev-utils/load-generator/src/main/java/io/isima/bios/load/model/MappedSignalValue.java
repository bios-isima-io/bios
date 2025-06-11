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
package io.isima.bios.load.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedSignalValue {
  private static final Logger logger = LoggerFactory.getLogger(MappedSignalValue.class);

  // map to store list of keys against stream and attribute
  private static Map<String, List<String>> streamValueMap = new HashMap<>();

  // to store key list size of a stream, size between 0 and rand(maxPeekSize/3)
  private static Map<String, Integer> streamKeySizeMap = new HashMap<>();
  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();

  {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        for (String key : streamValueMap.keySet()) {
          streamValueMap.put(key, new ArrayList<String>());
          logger.info(String.format("After cleanup key size : %d for key %s",
              streamValueMap.get(key).size(), key));
        }
      }
    };
    executorService.scheduleAtFixedRate(runnable, 0, 8, TimeUnit.HOURS);
  }

  /**
   * this method will add attribute value to a list of keys
   *
   * @param keyToAdd key to add to key list
   */
  public static void addKey(final String streamName, final String attrName, final String keyToAdd) {
    if (null == keyToAdd || keyToAdd.trim().isEmpty()) {
      return;
    }
    final String streamValueMapKey = getMapKeyName(streamName, attrName);
    if (!streamValueMap.containsKey(streamValueMapKey)) {
      List<String> keys = new ArrayList<String>();
      streamValueMap.put(streamValueMapKey, keys);
    }

    if (null == streamValueMap.get(streamValueMapKey)
        || streamValueMap.get(streamValueMapKey).size() < streamKeySizeMap.get(streamName)) {
      streamValueMap.get(streamValueMapKey).add(keyToAdd);
      logger.debug(String.format("Current key size for key: %s, Size %d", streamValueMapKey,
          streamValueMap.get(streamValueMapKey).size()));
    }
  }

  /**
   * Method to get random key from list of keys
   *
   * @param streamName
   * @param attrName
   * @return random key from the list, null if list is empty
   */
  public static String getKey(final String streamName, final String attrName) {
    final String streamValueMapKey = getMapKeyName(streamName, attrName);
    if (!streamValueMap.containsKey(streamValueMapKey)) {
      return null;
    }
    if (null == streamValueMap.get(streamValueMapKey)
        || streamValueMap.get(streamValueMapKey).isEmpty()) {
      return null;
    }
    int keyListSize = streamValueMap.get(streamValueMapKey).size();
    return streamValueMap.get(streamValueMapKey).get(RandomUtils.nextInt(0, keyListSize));
  }

  public static void initStreamKeySizeMap(final String stream, final Integer keySize) {
    streamKeySizeMap.putIfAbsent(stream, keySize);
  }

  private static String getMapKeyName(final String streamName, final String attrName) {
    return streamName + "-" + attrName;
  }

}
