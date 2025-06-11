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
package io.isima.bios.models.isql;

import java.util.HashMap;
import java.util.Map;

/**
 * If passed to the validator, the validator will fill up with derived data that can be then used by
 * request adapters such as extract and summarize request adapters.
 */
public final class DerivedQueryComponents {
  public static final String COMPILED_SORT = "compiledSort";

  private final Map<String, String> attributeKeyMap;

  private long endTime;

  // TODO(BIOS-5491): Avoid using Object. It's a hack to avoid internal class exposed in the
  //   common jar file, which is used by SDK. But you need to downcast to use the objects.
  private final Map<String, Object> derivedComponents;

  public DerivedQueryComponents() {
    attributeKeyMap = new HashMap<>();
    endTime = 0;
    derivedComponents = new HashMap<>();
  }

  public void addAttributeKey(String valueInQuery, String mappedKey) {
    attributeKeyMap.put(valueInQuery, mappedKey);
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getEndTime(long currentEndTime) {
    return (endTime > 0) ? endTime : currentEndTime;
  }

  public String getMappedKey(String valueInQuery) {
    String key = attributeKeyMap.get(valueInQuery);
    return (key == null) ? valueInQuery : key;
  }

  public void putComponent(String name, Object component) {
    derivedComponents.put(name, component);
  }

  public Object getComponent(String name) {
    return derivedComponents.get(name);
  }

  public <T> T getComponent(String name, Class<T> clazz) {
    return clazz.cast(derivedComponents.get(name));
  }
}
