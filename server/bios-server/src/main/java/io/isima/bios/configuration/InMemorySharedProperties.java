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
package io.isima.bios.configuration;

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModule;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concrete class of abstract {@link SharedProperties}.
 *
 * <p>This class does not persist properties but keeps configuration parameters in memory. Because
 * of this implementation, the class does not values among servers. Useful only for testing.
 */
public class InMemorySharedProperties extends SharedProperties implements BiosModule {
  final Map<String, String> properties;

  public InMemorySharedProperties() {
    properties = new ConcurrentHashMap<>();
  }

  @Override
  public CompletionStage<String> getProperty(String key, ExecutionState state) {
    return CompletableFuture.completedStage(properties.get(key));
  }

  @Override
  public CompletionStage<Void> setProperty(String key, String value, ExecutionState state) {
    properties.put(key, value);
    return CompletableFuture.completedStage(null);
  }

  @Override
  public CompletionStage<Void> deleteProperty(String key, ExecutionState state) {
    properties.remove(key);
    return CompletableFuture.completedStage(null);
  }

  @Override
  public void shutdown() {}
}
