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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;

/** Properties that is used to share configuration parameters among service nodes. */
public abstract class SharedProperties {

  // The abstract methods below are the minimum set of methods that need to be implemented by a
  // concrete implementation class that inherits from this abstract class.

  /**
   * Gets a property value.
   *
   * @param key the property key
   * @return the value in the property list with the specified key value. Null is returned if the
   *     value is missing.
   */
  public abstract CompletionStage<String> getProperty(String key, ExecutionState state);

  /**
   * Sets a property with the specified key.
   *
   * @param key the key to be placed into the property list.
   * @param value the value corresponding to key.
   */
  public abstract CompletionStage<Void> setProperty(String key, String value, ExecutionState state);

  /**
   * Deletes a property value.
   *
   * @param key Property key
   */
  public abstract CompletionStage<Void> deleteProperty(String key, ExecutionState state);

  // The methods below have an implementation in this class that should be sufficient for common
  // use cases, and are not expected to be overloaded by inheriting classes.

  /** Map key is shared property key, map value is a tuple: (shared property value, load time). */
  private final Map<String, Pair<String, Long>> cachedValues = new ConcurrentHashMap<>();

  private Long cacheExpiryMilliseconds;
  private long cacheExpiryMillisecondsLoadTime = 0;

  public static final String SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY =
      "prop.sharedPropertiesCacheExpiryMillis";
  private static final long SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_DEFAULT = 1000L * 60 * 5;

  /**
   * Searches for the property with the specified key in the property list.
   *
   * <p>Once the property has been retrieved, it is cached aggressively and subsequent calls to get
   * the same property key will return the cached value until it expires. A subsequent call after
   * the cached value expires will result in calling the underlying store again.
   *
   * <p>Note that calls to the uncached methods e.g. {@link #getProperty(String, ExecutionState)}
   * and {@link #setProperty(String, String, ExecutionState)} do not update the cache. Calls to this
   * method will return the old cached values even after a set.
   *
   * @param key the property key.
   * @return the value in the property list with the specified key value; null if absent.
   */
  public CompletionStage<String> getPropertyCached(String key, ExecutionState state) {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }

    final long currentTime = System.currentTimeMillis();

    // First load the value of cacheExpiryMilliseconds if it is not loaded or has expired.
    final CompletionStage<Void> initial;
    if ((cacheExpiryMilliseconds == null)
        || (currentTime > cacheExpiryMillisecondsLoadTime + cacheExpiryMilliseconds)) {
      initial =
          getProperty(SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY, state)
              .thenAccept(
                  (expiryString) -> {
                    if (expiryString == null) {
                      cacheExpiryMilliseconds = SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_DEFAULT;
                    } else {
                      cacheExpiryMilliseconds = Long.parseLong(expiryString);
                    }
                    cacheExpiryMillisecondsLoadTime = currentTime;
                  });
    } else {
      initial = CompletableFuture.completedStage(null);
    }
    return initial
        .thenApplyAsync(
            (none) -> {
              // If the requested key is absent or expired, load it, otherwise return cached value.
              final Pair<String, Long> cachedValue = cachedValues.get(key);
              if (cachedValue != null
                  && currentTime < cachedValue.getRight() + cacheExpiryMilliseconds) {
                return cachedValue.getLeft();
              } else {
                return null;
              }
            },
            state.getExecutor())
        .thenComposeAsync(
            (value) -> {
              if (value != null) {
                return CompletableFuture.completedStage(value);
              } else {
                return getProperty(key, state)
                    .thenApply(
                        (value2) -> {
                          if (value2 != null) {
                            cachedValues.put(key, Pair.of(value2, currentTime));
                          }
                          return value2;
                        });
              }
            },
            state.getExecutor());
  }

  /**
   * Similar to {@link #getPropertyCached(String, ExecutionState)}, except that if the key is not
   * found, returns the provided default instead of null.
   */
  public CompletionStage<String> getPropertyCached(
      String key, String defaultValueIfAbsent, ExecutionState state) {
    return getPropertyCached(key, state)
        .thenApply((value) -> value != null ? value : defaultValueIfAbsent);
  }

  /**
   * Similar to {@link #getPropertyCachedLong(String, ExecutionState)}, except that if the key is
   * not found, returns the provided default instead of null.
   */
  public CompletionStage<Long> getPropertyCached(
      String key, Long defaultValueIfAbsent, ExecutionState state) {
    return getPropertyCachedLong(key, state)
        .thenApply((value) -> value != null ? value : defaultValueIfAbsent);
  }

  /**
   * Similar to {@link #getPropertyCachedBoolean(String, ExecutionState)}, except that if the key is
   * not found, returns the provided default instead of null.
   */
  public CompletionStage<Boolean> getPropertyCached(
      String key, Boolean defaultValueIfAbsent, ExecutionState state) {
    return getPropertyCachedBoolean(key, state)
        .thenApply((value) -> value != null ? value : defaultValueIfAbsent);
  }

  /**
   * Similar to {@link #getPropertyCached(String, ExecutionState)}, except that the value is cast to
   * a Long before being returned.
   */
  public CompletionStage<Long> getPropertyCachedLong(String key, ExecutionState state) {
    return getPropertyCached(key, state)
        .thenApply((value) -> value != null ? Long.parseLong(value) : null);
  }

  /**
   * Similar to {@link #getPropertyCached(String, ExecutionState)}, except that the value is cast to
   * a Boolean before being returned.
   */
  public CompletionStage<Boolean> getPropertyCachedBoolean(String key, ExecutionState state) {
    return getPropertyCached(key, state)
        .thenApply((value) -> value != null ? Boolean.parseBoolean(value) : null);
  }

  /** Removes all cached values from the cache; the next read for every key will be uncached. */
  public void clearPropertyCache() {
    cachedValues.clear();
    cacheExpiryMilliseconds = null;
  }
}
