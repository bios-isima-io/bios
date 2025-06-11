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

import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Interface that provides list of properties that is shared among multiple VMs. */
public abstract class SharedProperties {
  private static final Logger logger = LoggerFactory.getLogger(SharedProperties.class);

  // The abstract methods below are the minimum set of methods that need to be implemented by a
  // concrete implementation class that inherits from this abstract class.

  /**
   * Searches for the property with the specified key in the property list.
   *
   * @param key the property key
   * @return the value in the property list with the specified key value.
   */
  public abstract String getProperty(String key);

  public final CompletableFuture<String> getPropertyAsync(String key, ExecutionState state) {
    final var future = new CompletableFuture<String>();
    getPropertyAsync(key, state, future::complete, future::completeExceptionally);
    return future;
  }

  public final CompletableFuture<Long> getPropertyLongAsync(
      String key, long defaultValue, ExecutionState state) {
    return getPropertyAsync(key, state)
        .thenApplyAsync(
            (value) -> {
              if (value != null && !value.isBlank()) {
                try {
                  return Long.parseLong(value);
                } catch (NumberFormatException e) {
                  throw new CompletionException(
                      String.format("Invalid shared property %s value: %s", key, value), e);
                }
              }
              return defaultValue;
            },
            state.getExecutor());
  }

  public final CompletableFuture<Boolean> getPropertyBooleanAsync(
      String key, boolean defaultValue, ExecutionState state) {
    return getPropertyAsync(key, state)
        .thenApplyAsync(
            (value) -> {
              if (value != null && !value.isBlank()) {
                return Boolean.parseBoolean(value);
              }
              return defaultValue;
            },
            state.getExecutor());
  }

  public abstract void getPropertyAsync(
      String key,
      ExecutionState state,
      Consumer<String> acceptor,
      Consumer<Throwable> errorHandler);

  /**
   * Searches for the property with the specified key in the property list.
   *
   * <p>Fails if unable to get property from the underlying storage, instead of simply returning
   * null. Runtime exception may be thrown by implementations, if it cannot fetch the property from
   * the underlying implementations. One such example is NoHostAvailableException from database.
   *
   * @param key the property key
   * @return the value in the property list with the specified key value.
   */
  public abstract String getPropertyFailOnError(String key);

  /**
   * Sets a property with the specified key.
   *
   * @param key the key to be placed into the property list.
   * @param value the value corresponding to key.
   * @throws ApplicationException when an internal error happens.
   */
  public abstract void setProperty(String key, String value) throws ApplicationException;

  /**
   * Add a property with the specified key, but the operation fails if a property with the same key
   * already exists.
   *
   * @param key the key to be placed into the property list.
   * @param value the value corresponding to key.
   * @return Null is returned if the property is added successfully. The existing property with the
   *     key when the operation fails.
   * @throws ApplicationException when an internal error happens.
   */
  public abstract String safeAddProperty(String key, String value) throws ApplicationException;

  /**
   * Execute compare-and-set operation against a property with the specified key.
   *
   * <p>Existing value must be fetched using {@link #getProperty(String)} operation before calling
   * this method. Use the value for parameter oldValue. Operation succeeds if the property value is
   * still the same with specified oldValue when executing this method. If the existing value has
   * changed, the operation fails and the method returns the new existing value. The existing value
   * in the property list is unchanged in case of failure.
   *
   * @param key the key to be placed into the property list.
   * @param oldValue existing value with corresponding to key.
   * @param newValue value to update with corresponding to key.
   * @return The method returns null if value is modified successfully. New existing value is
   *     returned in case of failure.
   * @throws ApplicationException when an internal error happens.
   */
  public abstract String safeUpdateProperty(String key, String oldValue, String newValue)
      throws ApplicationException;

  /** Used internally for maintenance and testing. */
  public abstract void deleteProperty(String key);

  // The methods below have an implementation in this class that should be sufficient for common
  // use cases, and are not expected to be overloaded by inheriting classes.

  public static final String SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY =
      "prop.sharedPropertiesCacheExpiryMillis";
  private static final long SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_DEFAULT = 1000L * 60 * 5;

  /** Map key is shared property key, map value is a tuple: (shared property value, load time). */
  private final Map<String, CacheItem> cachedValues = new ConcurrentHashMap<>();

  private Long cacheExpiryMilliseconds = SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_DEFAULT;
  private long cacheExpiryMillisecondsLoadTime = 0;
  private final AtomicBoolean cacheExpiryMillisecondsReloading = new AtomicBoolean(false);

  // retry parameters
  private static final int RETRY = 10;
  private static final long INITIAL_SLEEP_MILLIS = 10;

  /**
   * Searches for the property with the specified key in the property list.
   *
   * <p>Once the property has been retrieved, it is cached aggressively and subsequent calls to get
   * the same property key will return the cached value until it expires. A subsequent call after
   * the cached value expires will result in calling the underlying store again.
   *
   * <p>Note that calls to the uncached methods e.g. {@link #getProperty(String)} and {@link
   * #setProperty(String, String)} do not update the cache. Calls to this method will return the old
   * cached values even after a set.
   *
   * @param key the property key.
   * @return the value in the property list with the specified key value; null if absent.
   * @throws IllegalArgumentException If the key is null
   */
  public String getPropertyCached(String key) {
    assert !ExecutorManager.isInIoThread();

    final var currentTime = System.currentTimeMillis();
    mayReloadCacheExpirationTime(currentTime);
    final var cacheItem = cachedValues.get(key);
    if (cacheItem == null
        || (isExpired(cacheItem, currentTime) && cacheItem.reloading.compareAndSet(false, true))) {
      String value = getProperty(key);
      cachedValues.put(key, new CacheItem(value, System.currentTimeMillis()));
      return value;
    }
    return cacheItem.value;
  }

  /**
   * Searches for the property with the specified key in the property list asynchronously.
   *
   * <p>Once the property has been retrieved, it is cached aggressively and subsequent calls to get
   * the same property key will return the cached value until it expires. A subsequent call after
   * the cached value expires will result in calling the underlying store again.
   *
   * <p>Note that calls to the uncached methods e.g. {@link #getProperty(String)} and {@link
   * #setProperty(String, String)} do not update the cache. Calls to this method will return the old
   * cached values even after a set.
   *
   * @param key the property key.
   * @param defaultValue default value
   * @param state execution state
   * @return the value in the property list with the specified key value; null if absent.
   * @throws IllegalArgumentException If the key is null
   */
  public CompletableFuture<String> getPropertyCachedAsync(
      String key, String defaultValue, ExecutionState state) {
    final var future = new CompletableFuture<String>();
    getPropertyCachedAsync(
        key, defaultValue, state, future::complete, future::completeExceptionally);
    return future;
  }

  public void getPropertyCachedAsync(
      String key,
      String defaultValue,
      ExecutionState state,
      Consumer<String> acceptor,
      Consumer<Throwable> errorHandler) {

    final var currentTime = System.currentTimeMillis();
    mayReloadCacheExpirationTime(currentTime);
    final var cacheItem = cachedValues.get(key);
    boolean isCacheItemPresent = cacheItem != null;

    if (cacheItem == null
        || (isExpired(cacheItem, currentTime) && cacheItem.reloading.compareAndSet(false, true))) {

      // In testing mode, cache value should be updated on the call.
      // In default behavior, the cache is updated in sideline for later access and this
      // call would return old value.
      if (TfosConfig.isTestMode()) {
        isCacheItemPresent = false;
      }

      final boolean doSendResponse = !isCacheItemPresent;
      // We don't use the given state as is since the execution may annoy the original state
      // such as metrics.
      final var queryState = new GenericExecutionState("getProperty " + key, state.getExecutor());
      state.addBranch(queryState);
      getPropertyAsync(
          key,
          queryState,
          (value) -> {
            cachedValues.put(key, new CacheItem(value, System.currentTimeMillis()));
            if (doSendResponse) {
              acceptor.accept(value != null ? value : defaultValue);
            }
          },
          (t) -> {
            if (doSendResponse) {
              errorHandler.accept(t);
            } else {
              logger.error(
                  "Failed to reload a shared property cache item; key={}, error={}", key, t, t);
            }
          });
    }

    if (isCacheItemPresent) {
      try {
        acceptor.accept(cacheItem.value != null ? cacheItem.value : defaultValue);
      } catch (Throwable t) {
        errorHandler.accept(t);
      }
    }
  }

  public CompletableFuture<Long> getPropertyCachedLongAsync(
      String key, Long defaultValue, ExecutionState state) {
    final var future = new CompletableFuture<Long>();
    getPropertyCachedLongAsync(
        key, defaultValue, state, future::complete, future::completeExceptionally);
    return future;
  }

  public void getPropertyCachedLongAsync(
      String key,
      Long defaultValue,
      ExecutionState state,
      Consumer<Long> acceptor,
      Consumer<Throwable> errorHandler) {
    getPropertyCachedAsync(
        key,
        null,
        state,
        (value) -> {
          if (StringUtils.isBlank(value)) {
            acceptor.accept(defaultValue);
          } else {
            try {
              acceptor.accept(Long.parseLong(value));
            } catch (NumberFormatException e) {
              logger.error(
                  "Invalid long property value format, falling back to default;"
                      + " key={}, value={}, default={}",
                  key,
                  value,
                  defaultValue);
              acceptor.accept(defaultValue);
            }
          }
        },
        errorHandler::accept);
  }

  public void getPropertyCachedIntAsync(
      String key,
      Integer defaultValue,
      ExecutionState state,
      Consumer<Integer> acceptor,
      Consumer<Throwable> errorHandler) {
    getPropertyCachedAsync(
        key,
        null,
        state,
        (value) -> {
          if (StringUtils.isBlank(value)) {
            acceptor.accept(defaultValue);
          } else {
            try {
              acceptor.accept(Integer.parseInt(value));
            } catch (NumberFormatException e) {
              logger.error(
                  "Invalid long property value format, falling back to default;"
                      + " key={}, value={}, default={}",
                  key,
                  value,
                  defaultValue);
              acceptor.accept(defaultValue);
            }
          }
        },
        errorHandler::accept);
  }

  public void getLocalEndpointAsync(
      List<String> endpoints,
      ExecutionState state,
      Consumer<String> acceptor,
      Consumer<Throwable> errorHandler) {
    if (endpoints.isEmpty()) {
      acceptor.accept("");
      return;
    }
    // retrieve a special cache-only value for my endpoint
    final var myEndpoint = "_LOCAL_ENDPOINT";
    final var cachedValue = getCachedValue(myEndpoint);
    if (cachedValue != null) {
      acceptor.accept(cachedValue.value);
    } else {
      // The key is absent or expired, build one
      final var nodeName = Utils.getNodeName();
      final var remaining = new AtomicInteger(endpoints.size());
      for (var endpoint : endpoints) {
        final var key = "endpoint:" + endpoint;
        getPropertyAsync(
            key,
            new GenericExecutionState(key, state),
            (value) -> {
              final int pos = remaining.decrementAndGet();
              if (nodeName.equals(value)) {
                cachedValues.put(myEndpoint, new CacheItem(endpoint, System.currentTimeMillis()));
                acceptor.accept(endpoint);
              } else if (pos == 0) {
                cachedValues.put(myEndpoint, new CacheItem("", System.currentTimeMillis()));
                acceptor.accept("");
              }
            },
            errorHandler::accept);
      }
    }
  }

  /**
   * Similar to {@link #getPropertyCached(String)}, except that if the key is not found, returns the
   * provided default instead of null.
   */
  public String getPropertyCached(String key, String defaultValueIfAbsent) {
    String value = getPropertyCached(key);
    if (value == null) {
      return defaultValueIfAbsent;
    } else {
      return value;
    }
  }

  /**
   * Similar to {@link #getPropertyCachedLong(String)}, except that if the key is not found, returns
   * the provided default instead of null.
   */
  public Long getPropertyCached(String key, Long defaultValueIfAbsent) {
    Long value = getPropertyCachedLong(key);
    if (value == null) {
      return defaultValueIfAbsent;
    } else {
      return value;
    }
  }

  /**
   * Similar to {@link #getPropertyCachedLong(String)}, except that if the key is not found, returns
   * the provided default instead of null.
   */
  public Integer getPropertyCached(String key, Integer defaultValueIfAbsent) {
    Integer value = getPropertyCachedInteger(key);
    if (value == null) {
      return defaultValueIfAbsent;
    } else {
      return value;
    }
  }

  /**
   * Similar to {@link #getPropertyCachedDouble(String)}, except that if the key is not found,
   * returns the provided default instead of null.
   */
  public Double getPropertyCached(String key, Double defaultValueIfAbsent) {
    Double value = getPropertyCachedDouble(key);
    if (value == null) {
      return defaultValueIfAbsent;
    } else {
      return value;
    }
  }

  /**
   * Similar to {@link #getPropertyCachedBoolean(String)}, except that if the key is not found,
   * returns the provided default instead of null.
   */
  public Boolean getPropertyCached(String key, Boolean defaultValueIfAbsent) {
    Boolean value = getPropertyCachedBoolean(key);
    if (value == null) {
      return defaultValueIfAbsent;
    } else {
      return value;
    }
  }

  /**
   * Similar to {@link #getPropertyCached(String)}, except that the value is cast to a Long before
   * being returned.
   */
  public Long getPropertyCachedLong(String key) {
    String value = getPropertyCached(key);
    if (StringUtils.isBlank(value)) {
      return null;
    } else {
      return Long.parseLong(value);
    }
  }

  /**
   * Similar to {@link #getPropertyCached(String)}, except that the value is cast to a Long before
   * being returned.
   */
  public Integer getPropertyCachedInteger(String key) {
    String value = getPropertyCached(key);
    if (StringUtils.isBlank(value)) {
      return null;
    } else {
      return Integer.parseInt(value);
    }
  }

  /**
   * Similar to {@link #getPropertyCached(String)}, except that the value is cast to a Double before
   * being returned.
   */
  public Double getPropertyCachedDouble(String key) {
    String value = getPropertyCached(key);
    if (StringUtils.isBlank(value)) {
      return null;
    } else {
      return Double.parseDouble(value);
    }
  }

  /**
   * Similar to {@link #getPropertyCached(String)}, except that the value is cast to a Boolean
   * before being returned.
   */
  public Boolean getPropertyCachedBoolean(String key) {
    String value = getPropertyCached(key);
    if (StringUtils.isBlank(value)) {
      return null;
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Retrieve cached value.
   *
   * <p>The return value is null if there's no cache entry or if the entry is expired. Note that the
   * value of returned non-null entry may be null.
   *
   * @param key Property key
   * @throws IllegalArgumentException If the specified key is null.
   * @returns Pair of value and timestamp if the cache entry exists, otherwise null.
   */
  private CacheItem getCachedValue(String key) {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }

    final long currentTime = System.currentTimeMillis();
    mayReloadCacheExpirationTime(currentTime);
    final var cachedValue = cachedValues.get(key);

    return validateCacheItem(cachedValue, currentTime);
  }

  protected void mayReloadCacheExpirationTime(long currentTime) {
    if (currentTime > cacheExpiryMillisecondsLoadTime + cacheExpiryMilliseconds
        && cacheExpiryMillisecondsReloading.compareAndSet(false, true)) {
      ExecutorManager.getSidelineExecutor()
          .execute(
              () -> {
                cacheExpiryMillisecondsLoadTime = currentTime;
                final String expiryString = getProperty(SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_KEY);
                if (StringUtils.isBlank(expiryString)) {
                  cacheExpiryMilliseconds = SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_DEFAULT;
                } else {
                  cacheExpiryMilliseconds = Long.parseLong(expiryString);
                }
                cacheExpiryMillisecondsReloading.set(false);
              });
    }
  }

  private CacheItem validateCacheItem(CacheItem cachedValue, long currentTime) {
    // return null if the requested key is absent or expired
    if (cachedValue == null || currentTime > cachedValue.timestamp + cacheExpiryMilliseconds) {
      return null;
    }
    return cachedValue;
  }

  private boolean isExpired(CacheItem cachedValue, long currentTime) {
    final boolean expired = currentTime > cachedValue.timestamp + cacheExpiryMilliseconds;
    return expired;
  }

  /** Removes all cached values from the cache; the next read for every key will be uncached. */
  public void clearPropertyCache() {
    cachedValues.clear();
    cacheExpiryMillisecondsLoadTime = 0;
    cacheExpiryMilliseconds = SHARED_PROPERTIES_CACHE_EXPIRY_MILLIS_DEFAULT;
    cacheExpiryMillisecondsReloading.set(false);
  }

  /**
   * Appends a property with the specified value using specified delimiter, if it is not already
   * contained in the existing property.
   *
   * <p>Write consistency is guaranteed with successful operation. The method does not append if the
   * same entry already exists.
   *
   * @param key the key to be placed into the property list.
   * @param value value to append.
   * @param delimiter delimiter for multiple entries.
   * @param ignoreCase case sensitivity flag for duplicate check.
   * @throws ApplicationException When write fails due to failure of arbitration among conflicting
   *     writes.
   */
  public void appendPropertyIfAbsent(String key, String value, String delimiter, boolean ignoreCase)
      throws ApplicationException {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }
    if (delimiter == null || delimiter.isEmpty()) {
      throw new IllegalArgumentException("invalid delimiter");
    }
    value = value.trim();
    int retry = RETRY;
    long millis = INITIAL_SLEEP_MILLIS;
    while (retry-- >= 0) {
      String current = getProperty(key);
      String newValue;
      if (current == null || current.trim().isEmpty()) {
        newValue = value;
      } else {
        String temp;
        if (ignoreCase) {
          temp = delimiter + current.trim().toLowerCase() + delimiter;
        } else {
          temp = delimiter + current.trim() + delimiter;
        }

        if (temp.contains(delimiter + (ignoreCase ? value.toLowerCase() : value) + delimiter)) {
          // value already exists
          return;
        }
        newValue = current.trim() + delimiter + value;
      }
      String result = safeUpdateProperty(key, current, newValue);
      if (result == null) {
        return;
      }
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ApplicationException("Retry interrupted", e);
      }
      millis *= 2;
    }
    throw new ApplicationException("failed to append value");
  }

  /**
   * Removes a value entry from a property with the specified key; entries are derived by splitting
   * the property by the specified delimiter.
   *
   * <p>Write consistency is guaranteed with successful operation. The method does nothing if
   * specified value does not exist.
   *
   * @param key the key to be placed into the property list.
   * @param value value to append.
   * @param delimiter delimiter for multiple entries.
   * @param ignoreCase case sensitivity flag for duplicate check.
   * @throws ApplicationException When write fails due to failure of arbitration among conflicting
   *     writes.
   */
  public void removeProperty(String key, String value, String delimiter, boolean ignoreCase)
      throws ApplicationException {
    removeProperties(key, new String[] {value}, delimiter, ignoreCase);
  }

  /**
   * Removes multiple value entries from a property with the specified key, split using specified
   * delimiter.
   *
   * <p>Write consistency is guaranteed with successful operation. The method does nothing if
   * specified value does not exist.
   *
   * @param key the key to be placed into the property list.
   * @param values values to remove.
   * @param delimiter delimiter for multiple entries.
   * @param ignoreCase case sensitivity flag for duplicate check.
   * @throws ApplicationException When write fails due to failure of arbitration among conflicting
   *     writes.
   */
  public void removeProperties(String key, String[] values, String delimiter, boolean ignoreCase)
      throws ApplicationException {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }
    if (delimiter == null || delimiter.isEmpty()) {
      throw new IllegalArgumentException("invalid delimiter");
    }
    int retry = RETRY;
    long millis = INITIAL_SLEEP_MILLIS;
    while (retry-- >= 0) {
      String current = getProperty(key);
      if (current == null) {
        return;
      }

      String[] entries = current.split(delimiter);
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (String entry : entries) {
        boolean isMatched = false;
        for (String value : values) {
          if (ignoreCase ? entry.equalsIgnoreCase(value) : entry.equals(value)) {
            isMatched = true;
            break;
          }
        }

        if (isMatched) {
          continue;
        }

        if (!first) {
          sb.append(delimiter);
        }
        first = false;
        sb.append(entry);
      }
      String result = safeUpdateProperty(key, current, sb.toString());
      if (result == null) {
        return;
      }
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ApplicationException("Retry interrupted");
      }
      millis *= 2;
    }
    throw new ApplicationException("failed to append value");
  }

  // Store a static instance of a SharedProperties implementation for easy access to shared
  // properties from anywhere in the codebase without having to pass sharedProperties
  // throughout many methods and storing it in many classes.
  // Also provide a default implementation that doesn't depend on an external database,
  // for use by unit tests.
  @Getter @Setter private static SharedProperties instance = new DefaultSharedProperties();

  public static class DefaultSharedProperties extends SharedProperties {
    protected final Map<String, String> propertiesMap = new ConcurrentHashMap<>();

    @Override
    public String getProperty(String key) {
      return propertiesMap.get(key);
    }

    @Override
    public void getPropertyAsync(
        String key,
        ExecutionState state,
        Consumer<String> acceptor,
        Consumer<Throwable> errorHandler) {
      acceptor.accept(getProperty(key));
    }

    @Override
    public String getPropertyFailOnError(String key) {
      return getProperty(key);
    }

    @Override
    public void setProperty(String key, String value) {
      propertiesMap.put(key, value);
    }

    @Override
    public String safeAddProperty(String key, String value) {
      return propertiesMap.putIfAbsent(key, value);
    }

    @Override
    public String safeUpdateProperty(String key, String oldValue, String newValue) {
      if (oldValue == null) {
        return propertiesMap.putIfAbsent(key, newValue);
      } else {
        if (propertiesMap.replace(key, oldValue, newValue)) {
          return null;
        } else {
          String existing = propertiesMap.get(key);
          return existing;
        }
      }
    }

    @Override
    public void deleteProperty(String key) {
      propertiesMap.remove(key);
    }
  }

  // Add convenient static methods (similar to System.getProperty()) that use the static instance.

  /** See {@link #getProperty(String)}. */
  public static String get(String key) {
    return instance.getProperty(key);
  }

  public static int getInteger(String key, int defaultValue) {
    final var prop = get(key);
    try {
      return StringUtils.isNotBlank(prop) ? Integer.parseInt(prop) : defaultValue;
    } catch (NumberFormatException e) {
      logger.warn("Invalid format in property {}; src={}, error={}", key, prop);
      return defaultValue;
    }
  }

  public static long getLong(String key, long defaultValue) {
    final var prop = get(key);
    try {
      return StringUtils.isNotBlank(prop) ? Long.parseLong(prop) : defaultValue;
    } catch (NumberFormatException e) {
      logger.warn("Invalid format in property {}; src={}, error={}", key, prop);
      return defaultValue;
    }
  }

  public static boolean getBoolean(String key, boolean defaultValue) {
    final var prop = get(key);
    return StringUtils.isNotBlank(prop) ? Boolean.parseBoolean(prop) : defaultValue;
  }

  /** See {@link #getPropertyCached(String)}. */
  public static String getCached(String key) {
    return instance.getPropertyCached(key);
  }

  public static CompletionStage<String> getCachedAsync(String key, ExecutionState state) {
    return getCachedAsync(key, null, state);
  }

  /** See {@link #getPropertyCached(String, String)}. */
  public static String getCached(String key, String defaultValueIfAbsent) {
    return instance.getPropertyCached(key, defaultValueIfAbsent);
  }

  public static CompletionStage<String> getCachedAsync(
      String key, String defaultValue, ExecutionState state) {
    return instance.getPropertyCachedAsync(key, defaultValue, state);
  }

  public static void getCachedAsync(
      String key,
      String defaultValue,
      ExecutionState state,
      Consumer<String> acceptor,
      Consumer<Throwable> errorHandler) {
    instance.getPropertyCachedAsync(key, defaultValue, state, acceptor, errorHandler);
  }

  public static void getCachedAsync(
      String key,
      Long defaultValueIfAbsent,
      ExecutionState state,
      Consumer<Long> acceptor,
      Consumer<Throwable> errorHandler) {
    instance.getPropertyCachedLongAsync(key, defaultValueIfAbsent, state, acceptor, errorHandler);
  }

  /** See {@link #getPropertyCached(String, Long)}. */
  public static Long getCached(String key, Long defaultValueIfAbsent) {
    return instance.getPropertyCached(key, defaultValueIfAbsent);
  }

  /** See {@link #getPropertyCached(String, Long)}. */
  public static Integer getCached(String key, Integer defaultValueIfAbsent) {
    return instance.getPropertyCached(key, defaultValueIfAbsent);
  }

  /** See {@link #getPropertyCached(String, Double)}. */
  public static Double getCached(String key, Double defaultValueIfAbsent) {
    return instance.getPropertyCached(key, defaultValueIfAbsent);
  }

  /** See {@link #getPropertyCached(String, Boolean)}. */
  public static Boolean getCached(String key, Boolean defaultValueIfAbsent) {
    return instance.getPropertyCached(key, defaultValueIfAbsent);
  }

  /** See {@link #getPropertyCachedLong(String)} (String)}. */
  public static Long getCachedLong(String key) {
    return instance.getPropertyCachedLong(key);
  }

  /** See {@link #getPropertyCachedLong(String)} (String)}. */
  public static Boolean getCachedBoolean(String key) {
    return instance.getPropertyCachedBoolean(key);
  }

  /** See {@link #clearPropertyCache()}. */
  public static void clearCache() {
    instance.clearPropertyCache();
  }

  @ToString
  private static class CacheItem {
    public final String value;
    public final Long timestamp;
    public final AtomicBoolean reloading;

    public CacheItem(String value, Long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
      reloading = new AtomicBoolean(false);
    }
  }
}
