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
package io.isima.bios.recorder;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.isima.bios.models.AppType;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * OperationMeasurementRegistry keeps metrics recorders for group keys.
 *
 * <p>They are used during a metrics measurement period, the results collected at the end for each
 * group key.
 *
 * <p>Operations measurement registry is typically a singleton, but is externally enforced by
 * clients of the registry.
 */
public class OperationsMeasurementRegistry implements InternalRegister {
  public static OperationsMeasurementRegistry.Builder newRegistryBuilder(String systemTenantName) {
    return new Builder(systemTenantName);
  }

  private static final int DEFAULT_CONTROL_REGISTRY_EXPIRY = 10;

  private final LoadingCache<OperationsMetricsDimensions, OperationMetricGroupRecorder>
      recorderRegistry;
  private final OperationsMetricsCollector requestMetricManager;
  private final String systemTenantName;

  OperationsMeasurementRegistry(
      String systemTenantName,
      Ticker ticker,
      int maxSuccessBandwidth,
      int maxErrorBandwidth,
      Supplier<String> nodeNameSupplier,
      int numDestinations,
      int maxRatePerMinute) {
    this.systemTenantName = systemTenantName;
    requestMetricManager =
        new OperationsMetricsCollector(
            this, maxSuccessBandwidth, maxErrorBandwidth, nodeNameSupplier, numDestinations);

    // using a simple cache here allows the system to be independent of tenants getting deleted
    // Also, using a cache for control operations allows to track operations of tenants and streams
    // without worrying about recorders getting deleted from underneath when a tenant or
    // stream gets deleted or recorders getting leaked when tenants and streams are deleted over
    // time.
    recorderRegistry =
        CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(DEFAULT_CONTROL_REGISTRY_EXPIRY))
            .removalListener(new ControlRecorderRemovalListener())
            .ticker(ticker)
            .maximumSize(5000)
            .build(
                new CacheLoader<>() {
                  @Override
                  public OperationMetricGroupRecorder load(OperationsMetricsDimensions key) {
                    return new OperationMetricGroupRecorder(
                        key.getTenantName(),
                        key.getStreamName(),
                        key.getAppName(),
                        key.getAppType(),
                        key.getRequestType());
                  }
                });
  }

  OperationsMeasurementRegistry(
      String systemTenantName,
      int maxSuccessBandwidth,
      int maxErrorBandwidth,
      Supplier<String> nodeNameSupplier,
      int numDestinations,
      int maxRatePerMinute) {
    this(
        systemTenantName,
        Ticker.systemTicker(),
        maxSuccessBandwidth,
        maxErrorBandwidth,
        nodeNameSupplier,
        numDestinations,
        maxRatePerMinute);
  }

  /**
   * Get a specific recorder for a metrics group key.
   *
   * @param tenantName Name of the tenant
   * @param streamName Optional Name of the stream, could be null or empty for control operations
   *     unrelated to tenant
   * @param appName Name of the client application
   * @param appType Application type
   * @param requestType Request type
   * @return Recorder to accumulate stats
   */
  public OperationMetricGroupRecorder getRecorder(
      String tenantName,
      String streamName,
      String appName,
      AppType appType,
      RequestType requestType) {
    if (appName == null) {
      appName = "";
    }
    if (appType == null) {
      appType = AppType.UNKNOWN;
    }
    return recorderRegistry.getUnchecked(
        new OperationsMetricsDimensions(tenantName, streamName, appName, appType, requestType));
  }

  /** Gets operation manager. */
  public OperationsMetricsCollector getOperationsMetricsCollector() {
    return requestMetricManager;
  }

  /** Reset the registry. */
  public void resetRegistry() {
    // no need to reset control plane as that will be cleared automatically
    // checkpoint before clearing
    /*
    dataRecorderRegistry.forEach(requestMetricManager::checkpointOnDeregister);
    dataRecorderRegistry.clear();
     */
    // registerDefault(systemTenantName);
  }

  /** Called when a tenant is successfully added. */
  public void addTenant(String tenantName) {}

  /** Called after a tenant is successfully removed. */
  public void removeTenant(String tenantName) {}

  private final class ControlRecorderRemovalListener
      implements RemovalListener<OperationsMetricsDimensions, OperationMetricGroupRecorder> {

    @Override
    public void onRemoval(
        RemovalNotification<OperationsMetricsDimensions, OperationMetricGroupRecorder>
            removalNotification) {
      final var key = removalNotification.getKey();
      OperationMetricGroupRecorder recorder = removalNotification.getValue();
      if (recorder != null && key != null) {
        requestMetricManager.checkpointOnRemoval(key, recorder);
      }
    }
  }

  public static class Builder {
    private static final int DEFAULT_MAX_SUCCESS_BANDWIDTH = 100;
    private static final int DEFAULT_MAX_ERROR_BANDWIDTH = 2;
    // max rate for control operations (creation only)..
    // Only to control allocated recorders, in case there is a DOS attack by non-authenticated
    // users. Does not rate limit the request itself
    private static final int DEFAULT_MAX_RATE_LIMIT_PER_MINUTE = 50;

    private static final int DEFAULT_NUM_DESTINATIONS_PER_RECORDER = 1;
    private static final int MAX_ALLOWED_DESTINATIONS = 3;

    private static final Supplier<String> DEFAULT_NODE_NAME_SUPPLIER = () -> "";

    private final String systemTenantName;
    private int maxSuccessBandwidth;
    private int maxErrorBandwidth;
    private Supplier<String> nodeNameSupplier;
    private int numDestinationsPerRecorder;
    private int maxRateLimitPerMinute;

    private Builder(String systemTenantName) {
      Objects.requireNonNull(systemTenantName, "Must specify one system tenant");
      this.maxSuccessBandwidth = DEFAULT_MAX_SUCCESS_BANDWIDTH;
      this.maxErrorBandwidth = DEFAULT_MAX_ERROR_BANDWIDTH;
      this.nodeNameSupplier = DEFAULT_NODE_NAME_SUPPLIER;
      this.numDestinationsPerRecorder = DEFAULT_NUM_DESTINATIONS_PER_RECORDER;
      this.maxRateLimitPerMinute = DEFAULT_MAX_RATE_LIMIT_PER_MINUTE;
      this.systemTenantName = systemTenantName;
    }

    private Builder(Builder other) {
      this.systemTenantName = other.systemTenantName;
      this.maxSuccessBandwidth = other.maxSuccessBandwidth;
      this.maxErrorBandwidth = other.maxErrorBandwidth;
      this.nodeNameSupplier = other.nodeNameSupplier;
      this.numDestinationsPerRecorder = other.numDestinationsPerRecorder;
      this.maxRateLimitPerMinute = other.maxRateLimitPerMinute;
    }

    public Builder maxSuccessBandwidth(int bandwidth) {
      if (bandwidth < 2) {
        throw new IllegalArgumentException("Success Bandwidth must be at least 2");
      }
      // create another builder every time, in case user wants to retain old builder state
      // and reuse it.
      Builder otherBuilder = new Builder(this);
      otherBuilder.maxSuccessBandwidth = bandwidth;
      return otherBuilder;
    }

    public Builder maxErrorBandwidth(int bandwidth) {
      if (bandwidth < 2) {
        throw new IllegalArgumentException("Error Bandwidth must be at least 2");
      }
      Builder otherBuilder = new Builder(this);
      otherBuilder.maxErrorBandwidth = bandwidth;
      return otherBuilder;
    }

    public Builder nodeNameSupplier(Supplier<String> nodeSupplier) {
      Objects.requireNonNull(nodeSupplier, "Node name supplier must be non null");
      Builder otherBuilder = new Builder(this);
      otherBuilder.nodeNameSupplier = nodeSupplier;
      return otherBuilder;
    }

    public Builder numDestinations(int numDestinations) {
      if (numDestinations < 1 || numDestinations > MAX_ALLOWED_DESTINATIONS) {
        throw new IllegalArgumentException(
            "Illegal number of destinations."
                + " Must be between 1 and "
                + MAX_ALLOWED_DESTINATIONS);
      }
      Builder otherBuilder = new Builder(this);
      otherBuilder.numDestinationsPerRecorder = numDestinations;
      return otherBuilder;
    }

    public Builder maxRatePerMinute(int maxRatePerMinute) {
      if (maxRateLimitPerMinute < 10) {
        throw new IllegalArgumentException(
            "Illegal rate per minute for create operations."
                + " Must be greater than or equal to 10.");
      }
      Builder otherBuilder = new Builder(this);
      otherBuilder.maxRateLimitPerMinute = maxRateLimitPerMinute;
      return otherBuilder;
    }

    public OperationsMeasurementRegistry build() {
      return new OperationsMeasurementRegistry(
          this.systemTenantName,
          this.maxSuccessBandwidth,
          this.maxErrorBandwidth,
          this.nodeNameSupplier,
          this.numDestinationsPerRecorder,
          this.maxRateLimitPerMinute);
    }
  }

  @Override
  public void runForAllRecorders(
      BiConsumer<OperationsMetricsDimensions, OperationMetricGroupRecorder> consumer) {
    final Set<OperationsMetricsDimensions> processed = new HashSet<>();
    for (final var entry : recorderRegistry.asMap().entrySet()) {
      final var key = entry.getValue().toRecorderKey();
      if (processed.contains(key)) {
        continue;
      }
      processed.add(key);
      consumer.accept(key, entry.getValue());
    }
  }

  @Override
  public String getSystemTenant() {
    return systemTenantName;
  }
}
