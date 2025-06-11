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
package io.isima.bios.export;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExportDestinationConfig;
import java.time.Duration;
import java.util.List;

/**
 * Encapsulates transformers that does signal to arrow and arrow to parquet transformers.
 *
 * <p>Since signals come and go (including new versions), to reduce bookkeeping this uses a cache of
 * transformers that will eventually get removed if there is no more data being pushed to it for
 * some time. Since transformers are stateless post construction (and since the worker always drains
 * the pipe it is ok to slowly garbage collect unused transformers).
 */
public class Transformers {
  // if no activity for a given signal for 60 minutes, automatically remove the transformer
  private static final long MAX_IDLE_MINUTES = 60;
  private static final String KEY_DELIMITER = ":";

  private final DataExportWorker exportWorker;
  private final Cache<String, PerSignalTransformer> signalTransformerCache;
  private final Cache<String, ParquetToS3Loader> s3LoaderCache;

  public Transformers(DataExportWorker exportWorker) {
    this.exportWorker = exportWorker;
    this.signalTransformerCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(MAX_IDLE_MINUTES))
            .removalListener(new ProviderRemovalListener())
            .build();
    // keep s3 loaders idle atleast for a day once created
    this.s3LoaderCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(MAX_IDLE_MINUTES * 24))
            .removalListener(new S3LoaderRemovalListener())
            .build();
  }

  public void transformEvents(
      StreamDesc signalConfig, List<Event> events, ExportDestinationConfig exportConfig) {
    final var tenantName = signalConfig.getParent().getName().toLowerCase();
    final var signalName = signalConfig.getName().toLowerCase();
    final long version = signalConfig.getSchemaVersion();
    final var compositeKey = toKey(tenantName, signalName, String.valueOf(version));

    var signalXformer = signalTransformerCache.getIfPresent(compositeKey);
    if (signalXformer == null) {
      var s3Loader = s3LoaderCache.getIfPresent(tenantName);
      if (s3Loader == null) {
        s3Loader = new ParquetToS3Loader(new S3ExportConfig(exportConfig));
        s3LoaderCache.put(tenantName, s3Loader);
      }
      final var arrowTransformer = new SignalToArrowTransformer(signalConfig);
      final var parquetTransformer =
          new ArrowToParquetTransformer(
              tenantName, signalName, version, arrowTransformer.getDataPipe());
      signalXformer = new PerSignalTransformer(arrowTransformer, parquetTransformer);
      signalTransformerCache.put(compositeKey, signalXformer);
      exportWorker.registerDataProvider(
          parquetTransformer, s3Loader.getExporter(signalName, version));
    }
    signalXformer.getArrowTransformer().transformEvents(events);
  }

  public void reLoadIfRequired(String tenantName, ExportDestinationConfig exportConfig) {
    final var s3WithVersion = s3LoaderCache.getIfPresent(tenantName);
    if (s3WithVersion == null || !s3WithVersion.getExportConfig().isConfigEquals(exportConfig)) {
      s3LoaderCache.invalidate(tenantName);
      final var s3Config = new S3ExportConfig(exportConfig);
      final var s3Loader = new ParquetToS3Loader(s3Config);
      s3LoaderCache.put(tenantName, s3Loader);
    }
  }

  private static String toKey(String tenantName, String signalName, String version) {
    return signalName + KEY_DELIMITER + tenantName + KEY_DELIMITER + version;
  }

  private static String[] fromKey(String key) {
    return key.split(KEY_DELIMITER);
  }

  public void shutdown() {
    signalTransformerCache.invalidateAll();
    exportWorker.shutdown();
    s3LoaderCache.invalidateAll();
  }

  private final class S3LoaderRemovalListener
      implements RemovalListener<String, ParquetToS3Loader> {

    @Override
    public void onRemoval(RemovalNotification<String, ParquetToS3Loader> removalNotification) {
      final var key = removalNotification.getKey();
      final var value = removalNotification.getValue();
      if (key != null && value != null) {
        try {
          value.close();
        } catch (Exception e) {
        }
      }
    }
  }

  private static final class PerSignalTransformer {
    private final ArrowToParquetTransformer parquetTransformer;
    private final SignalToArrowTransformer arrowTransformer;

    private PerSignalTransformer(
        SignalToArrowTransformer arrowTransformer, ArrowToParquetTransformer parquetTransformer) {
      this.arrowTransformer = arrowTransformer;
      this.parquetTransformer = parquetTransformer;
    }

    public ArrowToParquetTransformer getParquetTransformer() {
      return parquetTransformer;
    }

    public SignalToArrowTransformer getArrowTransformer() {
      return arrowTransformer;
    }
  }

  private final class ProviderRemovalListener
      implements RemovalListener<String, PerSignalTransformer> {

    @Override
    public void onRemoval(RemovalNotification<String, PerSignalTransformer> removalNotification) {
      final var key = removalNotification.getKey();
      final var value = removalNotification.getValue();
      if (key != null && value != null) {
        try {
          final var pt = value.getParquetTransformer();
          final var s3Loader = s3LoaderCache.getIfPresent(pt.getTenantName());
          if (s3Loader != null) {
            s3Loader.removeExporter(pt.getSignalName(), pt.getSignalVersion());
          }
          exportWorker.deregisterDataProvider(value.getParquetTransformer());
        } catch (Exception e) {
        }
      }
    }
  }
}
