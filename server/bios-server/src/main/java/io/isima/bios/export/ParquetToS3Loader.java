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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class ParquetToS3Loader implements AutoCloseable {
  private static final String S3_KEY_FORMAT =
      "bios/signals/%s/version=%s/year=%d/month=%d/day=%d/part-%s.parquet";
  private static final String VERSION_TIMESTAMP_FORMAT = "dd-MM-yyyy'T'HH_mm_ss.SSS";
  private static final String FILE_TIMESTAMP_FORMAT = "dd-MM-yyyy'T'HH_mm_ss";

  private final String s3BucketName;
  private final S3AsyncClient s3AsyncClient;
  private final Map<String, S3Exporter> signalExporter;
  private final S3ExportConfig exportConfig;

  public ParquetToS3Loader(S3ExportConfig config) {
    this.exportConfig = config;
    s3AsyncClient =
        S3AsyncClient.builder()
            .httpClientBuilder(
                NettyNioAsyncHttpClient.builder()
                    .connectionMaxIdleTime(Duration.ofSeconds(180))
                    .connectionAcquisitionTimeout(Duration.ofSeconds(60))
                    .connectionTimeout(Duration.ofSeconds(120))
                    .maxConcurrency(120)
                    .maxPendingConnectionAcquires(50000))
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(
                        RetryPolicy.builder()
                            .backoffStrategy(BackoffStrategy.defaultStrategy())
                            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
                            .numRetries(5)
                            .retryCondition(RetryCondition.defaultRetryCondition())
                            .build())
                    .apiCallTimeout(Duration.ofSeconds(120))
                    .build())
            .credentialsProvider(config)
            .region(config.region())
            .build();
    this.s3BucketName = config.bucketName();
    this.signalExporter = new ConcurrentHashMap<>();
  }

  public DataExporter<ByteBuffer> getExporter(String signalName, long versionStamp) {
    return signalExporter.computeIfAbsent(
        toKey(signalName, versionStamp), (k) -> new S3Exporter(signalName, versionStamp));
  }

  public void removeExporter(String signalName, long version) {
    signalExporter.remove(toKey(signalName, version));
  }

  @Override
  public void close() throws Exception {
    signalExporter.clear();
    s3AsyncClient.close();
  }

  public S3ExportConfig getExportConfig() {
    return exportConfig;
  }

  private final class S3Exporter implements DataExporter<ByteBuffer> {
    private final String signalName;
    private final String versionStamp;

    private S3Exporter(String signalName, long versionStamp) {
      this.signalName = signalName;
      this.versionStamp = formatTimestamp(toZdt(versionStamp), VERSION_TIMESTAMP_FORMAT);
    }

    @Override
    public CompletableFuture<Void> exportDataAsync(ByteBuffer parquetdata, long timestamp) {
      PutObjectRequest request =
          PutObjectRequest.builder().bucket(s3BucketName).key(toS3PartitionKey(timestamp)).build();

      return s3AsyncClient
          .putObject(request, AsyncRequestBody.fromByteBuffer(parquetdata))
          .thenAccept((r) -> {});
    }

    private String toS3PartitionKey(long timestamp) {
      final var zdt = toZdt(timestamp);
      return String.format(
          S3_KEY_FORMAT,
          signalName,
          versionStamp,
          zdt.getYear(),
          zdt.getMonth().getValue(),
          zdt.getDayOfMonth(),
          formatTimestamp(zdt, FILE_TIMESTAMP_FORMAT));
    }
  }

  private static String formatTimestamp(ZonedDateTime zdt, String pattern) {
    return zdt.format(DateTimeFormatter.ofPattern(pattern));
  }

  private static ZonedDateTime toZdt(long timestamp) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC"));
  }

  private static String toKey(String signalName, long version) {
    // signal name is assumed to be lower case when it reaches here
    return signalName + ":" + version;
  }
}
