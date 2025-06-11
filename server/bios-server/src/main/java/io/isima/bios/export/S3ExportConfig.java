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

import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ExportStatus;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

public class S3ExportConfig
    implements ExportConfigValidator, AwsCredentialsProvider, AwsCredentials {
  private static final String S3_BUCKET_NAME_KEY = "s3BucketName";
  private static final String S3_ACCESS_KEY_ID = "s3AccessKeyId";
  private static final String S3_SECRET_ACCESS_KEY = "s3SecretAccessKey";
  private static final String S3_REGION_KEY = "s3Region";
  private static final String STORAGE_TYPE = "S3";

  private final ExportDestinationConfig delegate;

  public S3ExportConfig(ExportDestinationConfig config) {
    this.delegate = config;
  }

  public String bucketName() {
    return delegate.getStorageConfig().get(S3_BUCKET_NAME_KEY);
  }

  @Override
  public String accessKeyId() {
    return delegate.getStorageConfig().get(S3_ACCESS_KEY_ID);
  }

  @Override
  public String secretAccessKey() {
    return delegate.getStorageConfig().get(S3_SECRET_ACCESS_KEY);
  }

  public Region region() {
    return Region.of(delegate.getStorageConfig().get(S3_REGION_KEY));
  }

  /**
   * Do a sanity check of the incoming configuration.
   *
   * @throws InvalidRequestException if the configuration is invalid
   */
  @Override
  public void validateExportConfig() throws InvalidRequestException {
    validate(delegate.getStorageConfig(), "storageConfig");
    validate(delegate.getExportDestinationId(), "exportDestinationId");
    validate(delegate.getExportDestinationName(), "exportDestinationName");
    validate(delegate.getStatus(), "status");
    validate(bucketName(), "storageConfig." + S3_BUCKET_NAME_KEY);
    validate(accessKeyId(), "storageConfig." + S3_ACCESS_KEY_ID);
    validate(secretAccessKey(), "storageConfig." + S3_SECRET_ACCESS_KEY);
    validate(delegate.getStorageConfig().get(S3_REGION_KEY), "storageConfig." + S3_REGION_KEY);

    try (var s3Client =
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
            .credentialsProvider(this)
            .region(region())
            .build()) {
      final var async =
          s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName()).build());
      try {
        async.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        if (e.getCause() != null) {
          if (e.getCause() instanceof NoSuchBucketException) {
            throw new InvalidRequestException(
                "Target Bucket not found " + e.getCause().getMessage());
          }
          throw new InvalidRequestException(
              "Unable to access AWS storage service:  " + e.getCause().getMessage());
        }
        throw new InvalidRequestException(
            "Unable to access AWS storage service: " + e.getMessage());
      }
    } catch (Throwable e) {
      if (e instanceof InvalidRequestException) {
        throw e;
      }
      throw new InvalidRequestException("Unable to access AWS storage service: " + e.getMessage());
    }
  }

  private void validate(Object value, String paramName) throws InvalidRequestException {
    if (value == null || ((value instanceof String) && ((String) value).isBlank())) {
      throw new InvalidRequestException(String.format("Parameter '%s' is required", paramName));
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return this;
  }

  public boolean isConfigEquals(ExportDestinationConfig config) {
    return config.getStorageConfig().equals(delegate.getStorageConfig());
  }

  protected ExportDestinationConfig getDelegate() {
    return delegate;
  }

  public static S3ExportConfig from(
      String exportTarget,
      String bucketName,
      String accessKeyId,
      String secretAccessKey,
      String region) {
    final var exportConfig = new ExportDestinationConfig();
    exportConfig.setStorageType(STORAGE_TYPE);
    exportConfig.setStatus(ExportStatus.DISABLED);
    exportConfig.setExportDestinationName(exportTarget);
    exportConfig.setStorageConfig(
        Map.of(
            S3_BUCKET_NAME_KEY, bucketName,
            S3_ACCESS_KEY_ID, accessKeyId,
            S3_SECRET_ACCESS_KEY, secretAccessKey,
            S3_REGION_KEY, region));
    return new S3ExportConfig(exportConfig);
  }
}
