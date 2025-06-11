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
package io.isima.bios.data.impl.storage;

import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.data.storage.cassandra.CassandraDataStoreUtils;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class CassTenant {
  private static Logger logger = LoggerFactory.getLogger(CassTenant.class);

  private final String name;
  private final Long version;
  private final String keyspaceName;
  private final Map<String, CassStream> cassStreams;
  private final Map<String, String> deletedStreams; // For debugging.

  public CassTenant(TenantDesc tenantDesc) {
    if (tenantDesc == null
        || tenantDesc.getName() == null
        || tenantDesc.getName().isEmpty()
        || tenantDesc.getVersion() == null) {
      throw new IllegalArgumentException("tenant config must have name and version parameters");
    }
    this.name = tenantDesc.getName();
    this.version = tenantDesc.getVersion();
    this.keyspaceName = CassandraDataStoreUtils.generateKeyspaceName(name, version);
    cassStreams = new ConcurrentHashMap<>();
    deletedStreams = new ConcurrentHashMap<>();
  }

  /**
   * Find CassStream object registered to this object by specified StreamConfig.
   *
   * @param streamConfig Key to find the entry.
   * @return The method returns a CassStream object when specified instance is found, otherwise
   *     returns null.
   */
  public <T extends CassStream> T getCassStream(StreamConfig streamConfig) {
    return getCassStream(streamConfig, false);
  }

  public <T extends CassStream> T getCassStream(
      StreamConfig streamConfig, boolean logDetailsIfNotFound) {
    if (streamConfig == null) {
      throw new IllegalArgumentException("cass stream config must be non-null");
    }
    return getCassStream(streamConfig.getName(), streamConfig.getVersion(), logDetailsIfNotFound);
  }

  /**
   * Find CassStream object registered to this object by name and version.
   *
   * @param name Stream name
   * @param version Stream version
   * @return Found stream, or null if not found.
   */
  public <T extends CassStream> T getCassStream(String name, Long version) {
    return getCassStream(name, version, false);
  }

  @SuppressWarnings("unchecked")
  public <T extends CassStream> T getCassStream(
      String name, Long version, boolean logDetailsIfNotFound) {
    if (name == null || version == null) {
      throw new IllegalArgumentException("parameters may not be null");
    }
    final var key = toCassStreamKey(name, version);
    final var cassStream = (T) cassStreams.get(key);
    if (logDetailsIfNotFound && (cassStream == null)) {
      logger.warn(
          "CassStream not found: stream={}, version={}, key={}, keysPresent={}",
          name,
          version,
          key,
          cassStreams.keySet());
    }
    if (deletedStreams.containsKey(key)) {
      logger.warn(
          "Requested CassStream was found in deletedStreams; key={}, deletedStreams={}",
          key,
          deletedStreams);
    }
    return cassStream;
  }

  public List<CassStream> getCassStreams() {
    return new ArrayList<>(cassStreams.values());
  }

  /**
   * Register a CassStream object.
   *
   * @param config CassStream object to register.
   */
  public void putCassStream(CassStream config) {
    if (config == null) {
      throw new IllegalArgumentException("putting cass stream config must be non-null");
    }
    String stream = config.getStreamName();
    long version = config.getStreamVersion();
    logger.debug("putting stream {}.{} with version {}", getName(), stream, version);
    final var key = toCassStreamKey(stream, version);
    logger.debug("Adding CassStream tenant={}, key={}", getName(), key);
    cassStreams.put(key, config);
    if (deletedStreams.containsKey(key)) {
      logger.warn(
          "Newly added CassStream found in deletedStreams; key={}, deletedStreams={}",
          key,
          deletedStreams);
    }
  }

  /**
   * Unregister a CassStream object.
   *
   * @param cassStream CassStream object to remove
   */
  public void removeCassStream(CassStream cassStream) {
    if (cassStream == null) {
      throw new IllegalArgumentException("cass stream may not be null");
    }
    final var key = toCassStreamKey(cassStream.getStreamName(), cassStream.getStreamVersion());
    logger.debug("Removing CassStream tenant={}, key={}", getName(), key);
    if (!cassStreams.containsKey(key)) {
      logger.warn("Attempting to remove stream that is not present; key={}", key);
    }
    cassStreams.remove(key);
    if (deletedStreams.containsKey(key)) {
      logger.warn(
          "This stream was previously removed; key={}, timestamp={}", key, deletedStreams.get(key));
    }
    deletedStreams.put(key, StringUtils.tsToIso8601(System.currentTimeMillis()));
  }

  // this will be automatically inlined by JVM
  private static String toCassStreamKey(String name, Long version) {
    return name.toLowerCase() + "." + version;
  }

  private static String toVirtualStreamKey(String name) {
    // only latest versions of virtual streams are cached
    return name.toLowerCase();
  }
}
