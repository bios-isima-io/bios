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
package io.isima.bios.admin.v1.impl;

import static io.isima.bios.common.BiosConstants.TENANT_SYSTEM;

import com.google.common.base.Preconditions;
import io.isima.bios.admin.v1.FeatureAsContextInfo;
import io.isima.bios.admin.v1.StreamComparators;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.TenantStoreDesc;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tenant configuration data container class. */
public class Tenants {
  private static final Logger logger = LoggerFactory.getLogger(Tenants.class);

  private static final long CLEANUP_GRACE_MILLIS = 60L * 1000; // 10 minutes

  protected final Map<String, List<TenantDesc>> tenants;

  public Tenants() {
    tenants = new ConcurrentHashMap<>();
  }

  /**
   * Lists available tenant names.
   *
   * @return List of available tenant names.
   */
  public List<String> listTenantNames() {
    List<String> tenantNames = new ArrayList<>();

    tenants.forEach(
        (name, entry) -> {
          TenantDesc config = getTenantDescOrNull(name, false);
          if (config != null) {
            tenantNames.add(config.getName());
          }
        });
    Collections.sort(tenantNames, String.CASE_INSENSITIVE_ORDER);
    return tenantNames;
  }

  /**
   * Lists available tenant names.
   *
   * @return List of available tenant names with version
   */
  public Set<String> listTenantNamesWithVersion() {
    Set<String> tenantNames = new HashSet<>();

    tenants.forEach(
        (name, entry) -> {
          TenantDesc config = getTenantDescOrNull(name, false);
          if (config != null) {
            tenantNames.add(config.getName() + "." + config.getVersion());
          }
        });
    return tenantNames;
  }

  /**
   * Lists all active table name identifiers.
   *
   * @return List of all active table name identifiers
   */
  public Set<String> listAllActiveTableNameIdentifiers() {
    Set<String> streamNames = new HashSet<>();

    tenants.forEach(
        (name, entry) -> {
          TenantDesc tenantDesc = getTenantDescOrNull(name, false);
          if (tenantDesc != null) {
            List<StreamDesc> streamDescList =
                tenantDesc.getNonDeletedStreamVersionsInVersionOrder();
            streamDescList.forEach(
                streamDesc -> {
                  streamNames.add(
                      tenantDesc.getName()
                          + "."
                          + streamDesc.getName()
                          + "."
                          + streamDesc.getSchemaVersion());
                  streamNames.add(
                      tenantDesc.getName()
                          + "."
                          + streamDesc.getName()
                          + "."
                          + streamDesc.getVersion());
                });
          }
        });
    return streamNames;
  }

  /**
   * Method to get tenant configuration.
   *
   * <p>The method is meant to be used for serving external components. Modifying returned object
   * does not affect the internal data of this class.
   *
   * @param tenantName Target tenant name.
   * @return Found tenant config object.
   * @throws NoSuchTenantException When no entries are found with specified tenant name.
   */
  public TenantConfig getTenantConfig(String tenantName) throws NoSuchTenantException {
    TenantConfig tenantConfig = getTenantConfigOrNull(tenantName);
    if (tenantConfig == null) {
      throw new NoSuchTenantException(tenantName);
    }
    return tenantConfig;
  }

  protected TenantConfig getTenantConfigOrNull(String tenantName) {
    final TenantDesc desc = getTenantDescOrNull(tenantName, false);
    return desc != null ? desc.toTenantConfig() : null;
  }

  /**
   * Package-scope method to get tenant descriptor by name.
   *
   * <p>Modifying the output object affects internal data of this class.
   *
   * @param tenantName Target tenant name
   * @param ignoreFlags The method ignores deleted flag when set true. Otherwise the method treats
   *     an entry with deleted flag as non-existing.
   * @return Found tenant config object.
   * @throws NoSuchTenantException When none of tenant entries with specified name exist.
   */
  TenantDesc getTenantDesc(String tenantName, boolean ignoreFlags) throws NoSuchTenantException {
    TenantDesc tenantDesc = getTenantDescOrNull(tenantName, ignoreFlags);
    if (tenantDesc == null) {
      if (tenantName.equals(TENANT_SYSTEM)) {
        logger.warn(
            "System tenant not found; tenants={}, keys={}",
            System.identityHashCode(tenants),
            tenants.keySet());
      } else {
        logger.debug(
            "Tenant={} not found; tenants={}, keys={}",
            tenantName,
            System.identityHashCode(tenants),
            tenants.keySet());
      }
      throw new NoSuchTenantException(tenantName);
    }
    return tenantDesc;
  }

  /**
   * Package-scope method to provide tenant descriptor.
   *
   * @param tenantName Target tenant name
   * @param ignoreFlags The method ignores deleted flag when set true. Otherwise the method treats
   *     an entry with deleted flag as non-existing.
   * @return Tenant config object when the target is found, otherwise null.
   */
  TenantDesc getTenantDescOrNull(String tenantName, boolean ignoreFlags) {
    List<TenantDesc> tenantDescs = tenants.get(tenantName.toLowerCase());
    if (tenantDescs == null || tenantDescs.isEmpty()) {
      return null;
    }
    final var desc = tenantDescs.get(0);
    if (desc.isDeleted() && !ignoreFlags) {
      return null;
    }
    return desc;
  }

  /**
   * Adds a tenant config object.
   *
   * @param tenantDesc Tenant descriptor object to add.
   */
  public synchronized void addTenant(TenantDesc tenantDesc) {
    if (tenantDesc == null) {
      throw new IllegalArgumentException("tenantDesc may not be null");
    }
    if (tenantDesc.getName() == null || tenantDesc.getName().isEmpty()) {
      throw new IllegalArgumentException("tenantDesc name must be set");
    }
    if (tenantDesc.getVersion() == null) {
      throw new IllegalArgumentException("tenantDesc version may not be null");
    }
    final String key = tenantDesc.getName().toLowerCase();
    List<TenantDesc> oldList = tenants.get(key);
    List<TenantDesc> newList = new ArrayList<>();
    boolean inserted = false;
    if (oldList != null) {
      for (TenantDesc desc : oldList) {
        if (!inserted && tenantDesc.getVersion() >= desc.getVersion()) {
          newList.add(tenantDesc);
          inserted = true;
        }
        if (!tenantDesc.getVersion().equals(desc.getVersion())) {
          newList.add(desc);
        }
      }
    }
    if (!inserted) {
      newList.add(tenantDesc);
    }
    tenants.put(key, newList);
    logger.debug("Added key={} to tenants={}", key, System.identityHashCode(tenants));
  }

  /**
   * Package-scope method to find stream desc object.
   *
   * @param tenantName Target tenant name.
   * @param ignoreTenantFlags Whether if the method ignores deleted flags of tenant config objects.
   * @param streamName Target stream name.
   * @param ignoreStreamFlags Whether if the method ignores deleted flags of stream config objects.
   * @return Found stream config or null.
   * @throws NoSuchTenantException Specified tenant does not exist.
   */
  StreamDesc getStreamConfigOrNull(
      String tenantName, boolean ignoreTenantFlags, String streamName, boolean ignoreStreamFlags)
      throws NoSuchTenantException {
    TenantDesc tenantDesc = getTenantDescOrNull(tenantName, ignoreTenantFlags);
    if (tenantDesc == null || (!ignoreTenantFlags && tenantDesc.isDeleted())) {
      throw new NoSuchTenantException(tenantName);
    }
    return tenantDesc.getStream(streamName, ignoreStreamFlags);
  }

  /**
   * List available streams with the specified tenant.
   *
   * @param tenantConfig The target tenant.
   * @return list of StreamConfigs
   */
  public static List<StreamConfig> listStreams(TenantConfig tenantConfig) {
    Map<String, StreamConfig> streams = new LinkedHashMap<>();
    for (StreamConfig streamConfig : tenantConfig.getStreams()) {
      StreamConfig existing = streams.get(streamConfig.getName().toLowerCase());
      if (existing == null || streamConfig.getVersion() >= existing.getVersion()) {
        streams.put(streamConfig.getName().toLowerCase(), streamConfig);
      }
    }
    List<StreamConfig> streamList = new ArrayList<>();
    streams.forEach(
        (key, value) -> {
          streamList.add(value);
        });
    return streamList;
  }

  /**
   * Utility method to find a stream store description out of ones the specified tenant config
   * contains.
   *
   * @param tenantStoreDesc Tenant store descsription that provides collection of streams.
   * @param streamName Target stream name.
   * @param ignoreStreamFlags If true, the method includes stream config objects with deleted flags
   *     to the search target.
   * @return Found stream config object. If none found, null is returned.
   */
  public static StreamStoreDesc findStreamStoreDesc(
      TenantStoreDesc tenantStoreDesc, String streamName, boolean ignoreStreamFlags) {
    StreamStoreDesc out = null;
    for (StreamStoreDesc streamStoreDesc : tenantStoreDesc.getStreams()) {
      if (streamStoreDesc.getName().equalsIgnoreCase(streamName)) {
        if (out == null || streamStoreDesc.getVersion() >= out.getVersion()) {
          out = streamStoreDesc;
        }
      }
    }
    return (out != null && (ignoreStreamFlags || !out.isDeleted())) ? out : null;
  }

  /**
   * Utility method to check if two TenantConfig objects are identical.
   *
   * <p>This method is meant to be used for checking if an input object changes existing one. So the
   * method ignores deleted flag and version.
   *
   * @param left Left parameter
   * @param right Right parameter
   * @return true if two parameters are identical, false otherwise.
   */
  public static boolean equals(TenantConfig left, TenantConfig right) {
    Preconditions.checkArgument(left != null && right != null, "arguments may not be null");
    if (left == right) {
      return true;
    }
    if (!pointerConsistencyCheck(left.getName(), right.getName())
        || (left.getName() != null && !left.getName().equalsIgnoreCase(right.getName()))) {
      return false;
    }
    List<StreamConfig> ls = left.getStreams();
    List<StreamConfig> rs = right.getStreams();
    if ((ls != null && rs == null) || (ls == null && rs != null)) {
      return false;
    }
    if (ls != null) {
      if (ls.size() != rs.size()) {
        return false;
      }
      for (int i = 0; i < ls.size(); ++i) {
        if (!StreamComparators.equals(ls.get(i), rs.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  private static <T> boolean pointerConsistencyCheck(T left, T right) {
    return (left == null && right == null) || (left != null && right != null);
  }

  /** Executes cache cleanup. */
  public void cleanUp() {
    long oldEnough = System.currentTimeMillis() - CLEANUP_GRACE_MILLIS;
    trimOldTenants(oldEnough);
  }

  protected void trimOldTenants(long expiry) {
    for (String key : tenants.keySet()) {
      final List<TenantDesc> trash = new ArrayList<>();
      synchronized (this) {
        final List<TenantDesc> tenantsList = tenants.get(key);
        final List<TenantDesc> newList = new ArrayList<>();
        boolean first = true;
        for (TenantDesc tenantDesc : tenantsList) {
          if ((first && !tenantDesc.isDeleted()) || tenantDesc.getVersion() > expiry) {
            newList.add(tenantDesc);
          } else {
            trash.add(tenantDesc);
          }
          first = false;
        }
        if (newList.isEmpty()) {
          tenants.remove(key);
          logger.debug("Removed key={} from tenants={}", key, System.identityHashCode(tenants));
        } else {
          newList.get(0).trimOldStreams(expiry);
          tenants.put(key, newList);
          logger.debug("Added key={} to tenants={}", key, System.identityHashCode(tenants));
        }
      }
      if (!trash.isEmpty()) {
        // TODO(TFOS-1584): Handle trash
        if (logger.isDebugEnabled()) {
          StringBuilder versions = new StringBuilder();
          String delimiter = "[";
          for (TenantDesc desc : trash) {
            versions
                .append(delimiter)
                .append(desc.getVersion())
                .append("(del=")
                .append(desc.isDeleted() ? "y" : "n")
                .append(")");
            delimiter = ",";
          }
          versions.append("]");
          logger.debug(
              "Removed old tenant entries; tenant={}, versions={}",
              trash.get(0).getName(),
              versions);
        }
      }
    }
  }

  public FeatureAsContextInfo getFeatureAsContextInfo(String tenantName, String contextName) {
    final var tenantDesc = getTenantDescOrNull(tenantName, false);
    if (tenantDesc == null) {
      return null;
    }
    return tenantDesc.getFacInfoMap().get(contextName.toLowerCase());
  }

  public void addFeatureAsContextInfo(
      String tenantName, String contextName, FeatureAsContextInfo info) {
    final var tenantDesc = getTenantDescOrNull(tenantName, false);
    if (tenantDesc == null) {
      return;
    }
    tenantDesc.getFacInfoMap().put(contextName.toLowerCase(), info);
  }

  public void deleteFeatureAsContextInfo(String tenantName, String contextName) {
    final var tenantDesc = getTenantDescOrNull(tenantName, false);
    if (tenantDesc == null) {
      return;
    }
    tenantDesc.getFacInfoMap().remove(contextName.toLowerCase());
  }
}
