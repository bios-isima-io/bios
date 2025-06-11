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
package io.isima.bios.admin.v1;

import static io.isima.bios.models.v1.StreamType.CONTEXT;
import static java.lang.Boolean.FALSE;

import io.isima.bios.common.Constants;
import io.isima.bios.data.impl.TenantId;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.exceptions.DirectedAcyclicGraphException;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.DirectedAcyclicGraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Tenant configuration includes name and stream config.
 */
@ToString
public class TenantDesc extends Duplicatable {
  private static final Logger logger = LoggerFactory.getLogger(TenantDesc.class);

  private static final String KEY_DELIMITER = "\t";

  /*
   * Tenant name.
   */
  @Getter private final String name;

  @Getter private final String normalizedName;

  @Getter @Setter private String domain;

  /*
   * Tenant can have multiple streams defined.
   */
  protected final Map<String, List<StreamDesc>> streamsMap;

  @Getter private final Long version;

  @Getter private Integer maxAllocatedStreamNameProxy;

  /**
   * Whether stream or attribute proxy information was changed in memory after the last read from
   * the database. If it is dirty, it needs to be written to the database soon.
   */
  @Getter @Setter private boolean proxyInformationDirty = false;

  @Setter()
  @Accessors(chain = true)
  private Boolean deleted;

  @Getter
  @Setter()
  @Accessors(chain = true)
  private String appMaster;

  /** Map of signal aliases (used for BI) and stream names. */
  private Map<String, String> signalAliases;

  private final DirectedAcyclicGraph streamDependencies;

  @Getter private final Map<String, FeatureAsContextInfo> facInfoMap;

  /**
   * Constructor with name.
   *
   * @param name Tenant name
   */
  private TenantDesc(String name, Long version, Boolean deleted, DirectedAcyclicGraph graph) {
    this.name = name;
    this.normalizedName = this.name.toLowerCase();
    this.version = version;
    this.deleted = deleted;
    streamsMap = new ConcurrentHashMap<>();
    signalAliases = new HashMap<>();
    this.streamDependencies = graph;
    this.facInfoMap = new ConcurrentHashMap<>();
  }

  public TenantDesc(String name, Long version, Boolean deleted) {
    this(name, version, deleted, new DirectedAcyclicGraph());
  }

  public TenantDesc(TenantConfig tenantConfig) {
    this(
        tenantConfig,
        tenantConfig.getVersion() != null ? tenantConfig.getVersion() : System.currentTimeMillis());
  }

  public TenantDesc(TenantConfig tenantConfig, Long version) {
    this.name = tenantConfig.getName();
    this.normalizedName = this.name.toLowerCase();
    this.domain = tenantConfig.getDomain();
    this.version = version;
    this.deleted = tenantConfig.isDeleted();
    this.appMaster = tenantConfig.getAppMaster();
    streamsMap = new ConcurrentHashMap<>();
    signalAliases = new HashMap<>();
    this.streamDependencies = new DirectedAcyclicGraph();
    for (StreamConfig streamConfig : tenantConfig.getStreams()) {
      StreamDesc streamDesc = new StreamDesc(streamConfig);
      streamDesc.setParent(this);
      addStream(streamDesc);
    }
    this.facInfoMap = new ConcurrentHashMap<>();
  }

  public TenantId getId() {
    return TenantId.of(normalizedName, version);
  }

  /**
   * Picks up only necessary stream store descriptions.
   *
   * <p>The method takes only non-deleted streams. Also, for a context, only the latest version of
   * the descriptions is picked up to avoid unwanted side effects during the following AdminInternal
   * initialization.
   *
   * @param streams All loaded stream store descriptions
   * @return Necessary stream store descriptions. Entries are sorted by version for each stream
   *     name.
   */
  private List<StreamStoreDesc> selectStreamStoreDescs(List<StreamStoreDesc> streams) {
    Map<String, List<StreamStoreDesc>> sortedStreams = new HashMap<>();
    for (StreamStoreDesc streamStoreDesc : streams) {
      final var streamName =
          streamStoreDesc.getType() + "." + streamStoreDesc.getName().toLowerCase();
      final var streamVersions =
          sortedStreams.computeIfAbsent(streamName, (key) -> new ArrayList<>());
      streamVersions.add(streamStoreDesc);
    }
    List<StreamStoreDesc> selectedStreams = new ArrayList<>();
    sortedStreams.forEach(
        (streamName, streamStoreDescVersions) -> {
          var latestStream = streamStoreDescVersions.get(streamStoreDescVersions.size() - 1);
          // Deleted signals are unnecessary, but any versions and statuses of contexts are
          // necessary.
          // Such contexts are used to build enriched attributes of older signals.
          if (latestStream.getType() == CONTEXT) {
            selectedStreams.addAll(streamStoreDescVersions);
          } else {
            int i = streamStoreDescVersions.size();
            while (--i >= 0) {
              if (streamStoreDescVersions.get(i).isDeleted()) {
                break;
              }
            }
            selectedStreams.addAll(
                streamStoreDescVersions.subList(i + 1, streamStoreDescVersions.size()));
          }
        });
    return selectedStreams;
  }

  public TenantDesc(TenantStoreDesc tenantStoreDesc) throws DirectedAcyclicGraphException {
    this.name = tenantStoreDesc.getName();
    this.normalizedName = this.name.toLowerCase();
    this.version = tenantStoreDesc.getVersion();
    this.maxAllocatedStreamNameProxy = tenantStoreDesc.getMaxAllocatedStreamNameProxy();
    this.deleted = tenantStoreDesc.isDeleted();
    this.appMaster = tenantStoreDesc.getAppMaster();
    streamsMap = new ConcurrentHashMap<>();
    signalAliases = new HashMap<>();
    this.streamDependencies = new DirectedAcyclicGraph();
    final var streamStoreDescList = selectStreamStoreDescs(tenantStoreDesc.getStreams());
    for (StreamStoreDesc streamStoreDesc : streamStoreDescList) {
      StreamDesc streamDesc = new StreamDesc(streamStoreDesc);
      streamDesc.setParent(this);
      addStream(streamDesc);
      streamDesc.setIsLatestVersion(false);
    }
    for (var streams : streamsMap.values()) {
      final var latestStreamDesc = streams.get(0);
      latestStreamDesc.setIsLatestVersion(true);
      for (int i = 1; i < streams.size(); ++i) {
        streams.get(i).setIsLatestVersion(false);
      }
      if (latestStreamDesc.isDeleted() || latestStreamDesc.getReferencedStreams() == null) {
        continue;
      }
      for (final var parentStreamName : latestStreamDesc.getReferencedStreams()) {
        final var parentStream = getStream(parentStreamName);
        if (parentStream != null) {
          addDependency(parentStream, latestStreamDesc);
        } else {
          logger.warn("Parent stream not found: {}", parentStreamName);
        }
      }
    }
    this.facInfoMap = new ConcurrentHashMap<>();
  }

  /**
   * Method to duplicate the object.
   *
   * <p>Modification in the duplicated object does not affect the original object.
   *
   * @return Duplicated instance.
   */
  @Override
  public TenantDesc duplicate() {
    TenantDesc dup = new TenantDesc(name, version, deleted, streamDependencies.clone());
    dup.maxAllocatedStreamNameProxy = maxAllocatedStreamNameProxy;
    dup.proxyInformationDirty = proxyInformationDirty;
    dup.domain = domain;
    streamsMap.forEach(
        (key, streams) -> {
          streams.forEach(stream -> dup.addStream(stream.duplicate()));
        });
    dup.deleted = deleted;
    dup.appMaster = appMaster;
    return dup;
  }

  /**
   * Add stream to the tenant.
   *
   * @param stream Stream to add
   * @return Self
   */
  public synchronized TenantDesc addStream(StreamDesc stream) {
    final String key = stream.getName().toLowerCase();
    List<StreamDesc> newList = new ArrayList<>();
    List<StreamDesc> streamList = streamsMap.get(key);
    stream.setParent(this);
    if (streamList == null) {
      newList.add(stream);
    } else {
      boolean inserted = false;
      for (StreamDesc existing : streamList) {
        if (!inserted && stream.getVersion() >= existing.getVersion()) {
          newList.add(stream);
          inserted = true;
        }
        if (!Objects.equals(stream.getVersion(), existing.getVersion())) {
          newList.add(existing);
        }
      }
      if (!inserted) {
        newList.add(stream);
      }
    }
    streamsMap.put(key, newList);
    return this;
  }

  public void clearStreams() {
    streamsMap.clear();
  }

  /**
   * Get the latest version of non-deleted stream; null if not found.
   *
   * <p>The method finds a specified stream in case insensitive manner and returns the stream. The
   * method returns null if there is no stream with the specified name.
   *
   * @param name Stream name
   */
  public StreamDesc getStream(String name) {
    return getStream(name, false);
  }

  /**
   * Get a stream with specified name and version; null if not found.
   *
   * <p>The method returns the stream specified by name and version. The object is returned even if
   * the version is not the latest. The method returns null if the target stream does not exists or
   * soft deleted. The parameter ignoreDeletedFlags overrides the soft deletion flag and returns the
   * object (with soft-deleted flag) no matter whether the stream is soft deleted.
   *
   * @param ignoreDeletedFlag If true, deleted streams are included, else not.
   */
  public StreamDesc getStream(String streamName, Long version, boolean ignoreDeletedFlag) {
    Objects.requireNonNull(streamName);
    Objects.requireNonNull(version);
    List<StreamDesc> streams = streamsMap.get(streamName.toLowerCase());
    if (streams == null) {
      return null;
    }
    for (StreamDesc config : streams) {
      if (version.equals(config.getVersion())) {
        return !config.isDeleted() || ignoreDeletedFlag ? config : null;
      }
    }
    return null;
  }

  /**
   * Get the latest version of a stream; null if not found.
   *
   * <p>The method finds a specified stream in case insensitive manner and returns the stream. The
   * method returns null if there is no stream with the specified name. However, if the stream
   * exists with soft-deleted flag, the method returns it if parameter ignoreDeletedFlag is true.
   *
   * @param ignoreDeletedFlag If true, deleted streams are included, else not.
   */
  public StreamDesc getStream(String streamName, boolean ignoreDeletedFlag) {
    List<StreamDesc> streams = streamsMap.get(streamName.toLowerCase());
    if (streams == null || streams.isEmpty()) {
      return null;
    }
    final StreamDesc config = streams.get(0);
    return !config.isDeleted() || ignoreDeletedFlag ? config : null;
  }

  /**
   * Gets a stream that another stream depends on.
   *
   * <p>If the dependent stream is the latest version, then the method looks for the latest
   * available prerequisite.
   *
   * @param streamName name of the prerequisite stream
   * @param dependent The dependent stream
   * @return The prerequisite stream. Null is returned when the prerequisite is not found.
   */
  public StreamDesc getDependingStream(String streamName, StreamDesc dependent) {
    // NOTE: We treat "unknown" as "latest"
    if (dependent.getIsLatestVersion() == FALSE) {
      return getStreamByNameAndTimestamp(streamName, dependent.getVersion());
    }
    return getStream(streamName);
  }

  /**
   * Get a stream specified by name and timestamp.
   *
   * <p>The method finds a stream of the specified name with the latest version that is equal to or
   * earlier than the timestamp. Null is returned if there is no such stream.
   */
  private StreamDesc getStreamByNameAndTimestamp(String streamName, Long timestamp) {
    Objects.requireNonNull(streamName);
    Objects.requireNonNull(timestamp);
    List<StreamDesc> streams = streamsMap.get(streamName.toLowerCase());
    if (streams == null) {
      return null;
    }
    for (StreamDesc config : streams) {
      if (timestamp < config.getVersion() || config.isDeleted()) {
        continue;
      }
      return config;
    }
    return null;
  }

  /**
   * Get list of active (non-deleted) streams for tenant excluding internal streams.
   *
   * <p>This lists active (the latest non-deleted version of) streams in this tenant whose names do
   * not start with underscore ('_').
   */
  public List<StreamDesc> getStreams() {
    return getStreams(stream -> !stream.getName().startsWith(Constants.PREFIX_INTERNAL_NAME));
  }

  /**
   * Get latest version of non-deleted streams that meet the specified predicate.
   *
   * @param predicate Predicate function that defines the conditions to match. The predicate
   *     function should take a StreamDesc object as an input. It should return true when the
   *     conditions match.
   */
  public List<StreamDesc> getStreams(Predicate<StreamDesc> predicate) {
    final List<StreamDesc> streams = new ArrayList<>();
    for (String key : streamsMap.keySet()) {
      StreamDesc stream = getStream(key);
      if (stream != null && predicate.test(stream)) {
        streams.add(stream);
      }
    }
    return streams;
  }

  /**
   * Get list of active (non-deleted) streams for tenant including internal streams.
   *
   * <p>This lists active (the latest non-deleted version of) streams in this tenant.
   */
  public List<StreamDesc> getAllStreams() {
    return getStreams(stream -> true);
  }

  private String streamName(StreamDesc streamDesc) {
    return streamDesc.getName().toLowerCase() + KEY_DELIMITER + streamDesc.getVersion();
  }

  /**
   * Get a list of active streams (non-deleted) including internal streams, in the correct
   * dependency order to use when one stream is being modified.
   *
   * @param modifiedStreamDesc New stream descriptor of the stream being modified.
   */
  public List<StreamDesc> getAllStreamsInDependencyOrder(
      StreamDesc modifiedStreamDesc, StreamDesc existingStreamDesc) {
    updateLatestStreamFlags();
    // Existing stream X is being modified. streamDependencies DAG currently contains dependencies
    // based on the stream X. From it we can get the set of descendants of X, i.e. the
    // streams that depend on X. Stream X needs to be initialized before any of these descendant
    // streams are initialized. So divide the full list of streams into 3 parts:
    // #1 streams not dependent on X
    // #2 stream X
    // #3 streams dependent on X
    // Both #1 and #3 themselves need to have streams in topological order of their dependencies.
    // Because there are no cycles in our graph (it is a DAG), there cannot be any stream in #1 that
    // depends on #2 or #3, so what we get should be a valid topological order.
    // In the current modification, if stream X introduces a dependency on any of its descendants,
    // that will create a cycle and should be failed. This constraint checking will happen during
    // stream initialization.
    final String x = streamName(existingStreamDesc);
    final Set<String> allStreams =
        streamsMap.values().stream()
            .filter((streams) -> !streams.isEmpty() && !streams.get(0).isDeleted())
            .map((streams) -> streamName(streams.get(0)))
            .collect(Collectors.toSet());
    final Set<String> streamsDependentOnX = streamDependencies.getDescendants(x);

    // Construct #1 part of the list above:
    // #1a Get all streams.
    // #1b Remove streams that are tracked in the DAG.
    // #1c Append all streams from the DAG in topological order.
    // #1d Remove X and descendants of X.
    // #1e All the above was done with String names; now translate that to StreamDesc.
    // #1f The StreamDesc list may contain previous versions. Pick up only the latest versions.
    final List<String> listPart1 = new ArrayList<>(allStreams);
    listPart1.removeIf(streamDependencies::nodeExists);
    streamDependencies.appendNodesInTopologicalOrder(listPart1);
    listPart1.removeIf(
        (node) -> node.startsWith(modifiedStreamDesc.getName().toLowerCase() + "\t"));
    listPart1.removeIf(streamsDependentOnX::contains);
    final List<StreamDesc> streamsToReturn = new ArrayList<>();
    listPart1.forEach(
        (key) -> {
          final var split = key.split(KEY_DELIMITER);
          final String name = split[0];
          if (split.length < 2) {
            logger.error("Unexpected node name: {}", key);
            return;
          }
          final Long version = Long.valueOf(split[1]);
          StreamDesc stream = getStream(name, version, false);
          if (stream != null && stream.getIsLatestVersion() == Boolean.TRUE) {
            streamsToReturn.add(stream);
          }
        });

    // #2: Add x to the list at the end.
    streamsToReturn.add(modifiedStreamDesc);

    // #3a Get a list of all streams in the DAG in topological order.
    // #3b Remove streams that are not descendants of X.
    // #3c Append this new list to the end of the list already built above with parts 1 and 2.
    final List<String> listPart3 = new ArrayList<>();
    streamDependencies.appendNodesInTopologicalOrder(listPart3);
    listPart3.removeIf(Predicate.not(streamsDependentOnX::contains));
    listPart3.forEach(
        (key) -> {
          final var split = key.split(KEY_DELIMITER);
          final String name = split[0];
          if (split.length < 2) {
            logger.error("Unexpected node name: {}", key);
            return;
          }
          final Long version = Long.valueOf(split[1]);
          StreamDesc stream = getStream(name, version, false);
          if (stream != null && stream.getIsLatestVersion() == Boolean.TRUE) {
            streamsToReturn.add(stream);
          }
        });
    return streamsToReturn;
  }

  private void updateLatestStreamFlags() {
    for (var streams : streamsMap.values()) {
      boolean isLatest = true;
      for (var stream : streams) {
        stream.setIsLatestVersion(isLatest);
        isLatest = false;
      }
    }
  }

  /**
   * Get list of non-deleted streams including all versions sorted in ascending order.
   *
   * <p>The output list contains old streams, but soft-deleted streams are excluded. The versions of
   * each stream appear in chronological order.
   *
   * @return List of all available streams.
   */
  public synchronized List<StreamDesc> getNonDeletedStreamVersionsInVersionOrder() {
    final var streamQ = new PriorityQueue<>(new StreamDescComparatorByVersionAndName());
    streamsMap.forEach(
        (key, value) -> {
          if (value != null) {
            for (StreamDesc desc : value) {
              if (desc.isDeleted()
                  || (desc.getType() == CONTEXT && desc.getIsLatestVersion() == FALSE)) {
                break;
              }
              streamQ.add(desc);
            }
          }
        });

    // Create the ordered list of streams by getting them out of the priority queue in order.
    final List<StreamDesc> orderedStreams = new ArrayList<>();
    while (!streamQ.isEmpty()) {
      orderedStreams.add(streamQ.poll());
    }
    return orderedStreams;
  }

  /**
   * Get list of all streams sorted in ascending order of versions.
   *
   * <p>The output list contains old streams, but soft-deleted signals are excluded. The versions of
   * each stream appear in chronological order.
   *
   * @return List of all available streams.
   */
  public synchronized List<StreamDesc> getAllStreamVersionsInVersionOrder() {
    final PriorityQueue<StreamDesc> streamQ =
        new PriorityQueue<>(new StreamDescComparatorByVersionAndName());
    streamsMap.forEach(
        (key, value) -> {
          if (value != null) {
            for (StreamDesc desc : value) {
              streamQ.add(desc);
            }
          }
        });

    // Create the ordered list of streams by getting them out of the priority queue in order.
    final List<StreamDesc> orderedStreams = new ArrayList<>();
    while (!streamQ.isEmpty()) {
      orderedStreams.add(streamQ.poll());
    }
    final var streamNames = new HashSet<String>();
    for (int i = orderedStreams.size() - 1; i >= 0; --i) {
      final var stream = orderedStreams.get(i);
      final var canonName = stream.getName().toLowerCase();
      stream.setIsLatestVersion(!streamNames.contains(canonName));
      streamNames.add(canonName);
    }
    return orderedStreams;
  }

  /**
   * Returns a set of names of streams that depend on this stream directly; empty set if no streams
   * depend on it.
   */
  public List<StreamDesc> getDependentStreams(StreamDesc parentStream) {
    final var keys = streamDependencies.getChildren(streamName(parentStream));
    return keys.stream()
        .map(
            (key) -> {
              final var split = key.split(KEY_DELIMITER);
              final String name = split[0];
              if (split.length < 2) {
                logger.warn("Unexpected node name: {}", key);
              }
              final Long version = Long.valueOf(split[1]);
              return getStream(name, version, false);
            })
        .collect(Collectors.toList());
  }

  public boolean isDeleted() {
    return deleted != null ? deleted : false;
  }

  /**
   * Method to convert to a TenantConfig object.
   *
   * @return The converted object.
   */
  public TenantConfig toTenantConfig() {
    return toTenantConfig(streamConfig -> true);
  }

  /**
   * Method to convert to a TenantConfig object with stream filter.
   *
   * @param streamFilter Stream filter
   * @return The converted object.
   */
  public TenantConfig toTenantConfig(Predicate<StreamConfig> streamFilter) {
    TenantConfig tenantConfig =
        new TenantConfig(name)
            .setVersion(version)
            .setDomain(domain)
            .setDeleted(deleted)
            .setAppMaster(appMaster);
    streamsMap.forEach(
        (key, value) -> {
          StreamDesc stream = this.getStream(key);
          if (stream != null && streamFilter.test(stream)) {
            StreamConfig clone = new StreamConfig(stream);
            if (clone.getVersion() == null) {
              clone.setVersion(version);
            }
            tenantConfig.addStream(clone);
          }
        });
    return tenantConfig;
  }

  /**
   * Method to convert to a TenantStoreDesc object.
   *
   * @return The converted object.
   */
  public TenantStoreDesc toTenantStore() {
    TenantStoreDesc tenantStoreDesc =
        new TenantStoreDesc(name).setVersion(version).setDeleted(deleted).setAppMaster(appMaster);
    tenantStoreDesc.setMaxAllocatedStreamNameProxy(maxAllocatedStreamNameProxy);
    streamsMap.forEach(
        (key, value) -> {
          StreamStoreDesc stream = this.getStream(key);
          if (stream != null) {
            StreamStoreDesc clone = stream.duplicate();
            if (clone.getVersion() == null) {
              clone.setVersion(version);
            }
            tenantStoreDesc.addStream(clone);
          }
        });
    return tenantStoreDesc;
  }

  /**
   * Method to remove streams that are equal or older than specified expiration time.
   *
   * <p>The method keeps a stream that is in a history of modification of the non-deleted latest
   * entry, even if it has a old timestamp.
   *
   * @param expiry Expiration time in epoch milliseconds.
   */
  public void trimOldStreams(long expiry) {
    for (String key : streamsMap.keySet()) {
      final List<StreamDesc> trash = new ArrayList<>();
      synchronized (this) {
        final List<StreamDesc> originalStreams = streamsMap.get(key);
        final List<StreamDesc> newList = new ArrayList<>();
        Long versionLinked = null;
        for (int i = 0; i < originalStreams.size(); ++i) {
          final StreamDesc stream = originalStreams.get(i);
          List<StreamDesc> streams = newList;
          if ((i == 0 && !stream.isDeleted()) || stream.getVersion().equals(versionLinked)) {
            versionLinked = stream.getPrevVersion();
          } else if (stream.getVersion() <= expiry) {
            streams = trash;
          }
          streams.add(stream);
        }
        if (newList.isEmpty()) {
          streamsMap.remove(key);
        } else {
          streamsMap.put(key, newList);
        }
      }
      if (!trash.isEmpty()) {
        // TODO(TFOS-1584): Handle trash
        if (logger.isDebugEnabled()) {
          StringBuilder versions = new StringBuilder();
          String delimiter = "[";
          for (StreamDesc desc : trash) {
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
              "Removed old stream entries; tenant={}, stream={}, versions={}",
              getName(),
              trash.get(0).getName(),
              versions);
        }
      }
    }
  }

  /**
   * Method to rebuild signal alias table. This is a hack for Angular BI code; it depends on some
   * implicit naming. TODO(BIOS-1574) Remove this after we remove Angular BI and control plane use
   * of old JSON schema.
   */
  public void buildSignalAliases() {
    Map<String, String> newTable = new HashMap<>();
    final List<StreamDesc> streams =
        getStreams(
            stream -> {
              return stream.getType() == StreamType.SIGNAL
                  || stream.getType() == StreamType.METRICS;
            });
    for (StreamDesc stream : streams) {
      final String alias = makeAlias(stream.getName());
      newTable.put(alias, stream.getName());
    }
    signalAliases = newTable;
  }

  protected String makeAlias(String signalName) {
    if (signalName == null) {
      throw new IllegalArgumentException("signalName may not be null");
    }
    return signalName.toLowerCase().replaceAll("_signal[a-zA-Z0-9_]*$", "");
  }

  /**
   * Resolves a signal name by its alias.
   *
   * @param alias Signal alias
   * @return Signal name
   * @throws NoSuchStreamException When failed to resolve the stream.
   */
  public String resolveSignalName(String alias) throws NoSuchStreamException {
    if (alias == null) {
      throw new IllegalArgumentException("alias may not be null");
    }
    final String streamName = signalAliases.get(alias.toLowerCase());
    if (streamName == null) {
      throw new NoSuchStreamException(name, alias + " (alias)");
    }
    return streamName;
  }

  /**
   * Adds a dependency between a given stream and a stream that is referenced by this stream.
   *
   * <p>This dependency graph is used to ensure that getAllStreamsInDependencyOrder() returns the
   * streams in dependency order, rather than a random order.
   *
   * @param parentStream Name of stream referred to by the child stream.
   * @param childStream Name of child stream that depends on the parent stream.
   */
  public void addDependency(StreamDesc parentStream, StreamDesc childStream)
      throws DirectedAcyclicGraphException {
    Objects.requireNonNull(parentStream, "Parent stream desc must be non null");
    Objects.requireNonNull(childStream, "Child stream desc must be non null");
    streamDependencies.addDirectedEdge(streamName(parentStream), streamName(childStream));
  }

  /**
   * Removes any existing dependencies that this stream has on other streams and stops tracking it
   * in the DAG.
   */
  public void stopTrackingDependenciesForStream(StreamDesc streamToDelete)
      throws DirectedAcyclicGraphException {
    Objects.requireNonNull(streamToDelete, "streamToDelete name must be non null");
    streamDependencies.deleteNodeIfNoChildren(streamName(streamToDelete));
  }

  public void clearDependencies() {
    streamDependencies.clearGraph();
  }

  /** Assign an integer proxy to a stream if not already assigned. */
  public void assignStreamNameProxy(StreamDesc streamDesc) {
    if (maxAllocatedStreamNameProxy == null) {
      assert (streamDesc.getStreamNameProxy() == null);
      maxAllocatedStreamNameProxy = 0;
      proxyInformationDirty = true;
      logger.debug("Initialized maxAllocatedStreamNameProxy to 0 for tenant={}", name);
    }

    if (streamDesc.getStreamNameProxy() == null) {
      maxAllocatedStreamNameProxy++;
      proxyInformationDirty = true;
      streamDesc.setStreamNameProxy(maxAllocatedStreamNameProxy);
      logger.debug(
          "Incremented maxAllocatedStreamNameProxy to {} for tenant={}, stream={}",
          maxAllocatedStreamNameProxy,
          name,
          streamDesc.getName());
    } else {
      // Verify that the proxy assigned to this stream is accounted for in the maximum.
      // This can be violated in the case of a torn write, i.e. stream was written but tenant was
      // not. If so, set the dirty bit, so that we write out the tenant to the database.
      if (streamDesc.getStreamNameProxy() > maxAllocatedStreamNameProxy) {
        maxAllocatedStreamNameProxy = streamDesc.getStreamNameProxy();
        proxyInformationDirty = true;
        logger.warn(
            "Torn write detected. Stream {} has streamNameProxy={} but tenant {} has "
                + "a smaller maxAllocatedStreamNameProxy={}",
            streamDesc.getName(),
            streamDesc.getStreamNameProxy(),
            name,
            maxAllocatedStreamNameProxy);
      }
    }
  }

  /**
   * Copies information related to all streams in this tenant that is not provided by the user but
   * is stored in the DB (via StreamStoreDesc objects). This is needed when modifying a tenant - the
   * new TenantDesc object is created with the new tenant config, but the old TenantDesc information
   * gets left behind and needs to be copied separately.
   */
  public void copyDbStoredInformationFromExisting(TenantDesc existing) {
    assert (maxAllocatedStreamNameProxy == null);
    maxAllocatedStreamNameProxy = existing.getMaxAllocatedStreamNameProxy();
    for (StreamDesc existingStreamDesc : existing.getAllStreams()) {
      final StreamDesc newStreamDesc = getStream(existingStreamDesc.getName());
      if (newStreamDesc != null) {
        newStreamDesc.copyDbStoredInformationFromExisting(existingStreamDesc);
      }
    }
  }

  /**
   * Deletes substreams that are not in use.
   *
   * <p>The strategy:
   *
   * <ul>
   *   <it>Find substreams in the tenant -- view, index, and rollup streams</it> <it>Get the parent
   *   stream (signal) of the substream</it> <it>Retrieve the parent signal name</it> <it>Fetch the
   *   signal via the tenant, it returns the latest active signal for the name</it> <it>Check
   *   whether the parent signal is the latest and active. If not, the substream can be removed</it>
   * </ul>
   */
  public void removeInactiveSubStreams() {
    final var streamNamesToDelete = new ArrayList<String>();
    for (var entry : streamsMap.entrySet()) {
      final var streams = entry.getValue();
      final var stream = streams.get(0);
      if (!Set.of(StreamType.VIEW, StreamType.INDEX, StreamType.ROLLUP)
          .contains(stream.getType())) {
        continue;
      }
      final var streamName = entry.getKey();
      final var parentStream = stream.getParentStream();
      final var latestStream = getStream(parentStream.getName());
      if (parentStream != latestStream) {
        streamNamesToDelete.add(streamName);
      } else {
        for (int i = streams.size(); --i > 0; ) {
          streams.remove(i);
        }
      }
    }
    for (var streamName : streamNamesToDelete) {
      streamsMap.remove(streamName);
    }
  }
}
