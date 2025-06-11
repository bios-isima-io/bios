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

import static io.isima.bios.admin.v1.AdminConstants.ENRICHED_ATTRIBUTE_DELIMITER_REGEX;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.exceptions.DirectedAcyclicGraphException;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.DirectedAcyclicGraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Orders streams from old version to new, from dependent to dependant.
 *
 * <p>This class is meant to be used to determine order to load streams retrieved from AdminStore.
 *
 * <p>Constraints:
 *
 * <ul>
 *   <li>Input streams should include only signals and contexts
 *   <li>All streams must be valid; this class does not run validation
 * </ul>
 *
 * <p>The rules to sort are:
 *
 * <ul>
 *   <li>Older version comes before newer one for the same stream.
 *   <li>Dependent comes before dependant.
 *   <li>When a stream depends on another, if the stream is the latest undeleted one, pick the
 *       latest version of dependent stream (the dependent stream may be newer than dependant). If
 *       the stream is not the latest, pick the latest version of dependent stream at the version of
 *       the dependant (the dependent stream is always equal or older than dependant).
 * </ul>
 */
public class StreamSorter {
  private static final String ROOT = ".ROOT";
  private static final String KEY_DELIMITER = "\t";

  private final List<StreamDesc> sourceStreams;
  private final Map<String, List<StreamDesc>> streamMap;
  private final DirectedAcyclicGraph streamDependencies;

  public StreamSorter(List<StreamDesc> src) {
    sourceStreams = src;
    streamMap = makeStreamMap(src);
    streamDependencies = new DirectedAcyclicGraph();
  }

  /** The method assumes the streams are sorted by timestamp in ascending order. */
  private Map<String, List<StreamDesc>> makeStreamMap(List<StreamDesc> streams) {
    final var streamMap = new HashMap<String, List<StreamDesc>>();
    for (var streamDesc : streams) {
      final var key = streamDesc.getName().toLowerCase();
      final var streamVersions = streamMap.computeIfAbsent(key, (k) -> new ArrayList<>());
      streamVersions.add(streamDesc);
    }
    return streamMap;
  }

  private String getStreamName(StreamDesc streamDesc) {
    return String.format(
        "%s%s%d", streamDesc.getName().toLowerCase(), KEY_DELIMITER, streamDesc.getVersion());
  }

  public List<StreamDesc> sortByDependencyOrder()
      throws DirectedAcyclicGraphException, ConstraintViolationException {
    // To ensure sorting streams of the same name in version order
    for (var streams : streamMap.values()) {
      String prevName = ROOT;
      for (var streamDesc : streams) {
        final var name = getStreamName(streamDesc);
        // A signal depends on previous versions
        if (streamDesc.getIsLatestVersion() || streamDesc.getType() == StreamType.SIGNAL) {
          streamDependencies.addDirectedEdge(prevName, name);
          prevName = name;
        }
      }
    }

    // Build dependencies for references
    for (var streamDesc : sourceStreams) {
      if (streamDesc.isDeleted()) {
        // a tombstone should not build dependencies
        continue;
      }
      if (streamDesc.getType() == StreamType.SIGNAL) {
        buildDependenciesOfSignal(streamDesc);
      } else if (streamDesc.getType() == StreamType.CONTEXT) {
        buildDependenciesOfContext(streamDesc);
      }
    }

    // Finalize
    final var sortedNames = new ArrayList<String>();
    streamDependencies.appendNodesInTopologicalOrder(sortedNames);
    final var result = new ArrayList<StreamDesc>();
    for (int i = 1; i < sortedNames.size(); ++i) {
      final var elements = sortedNames.get(i).split(KEY_DELIMITER);
      final var name = elements[0];
      final var version = Long.valueOf(elements[1]);
      result.add(getStream(name, version, false));
    }
    return result;
  }

  private void buildDependenciesOfSignal(StreamDesc streamDesc)
      throws DirectedAcyclicGraphException, ConstraintViolationException {
    final var version = streamDesc.getVersion();
    final var streamName = getStreamName(streamDesc);
    final var isLatest = streamDesc.getIsLatestVersion() == Boolean.TRUE;
    if (streamDesc.getPreprocesses() != null) {
      for (var preProcess : streamDesc.getPreprocesses()) {
        for (var action : preProcess.getActions()) {
          final var contextName = action.getContext();
          final var dependency = getStream(contextName, version, isLatest);
          if (dependency != null) {
            final String dependencyName = getStreamName(dependency);
            if (dependencyName.equals(streamName)) {
              throw new ConstraintViolationException(
                  String.format(
                      "Signal %s (%d) refers to itself for enrichment %s",
                      streamDesc.getName(), streamDesc.getVersion(), preProcess.getName()));
            }
            streamDependencies.addDirectedEdge(dependencyName, streamName);
          }
        }
      }
    }
    if (streamDesc.getViews() != null) {
      for (var view : streamDesc.getViews()) {
        final var feature = view.toFeatureConfig();
        final var facName = feature.getEffectiveFeatureAsContextName(streamDesc.getName());
        if (facName != null) {
          final var dependency = getStream(facName, version, isLatest);
          if (dependency != null) {
            final String dependencyName = getStreamName(dependency);
            if (dependencyName.equals(streamName)) {
              throw new ConstraintViolationException(
                  String.format(
                      "Signal %s (%d) refers to itself for materialization %s",
                      streamDesc.getName(), streamDesc.getVersion(), view.getName()));
            }
            streamDependencies.addDirectedEdge(dependencyName, streamName);
          }
        }
      }
    }
  }

  private void buildDependenciesOfContext(StreamDesc streamDesc)
      throws DirectedAcyclicGraphException, ConstraintViolationException {
    final var version = streamDesc.getVersion();
    final var streamName = getStreamName(streamDesc);
    final var isLatest = streamDesc.getIsLatestVersion() == Boolean.TRUE;
    if (streamDesc.getAuditEnabled() == Boolean.TRUE) {
      final var auditName = "audit" + streamDesc.getName();
      final var dependency = getStream(auditName, version, isLatest);
      if (dependency != null) {
        streamDependencies.addDirectedEdge(getStreamName(dependency), streamName);
      }
    }

    final var enrichments = streamDesc.getContextEnrichments();
    if (enrichments != null && !enrichments.isEmpty()) {
      for (final var enrichment : enrichments) {
        for (var enrichedAttribute : enrichment.getEnrichedAttributes()) {
          if (enrichedAttribute.getValue() != null) {
            // enrichment kind is SIMPLE_VALUE:
            // A simple value only has one context to join with.
            // Get the context this enrichment is referring to.
            final var referencedContextName =
                enrichedAttribute.getValue().split(ENRICHED_ATTRIBUTE_DELIMITER_REGEX)[0];
            final var dependency = getStream(referencedContextName, version, isLatest);
            if (dependency != null) {
              final String dependencyName = getStreamName(dependency);
              if (dependencyName.equals(streamName)) {
                throw new ConstraintViolationException(
                    String.format(
                        "Context %s (%d) refers to itself for enrichment %s",
                        streamDesc.getName(), streamDesc.getVersion(), enrichment.getName()));
              }
              streamDependencies.addDirectedEdge(dependencyName, streamName);
            }
          } else if (enrichedAttribute.getValuePickFirst() != null) {
            // enrichment kind is VALUE_PICKUP_FIRST
            final int numCandidates = enrichedAttribute.getValuePickFirst().size();
            for (int i = 0; i < numCandidates; i++) {
              // Basic validation.
              final String candidate = enrichedAttribute.getValuePickFirst().get(i);
              final String[] candidateSplit = candidate.split(ENRICHED_ATTRIBUTE_DELIMITER_REGEX);
              // Get the candidate context.
              final var referencedContextName = candidateSplit[0];
              final var dependency = getStream(referencedContextName, version, isLatest);
              if (dependency != null) {
                final String dependencyName = getStreamName(dependency);
                if (dependencyName.equals(streamName)) {
                  throw new ConstraintViolationException(
                      String.format(
                          "Context %s (%d) refers to itself for enrichment %s",
                          streamDesc.getName(), streamDesc.getVersion(), enrichment.getName()));
                }
                streamDependencies.addDirectedEdge(dependencyName, streamName);
              }
            }
          }
        }
      }
    }
  }

  private StreamDesc getStream(String streamName, Long version, boolean takeLatest) {
    final var streams = streamMap.get(streamName.toLowerCase());
    if (streams == null) {
      // shouldn't happen
      return null;
    }

    if (takeLatest) {
      return streams.get(streams.size() - 1);
    }

    for (int i = streams.size(); --i >= 0; ) {
      final var stream = streams.get(i);
      if (stream.getVersion() <= version) {
        return stream;
      }
    }
    return null;
  }
}
