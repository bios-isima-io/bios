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
package io.isima.bios.data.impl;

import static io.isima.bios.data.impl.storage.CassStream.translateInvalidQueryMessage;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.data.filter.FilterTerm;
import io.isima.bios.data.impl.feature.Comparators;
import io.isima.bios.data.impl.models.ContextFeatureQueryParams;
import io.isima.bios.data.impl.models.ContextSelectOpResources;
import io.isima.bios.data.impl.models.UseSketchPreference;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.ContextIndexCassStream;
import io.isima.bios.data.impl.storage.ContextQueryInterpreter;
import io.isima.bios.data.impl.storage.CqlFilterCompiler;
import io.isima.bios.data.impl.storage.RollupCassStream;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SelectOrder;
import io.isima.bios.errors.EventExtractError;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.FilterNotApplicableException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoFeatureFoundException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.GenericMetric;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.SingleColumnRelation;

/** DataEngine part that selects data from contexts. */
@AllArgsConstructor
public class ContextExtractor {
  private static final String PROP_MAX_SELECT_CONTEXT_LIMIT = "prop.select.context.response.limit";
  private static final String PROP_MAX_SELECT_CONTEXT_DB_LIMIT = "prop.select.context.db.limit";

  private static final long MAX_SELECT_CONTEXT_LIMIT = 2000000;
  private static final long MAX_SELECT_CONTEXT_DB_LIMIT = 10000000;

  private final DataEngineImpl dataEngine;
  private final SketchesExtractor sketchesExtractor;
  private final SharedConfig sharedConfig;

  /**
   * The starting point of context select.
   *
   * <p>The method determines the select methodology by the request, then calls a concrete select
   * method.
   */
  public CompletionStage<SelectContextEntriesResponse> selectContextEntriesAsync(
      ContextCassStream contextCassStream,
      SelectContextRequest request,
      ContextQueryState queryState,
      ContextOpState state) {
    final var contextDesc = state.getContextDesc();
    final var dbSelectContextLimit = new AtomicLong(MAX_SELECT_CONTEXT_DB_LIMIT);
    final var fut1 = new CompletableFuture<>();
    sharedConfig
        .getSharedProperties()
        .getPropertyCachedLongAsync(
            PROP_MAX_SELECT_CONTEXT_DB_LIMIT,
            MAX_SELECT_CONTEXT_DB_LIMIT,
            state,
            (value) -> {
              dbSelectContextLimit.set(value);
              fut1.complete(null);
            },
            fut1::completeExceptionally);

    final var responseSelectContextLimit = new AtomicLong(MAX_SELECT_CONTEXT_LIMIT);
    final var fut2 = new CompletableFuture<>();
    sharedConfig
        .getSharedProperties()
        .getPropertyCachedLongAsync(
            PROP_MAX_SELECT_CONTEXT_LIMIT,
            MAX_SELECT_CONTEXT_LIMIT,
            state,
            (value) -> {
              responseSelectContextLimit.set(value);
              fut2.complete(null);
            },
            fut2::completeExceptionally);

    return CompletableFuture.allOf(fut1, fut2)
        .thenCompose(
            (none) ->
                ExecutionHelper.supply(
                    () -> {
                      // sanity check
                      String whereClause = request.getWhere();
                      if (whereClause != null && !whereClause.isEmpty()) {
                        contextCassStream.validateFilter(queryState.getFilter());
                      }

                      final var selectOpResource =
                          determineSelectMethodology(
                              contextCassStream,
                              request,
                              queryState,
                              responseSelectContextLimit.get(),
                              dbSelectContextLimit.get());
                      if (selectOpResource.primaryKey != null) {
                        return contextCassStream
                            .getContextEntriesAsync(state, selectOpResource.primaryKey)
                            .thenApply((entries) -> entriesToSelectResponse(entries));
                      }
                      if (selectOpResource.indexStreamDesc != null) {
                        ContextIndexCassStream indexCassStream =
                            (ContextIndexCassStream)
                                dataEngine.getCassStream(selectOpResource.indexStreamDesc);

                        return indexCassStream.select(
                            contextCassStream, request, queryState, state);
                      }
                      if (selectOpResource.sketchPreference != null) {
                        return sketchSelectAsync(contextDesc, request, queryState, state);
                      }
                      if (selectOpResource.featureQueryParams != null) {
                        return featureSelectAsync(
                            request, selectOpResource.featureQueryParams, queryState, state);
                      }
                      return contextCassStream.genericSelectAsync(
                          request,
                          selectOpResource.statement,
                          queryState,
                          dbSelectContextLimit.get(),
                          state);
                    }))
        .thenApplyAsync(
            (response) -> postProcessRecords(request, response, queryState, contextCassStream),
            state.getExecutor())
        .thenApplyAsync((response) -> addDefinitions(response), state.getExecutor())
        .exceptionally(
            (throwable) -> {
              Throwable cause = throwable.getCause();
              if (cause instanceof InvalidQueryException) {
                throw new CompletionException(
                    new TfosException(
                        EventExtractError.INVALID_QUERY,
                        translateInvalidQueryMessage(cause.getMessage())));
              }
              if (throwable instanceof CompletionException) {
                throw (CompletionException) throwable;
              }
              throw new CompletionException(throwable);
            });
  }

  /** Remove attributes that are not in the select list. */
  private SelectContextEntriesResponse postProcessRecords(
      SelectContextRequest request,
      SelectContextEntriesResponse response,
      ContextQueryState queryState,
      ContextCassStream cassStream) {

    final var sortSpec = queryState.getSortSpec();
    if (sortSpec != null && !queryState.isSortSpecifiedInQuery()) {
      final var sortBy = sortSpec.getBy();
      final var attributeDesc = cassStream.getAttributeDesc(sortBy);
      final var type =
          attributeDesc != null ? attributeDesc.getAttributeType() : InternalAttributeType.DOUBLE;
      final int polarity = sortSpec.getReverse() == Boolean.TRUE ? -1 : 1;
      final var comparator =
          Comparators.generateMapComparator(sortBy, type, polarity, sortSpec.getCaseSensitive());
      Collections.sort(response.getEntries(), comparator);
    }

    final var limit = queryState.getLimit();
    if (limit > 0 && !queryState.isLimitSpecifiedInQuery()) {
      final var entries = response.getEntries();
      if (entries.size() > limit) {
        response.setEntries(entries.subList(0, (int) limit));
      }
    }

    if ((response.getEntries() == null)
        || (response.getEntries().size() == 0)
        || (((request.getAttributes() == null) || (request.getAttributes().size() == 0))
            && ((request.getMetrics() == null) || (request.getMetrics().size() == 0)))) {
      return response;
    }

    // Calculate the set of attributes to remove as the set of attributes present in the
    // response entries minus the set of attributes in the select list.
    //
    // All entries in the response should be uniform, so we can just look at the first entry to know
    // the set of attributes present in the response entries.
    //
    // Set of attributes in the select list comes from 2 places: attributes and metrics.
    final var entry1 = response.getEntries().get(0);
    final var attributesToRemove = new HashSet<String>(entry1.keySet());
    if (request.getAttributes() != null) {
      for (final var attr : request.getAttributes()) {
        for (final var attrToRemove : attributesToRemove) {
          if (attrToRemove.equalsIgnoreCase(attr)) {
            attributesToRemove.remove(attrToRemove);
            break;
          }
        }
      }
    }
    if (request.getMetrics() != null) {
      for (final GenericMetric metric : request.getMetrics()) {
        // Special case for samplecounts - the output attributes are different from those in the
        // select list, and do not need to be trimmed.
        if (metric.getFunction() == MetricFunction.SAMPLECOUNTS) {
          return response;
        }
        for (final var attrToRemove : attributesToRemove) {
          if (attrToRemove.equalsIgnoreCase(metric.getOutputAttributeName())) {
            attributesToRemove.remove(attrToRemove);
            break;
          }
        }
      }
    }
    if (attributesToRemove.isEmpty()) {
      return response;
    }

    // Now go through each entry in the response and remove attributes that should not be returned.
    for (final var entry : response.getEntries()) {
      for (final var attrName : attributesToRemove) {
        entry.remove(attrName);
      }
    }

    return response;
  }

  private SelectContextEntriesResponse addDefinitions(SelectContextEntriesResponse response) {
    if (response.getEntries().size() > 0) {
      // Populate the definitions by inferring data types from objects in the first entry.
      final var definitions = new HashMap<String, io.isima.bios.models.AttributeType>();
      response.setDefinitions(definitions);
      final var firstEntry = response.getEntries().get(0);
      // Iterate though the attributes in the first entry.
      for (final var attribute : firstEntry.entrySet()) {
        final var name = attribute.getKey();
        final var value = attribute.getValue();
        // Infer the data type from the value.
        final var type = io.isima.bios.models.AttributeType.infer(value);
        definitions.put(name, type);
      }
    }
    return response;
  }

  private ContextSelectOpResources determineSelectMethodology(
      ContextCassStream contextCassStream,
      SelectContextRequest request,
      ContextQueryState queryState,
      long responseSelectContextLimit,
      long dbSelectContextLimit)
      throws TfosException {
    final var contextDesc = contextCassStream.getStreamDesc();

    final Integer limit = request.getLimit();
    if (limit != null) {
      if (limit > responseSelectContextLimit) {
        throw new InvalidValueException(
            "Too large query scale: Select operation exceeds its size limitation."
                + " Please downsize the query stream="
                + request.getContext());
      }
    }

    if (request.getMetrics() != null && !request.getMetrics().isEmpty()) {
      queryState.setAggregates(
          request.getMetrics().stream()
              .map((metric) -> new Aggregate(metric.getFunction(), metric.getOf(), metric.getAs()))
              .collect(Collectors.toList()));
      final var sketchPreference = UseSketchPreference.decide(request, queryState, contextDesc);
      if (sketchPreference.useSketch()) {
        return ContextSelectOpResources.of(sketchPreference);
      }
      final var featureQueryParams =
          resolveFeatureStream(contextCassStream, request, queryState.getFilter());
      if (featureQueryParams == null) {
        throw new NoFeatureFoundException("No suitable features found for the specified query");
      }
      return ContextSelectOpResources.of(featureQueryParams);
    }

    String whereClause = request.getWhere();
    if (whereClause != null && !whereClause.isEmpty()) {
      StreamDesc indexStreamDesc = findIndexForFilter(contextDesc, request, queryState);
      if (indexStreamDesc != null) {
        return ContextSelectOpResources.of(indexStreamDesc);
      }
    }

    final var interpreter =
        new ContextQueryInterpreter(contextCassStream, request, dbSelectContextLimit, queryState);
    return interpreter.chooseExtractMethodology();
  }

  private ContextFeatureQueryParams resolveFeatureStream(
      ContextCassStream contextCassStream,
      SelectContextRequest request,
      List<SingleColumnRelation> filter)
      throws ConstraintViolationException, FilterNotApplicableException, InvalidFilterException {
    if (contextCassStream.getStreamDesc().getFeatures() == null) {
      return null;
    }

    final var admin = BiosModules.getAdminInternal();

    final var requiredAttributes = new HashSet<String>();
    final var requiredDimensions = new HashSet<String>();

    for (var metric : request.getMetrics()) {
      switch (metric.getFunction()) {
        case COUNT:
          break;
        case DISTINCTCOUNT:
          requiredDimensions.add(metric.getOf().toLowerCase());
          break;
        case MIN:
        case MAX:
        case AVG:
        case SUM:
          requiredAttributes.add(metric.getOf().toLowerCase());
          break;
        default:
          throw new ConstraintViolationException(
              "Function " + metric.getFunction() + " not supported for context select");
      }
    }

    if (request.getGroupBy() != null) {
      for (var dimension : request.getGroupBy()) {
        requiredDimensions.add(dimension.toLowerCase());
      }
    }

    Map<String, List<FilterTerm>> compiledFilter = null;
    if (filter != null) {
      compiledFilter = new HashMap<>();
      for (var relation : filter) {
        final var operator = relation.operator();
        if (!Set.of(Operator.EQ, Operator.IN).contains(operator)) {
          // not for an index table
          return null;
        }
        final String dimension = relation.getEntity().rawText();
        final var attribute = contextCassStream.getAttributeDesc(dimension);
        if (attribute == null) {
          throw new ConstraintViolationException(
              String.format("Attribute %s not found in where clause", dimension));
        }
        final var name = attribute.getName();
        List<FilterTerm> terms = compiledFilter.get(name);
        if (terms == null) {
          terms = new ArrayList<>();
          compiledFilter.put(name, terms);
        }
        terms.add(CqlFilterCompiler.createTerm(relation, attribute));

        requiredDimensions.add(dimension.toLowerCase());
      }
    }

    final var contextDesc = contextCassStream.getStreamDesc();
    StreamDesc bestFeatureStream = null;
    for (var feature : contextDesc.getFeatures()) {
      final var subStreamName =
          AdminUtils.makeRollupStreamName(contextDesc.getName(), feature.getName());
      final var candidate = admin.getStreamOrNull(contextDesc.getParent().getName(), subStreamName);
      if (candidate == null) {
        continue;
      }
      final var view = candidate.getViews().get(0);
      if (!view.getGroupBy().stream()
          .map((dimension) -> dimension.toLowerCase())
          .collect(Collectors.toSet())
          .containsAll(requiredDimensions)) {
        continue;
      }
      if (!view.getAttributes().stream()
          .map((attribute) -> attribute.toLowerCase())
          .collect(Collectors.toSet())
          .containsAll(requiredAttributes)) {
        continue;
      }
      if (bestFeatureStream == null
          || view.getGroupBy().size() < bestFeatureStream.getViews().get(0).getGroupBy().size()) {
        bestFeatureStream = candidate;
      }
    }

    return bestFeatureStream != null
        ? new ContextFeatureQueryParams(bestFeatureStream, compiledFilter)
        : null;
  }

  private StreamDesc findIndexForFilter(
      StreamDesc contextDesc, SelectContextRequest request, ContextQueryState queryState)
      throws NoFeatureFoundException {
    final var filter = queryState.getFilter();
    if (filter == null || !contextDesc.getAuditEnabled() || contextDesc.getFeatures() == null) {
      return null;
    }

    final var filterDimensions = new LinkedHashSet<String>();

    boolean rangeQuerySupportRequired = false;
    for (var relation : filter) {
      if (relation instanceof SingleColumnRelation) {
        final String dimension = relation.getEntity().rawText();
        filterDimensions.add(dimension.toLowerCase());
        final var operator = relation.operator();
        if (Set.of(Operator.GT, Operator.GTE, Operator.LT, Operator.LTE).contains(operator)) {
          rangeQuerySupportRequired = true;
        } else if (!Set.of(Operator.EQ, Operator.IN).contains(operator)) {
          // not for an index table
          return null;
        }
      }
    }

    final var primaryKey =
        contextDesc.getPrimaryKey().stream()
            .map((name) -> name.toLowerCase())
            .collect(Collectors.toSet());
    if (filterDimensions.equals(primaryKey) && !rangeQuerySupportRequired) {
      // go to the main table
      return null;
    }

    if (filterDimensions.size() > 0) {
      final var attributes = new LinkedHashSet<String>();
      if (request.getAttributes() != null && !request.getAttributes().isEmpty()) {
        request.getAttributes().stream().forEach(name -> attributes.add(name.toLowerCase()));
      } else {
        // select all
        contextDesc.getAttributes().stream()
            .forEach(attribute -> attributes.add(attribute.getName().toLowerCase()));
        if (contextDesc.getAdditionalAttributes() != null) {
          contextDesc.getAdditionalAttributes().stream()
              .forEach(attribute -> attributes.add(attribute.getName().toLowerCase()));
        }
      }

      final SelectOrder orderBy = request.getOrderBy();
      if (orderBy != null) {
        final var orderByKeyName = orderBy.getKeyName().toLowerCase();
        attributes.add(orderByKeyName);
      }

      for (var feature : contextDesc.getFeatures()) {
        if (rangeQuerySupportRequired && (feature.getIndexType() != IndexType.RANGE_QUERY)) {
          continue;
        }

        final var featureDimensions =
            feature.getDimensions().stream()
                .map(name -> name.toLowerCase())
                .collect(Collectors.toSet());

        if (!featureDimensions.containsAll(filterDimensions)) {
          continue;
        }

        // Make a the list of all the attributes present in the index.
        final var featureAllAttributes =
            feature.getAttributes().stream()
                .map(name -> name.toLowerCase())
                .collect(Collectors.toSet());
        featureAllAttributes.addAll(featureDimensions);
        featureAllAttributes.addAll(primaryKey);
        if (!featureAllAttributes.containsAll(attributes)) {
          continue;
        }

        final String indexName = contextDesc.getName() + ".index." + feature.getName();
        return BiosModules.getAdminInternal()
            .getStreamOrNull(contextDesc.getParent().getName(), indexName);
      }
      final var indexType =
          rangeQuerySupportRequired ? IndexType.RANGE_QUERY : IndexType.EXACT_MATCH;
      throw new NoFeatureFoundException(
          String.format("No suitable features found for the specified query"));
    }

    return null;
  }

  private List<String> makeAttributeList(Collection<String> names, StreamDesc contextDesc) {
    return names.stream()
        .map(
            (name) -> {
              var attribute = contextDesc.findAttribute(name);
              if (attribute == null) {
                attribute = contextDesc.findAdditionalAttribute(name);
              }
              if (attribute != null) {
                return attribute.getName();
              }
              return "";
            })
        .collect(Collectors.toList());
  }

  private CompletionStage<SelectContextEntriesResponse> sketchSelectAsync(
      StreamDesc contextDesc,
      SelectContextRequest request,
      ContextQueryState queryState,
      ContextOpState state) {
    // ensure tenant and stream names are set, the sketch summary engine requires them
    state.setTenantName(contextDesc.getParent().getName());
    state.setStreamName(contextDesc.getName());

    return dataEngine
        .getPostProcessScheduler()
        .getDigestionCoverage(contextDesc, null, state)
        .thenComposeAsync(
            (spec) -> {
              final var queryTime = spec != null ? spec.getDoneUntil() : System.currentTimeMillis();
              return sketchesExtractor
                  .summarizeContext(request, queryState.getAggregates(), queryTime, state)
                  .thenApplyAsync(
                      (events) -> {
                        // Convert list of events to a SelectContextEntriesResponse object
                        final var response = new SelectContextEntriesResponse();
                        response.setEntries(
                            events.stream()
                                .map((event) -> event.getAttributes())
                                .collect(Collectors.toList()));
                        return response;
                      },
                      state.getExecutor());
            },
            state.getExecutor());
  }

  private CompletionStage<SelectContextEntriesResponse> featureSelectAsync(
      SelectContextRequest request,
      ContextFeatureQueryParams queryParams,
      ContextQueryState queryState,
      ContextOpState state) {
    final var featureStreamDesc = queryParams.getFeatureStreamDesc();
    final var featureCassStream = (RollupCassStream) dataEngine.getCassStream(featureStreamDesc);
    if (featureCassStream == null) {
      final var contextDesc = state.getContextDesc();
      final var tenantName = contextDesc.getParent().getName();
      final var contextName = contextDesc.getName();
      throw new CompletionException(
          new ApplicationException(
              String.format(
                  "featureCassStream not found; tenant=%s, context=%s, feature=%s",
                  tenantName, contextName, featureStreamDesc.getName())));
    }
    return featureCassStream
        .extractFeatureRecords(
            request,
            queryState.getAggregates(),
            queryParams.getCompiledFilter(),
            queryState.getSortSpec(),
            state)
        .thenApply(
            (entries) -> {
              final var response = new SelectContextEntriesResponse();
              response.setEntries(entries);
              return response;
            });
  }

  private SelectContextEntriesResponse entriesToSelectResponse(Event[] entries) {
    final var response = new SelectContextEntriesResponse();
    final var records = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < entries.length; ++i) {
      final var event = entries[i];
      if (event != null) {
        records.add(event.getAttributes());
      }
    }
    response.setEntries(records);
    return response;
  }
}
