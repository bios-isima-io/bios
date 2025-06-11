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

import static io.isima.bios.admin.v1.impl.AdminImpl.DEFAULT_SKETCHES_INTERVAL_DEFAULT;
import static io.isima.bios.admin.v1.impl.AdminImpl.DEFAULT_SKETCHES_INTERVAL_KEY;
import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.Unit;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ToString
public class InferAttributeTags {

  public static final String ALREADY_SPECIFIED_PREFIX = "User_";
  private static final Logger logger = LoggerFactory.getLogger(InferAttributeTags.class);

  public static final String INFERENCE_LOOKBACK_KEY = "prop.inferenceLookback";
  public static final Long INFERENCE_LOOKBACK_DEFAULT = 1000L * 60 * 60; // one hour
  public static final String INFERENCE_LOW_CARDINALITY_LIMIT_KEY =
      "prop.inferenceLowCardinalityLimit";
  public static final Long INFERENCE_LOW_CARDINALITY_LIMIT_DEFAULT = 100L;
  public static final String INFERENCE_LOW_CARDINALITY_RATIO_KEY =
      "prop.inferenceLowCardinalityRatio";
  public static final Long INFERENCE_LOW_CARDINALITY_RATIO_DEFAULT = 100L;
  public static final String INFERENCE_TIMESTAMP_PAST_LIMIT_DAYS_KEY =
      "prop.inferenceTimestampPastLimitDays";
  public static final Long INFERENCE_TIMESTAMP_PAST_LIMIT_DAYS_DEFAULT = 100L;
  public static final String INFERENCE_TIMESTAMP_FUTURE_LIMIT_DAYS_KEY =
      "prop.inferenceTimestampFutureLimitDays";
  public static final Long INFERENCE_TIMESTAMP_FUTURE_LIMIT_DAYS_DEFAULT = 100L;
  public static final String INFERENCE_UNIT_KEYWORD_RULES_KEY =
      "long.prop.inferenceUnitKeywordRules";

  protected final DataEngine dataEngine;
  private final CassandraConnection cassandraConnection;
  private final SharedConfig sharedConfig;
  private final Session session;
  private final StreamDesc streamDesc;
  private final AttributeDesc attributeDesc;
  private final List<String> nameWords;

  public InferAttributeTags(
      DataEngine dataEngine,
      CassandraConnection cassandraConnection,
      SharedConfig sharedConfig,
      StreamDesc streamDesc,
      AttributeDesc attributeDesc) {
    if (cassandraConnection == null) {
      throw new NullPointerException("cassandraConnection must not be null.");
    }
    this.dataEngine = dataEngine;
    this.cassandraConnection = cassandraConnection;
    this.sharedConfig = sharedConfig;
    this.session = cassandraConnection.getSession();
    this.streamDesc = streamDesc;
    this.attributeDesc = attributeDesc;
    this.nameWords = StringUtils.splitIntoWords(attributeDesc.getName());
  }

  public CompletableFuture<InferredTagsToStore> infer(ExecutionState state) {
    final var type = attributeDesc.getAttributeType();
    final var userTags = attributeDesc.getTags();

    final var tags = new InferredTagsToStore();

    if ((userTags != null) && (userTags.getCategory() != null)) {
      tags.category = userTags.getCategory();
      tags.categoryToStore = ALREADY_SPECIFIED_PREFIX + tags.category.stringify();
    }
    // If category is not specified, infer it.
    final CompletableFuture<Void> checkpoint1;
    if (userTags != null && userTags.getCategory() != null) {
      checkpoint1 = CompletableFuture.completedFuture(null);
    } else {
      if (type == InternalAttributeType.BLOB) {
        tags.category = AttributeCategory.DESCRIPTION;
        tags.categoryToStore = tags.category.stringify();
        return CompletableFuture.completedFuture(tags);
      }
      checkpoint1 =
          isDimension(type, tags, state)
              .thenAccept(
                  (isDimension) -> {
                    if (isDimension) {
                      tags.category = AttributeCategory.DIMENSION;
                    } else {
                      if (type.isAddable()) {
                        tags.category = AttributeCategory.QUANTITY;
                      } else {
                        tags.category = AttributeCategory.DESCRIPTION;
                      }
                    }
                    tags.categoryToStore = tags.category.stringify();
                  });
    }

    return checkpoint1.thenComposeAsync(
        (none) -> {
          if (tags.category != AttributeCategory.QUANTITY) {
            return CompletableFuture.completedFuture(tags);
          }

          // Infer unit and kind if possible.
          final CompletableFuture<Void> checkpoint2;
          if ((userTags == null) || (userTags.getKind() == null)) {
            checkpoint2 = inferUnitAndKind(tags, state);
          } else {
            checkpoint2 = CompletableFuture.completedFuture(null);
          }

          return checkpoint2.thenApplyAsync(
              (x) -> {
                // Overwrite inferred tags with any tags specified by the user.
                if ((userTags != null) && (userTags.getKind() != null)) {
                  tags.kind = userTags.getKind();
                  tags.kindToStore = ALREADY_SPECIFIED_PREFIX + tags.kind.stringify();
                }
                if ((userTags != null) && (userTags.getUnit() != null)) {
                  tags.unit = userTags.getUnit();
                  tags.unitToStore = ALREADY_SPECIFIED_PREFIX + tags.unit.stringify();
                }

                // Populate the tag strings to store in table if not already specified.
                if (tags.kindToStore.equals("") && tags.kind != null) {
                  tags.kindToStore = tags.kind.stringify();
                }
                if (tags.unitToStore.equals("") && tags.unit != null) {
                  tags.unitToStore = tags.unit.stringify();
                }

                return tags;
              },
              state.getExecutor());
        },
        state.getExecutor());
  }

  private CompletableFuture<Boolean> isDimension(
      InternalAttributeType type, InferredTagsToStore tags, ExecutionState state) {
    if (type == BOOLEAN || type == ENUM || attributeIsUsedAsKey(tags)) {
      return CompletableFuture.completedFuture(true);
    }
    if (type == STRING) {
      return attributeHasLowCardinality(tags, state);
    }
    return CompletableFuture.completedFuture(false);
  }

  private CompletableFuture<Void> inferUnitAndKind(InferredTagsToStore tags, ExecutionState state) {
    return inferUnit(tags, state)
        .thenRun(
            () -> {
              if (tags.unit != null) {
                tags.kind = tags.unit.getKind();
              }
            });
  }

  private CompletableFuture<Void> inferUnit(InferredTagsToStore tags, ExecutionState state) {
    // Timestamp
    return checkWhetherTimestamp(state)
        .thenAcceptAsync(
            (timestampUnit) -> {
              if (timestampUnit != null) {
                tags.unit = timestampUnit;
                return;
              }

              // Infer unit based on keywords in the name of the attribute.
              // Tenant-specific rules take precedence over cluster-wide rules.
              // If no cluster-wide rules are specified, use hardcoded default rules.
              final List<UnitEntry> rules = new ArrayList<>();
              final var mapper = BiosObjectMapperProvider.get();
              final String tenantRulesJson =
                  sharedConfig.getTenantCached(
                      INFERENCE_UNIT_KEYWORD_RULES_KEY,
                      streamDesc.getParent().getName(),
                      (String) null,
                      false);
              if (tenantRulesJson != null) {
                try {
                  final Rules tenantRules = mapper.readValue(tenantRulesJson, Rules.class);
                  rules.addAll(tenantRules.getRules());
                } catch (Throwable t) {
                  logger.warn("Got exception deserializing INFERENCE_UNIT_KEYWORD_RULES_KEY", t);
                }
              }
              final String rulesJson = SharedProperties.getCached(INFERENCE_UNIT_KEYWORD_RULES_KEY);
              final Rules clusterRules;
              if (rulesJson != null) {
                try {
                  clusterRules = mapper.readValue(rulesJson, Rules.class);
                  rules.addAll(clusterRules.getRules());
                } catch (Throwable t) {
                  logger.warn("Got exception deserializing INFERENCE_UNIT_KEYWORD_RULES_KEY", t);
                }
              } else {
                rules.addAll(Rules.getDefault().getRules());
              }

              for (final var rule : rules) {
                if ((rule.getKeywords() == null) || (rule.getKeywords().isEmpty())) {
                  logger.warn("Invalid Attribute unit inference rule={}", rule);
                  continue;
                }
                if (nameWords.containsAll(rule.getKeywords())) {
                  tags.unit = rule.getUnit();
                  break;
                }
              }
            },
            state.getExecutor());
  }

  private boolean attributeIsUsedAsKey(InferredTagsToStore tags) {
    final var preprocesses = streamDesc.getPreprocesses();
    if (preprocesses != null) {
      for (final var preprocess : preprocesses) {
        if (attributeDesc.getName().equalsIgnoreCase(preprocess.getCondition())) {
          tags.usedAsKey = true;
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Checks whether the attribute has a low cardinality for the purpose of determining whether it is
   * of category dimension. It is not enough for the distinct count to be small - it may be small
   * because the total number of events is small. We want many different events to have the same
   * values of the attribute in order for this attribute to be a dimension.
   */
  private CompletableFuture<Boolean> attributeHasLowCardinality(
      InferredTagsToStore tags, ExecutionState state) {
    final var summarizeRequest = new SummarizeRequest();
    final SummarizeState summarizeState = initializeSummarizeRequest(summarizeRequest, state);

    final var distinctCountAggregate =
        new Aggregate(MetricFunction.DISTINCTCOUNT, attributeDesc.getName());
    final var countAggregate = new Aggregate(MetricFunction.COUNT);
    summarizeRequest.setAggregates(List.of(distinctCountAggregate, countAggregate));

    final var lowCardinalityLimit = new AtomicLong();
    final var lowCardinalityRatio = new AtomicLong();
    final var sharedProp = sharedConfig.getSharedProperties();
    return CompletableFuture.allOf(
            sharedProp
                .getPropertyCachedLongAsync(
                    INFERENCE_LOW_CARDINALITY_LIMIT_KEY,
                    INFERENCE_LOW_CARDINALITY_LIMIT_DEFAULT,
                    state)
                .thenAccept((value) -> lowCardinalityLimit.set(value)),
            sharedProp
                .getPropertyCachedLongAsync(
                    INFERENCE_LOW_CARDINALITY_RATIO_KEY,
                    INFERENCE_LOW_CARDINALITY_RATIO_DEFAULT,
                    state)
                .thenAccept((value) -> lowCardinalityRatio.set(value)))
        .thenComposeAsync((none) -> executeSummarize(summarizeState), state.getExecutor())
        .thenApplyAsync(
            (result) -> {
              if (result == null) {
                return false;
              }
              tags.distinctCount =
                  ((Number) result.get(distinctCountAggregate.getOutputAttributeName()))
                      .doubleValue();
              tags.count =
                  ((Number) result.get(countAggregate.getOutputAttributeName())).longValue();
              if ((tags.count < 1) || (tags.distinctCount.isNaN())) {
                return false;
              }
              if ((tags.distinctCount <= lowCardinalityLimit.get())
                  && (tags.count / tags.distinctCount >= lowCardinalityRatio.get())) {
                return true;
              }
              return false;
            },
            summarizeState.getExecutor());
  }

  private CompletableFuture<Unit> checkWhetherTimestamp(ExecutionState parentState) {
    final var summarizeRequest = new SummarizeRequest();
    final SummarizeState summarizeState = initializeSummarizeRequest(summarizeRequest, parentState);

    final var minAggregate = new Aggregate(MetricFunction.MIN, attributeDesc.getName());
    final var maxAggregate = new Aggregate(MetricFunction.MAX, attributeDesc.getName());
    final var countAggregate = new Aggregate(MetricFunction.COUNT);
    summarizeRequest.setAggregates(List.of(minAggregate, maxAggregate, countAggregate));

    return executeSummarize(summarizeState)
        .thenApplyAsync(
            (result) -> {
              if (result == null) {
                return null;
              }

              final double min =
                  ((Number) result.get(minAggregate.getOutputAttributeName())).doubleValue();
              final double max =
                  ((Number) result.get(maxAggregate.getOutputAttributeName())).doubleValue();
              final long count = (Long) result.get(countAggregate.getOutputAttributeName());
              if (count < 1) {
                return null;
              }

              final long pastLimitDays =
                  SharedProperties.getCached(
                      INFERENCE_TIMESTAMP_PAST_LIMIT_DAYS_KEY,
                      INFERENCE_TIMESTAMP_PAST_LIMIT_DAYS_DEFAULT);
              final long futureLimitDays =
                  SharedProperties.getCached(
                      INFERENCE_TIMESTAMP_FUTURE_LIMIT_DAYS_KEY,
                      INFERENCE_TIMESTAMP_FUTURE_LIMIT_DAYS_DEFAULT);
              final long currentTime = System.currentTimeMillis();
              final long pastLimit = pastLimitDays * 24 * 60 * 60 * 1000L;
              final long futureLimit = futureLimitDays * 24 * 60 * 60 * 1000L;

              if ((min > currentTime - pastLimit) && (max < currentTime + futureLimit)) {
                return Unit.UNIX_MILLISECOND;
              }

              if ((min * 1000 > currentTime - pastLimit)
                  && (max * 1000 < currentTime + futureLimit)) {
                return Unit.UNIX_SECOND;
              }

              return null;
            },
            summarizeState.getExecutor());
  }

  /**
   * Creates a summarize state and initializes the summarize request with time periods commonly used
   * by inference queries.
   */
  private SummarizeState initializeSummarizeRequest(
      final SummarizeRequest summarizeRequest, ExecutionState parentState) {
    final long inferenceLookback =
        SharedProperties.getCached(INFERENCE_LOOKBACK_KEY, INFERENCE_LOOKBACK_DEFAULT);
    final long sketchInterval =
        SharedProperties.getCached(
            DEFAULT_SKETCHES_INTERVAL_KEY, DEFAULT_SKETCHES_INTERVAL_DEFAULT);

    final long endTime = Utils.ceiling(System.currentTimeMillis(), sketchInterval);
    final long startTime = endTime - inferenceLookback;
    summarizeRequest.setStartTime(startTime);
    summarizeRequest.setSnappedStartTime(startTime);
    summarizeRequest.setEndTime(endTime);
    summarizeRequest.setInterval(inferenceLookback);
    summarizeRequest.setHorizon(inferenceLookback);
    summarizeRequest.setTimezone(TimeZone.getTimeZone("UTC"));

    final var summarizeState =
        DataEngineImpl.createSummarizeState(
            "inference", summarizeRequest, dataEngine, streamDesc, parentState);
    return summarizeState;
  }

  /** Executes a summarize request synchronously and returns a single event as the result. */
  private CompletableFuture<Event> executeSummarize(final SummarizeState summarizeState) {

    return dataEngine
        .summarize(summarizeState)
        .thenApplyAsync(
            (results) -> {
              if ((results == null) || (results.size() < 1)) {
                // There is no data.
                return null;
              }
              if (results.size() > 1) {
                throw new CompletionException(
                    new ApplicationException(
                        String.format(
                            "Got more than 1 window in results: %s", results.toString())));
              }

              final var eventList = results.entrySet().iterator().next().getValue();
              if ((eventList == null) || (eventList.size() < 1)) {
                return null;
              }
              if (eventList.size() > 1) {
                throw new CompletionException(
                    new ApplicationException(
                        String.format(
                            "Got more than 1 event in result eventList: %s",
                            eventList.toString())));
              }

              final Event resultEvent = eventList.get(0);
              return resultEvent;
            },
            summarizeState.getExecutor())
        .toCompletableFuture();
  }

  @EqualsAndHashCode
  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class UnitEntry {
    // The list of keywords, all of which need to be present in an attribute's name,
    // in order to infer that attribute to have a certain unit.
    private List<String> keywords = new ArrayList<>();
    private Unit unit = null;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Rules {
    // The following rules are applied in order.
    // Once a match is found, the remaining rules are ignored.
    private List<UnitEntry> rules = new ArrayList<>();

    public static Rules getDefault() {
      final Rules defaultRules = new Rules();
      final var r = defaultRules.rules;

      r.add(new UnitEntry(List.of("usd"), Unit.U_S_D));
      r.add(new UnitEntry(List.of("dollar"), Unit.U_S_D));
      r.add(new UnitEntry(List.of("dollars"), Unit.U_S_D));
      r.add(new UnitEntry(List.of("inr"), Unit.I_N_R));
      r.add(new UnitEntry(List.of("rupee"), Unit.I_N_R));
      r.add(new UnitEntry(List.of("rupees"), Unit.I_N_R));
      r.add(new UnitEntry(List.of("eur"), Unit.E_U_R));
      r.add(new UnitEntry(List.of("euro"), Unit.E_U_R));
      r.add(new UnitEntry(List.of("euros"), Unit.E_U_R));
      r.add(new UnitEntry(List.of("jpy"), Unit.J_P_Y));
      r.add(new UnitEntry(List.of("yen"), Unit.J_P_Y));
      r.add(new UnitEntry(List.of("yens"), Unit.J_P_Y));

      r.add(new UnitEntry(List.of("amount"), Unit.U_S_D));

      r.add(new UnitEntry(List.of("per", "second"), Unit.PER_SECOND));
      r.add(new UnitEntry(List.of("per", "minute"), Unit.PER_MINUTE));
      r.add(new UnitEntry(List.of("per", "hour"), Unit.PER_HOUR));
      r.add(new UnitEntry(List.of("per", "day"), Unit.PER_DAY));
      r.add(new UnitEntry(List.of("per", "week"), Unit.PER_WEEK));
      r.add(new UnitEntry(List.of("per", "month"), Unit.PER_MONTH));
      r.add(new UnitEntry(List.of("per", "quarter"), Unit.PER_QUARTER));
      r.add(new UnitEntry(List.of("per", "year"), Unit.PER_YEAR));

      r.add(new UnitEntry(List.of("milli", "second"), Unit.MILLISECOND));
      r.add(new UnitEntry(List.of("milli", "seconds"), Unit.MILLISECOND));
      r.add(new UnitEntry(List.of("millisecond"), Unit.MILLISECOND));
      r.add(new UnitEntry(List.of("milliseconds"), Unit.MILLISECOND));
      // Not including "second" because it could have another meaning e.g "first", "second".
      r.add(new UnitEntry(List.of("seconds"), Unit.SECOND));
      r.add(new UnitEntry(List.of("minute"), Unit.MINUTE));
      r.add(new UnitEntry(List.of("minutes"), Unit.MINUTE));
      r.add(new UnitEntry(List.of("hour"), Unit.HOUR));
      r.add(new UnitEntry(List.of("hours"), Unit.HOUR));
      r.add(new UnitEntry(List.of("day"), Unit.DAY));
      r.add(new UnitEntry(List.of("days"), Unit.DAY));
      r.add(new UnitEntry(List.of("week"), Unit.WEEK));
      r.add(new UnitEntry(List.of("weeks"), Unit.WEEK));
      r.add(new UnitEntry(List.of("month"), Unit.MONTH));
      r.add(new UnitEntry(List.of("months"), Unit.MONTH));
      r.add(new UnitEntry(List.of("quarter"), Unit.QUARTER));
      r.add(new UnitEntry(List.of("quarters"), Unit.QUARTER));
      r.add(new UnitEntry(List.of("year"), Unit.YEAR));
      r.add(new UnitEntry(List.of("years"), Unit.YEAR));

      r.add(new UnitEntry(List.of("count"), Unit.COUNT));
      r.add(new UnitEntry(List.of("rank"), Unit.RANK));
      r.add(new UnitEntry(List.of("percent"), Unit.PERCENT));
      r.add(new UnitEntry(List.of("per", "cent"), Unit.PERCENT));
      r.add(new UnitEntry(List.of("ratio"), Unit.RATIO));

      r.add(new UnitEntry(List.of("latitude"), Unit.LATITUDE_DEGREE));
      r.add(new UnitEntry(List.of("longitude"), Unit.LONGITUDE_DEGREE));

      return defaultRules;
    }
  }

  public static class InferredTagsToStore {
    public String categoryToStore = "";
    public String kindToStore = "";
    public String unitToStore = "";
    public AttributeCategory category = null;
    public AttributeKind kind = null;
    public Unit unit = null;
    public Boolean usedAsKey = null;
    public Double distinctCount = null;
    public Long count = null;
  }
}
