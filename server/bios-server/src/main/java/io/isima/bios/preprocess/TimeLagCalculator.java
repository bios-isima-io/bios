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
package io.isima.bios.preprocess;

import io.isima.bios.common.IngestState;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.Unit;
import io.isima.bios.server.handlers.InsertState;
import java.util.concurrent.CompletionStage;

/** Ingest pre-processor that calculates an ingest time lag. */
public class TimeLagCalculator implements PreProcessor {

  private final String refAttributeName;
  private final String outputAttributeName;
  private final double outputScale;
  private final double fillInValue;

  public static final String INGEST_TIME_LAG_PAST_LIMIT_SECONDS_KEY =
      "prop.ingestTimeLagPastLimitSeconds";
  public static final Long INGEST_TIME_LAG_PAST_LIMIT_SECONDS_DEFAULT = 60L * 60 * 24 * 100;
  public static final String INGEST_TIME_LAG_FUTURE_LIMIT_SECONDS_KEY =
      "prop.ingestTimeLagFutureLimitSeconds";
  public static final Long INGEST_TIME_LAG_FUTURE_LIMIT_SECONDS_DEFAULT = 60L * 60;

  public TimeLagCalculator(
      String refAttributeName, String outputAttributeName, Unit outputUnit, double fillInValue) {
    this.refAttributeName = refAttributeName;
    this.outputAttributeName = outputAttributeName;
    switch (outputUnit) {
      case SECOND:
        this.outputScale = 1000.0;
        break;
      case MINUTE:
        this.outputScale = 1000.0 * 60;
        break;
      case HOUR:
        this.outputScale = 1000.0 * 60 * 60;
        break;
      case DAY:
        this.outputScale = 1000.0 * 60 * 60 * 24;
        break;
      case WEEK:
        this.outputScale = 1000.0 * 60 * 60 * 24 * 7;
        break;
      default:
        this.outputScale = 1.0;
    }
    this.fillInValue = fillInValue;
  }

  @Override
  public IngestState apply(IngestState state) {
    state.addHistory("calculateLag");
    state.startPreProcess();

    final var event = state.getEvent();

    final long pastLimitSeconds =
        SharedProperties.getCached(
            INGEST_TIME_LAG_PAST_LIMIT_SECONDS_KEY, INGEST_TIME_LAG_PAST_LIMIT_SECONDS_DEFAULT);
    final long futureLimitSeconds =
        SharedProperties.getCached(
            INGEST_TIME_LAG_FUTURE_LIMIT_SECONDS_KEY, INGEST_TIME_LAG_FUTURE_LIMIT_SECONDS_DEFAULT);
    process(event, pastLimitSeconds, futureLimitSeconds);

    state.endPreProcess();
    return state;
  }

  @Override
  public CompletionStage<InsertState> applyAsync(InsertState state) {
    state.addHistory("(timeLag{(getProperties");

    final var sharedProperties = BiosModules.getSharedProperties();
    return sharedProperties
        .getPropertyCachedLongAsync(
            INGEST_TIME_LAG_PAST_LIMIT_SECONDS_KEY,
            INGEST_TIME_LAG_PAST_LIMIT_SECONDS_DEFAULT,
            state)
        .thenCombine(
            sharedProperties.getPropertyCachedLongAsync(
                INGEST_TIME_LAG_FUTURE_LIMIT_SECONDS_KEY,
                INGEST_TIME_LAG_FUTURE_LIMIT_SECONDS_DEFAULT,
                state),
            (pastLimitSeconds, futureLimitSeconds) -> {
              process(state.getEvent(true), pastLimitSeconds, futureLimitSeconds);
              return state;
            });
  }

  private void process(Event event, long pastLimitSeconds, long futureLimitSeconds) {
    final long pastLimit = pastLimitSeconds * 1000L;
    final long futureLimit = futureLimitSeconds * 1000L;

    long ingest = event.getIngestTimestamp().getTime();
    long reference = (Long) event.get(refAttributeName);

    final double lagMs;
    if ((reference * 1000 > ingest - pastLimit) && (reference * 1000 < ingest + futureLimit)) {
      lagMs = ingest - reference * 1000.0;
    } else if ((reference > ingest - pastLimit) && (reference < ingest + futureLimit)) {
      lagMs = ingest - (double) reference;
    } else {
      // Data seems to be out of range; use default value.
      lagMs = fillInValue * outputScale;
    }

    event.set(outputAttributeName, lagMs / outputScale);
  }
}
