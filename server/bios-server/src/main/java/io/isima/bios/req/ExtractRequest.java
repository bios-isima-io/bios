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
package io.isima.bios.req;

import java.util.List;

/**
 * Extract request interface and extract request query builder.
 *
 * <p>Hides protobuf or JSON implementations underneath this interface.
 */
public interface ExtractRequest extends Message {
  /**
   * Main entry point to the Extract request query builder.
   *
   * @param startTime Start time of time range
   * @param delta delta from start time of time range
   * @return Next steps in the building process
   */
  static ExtractCondition create(long startTime, long delta) {
    return BuilderProvider.getBuilderProvider().getExtractRequestBuilder(startTime, delta);
  }

  Long getStartTime();

  Long getEndTime();

  List<String> getAttributes();

  List<? extends Aggregate> getAggregates();

  List<View> getViews();

  Long getStreamVersion();

  String getFilter();

  Integer getLimit();

  boolean isOnTheFly();

  interface ExtractCondition {
    ExtractCondition attributes(List<String> attributes);

    ExtractCondition attributes(String... attributes);

    ExtractCondition filter(String filter);

    ViewConditionOnly withAggregates(Aggregate... aggregate);

    ViewConditionOnly withAggregates(List<? extends Aggregate> aggregates);

    ExtractCondition withView(View view);

    Aggregate.Op<Aggregate.End<ViewCondition>> aggregate();

    View.Op<View.End<Builder>, View.SortEnd<Builder>> view();

    ExtractCondition streamVersion(long version);

    ExtractRequest build();
  }

  interface ViewConditionOnly {
    ExtractCondition withView(View view);
  }

  interface ViewCondition {
    Aggregate.Op<Aggregate.End<ViewCondition>> aggregate();

    View.Op<View.End<Builder>, View.SortEnd<Builder>> view();
  }

  interface Builder extends ExtractCondition, ViewCondition, ViewConditionOnly {}
}
