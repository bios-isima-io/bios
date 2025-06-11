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
package io.isima.bios.common;

import io.isima.bios.data.filter.FilterElement;
import io.isima.bios.models.Sort;
import io.isima.bios.models.v1.Aggregate;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.cassandra.cql3.SingleColumnRelation;

/** Carries biproducts generated during a context select query for later use. */
// TODO(Naoki): Consolidate with class ContextSelectOpResources
// TODO(Naoki): Move the class to an appropriate package
@Getter
@Setter
public class ContextQueryState {
  private List<FilterElement> filterElements;

  private String attribute;
  private long limit;
  private boolean limitSpecifiedInQuery = false;
  private List<SingleColumnRelation> filter;
  private Sort sortSpec;
  private boolean sortSpecifiedInQuery = false;
  private List<Aggregate> aggregates;

  public ContextQueryState() {}
}
