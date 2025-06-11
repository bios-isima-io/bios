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
package io.isima.bios.dto;

import io.isima.bios.models.Record;
import io.isima.bios.req.IngestBulkResponse;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/** A DTO class to carry an ingestBulk response. */
public class IngestBulkResponseJson implements IngestBulkResponse {
  @NotNull List<IngestResponse> results = Collections.emptyList();

  public IngestBulkResponseJson() {}

  public IngestBulkResponseJson(List<? extends IngestResultOrError> results) {
    this.results =
        results.stream()
            .map((x) -> new IngestResponse(x.getEventId(), x.getTimestamp()))
            .collect(Collectors.toList());
  }

  @Override
  public List<? extends Record> getResults() {
    return results;
  }

  public void setResults(List<IngestResponse> results) {
    this.results = results;
  }

  @Override
  public String toString() {
    return "results=" + results;
  }
}
