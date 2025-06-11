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

import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.models.ComplexQueryRequest;
import io.isima.bios.models.ComplexQuerySingleResponse;
import io.isima.bios.models.MetricFunction;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;

/**
 * Component of DataEngine that takes care of executing complex queries that may make other
 * summarize calls and/or return multiple result sets.
 */
@RequiredArgsConstructor
public class ComplexQueryEngine {
  // static final Logger logger = LoggerFactory.getLogger(ComplexQueryEngine.class);

  private final DataEngine dataEngine;

  /**
   * Method to execute complex query operation.
   *
   * @param acceptor The asynchronous acceptor. The caller should assume that the The method may
   *     finish before the work completes and the result of a successful execution would come to
   *     this acceptor.
   * @param errorHandler The asynchronous error handler. The caller should assume that the method
   *     may finish before the work completes and any failure report would come to this error
   *     handler.
   */
  public void complexQuery(
      ComplexQueryState state,
      CassStream cassStream,
      Consumer<ComplexQuerySingleResponse[]> acceptor,
      Consumer<Throwable> errorHandler)
      throws Throwable {

    final ComplexQueryRequest request = state.getInput();

    assert (state.isValidated());
    assert (request.getAggregates().size() == 1);
    assert (request.getAggregates().get(0).getFunction() == MetricFunction.SYNOPSIS);
    assert (request.getHorizon().equals(request.getInterval()));

    final var attributeName = request.getAggregates().get(0).getBy();
    final ComplexQueryExecutor complexQueryExecutor;
    if (attributeName == null) {
      complexQueryExecutor =
          new SignalSynopsisQueryExecutor(dataEngine, state, acceptor, errorHandler);
    } else {
      complexQueryExecutor =
          new SignalAttributeSynopsisQueryExecutor(
              dataEngine, state, acceptor, errorHandler, attributeName);
    }
    complexQueryExecutor.executeAsync();
  }
}
