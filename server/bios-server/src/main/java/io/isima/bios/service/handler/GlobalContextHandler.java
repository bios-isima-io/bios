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
package io.isima.bios.service.handler;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface GlobalContextHandler {

  Event getContextEntry(List<Object> key) throws ApplicationException;

  CompletionStage<Event> getContextEntryAsync(ExecutionState state, List<Object> key);

  List<Event> getContextEntries(List<List<Object>> keys) throws TfosException, ApplicationException;

  void getContextEntriesAsync(
      List<List<Object>> keys,
      ContextOpState state,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler);
}
