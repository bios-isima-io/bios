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
package io.isima.bios.data;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.IngestState;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ContextReferrer {

  /**
   * Method to lookup an entry from a context stream.
   *
   * <p>The entry is returned as an Event object. Unlike other methods such as {@link
   * #ingestEvent()}, this method executes synchronously, i.e., the method blocks until the result
   * returns.
   *
   * @param state Ingestion operation state.
   * @param refStream The stream to lookup. This stream must be a Context type. Otherwise causes
   *     TfosException.
   * @param key The lookup key. The method finds an entry whose primary key matches this value.
   * @return Context entry as an Event when there is a match to the key.
   * @throws TfosException When execution error happens.
   * @throws ApplicationException When operation fails due to an unexpected error
   */
  Event lookupContext(IngestState state, StreamDesc refStream, List<Object> key)
      throws TfosException, ApplicationException;

  /**
   * Method to lookup an entry from a context stream.
   *
   * <p>The entry is returned as an Event object.
   *
   * @param state Ingestion operation state.
   * @param remoteContext The stream to lookup. This stream must be a Context type. Otherwise causes
   *     TfosException.
   * @param key The lookup key. The method finds an entry whose primary key matches this value.
   * @return Future for context entry as an Event when there is a match to the key.
   * @throws TfosException When execution error happens.
   * @throws ApplicationException When operation fails due to an unexpected error
   */
  CompletableFuture<Event> lookupContextAsync(
      ExecutionState state, StreamDesc remoteContext, List<Object> key);
}
