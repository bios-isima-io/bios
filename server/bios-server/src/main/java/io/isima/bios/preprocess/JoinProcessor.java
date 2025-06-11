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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.IngestState;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Event;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.server.handlers.InsertState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import lombok.Getter;

/** Class that executes pre-insert enrichment. */
public class JoinProcessor implements PreProcessor {

  @Getter private final List<String> foreignKeyNames;
  private final StreamDesc remoteContext;
  private final List<ActionDesc> actions;

  /**
   * Constructor with necessary parameters.
   *
   * @param foreignKeyAttributes Attribute in the signal stream to be used for lookup key
   * @param remoteContext Context to lookup.
   */
  public JoinProcessor(List<AttributeDesc> foreignKeyAttributes, StreamDesc remoteContext) {
    foreignKeyNames =
        foreignKeyAttributes.stream().map((attr) -> attr.getName()).collect(Collectors.toList());
    this.remoteContext = remoteContext;
    actions = new ArrayList<>();
  }

  public void addAction(ActionDesc attr) {
    actions.add(attr);
  }

  public List<ActionDesc> getActions() {
    return Collections.unmodifiableList(actions);
  }

  @Override
  public IngestState apply(IngestState state) {
    state.addHistory("(join");
    final List<Object> foreignKey = DataUtils.makeCompositeKey(foreignKeyNames, state.getEvent());
    try {
      state.startPreProcess();
      Event remoteEntry = state.getDataEngine().lookupContext(state, remoteContext, foreignKey);

      final var event = state.getEvent();

      final String tenantName = state.getTenantName();
      final String streamName = state.getStreamName();
      enrich(tenantName, streamName, foreignKey, remoteEntry, event);

      state.addHistory(")");
    } catch (TfosException | ApplicationException e) {
      throw new RuntimeException(e);
    }
    return state;
  }

  @Override
  public CompletionStage<InsertState> applyAsync(InsertState state) {
    state.addHistory("(join");
    final var event = state.getEvent(true);
    final List<Object> foreignKey = DataUtils.makeCompositeKey(foreignKeyNames, event);
    return state
        .getDataEngine()
        .lookupContextAsync(state, remoteContext, foreignKey)
        .thenApply(
            (remoteEntry) -> {
              final String tenantName = state.getTenantName();
              final String signalName = state.getStreamName();
              enrich(tenantName, signalName, foreignKey, remoteEntry, event);
              state.addHistory(")");
              return state;
            });
  }

  private void enrich(
      String tenantName,
      String signalName,
      List<Object> foreignKey,
      Event remoteEntry,
      Event event) {
    if (remoteEntry != null) {
      // lookup found
      for (ActionDesc action : actions) {
        final String attributeName =
            (action.getAs() != null) ? action.getAs() : action.getAttribute();
        event.set(attributeName, remoteEntry.get(action.getAttribute()));
      }
    } else {
      // lookup missed
      for (ActionDesc action : actions) {
        if (action.getMissingLookupPolicy() == MissingAttributePolicyV1.STRICT) {
          throw new CompletionException(
              new TfosException(
                  EventIngestError.FOREIGN_KEY_MISSING,
                  String.format(
                      "tenant=%s, signal=%s, foreignKey=%s, context=%s, primaryKey=%s",
                      tenantName,
                      signalName,
                      foreignKeyNames,
                      remoteContext.getName(),
                      remoteContext.getPrimaryKey())));
        }
        final String attributeName =
            (action.getAs() != null) ? action.getAs() : action.getAttribute();
        event.set(attributeName, action.getInternalDefaultValue());
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    sb.append("class=").append(this.getClass().getSimpleName());
    sb.append(", foreignKey=").append(foreignKeyNames);
    sb.append(", refStream=").append(remoteContext.getName());
    sb.append(", actions=").append(actions);
    return sb.append("}").toString();
  }
}
