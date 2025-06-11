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
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NullAttributeException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.Events;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamConfig;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  /**
   * Method to create a Signal Event object from plain-text input.
   *
   * @param input Ingest request object
   * @param streamConfig Attribute descriptors map used for interpreting values.
   * @param isKvPair True if the input values are key:value pairs.
   * @return Event object.
   * @throws TfosException When event data conversion fails
   */
  public static Event createSignalEvent(
      IngestRequest input, StreamConfig streamConfig, boolean isKvPair) throws TfosException {
    return parseEventCsv(
        input.getEventText(), streamConfig, isKvPair, Events.createEvent(input.getEventId()));
  }

  /**
   * Method to create a Context Entry from plain-text input.
   *
   * @param timestamp Entry timestamp.
   * @param entryText source text
   * @param context Context stream config
   * @param isKvPair True if the input values are key:value pairs.
   * @return Event object as a Context Entry
   * @throws TfosException When event data conversion fails
   */
  public static Event createContextEvent(
      long timestamp, String entryText, StreamConfig context, boolean isKvPair)
      throws TfosException {
    Event event = new EventJson();
    event.setIngestTimestamp(new Date(timestamp));
    return parseEventCsv(entryText, context, isKvPair, event);
  }

  /**
   * Method to fill attributes to Event object from plain-text input.
   *
   * @param src source text
   * @param streamConfig Stream configuration of the input text.
   * @param isKvPair True if the input values are key:value pairs.
   * @param event The event object
   * @return Event object.
   * @throws TfosException When event data conversion fails
   */
  public static Event parseEventCsv(
      String src, StreamConfig streamConfig, boolean isKvPair, Event event) throws TfosException {
    final EventCsvParser parser =
        new DefaultEventCsvParser(src, streamConfig.getAttributes(), isKvPair);
    event.setAttributes(parser.parse());

    return event;
  }

  /**
   * Utility method to execute pre-processes for an ingest request.
   *
   * <p>The method first creates an event object using input string and attribute descriptors. Given
   * preprocesses are applied then. Completion of async processes is handled by returning
   * CompletableFuture object.
   *
   * @param state State of this ingestion.
   * @param preProcesses List of process stages to execute.
   * @param isKvPair True if the input values are key:value pairs.
   * @return CompletableFuture that would handle the process completion.
   */
  public static CompletionStage<IngestState> preprocessIngestion(
      IngestState state, List<ProcessStage> preProcesses, boolean isKvPair) {

    CompletableFuture<IngestState> processChain =
        CompletableFuture.supplyAsync(
            () -> {
              state.addHistory("(preprocess){.createEvent");
              Event event;
              try {
                event = createSignalEvent(state.getInput(), state.getStreamDesc(), isKvPair);
              } catch (TfosException e) {
                throw new RuntimeException(e);
              }
              logger.trace(event.toString());
              state.setEvent(event);
              return state;
            });

    // execute preprocesses if any
    if (preProcesses != null) {
      for (ProcessStage stage : preProcesses) {
        processChain = processChain.thenApplyAsync(stage.getProcess());
      }
    }

    return processChain;
  }

  /**
   * Validates an event for a stream.
   *
   * <p>The method checks attributes for the specified stream and throws an exception when an
   * inappropriate attribute value is found.
   *
   * <p>This method is useful to prevent ingestion from getting polluted by potentially harmful
   * data.
   *
   * @param streamDesc Descriptor of the stream to be ingested.
   * @param event Event to ingest.
   * @throws InvalidValueException when the event has any invalid data.
   */
  public static void validateEvent(StreamDesc streamDesc, Event event)
      throws InvalidValueException {
    try {
      validateAttributes(streamDesc.getAttributes(), event);
      validateAttributes(streamDesc.getAdditionalAttributes(), event);
    } catch (NullAttributeException e) {
      throw new InvalidValueException(
          String.format(
              "Event has an invalid attribute; %s, event=%s", e.getMessage(), event.toString()),
          e);
    }
  }

  /**
   * Iterates attributes and check whether all attributes are valid.
   *
   * @param attributes List of attribute descriptions to check. The method accepts null here; Does
   *     nothing in the case.
   * @param event The event to be validated.
   * @throws NullAttributeException when a required attribute value is null.
   */
  private static void validateAttributes(List<AttributeDesc> attributes, Event event)
      throws NullAttributeException {
    if (attributes == null) {
      return;
    }
    for (AttributeDesc attributeDesc : attributes) {
      if (event.get(attributeDesc.getName()) == null) {
        throw new NullAttributeException("attribute=" + attributeDesc.getName());
      }
    }
  }
}
