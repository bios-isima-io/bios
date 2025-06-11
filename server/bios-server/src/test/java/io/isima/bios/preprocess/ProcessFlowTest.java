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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.IngestState;
import io.isima.bios.errors.exception.ServiceException;
import io.isima.bios.models.Event;
import io.isima.bios.models.Events;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.server.handlers.InsertState;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.Response;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessFlowTest {
  private static Logger logger = LoggerFactory.getLogger(ProcessFlowTest.class);

  private static ProcessStage joinHelloThere;
  private static ProcessStage saveConcreteName;
  private static ProcessStage substituteName;
  private static ProcessStage testError;

  /** Prepare preprocess stages. */
  @BeforeClass
  public static void setUpClass() {
    // Join an attribute hello=there
    joinHelloThere =
        new ProcessStage(
            "join_hello:there",
            new PreProcessor() {
              @Override
              public IngestState apply(IngestState state) {
                process(state.getEvent());
                return state;
              }

              @Override
              public CompletionStage<InsertState> applyAsync(InsertState state) {
                process(state.getEvent(true));
                return CompletableFuture.completedStage(state);
              }

              private void process(Event event) {
                event.set("hello", "there");
              }
            });

    // saves there=ElasticFlash in the variable table.
    saveConcreteName =
        new ProcessStage(
            "save_concrete_name",
            new PreProcessor() {
              @Override
              public IngestState apply(IngestState state) {
                state.getVariables().put("there", "ElasticFlash");
                return state;
              }

              @Override
              public CompletionStage<InsertState> applyAsync(InsertState state) {
                return null;
              }
            });

    // Lookup variable using value of attribute hello, and replace the attribute
    // by the looked-up value.
    substituteName =
        new ProcessStage(
            "substitute_name",
            new PreProcessor() {
              @Override
              public IngestState apply(IngestState state) {
                String key = (String) state.getEvent().getAttributes().get("hello");
                String newValue = (String) state.getVariables().get(key);
                if (newValue != null) {
                  state.getEvent().getAttributes().put("hello", newValue);
                }
                return state;
              }

              @Override
              public CompletionStage<InsertState> applyAsync(InsertState state) {
                return null;
              }
            });

    // Throw an error when the value of attribute "first" is "error".
    testError =
        new ProcessStage(
            "error_for_first:error",
            new PreProcessor() {
              @Override
              public IngestState apply(IngestState state) {
                String value = (String) state.getEvent().getAttributes().get("first");
                if ("error".equals(value)) {
                  throw new ServiceException(
                      "Received attribute first:error", Response.Status.FORBIDDEN);
                }
                return state;
              }

              @Override
              public CompletionStage<InsertState> applyAsync(InsertState state) {
                return null;
              }
            });
  }

  @Test
  public void testSingleProcessStage() {
    ProcessStage stage = joinHelloThere;

    CompletableFuture.supplyAsync(
            () -> {
              Event event = Events.createEvent(UUIDs.timeBased());
              IngestState mystate = new IngestState(event, null, null, null, null, null);
              assertNotNull(mystate);
              assertSame(event, mystate.getEvent());
              return mystate;
            })
        .thenApply(stage.getProcess())
        .thenAccept(
            state -> {
              assertEquals("there", state.getEvent().getAttributes().get("hello"));
            })
        .join();
  }

  @Test
  public void testMultiStages() {
    ArrayList<ProcessStage> stages = new ArrayList<>();
    stages.add(joinHelloThere);
    stages.add(saveConcreteName);
    stages.add(substituteName);

    Event event = Events.createEvent(UUIDs.timeBased());
    IngestState mystate = new IngestState(event, null, null, null, null, null);
    assertNotNull(mystate);
    assertSame(event, mystate.getEvent());
    CompletableFuture<IngestState> currentCf =
        CompletableFuture.supplyAsync(
            () -> {
              return mystate;
            });

    for (ProcessStage stage : stages) {
      currentCf = currentCf.thenApplyAsync(stage.getProcess());
    }

    currentCf.join();
    assertEquals("ElasticFlash", mystate.getEvent().getAttributes().get("hello"));
  }

  @Test
  public void ingestionPreprocessTest() throws InterruptedException, ExecutionException {
    StreamDesc streamDesc = new StreamDesc("abc", System.currentTimeMillis());
    streamDesc.setAttributes(new ArrayList<>());
    AttributeDesc desc = new AttributeDesc("first", InternalAttributeType.STRING);
    streamDesc.getAttributes().add(desc);
    desc = new AttributeDesc("hello", InternalAttributeType.STRING);
    streamDesc.getAttributes().add(desc);

    ArrayList<ProcessStage> preprocesses = new ArrayList<>();
    preprocesses.add(testError);
    preprocesses.add(saveConcreteName);
    preprocesses.add(substituteName);

    // 1st -- attribute substituted
    IngestRequest input = PreprocessTestUtils.makeInput("second,there", streamDesc.getVersion());
    IngestState state = new IngestState(null, null, streamDesc.getName(), null, null);
    state.setInput(input);
    state.setStreamDesc(streamDesc);
    CompletableFuture<IngestState> cf =
        Utils.preprocessIngestion(state, preprocesses, false).toCompletableFuture();
    cf.join();
    logger.debug(state.getEvent().toString());
    assertEquals("second", state.getEvent().getAttributes().get("first"));
    assertEquals("ElasticFlash", state.getEvent().getAttributes().get("hello"));

    // 2nd -- attribute not substituted
    input = PreprocessTestUtils.makeInput("second,world", streamDesc.getVersion());
    state = new IngestState(null, null, streamDesc.getName(), null, null);
    state.setStreamDesc(streamDesc);
    state.setInput(input);
    cf = Utils.preprocessIngestion(state, preprocesses, false).toCompletableFuture();
    cf.join();
    logger.debug(state.getEvent().toString());
    assertEquals("second", state.getEvent().getAttributes().get("first"));
    assertEquals("world", state.getEvent().getAttributes().get("hello"));

    // 3rd -- try error
    input = PreprocessTestUtils.makeInput("error,world", streamDesc.getVersion());
    state = new IngestState(null, null, streamDesc.getName(), null, null);
    state.setStreamDesc(streamDesc);
    state.setInput(input);
    cf = Utils.preprocessIngestion(state, preprocesses, false).toCompletableFuture();
    try {
      cf.get();
      fail();
    } catch (ExecutionException ex) {
      assertTrue(ex.getCause() instanceof ServiceException);
    }
  }
}
