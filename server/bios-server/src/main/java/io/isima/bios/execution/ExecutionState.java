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
package io.isima.bios.execution;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ActivityRecorder;
import io.isima.bios.common.ExecutionContext;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.recorder.OperationMetricsRecorder;
import io.isima.bios.recorder.RequestType;
import io.isima.bios.stats.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.Setter;

/**
 * Class that keeps an operation's state.
 *
 * <p>The class is responsible to
 *
 * <ul>
 *   <li>Keep operation states and intermediate parameters, such as signal config
 *   <li>Context based object providers, such as record object creator
 *   <li>Keep track of operations, in place of stack trace. Every operation is asynchronous, stack
 *       traces do not help troubleshooting much for BIOS operations
 * </ul>
 */
// TODO(Naoki): Are we going to bring Netty's buffer allocator here?
public abstract class ExecutionState implements ExecutionContext {
  // required parameters
  protected final String executionName;
  protected final Executor executor;
  protected final ExecutionState parent;

  /**
   * List of object to keep history.
   *
   * <p>The elements are either instances of class String or ExecutionStateBase. An
   * ExecutionStateBase instance is put when the process forks a concurrent subprocess, otherwise
   * just simple string to record progress in the chain of execution stages. At the beginning of the
   * stage, the process should add a history entry by addHistory(), whose format is
   * "(&lt;stageName&gt;". The process also should add another history entry ")" at the end of the
   * stage.
   *
   * <p>When a process aborts for an error, you can see the execution path by calling {@link
   * #getCallTraces()}. The method concatenates the histories of the process and subprocesses and
   * returns a list of strings. It looks as follows for example:
   *
   * <pre>
   * insert: (prepare)(insert(enrich)(makeStatement)(ingest
   * </pre>
   *
   * <p>In above case, you can know that the process failed at the ingest phase, since it does not
   * have closing parenthesis.
   */
  protected final List<Object> history;

  // Metrics management
  @Getter @Setter protected OperationMetricsTracer metricsTracer;

  @Getter @Setter protected OperationMetricsRecorder metricsRecorder;

  protected Timer preProcessTimer;

  protected Timer dbExecutionTimer;

  protected Timer postProcessTimer;

  @Getter @Setter protected boolean skipCountingErrors = false;

  // optional parameters
  @Getter @Setter protected UserContext userContext;

  @Getter @Setter protected String tenantName;

  // TODO(BIOS-4937): Consolidate signalName and contextName in inherited classes
  @Getter @Setter protected String streamName;

  @Getter @Setter protected StreamDesc streamDesc;

  @Getter @Setter protected RequestType requestType;

  // Context to log when an error happened
  @Getter protected final Map<String, Object> logContext;

  /** An atomic operation context is set when the operation writes data atomically. */
  @Getter @Setter protected AtomicOperationContext atomicOperationContext;

  // for debugging //////////////////////////////////////////

  /** Some operation restricts running in a single thread. This parameter is used to assert it. */
  protected long threadId;

  @Getter @Setter protected ActivityRecorder activityRecorder;

  @Getter @Setter protected boolean isDelegate = false;

  @Getter private final long createdAt = System.currentTimeMillis();

  @Getter @Setter private UUID executionId;

  public ExecutionState(String executionName, Executor executor) {
    Objects.requireNonNull(executionName);
    this.executionName = executionName;
    this.executor = executor;
    this.parent = null;
    history = new ArrayList<>();
    logContext = new LinkedHashMap<>();
  }

  public ExecutionState(String executionName, ExecutionState parent) {
    this(executionName, parent, parent.getExecutor());
  }

  public ExecutionState(String executionName, ExecutionState parent, Executor executor) {
    Objects.requireNonNull(executionName);
    Objects.requireNonNull(parent);
    this.executionName = executionName;
    this.executor = executor;
    this.tenantName = parent.tenantName;
    this.metricsRecorder = parent.metricsRecorder;
    this.userContext = parent.userContext;
    this.parent = parent;
    parent.addBranch(this);
    history = new ArrayList<>();
    logContext = parent.logContext;
    this.activityRecorder = parent.activityRecorder;
  }

  public ExecutionState getParent() {
    return parent;
  }

  public String getExecutionName() {
    return executionName;
  }

  public Executor getExecutor() {
    return executor;
  }

  public void addHistory(String stageName) {
    history.add(stageName);
  }

  public void addBranch(ExecutionState branchedState) {
    history.add(branchedState);
  }

  public void markDone() {
    history.add(":done");
  }

  public void markError() {
    history.add("*");
  }

  public UserContext getUserContext() {
    return userContext;
  }

  public void setUserContext(UserContext userContext) {
    this.userContext = userContext;
    if (this.parent != null) {
      parent.setUserContext(userContext);
    }
  }

  /**
   * Puts a log object entry.
   *
   * <p>The specified value is not stringify until getting logged.
   *
   * @param name Context name
   * @param value Value as an object. The object must provide a meaningful string by toString
   *     method.
   */
  public void putLogContext(String name, Object value) {
    logContext.put(name, value);
  }

  public List<String> getCallTraces() {
    final var result = new ArrayList<String>();
    dfsHistory(result);
    // deeper threads come first, reversing
    Collections.reverse(result);
    return result;
  }

  public String getCallTraceString() {
    return String.join("\n", getCallTraces());
  }

  /**
   * DFS the history.
   *
   * @param result Result to accumulate.
   */
  private void dfsHistory(ArrayList<String> result) {
    final var sb = new StringBuilder(String.format("%d %s: ", createdAt, executionName));
    for (var stage : history) {
      if (stage instanceof String) {
        sb.append(stage);
      } else {
        var forked = (ExecutionState) stage;
        sb.append("(->").append(forked.executionName).append(":)");
        forked.dfsHistory(result);
      }
    }
    result.add(sb.toString());
  }

  // metrics helpers

  /** Used by read operations to clear bytes written to the server. */
  public void clearBytesWritten() {
    if (metricsTracer != null) {
      metricsTracer.clearBytesWritten();
    }
  }

  /**
   * Ensure starting to count pre-processing.
   *
   * <p>If the state has attached metrics tracer, it may have pro-processing time that is already
   * started. The method just uses the timer in that case. Otherwise, the method creates a timer by
   * its own.
   */
  public final void startPreProcess() {
    preProcessTimer =
        metricsTracer != null
            ? metricsTracer.getPreProcessTimer().orElse(new Timer())
            : new Timer();
  }

  public final void clearPreProcessTimer() {
    preProcessTimer = null;
  }

  /**
   * Stops counting pro-processing.
   *
   * <p>The stop counting if a metrics recorder is attached and the pre-processing timer has
   * started.
   */
  public final void endPreProcess() {

    if (metricsRecorder != null && preProcessTimer != null) {
      metricsRecorder.getPreProcessMetrics().attachTimer(preProcessTimer);
      preProcessTimer.commit();
    }
  }

  /**
   * Method to start counting elapsed time for DB access in this execution.
   *
   * <p>Calling this method creates a metrics timer context for this operation.
   *
   * <p>This method takes no effect when metrics collection is disabled for this object.
   */
  public final void startDbAccess() {
    if (metricsRecorder != null) {
      dbExecutionTimer = metricsRecorder.getStorageAccessMetrics().createTimer();
    }
  }

  /**
   * Method to end counting elapsed time for DB access in this execution.
   *
   * <p>Calling this method stops the timer context for this operation.
   *
   * <p>This method takes no effect when metrics collection is disabled for this object.
   */
  public final void endDbAccess() {
    if (dbExecutionTimer != null) {
      dbExecutionTimer.commit();
    }
  }

  public final Optional<Timer> startStorageAccess() {
    if (metricsRecorder != null) {
      return Optional.of(metricsRecorder.getStorageAccessMetrics().createTimer());
    } else {
      return Optional.empty();
    }
  }

  public void startPostProcess() {
    if (metricsTracer != null) {
      metricsTracer.startPostProcess();
    }
  }

  public void endPostProcess() {
    if (metricsTracer != null) {
      metricsTracer.endPostProcess();
    }
  }

  public final void addRecordsRead(long count) {
    if (metricsRecorder != null) {
      metricsRecorder.getNumReads().add(count);
    }
  }

  public final void addRecordsWritten(long count) {
    if (metricsRecorder != null) {
      metricsRecorder.getNumWrites().add(count);
    }
  }

  public final void addError(boolean isDataValidationError) {
    if (metricsRecorder != null && !skipCountingErrors) {
      metricsRecorder.addError(isDataValidationError);
    }
  }

  public String makeErrorContext() {
    if (tenantName != null) {
      final var sb = new StringBuilder("; ");
      sb.append("tenant=").append(tenantName);
      if (streamDesc != null) {
        sb.append(streamDesc.getType() == StreamType.SIGNAL ? ", signal=" : ", context=")
            .append(streamDesc.getName());
      } else if (streamName != null) {
        sb.append(", stream=").append(streamName);
      }
      return sb.toString();
    }
    return "";
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder("{name=").append(executionName);
    return sb.append("}").toString();
  }

  // Following methods are used for debugging ///////////////////////////

  public void putThreadId() {
    threadId = Thread.currentThread().hashCode();
  }

  public void assertSingleThread() {
    assert assertSingleThreadCore();
  }

  public boolean assertSingleThreadCore() {
    final var currentId = Thread.currentThread().getId();
    if (threadId == 0) {
      threadId = currentId;
    }
    return currentId == threadId;
  }

  /** Records activity to an activity logger if set in this state. */
  public void logActivity(String format, Object... params) {
    ActivityRecorder.log(activityRecorder, format, params);
  }
}
