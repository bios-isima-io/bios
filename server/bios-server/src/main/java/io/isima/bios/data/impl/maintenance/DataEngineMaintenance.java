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
package io.isima.bios.data.impl.maintenance;

import static io.isima.bios.common.TfosConfig.ROLLUP_INTERVAL_SECONDS;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.SharedConfigProperties;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.ActivityRecorder;
import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataPublisher;
import io.isima.bios.data.StreamId;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.InferenceEngine;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.maintenance.MaintenanceWorker;
import io.isima.bios.maintenance.ServiceStatus;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataEngineMaintenance extends MaintenanceWorker {
  private static final Logger logger = LoggerFactory.getLogger(DataEngineMaintenance.class);

  // constant parameters to determine worker execution schedule
  private static final int INITIAL_DELAY = 0;
  public static final String FEATURE_WORKER_INTERVAL_KEY = "prop.featureWorkerInterval";

  public static final String KEY_ROLLUP_DETAIL_LOG_ENABLED = "prop.rollupDetailLogEnabled";

  private static final int NUM_ROLLUP_THREADS =
      TfosConfig.rollupThreadCount() > 0 ? TfosConfig.rollupThreadCount() : 1;

  public static final String NUM_FAST_LANES_KEY = "prop.maintenance.numFastLanes";

  // Collection of context names that are unable to rollup
  public static final Set<String> SKIP_LIST = Set.of("_ipblacklist", "_ip2geo");

  private static final WebhookNotifier webhookNotifier =
      new WebhookNotifier(MonitoringSystemConfigs.getMonitoringNotificationUrl());

  private final BiosModules parentModules;

  private final Map<String, Long> dataReceivedTimestampMap = new ConcurrentHashMap<>();
  private final MonitoringClientFailureDetector monitoringClientFailureDetector =
      new MonitoringClientFailureDetector(dataReceivedTimestampMap, webhookNotifier);

  // TODO(Naoki): Make the concurrency configurable by shared properties.
  @Getter private final TaskSlots taskSlots = new TaskSlots(NUM_ROLLUP_THREADS);

  private final Queue<PostProcessContext> scheduledTasks = new PriorityBlockingQueue<>();

  // TODO(BIOS-6728): Use maintenance scheduler instead
  private final ScheduledExecutorService taskChecker = Executors.newSingleThreadScheduledExecutor();

  // Parts necessary for collecting LastN
  @Getter private final List<DataPublisher> dataPublishers;

  private final CassandraConnection cassandraConnection;
  private final DataEngineImpl dataEngine;
  protected final PostProcessScheduler postProcessScheduler;
  protected final boolean isRollupEnabled;
  private boolean isRollupSuspended;
  @Getter private final ExecutorService regularLanes;
  private final PostProcessMonitor regularLanesMonitor;

  private final int numFastLanes;
  private final ExecutorService fastLanes;
  private final PostProcessMonitor fastLanesMonitor;

  private final InferenceEngine inferenceEngine;
  private final SharedConfig sharedConfig;
  private final SharedProperties sharedProperties;

  // sub components
  private final SignalDigestor signalDigestor;
  private final ContextDigestor contextDigestor;
  private final ContextIndexer contextIndexer;
  private final SketchStore sketchStore;

  // for debugging
  @Setter @Getter private ActivityRecorder activityRecorder = null;

  private final Map<StreamId, Set<PostProcessOperation>> processingStreams =
      new ConcurrentHashMap<>();
  private final Set<StreamId> deletedStreams = ConcurrentHashMap.newKeySet();

  /**
   * The constructor of RollupWorker class.
   *
   * @param ppScheduler Rollup scheduler to be used for scheduling rollup tasks.
   */
  public DataEngineMaintenance(
      CassandraConnection cassandraConnection,
      DataEngineImpl dataEngine,
      PostProcessScheduler ppScheduler,
      SketchStore sketchStore,
      InferenceEngine inferenceEngine,
      SharedConfig sharedConfig,
      SharedProperties sharedProperties,
      ServiceStatus serviceStatus,
      BiosModules parentModules) {
    super(INITIAL_DELAY, getDelay(), TimeUnit.MILLISECONDS, serviceStatus, parentModules);
    this.parentModules = parentModules;
    this.cassandraConnection = cassandraConnection;
    this.dataEngine = dataEngine;
    this.postProcessScheduler = ppScheduler;
    this.inferenceEngine = inferenceEngine;
    this.sharedConfig = sharedConfig;
    this.sharedProperties = sharedProperties;
    this.dataPublishers = new CopyOnWriteArrayList<>();
    isRollupEnabled = TfosConfig.rollupEnabled();
    isRollupSuspended = false;

    regularLanes =
        Executors.newFixedThreadPool(
            NUM_ROLLUP_THREADS,
            ExecutorManager.makeThreadFactory("bios-rollup", Thread.NORM_PRIORITY));
    this.regularLanesMonitor =
        new PostProcessMonitor(sharedProperties, "regular", NUM_ROLLUP_THREADS);

    numFastLanes = getNumFastLanes();
    fastLanes =
        Executors.newFixedThreadPool(
            numFastLanes,
            ExecutorManager.makeThreadFactory("bios-rollup-fast", Thread.NORM_PRIORITY));
    fastLanesMonitor = new PostProcessMonitor(sharedProperties, "fast track", numFastLanes);

    this.signalDigestor =
        new SignalDigestor(
            this, dataEngine, sharedConfig, sharedProperties, postProcessScheduler, sketchStore);
    this.contextIndexer =
        new ContextIndexer(this, dataEngine, sharedProperties, postProcessScheduler);
    this.contextDigestor =
        new ContextDigestor(this, dataEngine, postProcessScheduler, contextIndexer, sketchStore);
    this.sketchStore = sketchStore;
    if (!isRollupEnabled) {
      logger.info("Rollup is disabled on this node");
    }

    taskChecker.scheduleWithFixedDelay(this::checkTasks, 100, 100, TimeUnit.MILLISECONDS);
  }

  @Override
  public void runMaintenance() {
    runMaintenanceTask(
        () -> {
          regularLanesMonitor.check();
          fastLanesMonitor.check();
          postProcess();
          maintainDataEngine();
          // TODO(BIOS-5572): Move this elsewhere. This is not maintaining data plane.
          regularLanes.submit(
              () -> {
                monitoringClientFailureDetector.run();
              });
        });
  }

  void runMaintenanceTask(Runnable task) {
    // A regular maintenance should not run while the service is in maintenance mode.
    // The server is in maintenance mode during failure recovery (cache reloading), upgrading, etc.
    if (serviceStatus.isInMaintenance()) {
      return;
    }

    boolean suspending =
        sharedConfig != null
            && Boolean.valueOf(sharedConfig.getProperty(SharedConfigProperties.ROLLUP_SUSPEND));
    if (suspending) {
      if (!isRollupSuspended) {
        logger.info("Rollup suspended");
      }
      isRollupSuspended = true;
      return;
    } else if (isRollupSuspended) {
      logger.info("Rollup resumed");
    }
    isRollupSuspended = false;

    task.run();
  }

  private void postProcess() {
    if (!isRollupEnabled) {
      return;
    }
    try {
      postProcessCore();
    } catch (Throwable e) {
      logger.error("Rollup worker failed", e);
    }
  }

  /** Main worker. */
  private void postProcessCore() {
    final AdminInternal admin = BiosModules.getAdminInternal();
    final var targetStreams = new ArrayList<StreamDesc>();
    for (String tenant : admin.getAllTenants()) {
      try {
        targetStreams.addAll(
            admin
                .getTenant(tenant)
                .getStreams(
                    (stream) ->
                        Set.of(StreamType.SIGNAL, StreamType.CONTEXT).contains(stream.getType())));
      } catch (NoSuchTenantException e) {
        // the tenant is removed during the maintenance. skip this tenant.
      }
    }
    Collections.shuffle(targetStreams);
    for (StreamDesc streamDesc : targetStreams) {
      if (isShutdown) {
        break;
      }
      if (SKIP_LIST.contains(streamDesc.getName().toLowerCase()) || isStreamDeleted(streamDesc)) {
        continue;
      }

      if (reserveOp(streamDesc, PostProcessOperation.ROLLUP)) {
        scheduleTask(PostProcessSpecifiers.scheduleRollup(streamDesc));
      }

      if (reserveOp(streamDesc, PostProcessOperation.INFER_SIGNAL_TAGS)) {
        scheduleTask(PostProcessSpecifiers.scheduleSignalTagInference(streamDesc));
      }
    }
  }

  /** The reactor on task scheduling queue. */
  private void checkTasks() {
    final var currentTime = System.currentTimeMillis();
    while (true) {
      final var ctxToCheck = scheduledTasks.peek();
      if (ctxToCheck == null || ctxToCheck.getProcessExecutionTime() > currentTime) {
        return;
      }
      // this context object may be different from ctxtToCheck, but its execution time is the same
      // or earlier in the case, which also has to be processed.
      var ctx = scheduledTasks.poll();
      ctx.pickedUp();
      var specs = ctx.getSpecs();
      final var streamDesc = specs.getStreamDesc();
      final var state = new GenericExecutionState("PostProcessing master", ctx.getExecutor());
      handleRequest(ctx, state)
          .thenAccept(
              (nextSpecs) -> {
                ctx.done();
                if (nextSpecs != null && nextSpecs.hasTasks()) {
                  scheduleTask(nextSpecs);
                } else {
                  concludeOp(specs.getStreamDesc(), specs.getOp());
                }
                if (debugLogEnabled(streamDesc)) {
                  if (nextSpecs == null) {
                    logger.info("  No post-process scheduled");
                  } else {
                    final var timeRange = nextSpecs.getTimeRange();
                    logger.info(
                        "** Next schedule; stream={}, current={}, execTime={}, rollupPoint={},"
                            + " [{} - {}] rollup={} index={} sketches={} infer={}",
                        streamDesc.getName(),
                        StringUtils.tsToIso8601(System.currentTimeMillis()),
                        StringUtils.tsToIso8601(nextSpecs.getProcessExecutionTime()),
                        StringUtils.tsToIso8601(nextSpecs.getRollupPoint()),
                        timeRange != null ? StringUtils.tsToIso8601(timeRange.getBegin()) : "?",
                        timeRange != null ? StringUtils.tsToIso8601(timeRange.getEnd()) : "?",
                        nextSpecs.getRollups().size(),
                        nextSpecs.getIndexes().size(),
                        nextSpecs.getSketches().size(),
                        nextSpecs.isToDoInfer());
                  }
                }
              })
          .exceptionally(
              (t) -> {
                ctx.doneExceptionally(t);
                final var streamType = streamDesc.getType().name().toLowerCase();
                final Throwable cause = t instanceof CompletionException ? t.getCause() : t;
                if (cause instanceof InterruptedException) {
                  logger.info(
                      "Post-process cancaelled; tenant={} {}={}, op={}",
                      streamDesc.getParent().getName(),
                      streamType,
                      streamDesc.getName(),
                      specs.getOp());
                  return null;
                }
                logger.error(
                    "Post-process failed; tenant={} {}={}, op={}",
                    streamDesc.getParent().getName(),
                    streamType,
                    streamDesc.getName(),
                    specs.getOp(),
                    cause);
                logger.warn("Operation trace;\n{}", state.getCallTraceString());
                return null;
              });
    }
  }

  private CompletableFuture<PostProcessSpecifiers> handleRequest(
      PostProcessContext ctx, ExecutionState masterState) {
    switch (ctx.getSpecs().getOp()) {
      case ROLLUP:
        return handleRollupRequest(ctx, masterState);
      case INFER_SIGNAL_TAGS:
        return handleSignalTagInferenceRequest(ctx, masterState);
      case DERIVE_CONTEXT:
        return handleDeriveContextRequest(ctx, masterState);
      case SCHEDULE_ROLLUP:
        return handleScheduleRollupRequest(ctx, masterState);
      case SCHED_TAG_INFERENCE:
        return handleScheduleSignalTagInferenceRequest(ctx, masterState);
      case CANCEL:
        return handleCancelRequest(ctx);
      case CANCEL_ALL:
        return handleCancelAllRequest(ctx);
      default:
        logger.error("Unknown task scheduled: {}", ctx.getSpecs());
        return CompletableFuture.completedFuture(null);
    }
  }

  private CompletableFuture<PostProcessSpecifiers> handleRollupRequest(
      PostProcessContext ctx, ExecutionState masterState) {
    final var specs = ctx.getSpecs();
    final var streamDesc = specs.getStreamDesc();
    if (isStreamDeleted(streamDesc)) {
      concludeOp(streamDesc, PostProcessOperation.ROLLUP);
      return CompletableFuture.completedFuture(null);
    }

    final var executor = ctx.getExecutor();

    // final var ctx = monitor.taskStarting(specs, false);
    final var state =
        new DigestState(
            String.format(
                "post process %s=%s",
                streamDesc.getType().name().toLowerCase(), streamDesc.getName()),
            streamDesc,
            System.currentTimeMillis(),
            executor);
    masterState.addBranch(state);

    final CompletableFuture<PostProcessSpecifiers> future;
    if (streamDesc.getType() == StreamType.SIGNAL) {
      future =
          CompletableFuture.completedFuture(null)
              .thenComposeAsync(
                  (none) -> {
                    ctx.start();
                    return signalDigestor
                        .execute(specs, state)
                        .thenComposeAsync(
                            (doContinue) -> {
                              if (doContinue && !isShutdown) {
                                return scheduleNext(specs, state);
                              }
                              return CompletableFuture.completedFuture(null);
                            },
                            state.getExecutor());
                  },
                  state.getExecutor());
    } else {
      state.addHistory("(rollup");
      future =
          CompletableFuture.completedFuture(null)
              .thenComposeAsync(
                  (none) -> {
                    ctx.start();
                    state.addHistory("(digest");
                    return contextDigestor
                        .execute(specs, state)
                        .thenApply(
                            (derivativeCalculator) -> {
                              state.addHistory(")");
                              // Schedule a new line of post-process if derivative calculation is
                              // specified
                              // and this line terminates.
                              if (derivativeCalculator != null) {
                                state.addHistory("(scheduleDeriving");
                                scheduleTask(
                                    PostProcessSpecifiers.calculateContextDerivative(
                                        streamDesc, derivativeCalculator));
                                state.addHistory(")");
                              }
                              state.addHistory(")");
                              return null;
                            });
                  },
                  state.getExecutor());
    }
    return future.whenComplete(
        (r, t) -> {
          if (t == null) {
            ctx.getProcessedSpecs().addAll(state.getProcessedSpecifiers());
            state.markDone();
          } else {
            state.markError();
          }
        });
  }

  private CompletableFuture<PostProcessSpecifiers> handleSignalTagInferenceRequest(
      PostProcessContext ctx, ExecutionState masterState) {
    final var specs = ctx.getSpecs();
    final var streamDesc = specs.getStreamDesc();
    if (isStreamDeleted(streamDesc)) {
      return CompletableFuture.completedFuture(null);
    }

    final var state =
        new DigestState(
            String.format(
                "infer tags of %s %s",
                streamDesc.getType().name().toLowerCase(), streamDesc.getName()),
            streamDesc,
            System.currentTimeMillis(),
            masterState.getExecutor());
    masterState.addBranch(state);

    return CompletableFuture.completedFuture(null)
        .thenComposeAsync(
            (none) -> {
              ctx.start();
              final var inferenceState = new InferenceOverallState(System.currentTimeMillis());
              return inferenceEngine.inferAttributeTags(inferenceState, streamDesc, state);
            },
            state.getExecutor())
        .thenApplyAsync(
            (none) -> postProcessScheduler.scheduleTagInference(specs), state.getExecutor())
        .whenComplete(
            (r, t) -> {
              if (t == null) {
                state.markDone();
              } else {
                state.markError();
              }
            });
  }

  private CompletableFuture<PostProcessSpecifiers> handleDeriveContextRequest(
      PostProcessContext ctx, ExecutionState masterState) {
    final var digestionExecutor = BiosModules.getDigestor().getExecutor();
    final var specs = ctx.getSpecs();
    final var state =
        new DigestState(
            specs.getOp().name(),
            specs.getStreamDesc(),
            System.currentTimeMillis(),
            digestionExecutor);
    masterState.addBranch(state);
    final var calculator = specs.getContextDerivativeCalculator();
    assert calculator != null;
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync(
            (none) -> {
              ctx.start();
              return calculator.execute(digestionExecutor);
            },
            digestionExecutor)
        .handle(
            (r, t) -> {
              if (t == null) {
                state.markDone();
              } else {
                state.markError();
              }
              return null;
            });
  }

  private CompletableFuture<PostProcessSpecifiers> handleScheduleRollupRequest(
      PostProcessContext ctx, ExecutionState masterState) {
    final var specs = ctx.getSpecs();
    final var streamDesc = specs.getStreamDesc();
    if (isStreamDeleted(streamDesc)) {
      return CompletableFuture.completedFuture(null);
    }
    final var state =
        new DigestState(
            String.format(
                "post process %s %s",
                streamDesc.getType().name().toLowerCase(), streamDesc.getName()),
            streamDesc,
            System.currentTimeMillis(),
            regularLanes);
    masterState.addBranch(state);
    final long margin = streamDesc.getType() == StreamType.CONTEXT ? 15000 : 0;
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync(
            (none) -> {
              ctx.start();
              return postProcessScheduler.schedule(
                  streamDesc, System.currentTimeMillis(), margin, state);
            },
            state.getExecutor())
        .whenComplete(
            (r, t) -> {
              if (t == null) {
                state.markDone();
              } else {
                state.markError();
              }
            });
  }

  private CompletableFuture<PostProcessSpecifiers> handleScheduleSignalTagInferenceRequest(
      PostProcessContext ctx, ExecutionState masterState) {
    final var specs = ctx.getSpecs();
    if (isStreamDeleted(specs.getStreamDesc())) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.supplyAsync(
        () -> {
          ctx.start();
          return postProcessScheduler.scheduleTagInference(specs);
        },
        masterState.getExecutor());
  }

  /**
   * Removes scheduled tasks from the task list.
   *
   * <p>Never run this method outside the task checking loop since the method modifies the task
   * list.
   */
  private CompletableFuture<PostProcessSpecifiers> handleCancelRequest(PostProcessContext ctx) {
    ctx.start();
    final var specs = ctx.getSpecs();
    final var streamDesc = specs.getStreamDesc();
    final var streamIdToCancel = streamDesc.getStreamId();
    logger.info("Canceling post-processes for stream {}", streamIdToCancel);
    final var iterator = scheduledTasks.iterator();
    while (iterator.hasNext()) {
      final var task = iterator.next();
      final var streamId = task.getSpecs().getStreamDesc().getStreamId();
      if (streamId.equals(streamIdToCancel)) {
        iterator.remove();
        final boolean deleted = concludeOp(streamId, task.getSpecs().getOp());
        if (debugLogEnabled(task.getSpecs().getStreamDesc())) {
          logger.info("Found a request, canceled={}", deleted);
        }
      }
    }
    regularLanesMonitor.clearStream(streamDesc);
    fastLanesMonitor.clearStream(streamDesc);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Removes all panding tasks.
   *
   * <p>Never run this method outside the task checking loop since the method modifies the task
   * list.
   */
  private CompletableFuture<PostProcessSpecifiers> handleCancelAllRequest(PostProcessContext ctx) {
    ctx.start();
    logger.info("Canceling all post-processes");
    while (!scheduledTasks.isEmpty()) {
      final var task = scheduledTasks.poll();
      final var specs = task.getSpecs();
      final var streamDesc = specs.getStreamDesc();
      final var streamId = streamDesc.getStreamId();
      if (debugLogEnabled(streamDesc)) {
        logger.info("Canceling task for stream {}, op={}", streamDesc.getName(), specs.getOp());
      }
      concludeOp(streamId, specs.getOp());
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<PostProcessSpecifiers> scheduleNext(
      PostProcessSpecifiers specs, DigestState state) {
    final var streamDesc = specs.getStreamDesc();
    final var now = System.currentTimeMillis();
    final var interval = specs.getShortestInterval();
    final CompletableFuture<PostProcessSpecifiers> catchUp;
    if (now < specs.getRollupPoint() + interval && !isStreamDeleted(streamDesc)) {
      // try catching-up or retroactive rollup
      catchUp =
          postProcessScheduler
              .schedule(streamDesc, now, 0, specs, state)
              .thenApply(
                  (nextSpecs) -> {
                    if (nextSpecs != null && nextSpecs.isEmpty()) {
                      return null;
                    }
                    return nextSpecs;
                  });
    } else {
      catchUp = CompletableFuture.completedFuture(null);
    }

    return catchUp.thenComposeAsync(
        (nextSpecs) -> {
          if (nextSpecs != null || interval == 0) {
            // there's a task to execute already, or it's not possible to schedule next execution
            return CompletableFuture.completedFuture(nextSpecs);
          }
          return postProcessScheduler
              .getFeatureMarginTime(specs.isFastTrackRequired(), state)
              .thenComposeAsync(
                  (margin) -> {
                    final var nextTime =
                        (specs.getProcessExecutionTime() / interval + 1) * interval + margin + 1;
                    return postProcessScheduler.schedule(streamDesc, nextTime, 0, specs, state);
                  },
                  state.getExecutor());
        },
        state.getExecutor());
  }

  boolean isStreamDeleted(StreamDesc streamDesc) {
    if (deletedStreams.contains(streamDesc.getStreamId())) {
      return true;
    }
    return BiosModules.getAdminInternal()
            .getStreamOrNull(streamDesc.getParent().getName(), streamDesc.getName())
        == null;
  }

  private static int getDelay() {
    final int defaultValue =
        TfosConfig.rollupIntervalSeconds() >= 1 ? TfosConfig.rollupIntervalSeconds() * 1000 : 1000;
    int value = SharedProperties.getInteger(FEATURE_WORKER_INTERVAL_KEY, defaultValue);
    if (value < 0) {
      logger.warn(
          "Value of shared property {} or property {} is out of range, set to default {}",
          FEATURE_WORKER_INTERVAL_KEY,
          ROLLUP_INTERVAL_SECONDS,
          defaultValue);
      value = defaultValue;
    }
    return value;
  }

  public void registerPublisher(DataPublisher publisher) {
    dataPublishers.add(publisher);
  }

  public TaskSlot acquireSlot() throws InterruptedException {
    return new TaskSlot(taskSlots.getSlots(), logger);
  }

  void scheduleTask(PostProcessSpecifiers specs) {
    Objects.requireNonNull(specs);
    final var useFastTrack = specs.isFastTrackRequired();
    final var executor = useFastTrack ? fastLanes : regularLanes;
    final var monitor = useFastTrack ? fastLanesMonitor : regularLanesMonitor;
    final var ctx = monitor.taskScheduling(specs, true, executor);
    scheduledTasks.add(ctx);
  }

  boolean reserveOp(StreamDesc streamDesc, PostProcessOperation op) {
    return reserveOp(streamDesc.getStreamId(), op);
  }

  boolean reserveOp(StreamId streamId, PostProcessOperation op) {
    final var processStatus =
        processingStreams.computeIfAbsent(streamId, (k) -> ConcurrentHashMap.newKeySet());
    return processStatus.add(op);
  }

  boolean concludeOp(StreamDesc streamDesc, PostProcessOperation op) {
    return concludeOp(streamDesc.getStreamId(), op);
  }

  boolean concludeOp(StreamId streamId, PostProcessOperation op) {
    Objects.requireNonNull(streamId);
    Objects.requireNonNull(op);
    final var opToRemove =
        Map.of(
                PostProcessOperation.SCHEDULE_ROLLUP,
                PostProcessOperation.ROLLUP,
                PostProcessOperation.SCHED_TAG_INFERENCE,
                PostProcessOperation.INFER_SIGNAL_TAGS)
            .getOrDefault(op, op);

    final var processStatus = processingStreams.get(streamId);
    return processStatus != null ? processStatus.remove(opToRemove) : false;
  }

  private boolean anyPendingTasksExist() {
    return processingStreams.values().stream().anyMatch((ops) -> !ops.isEmpty());
  }

  private void maintainDataEngine() {
    logger.trace("Start maintaining DataEngine");
    final var streamId = new StreamId("__biosServer__", "dataEngine", 0L);
    try {
      // TODO(Naoki): complete this
      // processingStreams.add(streamId);  // add a special stream for graceful shutdown
      dataEngine.maintain(taskSlots);
    } finally {
      // processingStreams.remove(streamId);
    }
  }

  StreamDesc getIndexConfig(StreamDesc signalDesc, ViewDesc viewDesc)
      throws NoSuchStreamException, NoSuchTenantException {
    final String configName =
        AdminUtils.makeIndexStreamName(signalDesc.getName(), viewDesc.getName());
    return BiosModules.getAdminInternal()
        .getStreamOrNull(signalDesc.getParent().getName(), configName, signalDesc.getVersion());
  }

  @Override
  public void shutdown() {
    if (isShutdown) {
      return;
    }
    super.shutdown();
    stop();
    regularLanes.shutdownNow();
  }

  public void stop() {
    logger.info("oo Post processor is shutting down");
    long waitUntil = System.currentTimeMillis() + 10000;
    while (System.currentTimeMillis() < waitUntil) {
      // terminate when all tasks are done
      if (!anyPendingTasksExist()) {
        logger.info("** Post processor has stopped");
        return;
      }
      scheduleTask(PostProcessSpecifiers.cancelAll());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("Interruption happened");
        Thread.currentThread().interrupt();
        break;
      }
    }
    logger.warn("** Post processor did not stop, force terminating");
  }

  /**
   * Stops all maintenance tasks for a tenant.
   *
   * <p>This method is called by AdminInternal via DataEngine on tenant deletion to prevent any
   * errors due to remaining maintenance tasks.
   */
  public void clearTenant(TenantDesc tenantDesc) {
    if (!isRollupEnabled) {
      return;
    }

    final var streamsToDelete = new HashSet<StreamId>();
    for (var streamDesc : tenantDesc.getAllStreams()) {
      deletedStreams.add(streamDesc.getStreamId());
      streamsToDelete.add(streamDesc.getStreamId());
      scheduleTask(PostProcessSpecifiers.cancel(streamDesc));
    }

    final long maintenanceCompletionTime = 20000;
    final var tryUntil = System.currentTimeMillis() + maintenanceCompletionTime;
    boolean cleared = false;
    while (System.currentTimeMillis() < tryUntil) {
      if (streamsToDelete.stream()
          .allMatch(
              (streamId) ->
                  !processingStreams.containsKey(streamId)
                      || processingStreams.get(streamId).isEmpty())) {
        cleared = true;
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("Interrupted!");
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (!cleared) {
      logger.warn(
          "Data maintenance for tenant {} did not complete within {} seconds, forcing to clean up",
          tenantDesc.getName(),
          maintenanceCompletionTime);
    }
    inferenceEngine.clearTenant(tenantDesc.getName());
    sketchStore.clearTenant(tenantDesc.getId());
    postProcessScheduler.clearTenant(tenantDesc.getName());
  }

  public void clearStream(StreamDesc streamDesc) {
    if (!isRollupEnabled) {
      return;
    }
    final var streamId = streamDesc.getStreamId();
    final var streamType = streamDesc.getType();
    if (streamType == StreamType.SIGNAL || streamType == StreamType.CONTEXT) {
      deletedStreams.add(streamId);
      scheduleTask(PostProcessSpecifiers.cancel(streamDesc));
    }
  }

  /**
   * Stops all maintenance tasks for a stream.
   *
   * <p>This method is called by AdminInternal via DataEngine on deleting or modifying a stream to
   * prevent any errors due to remaining maintenance tasks.
   */
  private boolean debugLogEnabled(StreamDesc streamDesc) {
    return streamDesc
        .getName()
        .equalsIgnoreCase(BiosModules.getSharedConfig().getProperty("prop.rollupDebugSignal"));
  }

  private int getNumFastLanes() {
    int numFastLanes = 4;
    if (sharedProperties != null) {
      try {
        final var src = sharedProperties.getProperty(NUM_FAST_LANES_KEY);
        if (src != null && !src.isBlank()) {
          numFastLanes = Integer.parseInt(src);
        }
      } catch (NumberFormatException e) {
        logger.error(
            "Invalid property {}, using default {}: {}",
            NUM_FAST_LANES_KEY,
            numFastLanes,
            e.getMessage());
      }
    }
    return numFastLanes;
  }
}
