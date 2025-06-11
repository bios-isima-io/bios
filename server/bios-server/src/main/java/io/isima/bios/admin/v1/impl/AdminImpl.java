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
package io.isima.bios.admin.v1.impl;

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_OPERATION;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;
import static io.isima.bios.admin.v1.AdminConstants.ENRICHED_ATTRIBUTE_DELIMITER;
import static io.isima.bios.admin.v1.AdminConstants.ENRICHED_ATTRIBUTE_DELIMITER_REGEX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_COUNT_ATTRIBUTE;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_MAX_SUFFIX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_MIN_SUFFIX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_SUM_SUFFIX;
import static io.isima.bios.admin.v1.impl.AdminImplUtils.createExceptionMessage;
import static io.isima.bios.admin.v1.impl.AdminImplUtils.findAttributeReference;
import static io.isima.bios.models.v1.InternalAttributeType.BLOB;
import static io.isima.bios.models.v1.InternalAttributeType.DOUBLE;
import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.LONG;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.isima.bios.admin.v1.AdminChangeListener;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminOption;
import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.CompiledAttribute;
import io.isima.bios.admin.v1.CompiledEnrichment;
import io.isima.bios.admin.v1.EnrichmentKind;
import io.isima.bios.admin.v1.FeatureAsContextInfo;
import io.isima.bios.admin.v1.StreamComparators;
import io.isima.bios.admin.v1.StreamConversion;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.admin.v1.StreamTag;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.TenantStoreDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.feature.AccumulatingCountContextAdjuster;
import io.isima.bios.data.impl.feature.ContextAdjuster;
import io.isima.bios.errors.exception.AdminChangeRequestToSameException;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.ConstraintWarningException;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.InvalidAlertException;
import io.isima.bios.errors.exception.InvalidDefaultValueException;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.StreamAlreadyExistsException;
import io.isima.bios.errors.exception.TenantAlreadyExistsException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.DirectedAcyclicGraphException;
import io.isima.bios.export.ExportService;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.metrics.MetricsConstants;
import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeModAllowance;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.TimeInterval;
import io.isima.bios.models.TimeunitType;
import io.isima.bios.models.Unit;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.preprocess.TimeLagCalculator;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The main file for processing tenant and stream configuration. */
public class AdminImpl implements AdminInternal {
  private static final Logger logger = LoggerFactory.getLogger(AdminImpl.class);

  public static final String ADD_MOMENTS_SKETCH_TO_ALL_ROLLUPS_KEY =
      "prop.addMomentsSketchToAllRollups";
  private static final Boolean ADD_MOMENTS_SKETCH_TO_ALL_ROLLUPS_DEFAULT = Boolean.TRUE;
  public static final String DEFAULT_SKETCHES_FOR_ATTRIBUTES_KEY =
      "prop.defaultSketchesForAttributes";
  private static final String DEFAULT_SKETCHES_FOR_ATTRIBUTES_DEFAULT =
      "Moments,Quantiles," + "DistinctCount,SampleCounts";
  public static final String DEFAULT_SKETCHES_INTERVAL_KEY = "prop.defaultSketchesInterval";
  public static final Long DEFAULT_SKETCHES_INTERVAL_DEFAULT = 300000L;

  private final List<AdminChangeListener> subscribers;

  // sub components
  private final AdminStore adminStore;

  private final List<AdminConfig> adminConfigs;

  // AdminInternal cache
  private Tenants tenants;

  /**
   * The whole process of adding/modifying/removing a stream needs to happen under a lock, to
   * prevent multiple stream changes from happening concurrently. Since the tenant object itself
   * changes as part of a addStream/modifyStream/removeStream, we cannot synchronize on it; we need
   * to synchronize on a more stable object. The 'tenants' object is also not stable enough - it is
   * replaced on reload, so we need a 'final' object that never changes during the lifetime of this
   * AdminImpl object. Using a empty array of Object[] instead of a simple Object() because Object()
   * is not serializable, whereas 0-size array is. While it may not matter here right now, this is a
   * better pattern to follow. Another possibility is to just lock 'this', but it is a good coding
   * practice not to expose our locks, so using a private member.
   *
   * <p>The users of this lock must hold the lock before even retrieving the current tenantDesc
   * object, so that two concurrent modifications do not overwrite each-other's changes.
   */
  private final Object schemaModificationLock = new Object[0];

  private final IMetricsStreamProvider metricsStreamProvider;

  private final IStreamProvider ip2GeoStreamProvider;

  private final IStreamProvider ipBlacklistStreamProvider;

  private final IStreamProvider clientMetricsStreamProvider;

  private final IStreamProvider operationFailureStreamProvider;

  // This flag is turned on during initial cache loading.
  // Some parts in the schema verification behaves differently during initial loading.
  private boolean inInitialLoading;

  /** TEST ONLY: Flag to force data format version unspecified in creating/modifying a stream. */
  private boolean forceUnspecifiedFormat;

  // TODO(Naoki): The export service should not be here.  It's an unnecessary cross dependency
  private ExportService exportService;

  /**
   * Constructor of the class.
   *
   * @param adminStore AdminStore instance to be used for storing admin data.
   * @param listeners AdminInternal configuration change listeners to be registered to this
   *     instance. The subscribers get notified when a configuration change happens.
   */
  public AdminImpl(
      AdminStore adminStore,
      IMetricsStreamProvider metricsStreamProvider,
      AdminChangeListener... listeners) {
    this(adminStore, metricsStreamProvider, null, null, null, null, listeners);
  }

  // Using this constructor to avoid test fail
  // TODO: Fix the test
  public AdminImpl(
      AdminStore adminStore,
      IMetricsStreamProvider metricsStreamProvider,
      IStreamProvider ip2GeoStreamProvider,
      IStreamProvider ipBlacklistStreamProvider,
      IStreamProvider clientMetricsStreamProvider,
      IStreamProvider operationFailureStreamProvider,
      AdminChangeListener... listeners) {
    // initialize sub components
    this.adminStore = adminStore;
    try {
      // BB-1330 Disable resource usage monitor signals
      // TODO(BB-1330): Revisit here and fix properly. We haven't found the root cause.
      adminConfigs = List.of(new SystemAdminConfig());
    } catch (FileReadException e) {
      throw new RuntimeException(e);
    }

    // register change listeners
    subscribers = new ArrayList<>();
    subscribers.addAll(Arrays.asList(listeners));

    forceUnspecifiedFormat = false;

    this.metricsStreamProvider = metricsStreamProvider;

    this.ip2GeoStreamProvider = ip2GeoStreamProvider;

    this.ipBlacklistStreamProvider = ipBlacklistStreamProvider;

    this.clientMetricsStreamProvider = clientMetricsStreamProvider;

    this.operationFailureStreamProvider = operationFailureStreamProvider;

    initCache();
  }

  // necessary for mock testing.
  public AdminImpl() {
    this(null, null);
  }

  public AdminImpl(IMetricsStreamProvider provider) {
    this(null, provider);
  }

  public void injectExportService(ExportService exportService) {
    this.exportService = exportService;
  }

  private void initCache() {
    tenants = new Tenants();
    logger.info("Initializing admin cache.");
    if (adminStore != null) {
      try {
        final var tenantConfigs = adminStore.getTenantStoreDescMap();
        loadTenantConfigs(tenantConfigs);
        for (AdminConfig adminConfig : adminConfigs) {
          loadSystemConfigs(adminConfig);
        }
      } catch (TfosException | ApplicationException e) {
        logger.error("AdminStoreImpl initialization failure:", e);
        throw new RuntimeException(e);
      }
    }
  }

  private Integer formatVersionToUse() {
    return forceUnspecifiedFormat ? null : FormatVersion.LATEST;
  }

  /**
   * TEST ONLY: Set force unspecified format flag.
   *
   * @param flag Flag on or off.
   */
  @VisibleForTesting
  public void setForceUnspecifiedFormat(boolean flag) {
    forceUnspecifiedFormat = flag;
  }

  /**
   * TEST ONLY: Adds a subscriber.
   *
   * <p>For the product code, use the class constructor to add a subscriber.
   *
   * @param listener AdminChangeListener to add.
   */
  @VisibleForTesting
  public void addSubscriber(AdminChangeListener listener) {
    subscribers.add(listener);
  }

  /**
   * Set map of tenant names and tenant configs.
   *
   * @param tenantStoreDescs the tenantConfigs to set
   * @throws ApplicationException when a change subscriber fails to load
   */
  protected void loadTenantConfigs(Map<String, Deque<TenantStoreDesc>> tenantStoreDescs)
      throws TfosException, ApplicationException {
    for (Map.Entry<String, Deque<TenantStoreDesc>> entry : tenantStoreDescs.entrySet()) {
      final Deque<TenantStoreDesc> tenantStoreDescList = entry.getValue();
      // modify tenant is not supported, only the latest one is valid.
      if (tenantStoreDescList.isEmpty()) {
        continue;
      }
      final TenantStoreDesc tenantStoreDesc = tenantStoreDescList.getFirst();
      processTenantStoreaDesc(tenantStoreDesc);
    }
  }

  private void processTenantStoreaDesc(TenantStoreDesc tenantStoreDesc)
      throws ApplicationException, TfosException {
    TenantDesc tenantDesc;
    try {
      tenantDesc = new TenantDesc(tenantStoreDesc);
    } catch (DirectedAcyclicGraphException e) {
      String bug =
          "Bug: cycle detected in graph during load"
              + ", tenantConfig ="
              + tenantStoreDesc.getName()
              + ". Inner message: "
              + e.getMessage();
      throw new ApplicationException(bug);
    }
    if (tenantDesc.isDeleted()) {
      return;
    }

    try {
      inInitialLoading = true;
      var streams = tenantDesc.getAllStreamVersionsInVersionOrder();
      final var sorter = new StreamSorter(streams);
      streams = sorter.sortByDependencyOrder();
      for (StreamDesc streamDesc : streams) {
        final var subStreams = initStreamConfig(tenantDesc, streamDesc, Set.of());
        for (StreamDesc subStream : subStreams) {
          tenantDesc.addStream(subStream);
        }
      }
    } catch (DirectedAcyclicGraphException e) {
      throw new RuntimeException(e);
    } finally {
      inInitialLoading = false;
    }
    buildModChains(tenantDesc);
    tenantDesc.removeInactiveSubStreams();
    tenantDesc.buildSignalAliases();
    // notify subscribers
    for (AdminChangeListener subscriber : subscribers) {
      subscriber.createTenant(tenantDesc, RequestPhase.FINAL);
    }
    tenants.addTenant(tenantDesc);
    addMissingMetricsStream(tenantDesc);
    if (tenantDesc.isProxyInformationDirty()) {
      // This can only happen if there was a torn write, i.e. a newly added stream was
      // written but tenant was not.
      // Write the tenant and stream storeDesc information with proxies to the database.
      final var tenantStoreDescToWrite = tenantDesc.toTenantStore();
      adminStore.storeTenant(tenantStoreDescToWrite);
      tenantDesc.setProxyInformationDirty(false);
      logger.warn(
          "Detected a torn write. See BIOS-2319. tenant={}, " + "maxAllocatedStreamNameProxy={}",
          tenantStoreDescToWrite.getName(),
          tenantStoreDescToWrite.getMaxAllocatedStreamNameProxy());
    }
  }

  /**
   * add metrics stream if it is not present in the tenant, reason may be it is deleted for update
   * or it is a old tenant which doesn't have tenant level metrics.
   */
  void addMissingMetricsStream(TenantDesc tenantDesc) throws ApplicationException, TfosException {
    if (tenantDesc.getName().equalsIgnoreCase(BiosConstants.TENANT_SYSTEM)) {
      return;
    }

    final StreamDesc existingMetricsStream =
        tenantDesc.getStream(MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL);
    final StreamDesc newMetricsStream = generateForMissingOperationalMetricsStream();
    if (newMetricsStream == null) {
      return;
    }

    if (existingMetricsStream == null) {
      if (isTenantDescLatest(tenantDesc)) {
        logger.info(
            "Creating request metrics tenant={} stream={} version={}({})",
            tenantDesc.getName(),
            newMetricsStream.getName(),
            newMetricsStream.getVersion(),
            StringUtils.tsToIso8601(newMetricsStream.getVersion()));
        this.addStream(tenantDesc.getName(), newMetricsStream, RequestPhase.INITIAL);
        this.addStream(tenantDesc.getName(), newMetricsStream, RequestPhase.FINAL);
      }
      return;
    }

    StreamConfig existingStreamConfig = existingMetricsStream.asStreamConfig();
    stripStreamConfig(existingStreamConfig);
    StreamConfig newStreamConfig = newMetricsStream.asStreamConfig();
    stripStreamConfig(newStreamConfig);

    if (!existingStreamConfig.equals(newStreamConfig)) {
      if (isTenantDescLatest(tenantDesc)) {
        newMetricsStream.setVersion(System.currentTimeMillis());
        logger.info(
            "Updating request metrics tenant={} stream={} version={}({})",
            tenantDesc.getName(),
            newMetricsStream.getName(),
            newMetricsStream.getVersion(),
            StringUtils.tsToIso8601(newMetricsStream.getVersion()));
        this.modifyStream(
            tenantDesc.getName(),
            MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL,
            newMetricsStream,
            RequestPhase.INITIAL,
            AttributeModAllowance.FORCE,
            Set.of());
        this.modifyStream(
            tenantDesc.getName(),
            MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL,
            newMetricsStream,
            RequestPhase.FINAL,
            AttributeModAllowance.FORCE,
            Set.of());
      }
    }
  }

  /**
   * Generate a operational metrics stream. This stream config stores metrics related to
   * ingest/extract/admin operations.
   *
   * <p>This method is stateless. The method generates a stream but does not change its state. Use
   * method {@link #setMetricsStream(TenantDesc)} in order to register a metrics stream to itself.
   */
  public StreamDesc generateOperationalMetricsStream(TenantDesc tenantDesc) {
    if (metricsStreamProvider == null) {
      return null;
    }
    StreamConfig operationalMetricsStreamConfig =
        metricsStreamProvider.getOperationalMetricsStreamConfig();
    operationalMetricsStreamConfig.setVersion(tenantDesc.getVersion());
    StreamDesc streamDesc = new StreamDesc(operationalMetricsStreamConfig);
    return streamDesc;
  }

  StreamDesc generateForMissingOperationalMetricsStream() {
    if (metricsStreamProvider == null) {
      return null;
    }
    StreamConfig operationalMetricsStreamConfig =
        metricsStreamProvider.getOperationalMetricsStreamConfig();
    operationalMetricsStreamConfig.setVersion(System.currentTimeMillis());
    StreamDesc streamDesc = new StreamDesc(operationalMetricsStreamConfig);
    return streamDesc;
  }

  public StreamDesc generateIp2GeoStream(TenantDesc tenantDesc) {
    if (ip2GeoStreamProvider == null) {
      return null;
    }
    StreamConfig ip2GeoStreamConfig = ip2GeoStreamProvider.getStreamConfig();
    ip2GeoStreamConfig.setVersion(tenantDesc.getVersion());
    return new StreamDesc(ip2GeoStreamConfig);
  }

  public StreamDesc generateIpBlacklistStream(TenantDesc tenantDesc) {
    if (ipBlacklistStreamProvider == null) {
      return null;
    }
    StreamConfig ipBlacklistStreamConfig = ipBlacklistStreamProvider.getStreamConfig();
    ipBlacklistStreamConfig.setVersion(tenantDesc.getVersion());
    return new StreamDesc(ipBlacklistStreamConfig);
  }

  public StreamDesc generateClientMetricsStream(TenantDesc tenantDesc) {
    if (clientMetricsStreamProvider == null) {
      return null;
    }
    StreamConfig clientMetricsStreamConfig = clientMetricsStreamProvider.getStreamConfig();
    clientMetricsStreamConfig.setVersion(tenantDesc.getVersion());
    return new StreamDesc(clientMetricsStreamConfig);
  }

  public StreamDesc generateOperationFailureStream(TenantDesc tenantDesc) {
    if (operationFailureStreamProvider == null) {
      return null;
    }
    StreamConfig operationFailureStreamConfig = operationFailureStreamProvider.getStreamConfig();
    operationFailureStreamConfig.setVersion(tenantDesc.getVersion());
    return new StreamDesc(operationFailureStreamConfig);
  }

  /**
   * Method to set a metrics stream to this tenant descriptor.
   *
   * <p>The method does nothing in one of following cases:
   *
   * <dl>
   *   <li>A metrics stream has been set already.
   *   <li>The tenant should not have metrics, e.g., the system tenant.
   * </dl>
   *
   * In such a case, the descriptor has no change in its state.
   */
  public void setMetricsStream(TenantDesc tenantDesc) {
    if (!tenantDesc.getName().equals(BiosConstants.TENANT_SYSTEM)) {
      final StreamDesc operationalMetricsStream = generateOperationalMetricsStream(tenantDesc);
      if (operationalMetricsStream != null
          && tenantDesc.getStream(operationalMetricsStream.getName()) == null) {
        tenantDesc.addStream(operationalMetricsStream);
      }
    }
  }

  public void setIp2GeoStream(TenantDesc tenantDesc) {
    final StreamDesc Ip2GeoStream = generateIp2GeoStream(tenantDesc);
    if (Ip2GeoStream != null && tenantDesc.getStream(Ip2GeoStream.getName()) == null) {
      tenantDesc.addStream(Ip2GeoStream);
    }
  }

  public void setIpBlacklistStream(TenantDesc tenantDesc) {
    final StreamDesc ipBlacklistStream = generateIpBlacklistStream(tenantDesc);
    if (ipBlacklistStream != null && tenantDesc.getStream(ipBlacklistStream.getName()) == null) {
      tenantDesc.addStream(ipBlacklistStream);
    }
  }

  public void setClientMetricsStream(TenantDesc tenantDesc) {
    if (!tenantDesc.getName().equals(BiosConstants.TENANT_SYSTEM)) {
      final StreamDesc ClientMetricsStream = generateClientMetricsStream(tenantDesc);
      if (ClientMetricsStream != null
          && tenantDesc.getStream(ClientMetricsStream.getName()) == null) {
        tenantDesc.addStream(ClientMetricsStream);
      }
    }
  }

  public void setOperationFailureStream(TenantDesc tenantDesc) {
    if (!tenantDesc.getName().equals(BiosConstants.TENANT_SYSTEM)) {
      final StreamDesc operationFailureStream = generateOperationFailureStream(tenantDesc);
      if (operationFailureStream != null
          && tenantDesc.getStream(operationFailureStream.getName()) == null) {
        tenantDesc.addStream(operationFailureStream);
      }
    }
  }

  /**
   * Method to clear internal parameters of stream config.
   *
   * @param streamConfig the stream config
   */
  public void stripStreamConfig(StreamConfig streamConfig) {
    streamConfig.setVersion(null);

    if (streamConfig.getViews() != null) {
      streamConfig.getViews().forEach(view -> view.setSchemaVersion(null));
    }

    if (streamConfig.getPostprocesses() != null) {
      streamConfig
          .getPostprocesses()
          .forEach(
              postProcessDesc -> {
                if (postProcessDesc.getRollups() != null) {
                  postProcessDesc
                      .getRollups()
                      .forEach(
                          rollup -> {
                            rollup.setSchemaName(null);
                            rollup.setSchemaVersion(null);
                          });
                }
              });
    }

    if (streamConfig.getFeatures() != null) {
      streamConfig.getFeatures().forEach(feature -> feature.setBiosVersion(null));
    }

    streamConfig.setAdditionalAttributes(null);

    streamConfig
        .getAttributes()
        .forEach(
            attr -> {
              if (attr.getMissingValuePolicy() == streamConfig.getMissingValuePolicy()) {
                attr.setMissingValuePolicy(null);
              }
            });
  }

  boolean isTenantDescLatest(TenantDesc tenantDesc) throws NoSuchTenantException {
    final TenantDesc latest = tenants.getTenantDesc(tenantDesc.getName(), false);
    return latest == null || latest.getVersion().equals(tenantDesc.getVersion());
  }

  protected void loadSystemConfigs(AdminConfig adminConfig)
      throws TfosException, ApplicationException {
    for (TenantConfig tenantConfig : adminConfig.getTenantConfigList()) {
      final String tenantName = tenantConfig.getName();
      final TenantConfig existingTenant = tenants.getTenantConfigOrNull(tenantName);
      if (existingTenant == null) {
        final Long timestamp = System.currentTimeMillis();
        this.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
        this.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
      } else {
        for (StreamConfig streamConfig : tenantConfig.getStreams()) {
          final String streamName = streamConfig.getName();

          final StreamDesc existingStreamDesc =
              tenants.getStreamConfigOrNull(tenantName, false, streamName, false);
          if (existingStreamDesc == null) {
            streamConfig.setVersion(System.currentTimeMillis());
            this.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
            this.addStream(tenantName, streamConfig, RequestPhase.FINAL);
          } else {
            final StreamConfig existingStreamConfig = existingStreamDesc.asStreamConfig();
            stripStreamConfig(existingStreamConfig);
            stripStreamConfig(streamConfig);
            if (!existingStreamConfig.equals(streamConfig)) {
              logger.warn(
                  "Initial system {} {} found but different from the built in schema. "
                      + "The original schema is preserved, resolve the difference manually;"
                      + " tenant={}, {}={}",
                  streamConfig.getType().name().toLowerCase(),
                  streamConfig.getName(),
                  tenantName,
                  streamConfig.getName());
            }
          }
        }
      }
    }
  }

  protected void buildModChains(final TenantDesc tenantDesc)
      throws ApplicationException, TfosException {
    // NOTE: this method modifies streams of tenantDesc in place.
    for (StreamDesc streamDesc : tenantDesc.getStreams()) {
      final StreamType type = streamDesc.getType();
      if (Set.of(StreamType.INDEX, StreamType.VIEW, StreamType.CONTEXT).contains(type)) {
        continue;
      }
      try {
        buildModChain(tenantDesc, streamDesc);
      } catch (ApplicationException | TfosException e) {
        logger.error(e.getMessage(), e);
        if (BiosModules.isTestMode()) {
          throw e;
        }
      }
    }
  }

  private void buildModChain(final TenantDesc tenantDesc, final StreamDesc latestDesc)
      throws ApplicationException, TfosException {
    StreamDesc current = latestDesc;
    while (current.getPrevName() != null && current.getPrevVersion() != null) {
      final StreamDesc prev =
          tenantDesc.getStream(current.getPrevName(), current.getPrevVersion(), true);
      if (prev == null) {
        throw new ApplicationException(
            "Unresolved stream mod chain: " + current.getName() + "." + current.getVersion());
      }
      current.setPrev(prev);
      current = prev;
    }
    if (latestDesc.getSchemaName() == null) {
      latestDesc.setSchemaName(latestDesc.getName());
      latestDesc.setSchemaVersion(latestDesc.getVersion());
    }
    // trace mod history chain and build stream conversion tables
    updateHistory(tenantDesc, latestDesc, 0);
  }

  @Override
  public List<String> getAllTenants() {
    return tenants.listTenantNames();
  }

  public Set<String> getAllTenantsWithVersion() {
    return tenants.listTenantNamesWithVersion();
  }

  public Set<String> getAllActiveTableNameIdentifiers() {
    return tenants.listAllActiveTableNameIdentifiers();
  }

  @Override
  public TenantDesc getTenant(String tenant) throws NoSuchTenantException {
    return tenants.getTenantDesc(tenant, false).duplicate();
  }

  @Override
  public TenantDesc getTenantOrNull(String tenant) {
    return tenants.getTenantDescOrNull(tenant, false);
  }

  protected void validateStreamConfigForWriteOperation(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException {
    if (streamDesc == null) {
      throw new IllegalArgumentException("streamDesc may not be null");
    }
    if (streamDesc.getName() == null || streamDesc.getName().isBlank()) {
      throw new ConstraintViolationException(
          createExceptionMessage("stream name must be set", tenantDesc).toString());
    }
    if (streamDesc.getVersion() == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("stream version may not be null", tenantDesc, streamDesc)
              .toString());
    }
    if (streamDesc.getVersion() < tenantDesc.getVersion()) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "stream version number may not be less than current tenant version",
                  tenantDesc,
                  streamDesc)
              .toString());
    }
  }

  protected void validateTenantConfigForWriteOperation(TenantDesc tenantDesc)
      throws ConstraintViolationException {
    if (tenantDesc == null) {
      throw new IllegalArgumentException("tenantConfig may not be null");
    }
    if (tenantDesc.getName() == null || tenantDesc.getName().isEmpty()) {
      throw new ConstraintViolationException("tenant name must be set");
    }
    if (tenantDesc.getVersion() == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("tenant version may not be null", tenantDesc).toString());
    }
    for (StreamDesc streamDesc : tenantDesc.getStreams()) {
      validateStreamConfigForWriteOperation(tenantDesc, streamDesc);
    }
  }

  @Override
  public void addTenant(TenantConfig srcTenantConfig, RequestPhase phase, Long timestamp)
      throws TenantAlreadyExistsException,
          ConstraintViolationException,
          ApplicationException,
          InvalidDefaultValueException,
          InvalidEnumException,
          InvalidValueException,
          InvalidAlertException,
          NoSuchTenantException,
          InvalidRequestException {
    if (srcTenantConfig == null || phase == null || timestamp == null) {
      throw new IllegalArgumentException("addTenant parameters may not be null");
    }
    TenantConfig tenantConfigClone = srcTenantConfig.duplicate().setVersion(timestamp);
    tenantConfigClone.getStreams().forEach(streamConf -> streamConf.setVersion(timestamp));
    TenantDesc tenantDesc = new TenantDesc(tenantConfigClone);
    validateTenantConfigForWriteOperation(tenantDesc);

    if (phase == RequestPhase.INITIAL) {
      TenantDesc existing = tenants.getTenantDescOrNull(srcTenantConfig.getName(), false);
      if (existing != null) {
        throw new TenantAlreadyExistsException(srcTenantConfig.getName());
      }
    }

    setMetricsStream(tenantDesc);
    setIp2GeoStream(tenantDesc);
    setIpBlacklistStream(tenantDesc);
    setClientMetricsStream(tenantDesc);
    setOperationFailureStream(tenantDesc);
    final var sorter = new StreamSorter(tenantDesc.getAllStreamVersionsInVersionOrder());
    final List<StreamDesc> allStreams;
    try {
      allStreams = sorter.sortByDependencyOrder();
    } catch (DirectedAcyclicGraphException e) {
      throw new InvalidRequestException("Streams have circular dependency", e);
    }
    for (StreamDesc streamDesc : allStreams) {
      streamDesc.setFormatVersion(formatVersionToUse());
      final List<StreamDesc> subStreams = initStreamConfig(tenantDesc, streamDesc, Set.of());
      for (StreamDesc subStream : subStreams) {
        tenantDesc.addStream(subStream);
      }
    }
    tenantDesc.buildSignalAliases();

    for (AdminChangeListener subscriber : subscribers) {
      subscriber.createTenant(tenantDesc, phase);
    }

    if (phase == RequestPhase.FINAL) {
      logger.debug(
          "Adding new tenant={} version={}({})",
          tenantDesc.getName(),
          tenantDesc.getVersion(),
          StringUtils.tsToIso8601(tenantDesc.getVersion()));
      tenantDesc.setProxyInformationDirty(false);
      tenants.addTenant(tenantDesc);
    } else if (adminStore != null) {
      adminStore.storeTenant(tenantDesc.toTenantStore());
      tenantDesc.setProxyInformationDirty(false);
    }
  }

  @Override
  public void modifyTenant(
      String tenantName, TenantConfig newConfig, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException,
          ApplicationException,
          TenantAlreadyExistsException,
          ConstraintViolationException,
          AdminChangeRequestToSameException,
          InvalidDefaultValueException,
          InvalidEnumException,
          InvalidValueException,
          InvalidAlertException,
          InvalidRequestException {
    if (tenantName == null || newConfig == null || phase == null || timestamp == null) {
      throw new IllegalArgumentException("modifyTenant arguments may not be null");
    }

    boolean isRename = !tenantName.equalsIgnoreCase(newConfig.getName());

    TenantConfig tenantConfigClone = newConfig.duplicate().setVersion(timestamp);
    tenantConfigClone.getStreams().forEach(streamConfig -> streamConfig.setVersion(timestamp));
    TenantDesc tenantDesc = new TenantDesc(tenantConfigClone);
    validateTenantConfigForWriteOperation(tenantDesc);

    setMetricsStream(tenantDesc);
    final TenantDesc existing = tenants.getTenantDescOrNull(tenantName, false);
    if (phase == RequestPhase.INITIAL) {
      if (existing == null) {
        throw new NoSuchTenantException(tenantName);
      }
      // in rename case, destination name should be vacant.
      if (isRename) {
        TenantDesc toCreate = tenants.getTenantDescOrNull(newConfig.getName(), false);
        if (toCreate != null) {
          throw new TenantAlreadyExistsException(newConfig.getName());
        }
      }

      Predicate<StreamConfig> filter =
          stream -> {
            // limit only to user defined streams
            final StreamType type = stream.getType();
            return type == StreamType.SIGNAL || type == StreamType.CONTEXT;
          };
      if (Tenants.equals(existing.toTenantConfig(filter), tenantDesc.toTenantConfig(filter))) {
        // TODO(Naoki): tentative
        throw new AdminChangeRequestToSameException("tenant=" + tenantName);
      }
    }

    tenantDesc.copyDbStoredInformationFromExisting(existing);
    for (StreamDesc streamDesc : tenantDesc.getAllStreams()) {
      streamDesc.setFormatVersion(formatVersionToUse());
      List<StreamDesc> subStreams = initStreamConfig(tenantDesc, streamDesc, Set.of());
      if (streamDesc.getSchemaName() == null) {
        streamDesc.setSchemaName(streamDesc.getName());
        streamDesc.setSchemaVersion(streamDesc.getVersion());
      }
      for (StreamDesc subStream : subStreams) {
        tenantDesc.addStream(subStream);
      }
    }
    tenantDesc.buildSignalAliases();

    for (AdminChangeListener subscriber : subscribers) {
      subscriber.createTenant(tenantDesc, phase);
    }

    TenantDesc toDelete = isRename ? new TenantDesc(tenantName, timestamp, true) : null;

    if (phase == RequestPhase.FINAL) {
      logger.debug(
          "Modifying tenant={} version={}({})",
          tenantDesc.getName(),
          tenantDesc.getVersion(),
          StringUtils.tsToIso8601(tenantDesc.getVersion()));
      tenantDesc.setProxyInformationDirty(false);
      tenants.addTenant(tenantDesc);
      if (toDelete != null) {
        tenants.addTenant(toDelete);
      }
    } else if (adminStore != null) {
      adminStore.storeTenant(tenantDesc.toTenantStore());
      tenantDesc.setProxyInformationDirty(false);
      if (toDelete != null) {
        adminStore.storeTenant(toDelete.toTenantStore());
      }
    }
  }

  @Override
  public void removeTenant(TenantConfig tenantConfig, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException, ApplicationException, ConstraintViolationException {

    if (tenantConfig == null || phase == null || timestamp == null) {
      throw new IllegalArgumentException("removeTenant parameters may not be null");
    }

    if (phase == RequestPhase.INITIAL) {
      TenantDesc existing = tenants.getTenantDescOrNull(tenantConfig.getName(), false);
      if (existing == null) {
        throw new NoSuchTenantException(tenantConfig.getName());
      }
      validateTenantConfigForWriteOperation(existing);
    }

    final TenantDesc tenantDesc = new TenantDesc(tenantConfig.getName(), timestamp, true);
    for (AdminChangeListener subscriber : subscribers) {
      subscriber.deleteTenant(tenantDesc, phase);
    }

    if (phase == RequestPhase.FINAL) {
      tenants.addTenant(tenantDesc);
    } else if (adminStore != null) {
      adminStore.storeTenant(tenantDesc.toTenantStore());
    }
  }

  @Override
  public StreamDesc getStream(String tenant, String stream)
      throws NoSuchStreamException, NoSuchTenantException {
    Objects.requireNonNull(tenant);
    Objects.requireNonNull(stream);

    final TenantDesc tenantDesc = tenants.getTenantDesc(tenant, false);

    StreamDesc out = tenantDesc.getStream(stream, false);
    if (out != null) {
      logger.trace(
          "retrieved stream; tenant={} stream={}({}) version={}({})",
          tenant,
          stream,
          out.getName(),
          out.getVersion(),
          StringUtils.tsToIso8601(out.getVersion()));
      return out.duplicate();
    }
    throw new NoSuchStreamException(tenant, stream);
  }

  @Override
  public StreamDesc getStream(String tenant, String stream, Long version)
      throws NoSuchStreamException, NoSuchTenantException {
    Objects.requireNonNull(tenant);
    Objects.requireNonNull(stream);
    Objects.requireNonNull(version);
    final TenantDesc tenantDesc = tenants.getTenantDesc(tenant, false);
    final var streamDesc = tenantDesc.getStream(stream, version, false);
    if (streamDesc == null) {
      throw new NoSuchStreamException(tenant, stream);
    }
    return streamDesc.duplicate();
  }

  @Override
  public StreamDesc getStreamOrNull(String tenant, String stream, Long version) {
    Objects.requireNonNull(tenant);
    Objects.requireNonNull(stream);
    Objects.requireNonNull(version);
    final TenantDesc tenantDesc = tenants.getTenantDescOrNull(tenant, false);
    if (tenantDesc == null) {
      return null;
    }
    return tenantDesc.getStream(stream, version, false);
  }

  @Override
  public StreamDesc getStreamOrNull(String tenant, String stream) {
    Objects.requireNonNull(tenant);
    Objects.requireNonNull(stream);
    final TenantDesc tenantDesc = tenants.getTenantDescOrNull(tenant, false);
    if (tenantDesc == null) {
      return null;
    }
    return tenantDesc.getStream(stream, false);
  }

  @Override
  public List<String> listStreams(String tenant, StreamType streamType)
      throws NoSuchTenantException {
    TenantDesc tenantDesc = tenants.getTenantDesc(tenant, false);
    List<StreamDesc> streams = tenantDesc.getAllStreams();
    List<String> streamNames = new ArrayList<>();
    for (StreamDesc streamDesc : streams) {
      if (streamType != null) {
        if (streamDesc.getType().equals(streamType)) {
          streamNames.add(streamDesc.getName());
        }
      } else {
        if (streamDesc.isAccessibleType()) {
          streamNames.add(streamDesc.getName());
        }
      }
    }
    return streamNames;
  }

  @Override
  public void addStream(
      String tenant, StreamConfig streamConfig, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException,
          StreamAlreadyExistsException,
          ApplicationException,
          ConstraintViolationException,
          InvalidDefaultValueException,
          InvalidEnumException,
          InvalidValueException,
          InvalidAlertException,
          InvalidRequestException {
    synchronized (schemaModificationLock) {
      if (tenant == null || streamConfig == null || phase == null || timestamp == null) {
        throw new IllegalArgumentException("addStreamConfig arguments may not be null");
      }

      final TenantDesc tenantClone = tenants.getTenantDesc(tenant, false).duplicate();
      if (timestamp < tenantClone.getVersion()) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Stream version must be newer than tenant version", tenantClone, streamConfig)
                .append(", tenantVersion=")
                .append(tenantClone.getVersion())
                .append(", streamVersion=")
                .append(timestamp)
                .toString());
      }

      final StreamDesc newStream = new StreamDesc(streamConfig).setVersion(timestamp);
      newStream.setFormatVersion(formatVersionToUse());
      newStream.setIsLatestVersion(true);
      if (phase == RequestPhase.INITIAL) {
        StreamDesc existing = tenantClone.getStream(streamConfig.getName(), false);
        if (existing != null) {
          throw new StreamAlreadyExistsException(tenant, streamConfig.getName());
        }
      }
      final List<StreamDesc> subStreams = initStreamConfig(tenantClone, newStream, Set.of());

      for (AdminChangeListener subscriber : subscribers) {
        subscriber.createStream(tenant, newStream, phase);
        for (StreamDesc subStream : subStreams) {
          subscriber.createStream(tenant, subStream, phase);
        }
      }

      tenantClone.addStream(newStream);
      subStreams.forEach(tenantClone::addStream);
      // signal is now added to tenant, build the "signal alias" to "real signal name" map.
      // Doing this only during final phase, to avoid inconsistency across nodes.
      tenantClone.buildSignalAliases();

      if (phase == RequestPhase.FINAL) {
        logger.debug(
            "Adding stream; tenant={} stream={} version={}({})",
            tenant,
            newStream.getName(),
            newStream.getVersion(),
            StringUtils.tsToIso8601(newStream.getVersion()));
        tenantClone.setProxyInformationDirty(false);
        tenants.addTenant(tenantClone);
      } else if (adminStore != null) {
        assert (tenantClone.isProxyInformationDirty());
        adminStore.storeTenantOnly(tenantClone.toTenantStore());
        adminStore.storeStream(tenantClone.getName(), newStream);
        tenantClone.setProxyInformationDirty(false);
        for (StreamStoreDesc rollupConfig : subStreams) {
          adminStore.storeStream(tenantClone.getName(), rollupConfig);
        }
      }
    }
  }

  @Override
  public void modifyStream(
      String tenant,
      String stream,
      StreamConfig streamConfig,
      RequestPhase phase,
      AttributeModAllowance attributeModAllowance,
      Set<AdminOption> options)
      throws TfosException, ApplicationException {
    options = new HashSet<>(options);
    options.add(AdminOption.IS_MODIFICATION);
    synchronized (schemaModificationLock) {
      // Basic validation.
      if (tenant == null || stream == null || streamConfig == null || phase == null) {
        throw new IllegalArgumentException("modifyStream arguments may not be null");
      }
      if (streamConfig.getVersion() == null) {
        throw new IllegalArgumentException("version must be set to streamConfig");
      }
      final Long timestamp = streamConfig.getVersion();

      final TenantDesc tenantClone = tenants.getTenantDesc(tenant, false).duplicate();
      if (timestamp < tenantClone.getVersion()) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Stream version must be newer than tenant version", tenantClone, streamConfig)
                .append(", tenantVersion=")
                .append(tenantClone.getVersion())
                .append(", streamVersion=")
                .append(timestamp)
                .toString());
      }
      final StreamDesc existing = tenantClone.getStream(stream, false);
      if (existing == null) {
        throw new NoSuchStreamException(tenant, stream);
      }
      if (existing.getVersion() >= streamConfig.getVersion()) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "New stream must have newer version than existing", tenantClone, streamConfig)
                .append(", existing=")
                .append(existing.getVersion())
                .append(", new=")
                .append(streamConfig.getVersion())
                .toString());
      }
      if (existing.getType() != streamConfig.getType()) {
        throw new ConstraintViolationException(
            createExceptionMessage("Stream type may not be modified", tenantClone, streamConfig)
                .append(", existing=")
                .append(existing.getType().toValue())
                .append(", new=")
                .append(streamConfig.getType().toValue())
                .toString());
      }
      final boolean isRename = !stream.equalsIgnoreCase(streamConfig.getName());
      if (isRename) {
        throw new NotImplementedException("Renaming stream is unsupported yet");
      }
      if (phase == RequestPhase.INITIAL) {
        if (StreamComparators.equals(streamConfig, existing)) {
          throw new AdminChangeRequestToSameException(
              String.format("tenant=%s stream=%s", tenant, stream));
        }
      }

      // Create the internal StreamDesc data structure from a copy of the StreamConfig passed
      // in by the client, while keeping other DB-stored information from the existing stream.
      final StreamDesc newStreamDesc =
          new StreamDesc(streamConfig.duplicate().setVersion(timestamp));
      newStreamDesc.setFormatVersion(formatVersionToUse());
      newStreamDesc.copyDbStoredInformationFromExisting(existing);
      newStreamDesc.setIsLatestVersion(true);
      // Remove existing stream and replace it with the modified config for the stream.
      tenantClone.addStream(existing.duplicate().setVersion(timestamp).setDeleted(true));
      tenantClone.addStream(newStreamDesc);

      existing.setIsLatestVersion(false);

      // Make a copy of the list of substreams that will get deleted (the substreams that belong
      // to the stream being modified). This list will be used to inform subscribers later in this
      // method.
      Map<String, StreamDesc> subStreamsToDelete = new HashMap<>();
      for (String name : existing.getSubStreamNames()) {
        final StreamDesc subStream = tenantClone.getStream(name, existing.getVersion(), false);
        if (subStream == null) {
          throw new ApplicationException("substream not found: " + name);
        }
        // Add to list, which later used for notifying to the subscribers
        subStreamsToDelete.put(name.toLowerCase(), subStream.duplicate().setDeleted(true));
      }

      // We are going to re-initialize all the streams in this tenant clone in order to do
      // validation. We only want to call initStream on real (top-level) streams, not substreams.
      // So remove all the substreams from the tenant clone; they will get recreated as part of
      // initialization.
      // TODO(ramesh) : since we have a dependency graph now we can possibly optimize this
      // modifyStream algorithm.

      // #1 Remove all substreams.
      for (StreamDesc streamDesc : tenantClone.getAllStreams()) {
        if (!streamDesc.isAccessibleType()) {
          final StreamDesc subStream = streamDesc.duplicate().setDeleted(true);
          tenantClone.addStream(subStream);
        }
      }

      // #2 Build the list of streams to initialize.
      // Get a list of streams in the correct dependency order.
      final var orderedStreams =
          new ArrayList<>(tenantClone.getAllStreamsInDependencyOrder(newStreamDesc, existing));
      // Clear dependencies graph in the clone as the dependencies will be rebuilt all over again
      // as part of initializing the streams. This will ensure any stale dependencies of the
      // modified stream do not get carried forward.
      tenantClone.clearDependencies();

      // #3 Run static validation by initializing all the streams in the tenant, now with the new
      // version of the stream being modified. Substreams get created as part of stream
      // initialization.
      List<StreamDesc> newSubStreams = new ArrayList<>();
      for (StreamDesc streamDesc : orderedStreams) {
        List<StreamDesc> tempSubStreams = initStreamConfig(tenantClone, streamDesc, options);
        boolean isModifyingStream = streamDesc.getName().equalsIgnoreCase(newStreamDesc.getName());
        for (StreamDesc subStream : tempSubStreams) {
          if (tenantClone.getStream(subStream.getName(), false) != null) {
            throw new ConstraintViolationException(
                createExceptionMessage(
                        "Duplicate stream names are not allowed", tenantClone, subStream)
                    .toString());
          }
          if (isModifyingStream) {
            final var oldSubStream = subStreamsToDelete.get(subStream.getName().toLowerCase());
            if (oldSubStream != null && !oldSubStream.getName().equals(subStream.getName())) {
              throw new ConstraintViolationException(
                  createExceptionMessage(
                          "Changing the cases of a substream name is not supported",
                          tenantClone,
                          subStream)
                      .append(", oldSubStream=")
                      .append(oldSubStream.getName())
                      .toString());
            }
          }
          tenantClone.addStream(subStream);
        }
        if (streamDesc.getName().equals(newStreamDesc.getName())) {
          newSubStreams.addAll(tempSubStreams);
        }
      }

      tenantClone.buildSignalAliases();

      final Long oldVersion = existing.getVersion();
      final StreamDesc oldDesc = tenantClone.getStream(existing.getName(), oldVersion, true);
      buildChangeHistory(tenantClone, oldDesc, newStreamDesc, attributeModAllowance);
      updateHistory(tenantClone, newStreamDesc, 1);
      if (newStreamDesc.getType() == StreamType.CONTEXT) {
        newStreamDesc.setSchemaName(existing.getName());
        newStreamDesc.setSchemaVersion(existing.getSchemaVersion());
      }
      for (StreamDesc subStream : newSubStreams) {
        subStream.setParentStream(newStreamDesc);
        buildChangeHistory(
            tenantClone, subStream, subStream.getName(), oldVersion, attributeModAllowance);
      }

      // TODO(Naoki): We're not tracking dependents of dependents, also not checking the
      // dependencies among the dependents.
      final var dependentStreams = tenantClone.getDependentStreams(newStreamDesc);

      if (options.contains(AdminOption.DRY_RUN)) {
        return;
      }

      // Inform subscribers about the changes: old stream and its substreams are deleted, and
      // modified stream and its substreams are created.
      for (AdminChangeListener subscriber : subscribers) {
        for (StreamDesc subStream : subStreamsToDelete.values()) {
          subscriber.deleteStream(tenant, subStream.duplicate().setDeleted(true), phase);
        }
        subscriber.deleteStream(tenant, oldDesc, phase);
        subscriber.createStream(tenant, newStreamDesc, phase);
        for (StreamDesc subStream : newSubStreams) {
          subscriber.createStream(tenant, subStream, phase);
        }
        for (var dependentStream : dependentStreams) {
          subscriber.createStream(tenant, dependentStream, phase);
        }
      }

      if (phase == RequestPhase.FINAL) {
        tenantClone.setProxyInformationDirty(false);
        tenants.addTenant(tenantClone);
      } else if (adminStore != null) {
        adminStore.storeStream(tenantClone.getName(), newStreamDesc);
        tenantClone.setProxyInformationDirty(false);
      }
    }
  }

  private void validateAlertConditions(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws InvalidRuleException {
    final String tenantName = tenantDesc.getName();
    final String streamName = streamDesc.getName();
    logger.trace("Validating alert conditions; tenant={}, stream={}", tenantName, streamName);
    ExpressionParser expressionParser = new ExpressionParser();
    List<PostprocessDesc> postprocessDescList = streamDesc.getPostprocesses();
    Set<String> uniqueAlertNames = new HashSet<>();
    for (PostprocessDesc postprocessDesc : postprocessDescList) {
      final List<Rollup> rollupList = postprocessDesc.getRollups();
      if (rollupList != null) {
        Set<String> allowedAttributes =
            AlertValidator.getAllowedAttributesInAlertCondition(postprocessDesc, streamDesc);
        for (Rollup rollup : rollupList) {
          List<AlertConfig> alerts = rollup.getAlerts();
          if (alerts != null) {
            for (AlertConfig alert : alerts) {
              logger.debug(
                  "Validating alert condition; tenant={}, stream={}, alert={}",
                  tenantName,
                  streamName,
                  alert.getCondition());
              AlertValidator.validateUniqueAlertName(uniqueAlertNames, alert.getName());
              String alertCondition = alert.getCondition();
              ExpressionTreeNode treeRoot = expressionParser.processExpression(alertCondition);
              AlertValidator.validateAttributesInExpressionTree(
                  tenantName, streamName, treeRoot, allowedAttributes);
            }
          }
        }
      }
    }
  }

  private void buildChangeHistory(
      TenantDesc tenantDesc,
      StreamDesc newDesc,
      String oldName,
      Long oldVersion,
      AttributeModAllowance attributeModAllowance)
      throws TfosException, ApplicationException {
    final StreamDesc oldDesc = tenantDesc.getStream(oldName, oldVersion, true);
    buildChangeHistory(tenantDesc, oldDesc, newDesc, attributeModAllowance);
  }

  private void buildChangeHistory(
      TenantDesc tenantDesc,
      StreamDesc oldDesc,
      StreamDesc newDesc,
      AttributeModAllowance attributeModAllowance)
      throws TfosException, ApplicationException {
    // we don't track history of metrics or virtual streams yet
    final StreamType type = newDesc.getType();
    if (type == StreamType.METRICS) {
      return;
    }

    final boolean isMainStream = newDesc.isMainType();

    // take diff
    if (oldDesc != null) {
      boolean modified =
          validateStreamMod(tenantDesc, oldDesc, newDesc, isMainStream, attributeModAllowance);
      if (isMainStream) {
        if (modified) {
          newDesc.setPrevName(oldDesc.getName());
          newDesc.setPrevVersion(oldDesc.getVersion());
          newDesc.setPrev(oldDesc);
        } else {
          newDesc.setPrevName(oldDesc.getPrevName());
          newDesc.setPrevVersion(oldDesc.getPrevVersion());
          newDesc.setPrev(oldDesc.getPrev());
        }
      }
    }
  }

  /**
   * This method visits stream change history and updates their stream conversion tables to make
   * them convertible to the latest schema.
   *
   * <p>This method assumes that the first generation of the history has proper conversion table
   * already.
   *
   * @param tenantDesc Tenant descriptor that owns the streams.
   * @param latestDesc The target stream descriptor.
   * @param startPoint History depth where update shall start. The first history starts with 0.
   */
  private void updateHistory(TenantDesc tenantDesc, StreamDesc latestDesc, int startPoint) {
    // Only main streams support retrieval across versions
    if (!latestDesc.isMainType()) {
      return;
    }
    // we need to remember the latest default value for the latest type in case any old version
    // does not have convertible attribute
    int count = 0;
    final Map<String, Object> latestDefault = new HashMap<>();
    for (StreamDesc current = latestDesc; current != null; current = current.getPrev()) {
      collectLatestDefault(latestDesc, current, latestDefault, false);
      collectLatestDefault(latestDesc, current, latestDefault, true);
      // the latest and the first prev already has proper conversion table
      if (count++ <= startPoint) {
        continue;
      }
      final StreamConversion conv = new StreamConversion();
      rebuildConversions(latestDesc, current, latestDefault, false, conv);
      rebuildConversions(latestDesc, current, latestDefault, true, conv);
      current.setStreamConversion(conv);
      tenantDesc.addStream(current);
    }
  }

  void collectLatestDefault(
      StreamDesc latest,
      StreamDesc current,
      Map<String, Object> latestDefault,
      boolean isAdditional) {
    final List<AttributeDesc> attrs =
        isAdditional ? latest.getAdditionalAttributes() : latest.getAttributes();
    if (attrs == null) {
      return;
    }
    for (AttributeDesc attr : attrs) {
      if (latestDefault.containsKey(attr.getName())) {
        continue;
      }
      final AttributeDesc found =
          isAdditional
              ? current.findAdditionalAttribute(attr.getName())
              : current.findAttribute(attr.getName());
      if (found != null
          && found.getAttributeType() == attr.getAttributeType()
          && found.getInternalDefaultValue() != null) {
        latestDefault.put(attr.getName(), found.getInternalDefaultValue());
      }
    }
  }

  void rebuildConversions(
      StreamDesc target,
      StreamDesc current,
      Map<String, Object> defaultValues,
      boolean isAdditional,
      StreamConversion conv) {
    final List<AttributeDesc> attrs =
        isAdditional ? target.getAdditionalAttributes() : target.getAttributes();
    if (attrs == null) {
      return;
    }
    for (AttributeDesc attr : attrs) {
      final AttributeDesc oldAttr =
          isAdditional
              ? current.findAdditionalAttribute(attr.getName())
              : current.findAttribute(attr.getName());
      if (oldAttr == null) {
        final AttributeDesc newDesc =
            attr.duplicate().setInternalDefaultValue(defaultValues.get(attr.getName()));
        conv.addAttributeAdd(newDesc);
      } else if (oldAttr.getAttributeType() == attr.getAttributeType()) {
        conv.addAttributeNoChange(oldAttr, attr);
      } else if (isConvertible(oldAttr, attr)) {
        conv.addAttributeConvert(oldAttr, attr);
      } else {
        final AttributeDesc newDesc =
            attr.duplicate().setInternalDefaultValue(defaultValues.get(attr.getName()));
        conv.addAttributeAdd(newDesc);
      }
    }
  }

  /**
   * This method validates stream modification.
   *
   * <p>The method does following validations. The specified object oldDesc would be modified to add
   * stream conversion info.
   *
   * <dl>
   *   <li>Verify attribute conversion for no-change, add, and mod.
   *   <li>Verify attribute deletion.
   * </dl>
   *
   * @param tenantDesc Tenant descriptor
   * @param oldDesc Old stream
   * @param newDesc New stream
   * @param isMainStream True if the target is a main stream
   * @param allowance Attribute type modification allowance
   * @return true if the schema has been updated, false otherwise
   * @throws ConstraintViolationException When there is a constraint violation
   * @throws ConstraintWarningException When there is a constraint violation that is overridable by
   *     the force flag
   * @throws ApplicationException For unexpected application error
   * @throws NotImplementedException If the feature is not implemented
   */
  protected boolean validateStreamMod(
      TenantDesc tenantDesc,
      StreamDesc oldDesc,
      StreamDesc newDesc,
      boolean isMainStream,
      AttributeModAllowance allowance)
      throws TfosException, ApplicationException {
    // check special case: primary key may not be changed:
    if (oldDesc.getType() == StreamType.CONTEXT) {
      final List<AttributeDesc> oldKeyAttr =
          oldDesc.getPrimaryKey().stream()
              .map((name) -> oldDesc.findAttribute(name))
              .collect(Collectors.toList());
      final List<AttributeDesc> newKeyAttr =
          newDesc.getPrimaryKey().stream()
              .map((name) -> newDesc.findAttribute(name))
              .collect(Collectors.toList());
      if (diff(oldKeyAttr, newKeyAttr)) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Primary key attributes may not be modified", tenantDesc, oldDesc)
                .toString());
      }
    } else if (newDesc.getType() == StreamType.INDEX) {
      // An index stream will rely on the counterpart view stream.
      // Attributes of an index stream is a subset of view stream attributes,
      // so index has less chance of schema change. But we record only view versions
      // in AdminInternal DB in current architecture. So we use the counterpart view version
      // for the index schema version.
      return false;
    }

    final StreamConversion conv = new StreamConversion();
    buildAttributeConversion(tenantDesc, oldDesc, newDesc, true, conv, isMainStream, allowance);
    if (oldDesc.getType() != StreamType.CONTEXT) {
      // additionalAttributes for contexts are not stored, so they do not need conversion.
      buildAttributeConversion(tenantDesc, oldDesc, newDesc, false, conv, isMainStream, allowance);
    }
    boolean someDeleted = checkAttributeDeletions(oldDesc, conv);
    boolean indexTurnedOn = false;
    boolean timeIndexIntervalChanged = false;
    if (newDesc.getType() == StreamType.VIEW) {
      final boolean indexEnabled =
          newDesc.getViews().get(0).getIndexTableEnabled() == Boolean.TRUE
              || newDesc.getViews().get(0).getWriteTimeIndexing() == Boolean.TRUE;
      if (indexEnabled) {
        final boolean oldIndexEnabled =
            oldDesc.getViews().get(0).getIndexTableEnabled() == Boolean.TRUE
                || oldDesc.getViews().get(0).getWriteTimeIndexing() == Boolean.TRUE;
        indexTurnedOn = !oldIndexEnabled;
      }
      timeIndexIntervalChanged =
          !newDesc.getIndexWindowLength().equals(oldDesc.getIndexWindowLength());
    }
    boolean hasChange =
        conv.hasChange() || someDeleted || indexTurnedOn || timeIndexIntervalChanged;
    if (hasChange) {
      newDesc.setSchemaName(newDesc.getName());
      newDesc.setSchemaVersion(newDesc.getVersion());
    } else {
      // the new stream would keep using the same table if there is no change
      newDesc.setSchemaName(oldDesc.getSchemaName());
      newDesc.setSchemaVersion(oldDesc.getSchemaVersion());
    }
    final StreamDesc parentStream = newDesc.getParentStream();
    if (newDesc.isMainType()) {
      oldDesc.setStreamConversion(conv);
    } else if (newDesc.getType() == StreamType.ROLLUP) {
      // Rollup schema name and version is recorded as a part of parent signal config.
      // So we put the rollup schema version to parent here.
      final Rollup rollup;
      if (parentStream == null || (rollup = parentStream.findRollup(newDesc.getName())) == null) {
        throw new ApplicationException("something went wrong");
      }
      rollup.setSchemaName(newDesc.getSchemaName());
      rollup.setSchemaVersion(newDesc.getSchemaVersion());
    } else if (newDesc.getType() == StreamType.VIEW) {
      final ViewDesc view = newDesc.getViews().get(0);
      view.setSchemaVersion(newDesc.getSchemaVersion());
      // put the schema version to the counterpart index stream
      final StreamDesc indexStream =
          tenantDesc.getStream(parentStream.getName() + ".index." + view.getName());
      if (indexStream == null) {
        throw new ApplicationException("this shouldn't happen by design");
      }
      indexStream.setSchemaVersion(newDesc.getSchemaVersion());
    }
    return hasChange;
  }

  private boolean diff(List<AttributeDesc> oldAttributes, List<AttributeDesc> newAttributes) {
    if (Objects.requireNonNull(oldAttributes).size()
        != Objects.requireNonNull(newAttributes).size()) {
      return true;
    }
    for (int i = 0; i < oldAttributes.size(); ++i) {
      final var oldAttribute = oldAttributes.get(i);
      final var newAttribute = newAttributes.get(i);
      if (!oldAttribute.getName().equalsIgnoreCase(newAttribute.getName())
          || oldAttribute.getAttributeType() != newAttribute.getAttributeType()) {
        return true;
      }
    }
    return false;
  }

  protected void buildAttributeConversion(
      TenantDesc tenantDesc,
      StreamDesc oldDesc,
      StreamDesc newDesc,
      boolean isMain,
      StreamConversion conv,
      boolean isEventStream,
      AttributeModAllowance attributeModAllowance)
      throws ConstraintViolationException {
    final List<AttributeDesc> attrs =
        isMain ? newDesc.getAttributes() : newDesc.getAdditionalAttributes();
    // do nothing if the attribute list is not available
    if (attrs == null) {
      return;
    }

    for (AttributeDesc newAttr : attrs) {
      final AttributeDesc oldAttr =
          isMain
              ? oldDesc.findAttribute(newAttr.getName())
              : oldDesc.findAdditionalAttribute(newAttr.getName());
      if (isEventStream
          && newAttr.getInternalDefaultValue() == null
          && checkIfDefaultValueIsNecessary(newAttr, oldDesc, isMain)) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Default value must be configured for an adding attribute",
                    tenantDesc,
                    newDesc,
                    newAttr)
                .toString());
      }
      if (oldAttr == null) {
        conv.addAttributeAdd(newAttr);
      } else {
        final InternalAttributeType oldType = oldAttr.getAttributeType();
        final InternalAttributeType newType = newAttr.getAttributeType();
        if (oldType == newType) {
          if (newAttr.getAttributeType() == ENUM) {
            for (int i = 0; i < oldAttr.getEnum().size(); ++i) {
              final String oldEntry = oldAttr.getEnum().get(i);
              final String newEntry =
                  i < newAttr.getEnum().size() ? newAttr.getEnum().get(i) : null;
              if (!oldEntry.equals(newEntry)) {
                throw new ConstraintViolationException(
                    createExceptionMessage(
                            "Enum entry may not be modified", tenantDesc, newDesc, newAttr)
                        .append(", index=")
                        .append(i)
                        .append(", oldEntry=")
                        .append(oldEntry)
                        .append(", newEntry=")
                        .append(newEntry)
                        .toString());
              }
            }
          }
          conv.addAttributeNoChange(oldAttr, newAttr);
        } else if (attributeModAllowance == AttributeModAllowance.PROHIBITED) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "Attribute type may not be changed", tenantDesc, newDesc, newAttr)
                  .append(", fromType=")
                  .append(oldAttr.getAttributeType())
                  .append(", toType=")
                  .append(newAttr.getAttributeType())
                  .toString());
        } else if (isConvertible(oldAttr, newAttr)) {
          conv.addAttributeConvert(oldAttr, newAttr);
        } else if (isEventStream) {
          if (attributeModAllowance != AttributeModAllowance.FORCE) {
            throw new ConstraintViolationException(
                createExceptionMessage(
                        "Unsupported attribute type transition", tenantDesc, newDesc, newAttr)
                    .append(", fromType=")
                    .append(oldAttr.getAttributeType())
                    .append(", toType=")
                    .append(newAttr.getAttributeType())
                    .toString());
          } else if (newAttr.getInternalDefaultValue() == null) {
            throw new ConstraintViolationException(
                createExceptionMessage(
                        "Default value must be configured for non-convertible attribute modification",
                        tenantDesc,
                        newDesc,
                        newAttr)
                    .toString());
          } else {
            conv.addAttributeAdd(newAttr);
          }
        }
      }
    }
  }

  private boolean checkIfDefaultValueIsNecessary(
      AttributeDesc newAttr, StreamDesc oldDesc, boolean isMain) {
    var prevDesc = oldDesc;
    while (prevDesc != null) {
      final AttributeDesc prevAttr =
          isMain
              ? prevDesc.findAttribute(newAttr.getName())
              : prevDesc.findAdditionalAttribute(newAttr.getName());
      if (prevAttr == null) {
        return true;
      }
      prevDesc = prevDesc.getPrev();
    }
    return false;
  }

  protected boolean isConvertible(AttributeDesc from, AttributeDesc to) {
    final InternalAttributeType fromType = from.getAttributeType();
    final InternalAttributeType toType = to.getAttributeType();
    if (!toType.isConvertibleFrom(fromType)) {
      return false;
    } else if (toType == ENUM) {
      for (String entry : from.getEnum()) {
        if (!to.getEnum().contains(entry)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean checkAttributeDeletions(StreamDesc oldDesc, StreamConversion conv) {
    List<String> deleted = new ArrayList<>();
    for (AttributeDesc desc : oldDesc.getAttributes()) {
      if (conv.getAttributeConversion(desc.getName()) == null) {
        deleted.add(desc.getName());
      }
    }
    if (oldDesc.getAdditionalAttributes() != null) {
      for (AttributeDesc desc : oldDesc.getAdditionalAttributes()) {
        if (conv.getAttributeConversion(desc.getName()) == null) {
          deleted.add(desc.getName());
        }
      }
    }
    return !deleted.isEmpty();
  }

  @Override
  public void removeStream(String tenant, String stream, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ConstraintViolationException,
          ApplicationException {
    synchronized (schemaModificationLock) {
      // Basic validation.
      if (tenant == null || stream == null) {
        throw new IllegalArgumentException("tenant and streamConfig must be non-null");
      }

      final TenantDesc tenantClone = tenants.getTenantDesc(tenant, false).duplicate();
      boolean ignoreDeletedFlag = phase == RequestPhase.FINAL;
      StreamDesc existing = tenantClone.getStream(stream, ignoreDeletedFlag);
      if (existing == null) {
        throw new NoSuchStreamException(tenant, stream);
      }
      // More validation including constraint checks.
      if (phase == RequestPhase.INITIAL) {
        if (timestamp < existing.getVersion()) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "Stream version for deletion must be newer than existing version",
                      tenantClone,
                      existing)
                  .append(", existingVersion=")
                  .append(existing.getVersion())
                  .append(", deleteVersion=")
                  .append(timestamp)
                  .toString());
        }
        checkRemoveStreamConstraint(tenantClone, existing);
      }

      final var streamsToDelete = new ArrayList<StreamDesc>();
      streamsToDelete.add(existing);

      // Remove the audit signal if exists
      if (existing.getType() == StreamType.CONTEXT) {
        final var auditSignalName = CONTEXT_AUDIT_SIGNAL_PREFIX + existing.getName();
        final var auditSignal = tenantClone.getStream(auditSignalName);
        if (auditSignal != null) {
          streamsToDelete.add(auditSignal);
          for (var subStreamName : auditSignal.getSubStreamNames()) {
            final var subStream =
                tenantClone.getStream(subStreamName, auditSignal.getVersion(), true);
            if (subStream != null) {
              streamsToDelete.add(subStream);
            }
          }
        }
      }

      // Handle sub-streams
      for (String subStreamName : existing.getSubStreamNames()) {
        final StreamDesc subStream =
            tenantClone.getStream(subStreamName, existing.getVersion(), true);
        if (subStream == null) {
          logger.warn(
              "Substream {} is not registered to the tenant. This should not happen",
              subStreamName);
          continue;
        }
        streamsToDelete.add(subStream);
      }

      // Stop tracking dependencies that this stream has on other streams.
      for (var toDelete : streamsToDelete) {
        final boolean isPrimary = toDelete.getType().isPrimary();
        if (isPrimary) {
          try {
            tenantClone.stopTrackingDependenciesForStream(toDelete);
          } catch (DirectedAcyclicGraphException e) {
            String bug =
                "Bug: verified that stream to delete has no children earlier but it failed now"
                    + ", streamToDelete ="
                    + existing.getName()
                    + ". Inner message: "
                    + e.getMessage();
            throw new ApplicationException(bug);
          }
        }

        // Handle the stream being deleted.
        for (AdminChangeListener subscriber : subscribers) {
          subscriber.deleteStream(tenant, toDelete, phase);
        }
        final StreamDesc tombstone = toDelete.duplicate().setVersion(timestamp).setDeleted(true);
        tenantClone.addStream(tombstone);

        if (phase == RequestPhase.INITIAL && isPrimary && adminStore != null) {
          adminStore.storeStream(tenant, tombstone);
        }
      }

      // Update the cache
      if (phase == RequestPhase.FINAL) {
        tenants.addTenant(tenantClone);
      }
    }
  }

  /** Check constraint in deleting a stream config. */
  private void checkRemoveStreamConstraint(TenantDesc tenantDesc, StreamDesc toDelete)
      throws ConstraintViolationException {
    // Check to see if any other streams depend on this stream.
    final var dependents = tenantDesc.getDependentStreams(toDelete);
    if (!dependents.isEmpty()) {
      final var streamType = toDelete.getType() == StreamType.SIGNAL ? "Signal" : "Context";
      throw new ConstraintViolationException(
          createExceptionMessage(
                  String.format(
                      "%s cannot be deleted because other streams depend on it", streamType),
                  tenantDesc,
                  toDelete)
              .append(", referredBy=")
              .append(
                  dependents.stream()
                      .map(
                          (stream) ->
                              String.format("%s (%d)", stream.getName(), stream.getVersion()))
                      .collect(Collectors.toList()))
              .toString());
    }
  }

  @Override
  public List<ProcessStage> getPreProcesses(String tenant, String stream)
      throws NoSuchTenantException, NoSuchStreamException {
    StreamDesc streamDesc = getStream(tenant, stream);
    return streamDesc.getPreprocessStages();
  }

  /**
   * This method is called in the initial phase by the data engine maintenance job that infers
   * attribute tags (through the bios AdminImpl). In the initial phase inferred tags need to be
   * stored in the stream config in the DB. BiosServer AdminInternal then does fan routing to all
   * servers with the final phase. In the final phase each server updates their in-memory stream
   * with the inferred attribute tags.
   */
  @Override
  public void updateInferredTags(
      final String tenant,
      final String stream,
      final RequestPhase phase,
      final Map<Short, AttributeTags> attributes)
      throws ApplicationException {
    if (tenant == null || stream == null || attributes == null) {
      throw new ApplicationException("updateInferredTags arguments may not be null");
    }
    // Do this while holding a lock to ensure there is no other operation in progress that might
    // have made a different copy of the tenantDesc/streamDesc.
    synchronized (schemaModificationLock) {
      logger.debug(
          "updateInferredTags tenant={}, stream={}, phase={}, numAttributes={}",
          tenant,
          stream,
          phase,
          attributes.size());
      logger.debug("updateInferredTags attributes={}", attributes);
      final TenantDesc tenantDesc;
      try {
        tenantDesc = tenants.getTenantDesc(tenant, false);
      } catch (NoSuchTenantException e) {
        logger.warn("Got updateInferredTags for deleted tenant={}", tenant);
        return;
      }
      final StreamDesc streamToUpdate;
      streamToUpdate = tenantDesc.getStream(stream, false);
      if (streamToUpdate == null) {
        // This stream may be in the process of being created or deleted.
        return;
      }
      for (final var entry : attributes.entrySet()) {
        final var attributeName = streamToUpdate.getAttributeNameForProxy(entry.getKey());
        final var attribute = streamToUpdate.findAnyAttribute(attributeName);
        if (attribute != null) {
          attribute.setInferredTags(entry.getValue());
        } else {
          logger.warn(
              "stream={} attributeProxy={} not found in version={}",
              stream,
              entry.getKey(),
              streamToUpdate.getVersion());
        }
        // If this is an additional attribute, we also need to store the inferred tags separately
        // because additional attributes don't get written to the DB.
        final var additionalAttribute = streamToUpdate.findAdditionalAttribute(attributeName);
        if (additionalAttribute != null) {
          streamToUpdate
              .getInferredTagsForAdditionalAttributes()
              .put(attributeName, entry.getValue());
        }
      }

      if (phase == RequestPhase.INITIAL) {
        // In initial phase we write the inferred tags to the database.
        adminStore.storeStream(tenant, streamToUpdate);
      }
    }
  }

  /**
   * This method validates and initializes streamConfig.
   *
   * <p>The method runs following constraint checks:
   *
   * <dl>
   *   <li>missingValuePolicy must be set
   *   <li>Every property with missingValuePolicy=USE_DEFAULT must have defaultValue property
   *   <li>Stream type must be SIGNAL when the config has pre-process properties
   *   <li>Pre-process names must exist and should not conflict
   *   <li>Attribute names to join should not conflict
   *   <li>Condition attribute must exist in the signal config
   *   <li>Missing Lookup Policy for pre-process must exist
   *   <li>Merge action must contain attribute name
   *   <li>Merge action must specify a context
   *   <li>Merge action must specify an attribute of a context
   * </dl>
   *
   * <p>During the constraint check, the method also compiles configuration to preprocess function
   * objects.
   *
   * @param tenantClone Tenant configuration before the modification.
   * @param streamDesc Stream configuration to initialize. This object may be overwritten.
   * @param options Execution options
   * @return List of substreams of this stream, including rollups and virtual contexts.
   */
  protected List<StreamDesc> initStreamConfig(
      TenantDesc tenantClone, StreamDesc streamDesc, Set<AdminOption> options)
      throws ConstraintViolationException,
          InvalidDefaultValueException,
          InvalidEnumException,
          InvalidValueException,
          InvalidAlertException,
          ApplicationException,
          NoSuchTenantException {

    final boolean isModification = options.contains(AdminOption.IS_MODIFICATION);
    final boolean skipAuditValidation = options.contains(AdminOption.SKIP_AUDIT_VALIDATION);
    final boolean isDryRun = options.contains(AdminOption.DRY_RUN);

    if (inInitialLoading) {
      logger.info(
          "Loading stream; tenant={}, stream={}, version={}, proxy={},"
              + " maxAttributeProxy={}, isDeleted={}, isLatest={}",
          tenantClone.getName(),
          streamDesc.getName(),
          streamDesc.getVersion(),
          streamDesc.getStreamNameProxy(),
          streamDesc.getMaxAttributeProxy(),
          streamDesc.isDeleted(),
          streamDesc.getIsLatestVersion());
    }

    streamDesc.setParent(tenantClone);

    // Verify and initialize attributes
    initAttributes(tenantClone, streamDesc);

    // Context attributes have special restrictions
    validateContextAttributes(tenantClone, streamDesc);

    // Audit-enabled context  requires the signal to be present
    if (!skipAuditValidation && streamDesc.isActive()) {
      try {
        validateAuditSignal(tenantClone, streamDesc);
      } catch (ConstraintViolationException e) {
        if (inInitialLoading) {
          // This exception would happen on startup when a context and its audit signal become
          // inconsistent. It may occur, for example, when updating context with audit breaks
          // in the middle due to any accident. If this happens, the server cannot start service
          // with this context and it remains as a permanent problem, causing service outage.
          // In order to avoid such an issue, we disable audit and continue.
          final var contextName = streamDesc.getName();
          logger.error(
              "Audit signal validation failed during startup; tenant={}, context={}",
              tenantClone.getName(),
              contextName,
              e);
          streamDesc.setAuditEnabled(false);
          logger.error("Disabled auditing of context {}", contextName);
          if (BiosModules.isTestMode()) {
            throw e;
          }
        } else {
          throw e;
        }
      }
    }

    validateExports(tenantClone, streamDesc);

    // add the dependency for the audit signal
    // NOTE: dryRun must be set to skip this step since the streamDesc may become incorrect.
    // We cannot actually use the resulting streamDesc if we skip this step.
    if (streamDesc.isActive() && !(skipAuditValidation && isDryRun)) {
      addAuditDependency(tenantClone, streamDesc);
    }

    initPreprocesses(tenantClone, streamDesc);
    initDynamicEnrichments(tenantClone, streamDesc);
    initContextEnrichments(tenantClone, streamDesc);

    if (!inInitialLoading) {
      // We do not need to allocate proxies during initial loading, assuming all latest versions
      // of streams have proxies allocated. Some old versions of streams (those created before
      // proxies were implemented in Jan 2021) do not have proxies and trying to allocate proxies
      // to those stale versions is a mistake.
      final boolean proxiesChanged = streamDesc.initAttributeProxyInfo();
      if (proxiesChanged) {
        tenantClone.setProxyInformationDirty(true);
        logger.debug(
            "Proxy information changed in initStreamConfig for tenant={}, stream={}",
            tenantClone.getName(),
            streamDesc.getName());
        tenantClone.assignStreamNameProxy(streamDesc);
      }
    } else if (streamDesc.isActive()) {
      logger.info(
          "Loading inferred tags: tenant={}, version={}, proxy={}, maxAttributeProxy={}, "
              + "stream={}",
          tenantClone.getName(),
          streamDesc.getVersion(),
          streamDesc.getStreamNameProxy(),
          streamDesc.getMaxAttributeProxy(),
          streamDesc.getName());
      // Copy inferred tags for additional attributes from what was stored in the DB.
      // This should be done after all additional attributes have been computed.
      for (final var entry : streamDesc.getInferredTagsForAdditionalAttributes().entrySet()) {
        final var additionalAttribute = streamDesc.findAdditionalAttribute(entry.getKey());
        if (additionalAttribute == null) {
          logger.debug(
              "inferredTagsForAdditionalAttributes attribute={} not found", entry.getKey());
        } else {
          additionalAttribute.setInferredTags(entry.getValue());
        }
      }
    }

    List<StreamDesc> subStreams = new ArrayList<>();
    initViews(tenantClone, streamDesc, subStreams);
    initPostProcesses(tenantClone, streamDesc, subStreams);
    initDataSketches(tenantClone, streamDesc);

    initContextFeatures(tenantClone, streamDesc, subStreams, isModification);

    if (streamDesc.getIndexWindowLength() == null) {
      long interval = TfosConfig.timeIndexWidthMillis();
      if (interval == 0) {
        interval = TfosConfig.indexingDefaultIntervalSeconds() * 1000L;
        long targetWidth =
            TfosConfig.partitionSize()
                / (TfosConfig.operationsPerSec() * TfosConfig.averageEventSize())
                * 1000;
        // shrink interval down to targetWidth <= interval < targetWidth * 2
        while (interval >= targetWidth * 2) {
          interval /= 2;
        }
      }
      logger.debug("setting time index window width to " + interval);
      streamDesc.setIndexWindowLength(interval);
    }

    for (StreamDesc subStream : subStreams) {
      subStream.setParentStream(streamDesc);
      String rollupName = null;
      String viewName = null;
      if (subStream.getType() == StreamType.INDEX) {
        rollupName = subStream.getName().replace(".index.", ".rollup.");
      } else if (subStream.getType() == StreamType.VIEW) {
        rollupName = subStream.getName().replace(".view.", ".rollup.");
        final var viewDesc = subStream.getViews().get(0);
        if (viewDesc != null) {
          subStream.setIndexWindowLength(viewDesc.getTimeIndexInterval());
          viewName = viewDesc.getName();
        }
      } else if (subStream.getType() == StreamType.ROLLUP) {
        rollupName = subStream.getName();
      }
      final var timeIndexBaseInterval = new AtomicLong(streamDesc.getIndexWindowLength());
      if (rollupName != null) {
        final String rollupNameCopy = rollupName;
        final var counterpartRollup =
            subStreams.stream()
                .filter((stream) -> stream.getName().equalsIgnoreCase(rollupNameCopy))
                .findFirst();
        counterpartRollup.ifPresent(
            (rollupStream) -> {
              if (subStream.getIndexWindowLength() == null) {
                final var streamName = subStream.getParentStream().getName();
                if (subStream.getParentStream().getParent().getName().equalsIgnoreCase("pharmeasy")
                    && (streamName.equalsIgnoreCase("addToCart")
                        || streamName.equalsIgnoreCase("otcPdpSignal"))) {
                  subStream.setIndexWindowLength(1000L * 60 * 60 * 24 * 365 * 50);
                } else {
                  subStream.setIndexWindowLength(
                      (long) rollupStream.getRollupInterval().getValueInMillis());
                }
              }
              timeIndexBaseInterval.set((long) rollupStream.getRollupInterval().getValueInMillis());
            });
      }
      if (subStream.getIndexWindowLength() == null) {
        subStream.setIndexWindowLength(streamDesc.getIndexWindowLength());
      }
      if (viewName != null && subStream.getIndexWindowLength() % timeIndexBaseInterval.get() != 0) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "timeIndexInterval must be a multiple of the rollup interval",
                    tenantClone,
                    streamDesc)
                .append(", feature=")
                .append(viewName)
                .append(", rollupInterval=")
                .append(timeIndexBaseInterval.get())
                .append(", timeIndexInterval")
                .append(subStream.getIndexWindowLength())
                .toString());
      }
    }

    return subStreams;
  }

  /**
   * Validate and initialize attributes.
   *
   * @param tenantDesc Target tenant descriptor
   * @param streamDesc Descriptor of the initializing stream
   * @throws ConstraintViolationException when constraint violation happens
   * @throws InvalidEnumException when invalid enum configuration is found
   * @throws InvalidDefaultValueException when any specified default value is invalid
   */
  private void initAttributes(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException, InvalidEnumException, InvalidDefaultValueException {

    final MissingAttributePolicyV1 streamPolicy = streamDesc.getMissingValuePolicy();
    if (streamPolicy == null) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Property 'missingValuePolicy' must be set", tenantDesc, streamDesc)
              .toString());
    }
    for (AttributeDesc attr : streamDesc.getAttributes()) {
      verifyAttribute(attr, tenantDesc, streamDesc, streamPolicy);
    }
  }

  private void verifyAttribute(
      AttributeDesc attr,
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      MissingAttributePolicyV1 streamPolicy)
      throws ConstraintViolationException, InvalidEnumException, InvalidDefaultValueException {
    if (attr.getName() == null || attr.getName().isEmpty()) {
      throw new ConstraintViolationException(
          createExceptionMessage("Attribute name may not be null", tenantDesc, streamDesc, attr)
              .toString());
    }
    if (attr.getAttributeType() == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("Attribute type must be set", tenantDesc, streamDesc, attr)
              .toString());
    }
    if (attr.getMissingValuePolicy() == null) {
      attr.setMissingValuePolicy(streamPolicy);
    }
    if (attr.getMissingValuePolicy() == MissingAttributePolicyV1.USE_DEFAULT
        && attr.getDefaultValue() == null) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Property 'defaultValue' must be set when missing value policy is 'use_default'",
                  tenantDesc,
                  streamDesc,
                  attr)
              .toString());
    }
    if (attr.getAttributeType() == ENUM) {
      if (attr.getEnum() == null || attr.getEnum().isEmpty()) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Enum attribute must have entry list", tenantDesc, streamDesc, attr)
                .toString());
      }
      final Set<String> enumNames = new HashSet<>();
      for (int i = 0; i < attr.getEnum().size(); ++i) {
        final String entry = attr.getEnum().get(i);
        if (entry == null || entry.trim().isEmpty()) {
          throw new InvalidEnumException(
              createExceptionMessage(
                      "Enum entry may not be null or empty", tenantDesc, streamDesc, attr)
                  .append(", enum_index=")
                  .append(i)
                  .toString());
        }
        if (enumNames.contains(entry)) {
          throw new InvalidEnumException(
              createExceptionMessage("Enum entries may not duplicate", tenantDesc, streamDesc, attr)
                  .append(", value=")
                  .append(entry)
                  .toString());
        }
        enumNames.add(entry);
      }

      if (attr.getDefaultValue() != null && !attr.getEnum().contains(attr.getDefaultValue())) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Enum default must be one of the entries", tenantDesc, streamDesc, attr)
                .toString());
      }
    }
    if (attr.getDefaultValue() != null && attr.getInternalDefaultValue() == null) {
      try {
        attr.setInternalDefaultValue(Attributes.parseDefaultValue(attr.getDefaultValue(), attr));
      } catch (InvalidValueSyntaxException e) {
        throw new InvalidDefaultValueException(
            createExceptionMessage(e.getMessage(), tenantDesc, streamDesc, attr).toString());
      }
    }
  }

  private void validateViewAsContext(TenantDesc tenantDesc, StreamDesc signalDesc, ViewDesc view)
      throws ConstraintViolationException {
    final var numKeyAttributes = view.getGroupBy().size();
    if (numKeyAttributes != 1) {
      StringBuilder sb =
          createExceptionMessage(
                  "A feature exposed as a context may only have one attribute in 'dimensions' list",
                  tenantDesc,
                  signalDesc)
              .append(", featureName=")
              .append(view.getName());
      throw new ConstraintViolationException(sb.toString());
    }
  }

  /** Check whether primary key is valid. The method does nothing if stream type is not context. */
  private void validateContextAttributes(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException {
    final StreamType streamType = streamDesc.getType();
    if (streamType != StreamType.CONTEXT) {
      return;
    }

    // If primary key is not set, assume the first attribute is the primary key
    if (streamDesc.getPrimaryKey() == null) {
      streamDesc.setPrimaryKey(List.of(streamDesc.getAttributes().get(0).getName()));
    }

    final var primaryKeyAttributeConfigs = new ArrayList<AttributeDesc>();
    final var primaryKeyBiosAttributes = new ArrayList<AttributeConfig>();
    for (var primaryKeyAttributeName : streamDesc.getPrimaryKey()) {
      final AttributeDesc primaryKeyAttribute = streamDesc.findAttribute(primaryKeyAttributeName);
      primaryKeyAttributeConfigs.add(primaryKeyAttribute);
      final InternalAttributeType type = primaryKeyAttribute.getAttributeType();
      final Object defaultValue = primaryKeyAttribute.getInternalDefaultValue();
      // verify the key is not empty if the type is string or blob.
      if (primaryKeyAttribute.getMissingValuePolicy() == MissingAttributePolicyV1.USE_DEFAULT
          && ((type == STRING && ((String) defaultValue).isEmpty())
              || (type == BLOB && ((ByteBuffer) defaultValue).remaining() == 0))) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Default value of primary key may not be empty", tenantDesc, streamDesc)
                .toString());
      }
    }
    streamDesc.setPrimaryKeyAttributes(Collections.unmodifiableList(primaryKeyAttributeConfigs));
  }

  /**
   * Validate and initialize pre-processes.
   *
   * @param tenantDesc The target tenant descriptor
   * @param streamDesc The stream descriptor to add
   * @throws ConstraintViolationException When a constraint violation is found in the configuration
   * @throws InvalidDefaultValueException When a specified default value is invalid
   */
  private void initPreprocesses(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException, InvalidDefaultValueException {
    final var validator = new SignalEnrichmentValidator(this, tenantDesc, streamDesc);
    validator.validate();
  }

  /** Verify and initialize dynamic enrichments. */
  private void initDynamicEnrichments(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException {
    final var ingestTimeLag = streamDesc.getIngestTimeLag();
    if (ingestTimeLag == null || ingestTimeLag.isEmpty()) {
      return;
    }

    if (streamDesc.getType() != StreamType.SIGNAL) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "IngestTimeLag enrichment is supported only by signal stream type ",
                  tenantDesc,
                  streamDesc)
              .toString());
    }

    // We assume fundamental verification is done at this point.
    List<ProcessStage> preprocessStages = streamDesc.getPreprocessStages();
    if (preprocessStages == null) {
      preprocessStages = new ArrayList<>();
      streamDesc.setPreprocessStages(preprocessStages);
    }

    for (int i = 0; i < ingestTimeLag.size(); ++i) {
      var enrichmentEntry = ingestTimeLag.get(i);

      // check constraints in the referring attribute
      final var attributeName = enrichmentEntry.getAttribute();
      AttributeDesc attr = streamDesc.findAttribute(attributeName);
      if (attr == null) {
        attr = streamDesc.findAdditionalAttribute(attributeName);
      }
      assert attr != null;
      if (attr.getAttributeType() != LONG) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Reference attribute type must be Integer for a ingestTimeLag attribute",
                    tenantDesc,
                    streamDesc,
                    attr)
                .append(", enrichment=ingestTimeLag[")
                .append(i)
                .append("]")
                .toString());
      }

      // Create additional attribute
      final var additionalAttribute = new AttributeDesc(enrichmentEntry.getAs(), DOUBLE);
      additionalAttribute.setDefaultValue(enrichmentEntry.getFillInSerialized());
      double fillInValue = 0;
      if (enrichmentEntry.getFillIn() != null) {
        additionalAttribute.setInternalDefaultValue(enrichmentEntry.getFillIn().asObject());
        fillInValue = enrichmentEntry.getFillIn().asDouble();
      }
      final var tagsForAdditionalAttribute = new AttributeTags();
      tagsForAdditionalAttribute.setCategory(AttributeCategory.QUANTITY);
      tagsForAdditionalAttribute.setKind(AttributeKind.TIMESTAMP);
      if (enrichmentEntry.getTags() != null) {
        tagsForAdditionalAttribute.setUnit(enrichmentEntry.getTags().getUnit());
      } else {
        tagsForAdditionalAttribute.setUnit(Unit.MILLISECOND);
      }
      additionalAttribute.setTags(tagsForAdditionalAttribute);

      if (streamDesc.getAdditionalAttributes() == null
          || streamDesc.getAdditionalAttributes().stream()
              .noneMatch(
                  existing -> additionalAttribute.getName().equalsIgnoreCase(existing.getName()))) {
        streamDesc.addAdditionalAttribute(additionalAttribute);
      }

      final var outputUnit = tagsForAdditionalAttribute.getUnit();
      preprocessStages.add(
          new ProcessStage(
              streamDesc.getName() + "." + attr.getName(),
              new TimeLagCalculator(
                  attr.getName(), additionalAttribute.getName(), outputUnit, fillInValue)));
    }
  }

  /**
   * Verify and validate Audit signal.
   *
   * <p>
   */
  private void validateAuditSignal(TenantDesc tenantDesc, StreamDesc contextDesc)
      throws ConstraintViolationException {

    if (contextDesc.getType() != StreamType.CONTEXT) {
      return;
    }

    if (!contextDesc.isActive() || !contextDesc.getAuditEnabled()) {
      return;
    }

    final var contextName = contextDesc.getName();
    String auditSignalName =
        StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, contextName);

    StreamDesc auditSignalDesc = tenantDesc.getStream(auditSignalName);
    if (auditSignalDesc == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("Audit signal not found", tenantDesc, contextDesc)
              .append(", auditSignal=")
              .append(auditSignalName)
              .toString());
    }
    auditSignalDesc.addTag(StreamTag.CONTEXT_AUDIT);
    final List<AttributeDesc> contextAttributes = contextDesc.getAttributes();
    final Map<String, AttributeDesc> auditAttributes =
        auditSignalDesc.getAttributes().stream()
            .collect(Collectors.toMap((attr) -> attr.getName().toLowerCase(), (attr) -> attr));

    int expectedAuditAttributes = contextAttributes.size() * 2 + 1;
    if (auditAttributes.size() != expectedAuditAttributes) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Mismatch in number of attributes between context and its audit signal",
                  tenantDesc,
                  contextDesc)
              .append(", auditSignal=")
              .append(auditSignalName)
              .append(", numContextAttributes=")
              .append(contextAttributes.size())
              .append(", expectedAuditAttributes=")
              .append(expectedAuditAttributes)
              .append(", actual=")
              .append(auditAttributes.size())
              .toString());
    }

    final var opAttrDesc = auditAttributes.get(CONTEXT_AUDIT_ATTRIBUTE_OPERATION.toLowerCase());
    if (opAttrDesc == null) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  String.format(
                      "Context audit signal must have attribute %s",
                      CONTEXT_AUDIT_ATTRIBUTE_OPERATION),
                  tenantDesc,
                  contextDesc)
              .append(", auditSignal=")
              .append(auditSignalName)
              .toString());
    }
    if (opAttrDesc.getAttributeType() != STRING) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  String.format(
                      "Attribute %s of audit signal must be of type String",
                      CONTEXT_AUDIT_ATTRIBUTE_OPERATION),
                  tenantDesc,
                  contextDesc)
              .append(", type=")
              .append(opAttrDesc.getAttributeType())
              .toString());
    }

    for (var contextAttributeDesc : contextAttributes) {
      String attributeName = contextAttributeDesc.getName();

      verifyAuditAttribute(
          tenantDesc,
          contextDesc,
          contextAttributeDesc,
          attributeName,
          auditSignalName,
          auditAttributes);

      final var prevAttributeName = StringUtils.prefixToCamelCase("prev", attributeName);
      verifyAuditAttribute(
          tenantDesc,
          contextDesc,
          contextAttributeDesc,
          prevAttributeName,
          auditSignalName,
          auditAttributes);
    }
  }

  private void verifyAuditAttribute(
      TenantDesc tenantDesc,
      StreamDesc contextDesc,
      AttributeDesc contextAttributeDesc,
      String attributeName,
      String auditSignalName,
      Map<String, AttributeDesc> auditAttributes)
      throws ConstraintViolationException {
    AttributeDesc auditAttributeDesc = auditAttributes.get(attributeName.toLowerCase());
    if (auditAttributeDesc == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("Attribute not found in audit signal", tenantDesc, contextDesc)
              .append(", auditSignal=")
              .append(auditSignalName)
              .append(", attribute=")
              .append(attributeName)
              .toString());
    }

    final var contextAttributeType = contextAttributeDesc.getAttributeType();
    final var auditAttributeType = auditAttributeDesc.getAttributeType();
    if (contextAttributeType != auditAttributeType) {
      final var message =
          createExceptionMessage("Audit attribute type mismatch", tenantDesc, contextDesc)
              .append(", auditSignal=")
              .append(auditSignalName)
              .append(", attribute=")
              .append(attributeName)
              .append(", typeInContext=")
              .append(contextAttributeType)
              .append(", typeInAudit=")
              .append(auditAttributeType)
              .toString();
      if (contextAttributeType == ENUM && auditAttributeType == STRING) {
        logger.error("Accepted violation (enum to string audit), fix manually: {}", message);
      } else {
        throw new ConstraintViolationException(message);
      }
    }
  }

  private void validateExports(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException, ApplicationException {
    if (streamDesc.getIsLatestVersion() == Boolean.FALSE) {
      return;
    }
    final var exportDestinationId = streamDesc.getExportDestinationId();
    if (exportDestinationId == null) {
      return;
    }
    final var type = streamDesc.getType();
    if (type != StreamType.SIGNAL) {
      final var message =
          createExceptionMessage(
                  String.format("Exporting %s is not supported", type.name().toLowerCase()),
                  tenantDesc,
                  streamDesc)
              .toString();
      throw new ConstraintViolationException(message);
    }

    if (exportService != null) {
      try {
        exportService.getDestination(
            tenantDesc.getName(), tenantDesc.getVersion(), exportDestinationId);
      } catch (NoSuchEntityException e) {
        throw new ConstraintViolationException(
            createExceptionMessage("Destination not found", tenantDesc, streamDesc)
                .append(", exportDestinationId=")
                .append(exportDestinationId)
                .toString());
      }
    }
  }

  /**
   * Verify and initialize views.
   *
   * <p>This method must run after pre-process initialization since views depend on join attributes.
   */
  private void initViews(TenantDesc tenantDesc, StreamDesc signalDesc, List<StreamDesc> subStreams)
      throws ConstraintViolationException {
    if (!signalDesc.isActive() || signalDesc.getViews() == null) {
      return;
    }
    if (!Set.of(StreamType.SIGNAL, StreamType.ROLLUP, StreamType.METRICS)
        .contains(signalDesc.getType())) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Views are supported only for SIGNAL streams", tenantDesc, signalDesc)
              .toString());
    }

    Set<String> uniqueViewNames = new HashSet<>();
    for (ViewDesc view : signalDesc.getViews()) {
      if (view.getName() == null) {
        throw new ConstraintViolationException(
            createExceptionMessage("View name may not be null", tenantDesc, signalDesc).toString());
      }
      if (!uniqueViewNames.add(view.getName().toLowerCase())) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "View property 'name' may not have duplicates", tenantDesc, signalDesc)
                .append(", view=")
                .append(view.getName())
                .toString());
      }

      FeatureValidator.validateSignalFeature(tenantDesc, signalDesc, view);

      // create view and index config
      final StreamDesc viewStream = createViewStream(tenantDesc, signalDesc, view);
      signalDesc.addSubStreamName(viewStream.getName());
      viewStream.setParent(tenantDesc);
      subStreams.add(viewStream);
      final StreamDesc indexStream = createIndexStream(tenantDesc, signalDesc, view);
      signalDesc.addSubStreamName(indexStream.getName());
      indexStream.setParent(tenantDesc);
      subStreams.add(indexStream);
    }
  }

  private void initPostProcesses(
      TenantDesc tenantDesc, StreamDesc streamDesc, List<StreamDesc> subStreams)
      throws ConstraintViolationException, InvalidValueException, InvalidAlertException {
    if (streamDesc.getPostprocesses() == null) {
      return;
    }
    if (streamDesc.getType() != StreamType.SIGNAL && streamDesc.getType() != StreamType.METRICS) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Postprocess is supported only for SIGNAL streams", tenantDesc, streamDesc)
              .toString());
    }
    if (streamDesc.getViews() == null) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Views must be set to configure postprocesses", tenantDesc, streamDesc)
              .toString());
    }
    // This set is used for verifying uniqueness of the rollup names in the stream
    final Set<String> uniqueRollupNames = new HashSet<>();
    for (PostprocessDesc postProcess : streamDesc.getPostprocesses()) {
      initPostprocess(tenantDesc, streamDesc, postProcess, uniqueRollupNames, subStreams);
    }
    try {
      validateAlertConditions(tenantDesc, streamDesc);
    } catch (InvalidRuleException e) {
      throw new InvalidAlertException(e.getMessage());
    }
  }

  /**
   * This method verifies and initializes a post-process config entry.
   *
   * <p>This method does following:
   *
   * <ul>
   *   <li>Validate the view reference
   *   <li>Validate the rollup configuration
   *   <li>Check uniqueness of rollup names
   *   <li>Register the rollup name to the substream name list of the parent signal
   *   <li>Set the signal descriptor to the post process descriptor as its parent stream
   *   <li>Register the post process descriptor to the substreams list
   * </ul>
   *
   * @param tenantDesc The tenant descriptor
   * @param signalDesc The signal descriptor that is being verified
   * @param postProcess The post process entry to check
   * @param uniqueRollupNames Unique rollup names that appear so far in this signal. This method
   *     also adds its rollup names. When a rollup name conflict with another one in the stream,
   *     this method throws ConstraintViolationException.
   * @param subStreams Substream descriptors of this signal. This method also adds rollups in the
   *     post process descriptor.
   * @throws ConstraintViolationException When constraint violation is found in the config entry.
   * @throws InvalidValueException When an invalid value is found in the config entry.
   */
  private void initPostprocess(
      TenantDesc tenantDesc,
      StreamDesc signalDesc,
      PostprocessDesc postProcess,
      Set<String> uniqueRollupNames,
      List<StreamDesc> subStreams)
      throws ConstraintViolationException, InvalidValueException {
    ViewDesc view = validateViewReference(tenantDesc, signalDesc, postProcess);
    for (Rollup rollup : postProcess.getRollups()) {
      validateRollup(tenantDesc, signalDesc, view, rollup);
      // View for the rollup would be rebuilt since the attributes may include non-addable types.
      // Such attributes would be ignored silently.
      final var rollupView = view.duplicate();
      final var rebuiltAttributes = new ArrayList<String>();
      rollupView.setAttributes(rebuiltAttributes);
      for (final var attr : view.getAttributes()) {
        if (signalDesc.findAnyAttribute(attr).getAttributeType().isAddable()) {
          rebuiltAttributes.add(attr);
        }
      }
      final StreamDesc rollupDesc = createRollupStream(tenantDesc, signalDesc, rollupView, rollup);
      final String rollupName = rollupDesc.getName();
      if (!uniqueRollupNames.add(rollupName.toLowerCase())) {
        throw new ConstraintViolationException(
            createExceptionMessage("Duplicate rollup names are not allowed", tenantDesc, signalDesc)
                .append(", view=")
                .append(view.getName())
                .append(", rollup=")
                .append(rollupName)
                .toString());
      }
      signalDesc.addSubStreamName(rollupName);
      rollupDesc.setParent(tenantDesc);
      subStreams.add(rollupDesc);

      // If the feature is materialized, put an adjuster to the destination context
      if (signalDesc.getIsLatestVersion() == Boolean.TRUE && view.getSnapshot() == Boolean.TRUE) {
        final var contextDesc =
            tenantDesc.getStream(view.getEffectiveFeatureAsContextName(signalDesc.getName()));
        if (view.getWriteTimeIndexing() == Boolean.TRUE) {
          contextDesc.setContextAdjuster(
              new AccumulatingCountContextAdjuster(signalDesc, rollupDesc, contextDesc));
        } else {
          contextDesc.setContextAdjuster(
              ContextAdjuster.invalidAdjuster(
                  String.format(
                      "True must be set to property 'indexOnInsert' in feature '%s' of signal '%s'"
                          + " to enable onTheFly query for an AccumulatingCount materialized context;"
                          + " tenant=%s, context=%s",
                      view.getName(),
                      signalDesc.getName(),
                      tenantDesc.getName(),
                      contextDesc.getName())));
        }
      }
    }
  }

  /**
   * Method to validate the view name of the specified post-process descriptor.
   *
   * @param tenantDesc The tenant descriptor
   * @param streamDesc The stream descriptor that is being initialized currently
   * @param postProcess The post process descriptor to test
   * @return Corresponding view descriptor
   * @throws ConstraintViolationException in case no corresponding view descriptor is found
   */
  private ViewDesc validateViewReference(
      TenantDesc tenantDesc, StreamDesc streamDesc, PostprocessDesc postProcess)
      throws ConstraintViolationException {
    final String viewName = postProcess.getView();
    if (viewName == null) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "property 'view' is missing in postProcess", tenantDesc, streamDesc)
              .toString());
    }
    for (ViewDesc view : streamDesc.getViews()) {
      if (viewName.equalsIgnoreCase(view.getName())) {
        return view;
      }
    }
    throw new ConstraintViolationException(
        createExceptionMessage(
                "postProcess is referring to non-existing view", tenantDesc, streamDesc)
            .append(", view=")
            .append(viewName)
            .toString());
  }

  /**
   * Method to validate rollup configuration.
   *
   * @param tenantDesc The tenant descriptor
   * @param streamDesc The stream descriptor that is being initialized currently
   * @param view View descriptor that corresponds to the specified rollup
   * @param rollup The rollup configuration
   * @throws InvalidValueException when an invalid value is found
   * @throws ConstraintViolationException when a constraint violation is found
   */
  private void validateRollup(
      TenantDesc tenantDesc, StreamDesc streamDesc, ViewDesc view, Rollup rollup)
      throws ConstraintViolationException {
    if (rollup.getName() == null || rollup.getName().isEmpty()) {
      throw new ConstraintViolationException(
          createExceptionMessage("rollup name must be set", tenantDesc, streamDesc)
              .append(", view=")
              .append(view.getName())
              .toString());
    }
  }

  /**
   * Validate TODO(BIOS-1474) and initialize enrichments.
   *
   * @param tenantDesc The target tenant descriptor
   * @param streamDesc The stream descriptor to add
   * @throws ConstraintViolationException When a constraint violation is found in the configuration
   */
  private void initContextEnrichments(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException, InvalidDefaultValueException {
    final List<EnrichmentConfigContext> enrichments = streamDesc.getContextEnrichments();
    if (streamDesc.isDeleted() || enrichments == null || enrichments.isEmpty()) {
      return;
    }
    // Basic input validation.
    if (streamDesc.getType() != StreamType.CONTEXT) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Context Enrichments are supported only by contexts", tenantDesc, streamDesc)
              .append(", got stream type=")
              .append(streamDesc.getType().name())
              .toString());
    }
    final MissingLookupPolicy streamMlp =
        MissingAttributePolicyV1.translateToMlp(streamDesc.getMissingLookupPolicy());
    if ((streamMlp != null)
        && (streamMlp != MissingLookupPolicy.STORE_FILL_IN_VALUE)
        && (streamMlp != MissingLookupPolicy.FAIL_PARENT_LOOKUP)) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "A context with enrichments must have a missingLookupPolicy of StoreFillInValue or"
                      + " FailParentLookup specified",
                  tenantDesc,
                  streamDesc)
              .append(", PolicySpecified=")
              .append(streamMlp.toString())
              .toString());
    }

    final List<CompiledEnrichment> compiledEnrichments = new ArrayList<>();
    streamDesc.setCompiledEnrichments(compiledEnrichments);
    final Set<String> enrichmentNames = new HashSet<>();
    final Set<String> existingAttributeNames = new HashSet<>();
    for (final var attribute : streamDesc.getAttributes()) {
      existingAttributeNames.add(attribute.getName().toLowerCase());
    }

    // For every enrichment, validate it and create a compiled enrichment.
    // Also, process every enrichedAttribute in every enrichment and add to additional attributes.
    for (final var enrichment : enrichments) {
      // Validate the enrichment.
      if (Strings.isNullOrEmpty(enrichment.getName())) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Enrichment must have enrichmentName set", tenantDesc, streamDesc)
                .toString());
      }
      if (enrichmentNames.contains(enrichment.getName().toLowerCase())) {
        throw new ConstraintViolationException(
            createExceptionMessage("Duplicate enrichmentName not allowed", tenantDesc, streamDesc)
                .append(", enrichmentName=")
                .append(enrichment.getName())
                .toString());
      }
      enrichmentNames.add(enrichment.getName().toLowerCase());
      final MissingLookupPolicy enrichmentMlp = enrichment.getMissingLookupPolicy();
      final MissingLookupPolicy effectiveMlp = enrichmentMlp != null ? enrichmentMlp : streamMlp;
      if (effectiveMlp == null) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Neither stream nor enrichment has a valid 'missingLookupPolicy' property value",
                    tenantDesc,
                    streamDesc)
                .append(", enrichment=")
                .append(enrichment.getName())
                .toString());
      }
      if ((effectiveMlp != MissingLookupPolicy.STORE_FILL_IN_VALUE)
          && (effectiveMlp != MissingLookupPolicy.FAIL_PARENT_LOOKUP)) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "For context enrichments, only valid values of missingLookupPolicy are"
                        + " StoreFillInValue and FailParentLookup",
                    tenantDesc,
                    streamDesc)
                .append(", enrichment=")
                .append(enrichment.getName())
                .append(", effective policy=")
                .append(effectiveMlp.name())
                .toString());
      }
      if ((enrichment.getEnrichedAttributes() == null)
          || (enrichment.getEnrichedAttributes().size() == 0)) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Enrichment must have at least one enrichedAttribute", tenantDesc, streamDesc)
                .append(", enrichment=")
                .append(enrichment.getName())
                .toString());
      }

      if ((enrichment.getForeignKey() == null)
          || (enrichment.getForeignKey().size() == 0)
          || enrichment.getForeignKey().stream().anyMatch((key) -> Strings.isNullOrEmpty(key))) {
        throw new ConstraintViolationException(
            createExceptionMessage("Enrichment must have a foreign key set", tenantDesc, streamDesc)
                .append(", enrichment=")
                .append(enrichment.getName())
                .toString());
      }
      final List<String> foreignKeyNames = enrichment.getForeignKey();
      final List<AttributeDesc> foreignKey = new ArrayList<>();
      for (var name : foreignKeyNames) {
        foreignKey.add(
            getEnrichmentForeignKeyAttribute(tenantDesc, streamDesc, enrichment.getName(), name));
      }
      // set the foreign key names back with the original cases
      enrichment.setForeignKey(
          foreignKey.stream()
              .map((keyAttribute) -> keyAttribute.getName())
              .collect(Collectors.toList()));

      final var compiledEnrichment = new CompiledEnrichment();
      compiledEnrichment.setForeignKey(foreignKeyNames);
      compiledEnrichment.setMissingLookupPolicy(effectiveMlp);
      compiledEnrichments.add(compiledEnrichment);

      // Figure out what kind of enrichment this is, based on which field(s) are populated.
      // We only look at the first enrichedAttribute of the enrichment to decide the kind;
      // remaining enrichedAttributes must be of the same kind.
      final var firstEnrichedAttribute = enrichment.getEnrichedAttributes().get(0);
      final EnrichmentKind enrichmentKind;
      StreamDesc refContextStream = null;
      if (firstEnrichedAttribute.getValue() != null) {
        enrichmentKind = EnrichmentKind.SIMPLE_VALUE;
        // A simple value only has one context to join with.
        // Get the context this enrichment is referring to.
        final String referencedContextName =
            firstEnrichedAttribute.getValue().split(ENRICHED_ATTRIBUTE_DELIMITER_REGEX)[0];
        refContextStream =
            getEnrichingContextStream(
                tenantDesc, streamDesc, referencedContextName, enrichment.getName(), foreignKey);
        compiledEnrichment.setJoiningContext(refContextStream);

      } else if (firstEnrichedAttribute.getValuePickFirst() != null) {
        enrichmentKind = EnrichmentKind.VALUE_PICK_FIRST;

      } else {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "enrichedAttributes must have either 'value' or 'valuePickFirst' defined;"
                        + " the first enrichedAttribute does not have either of them",
                    tenantDesc,
                    streamDesc)
                .append(", enrichment=")
                .append(enrichment.getName())
                .append(", firstEnrichedAttribute=")
                .append(firstEnrichedAttribute.toString())
                .toString());
      }
      compiledEnrichment.setEnrichmentKind(enrichmentKind);

      // Process every enriched attribute and add it to additional attributes for this stream.
      // Lookup the joined attribute in the context(s) and validate that the type matches.
      for (final var enrichedAttribute : enrichment.getEnrichedAttributes()) {
        final var compiledAttribute = new CompiledAttribute();
        compiledEnrichment.addCompiledAttribute(compiledAttribute);
        AttributeDesc joinedAttr = null;
        String attributeName = null;
        // Basic validation.
        if (enrichedAttribute.getAs() != null && enrichedAttribute.getAs().isEmpty()) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "If the 'as' property is specified, it may not be empty",
                      tenantDesc,
                      streamDesc)
                  .append(", enrichment=")
                  .append(enrichment.getName())
                  .append(", enrichedAttribute=")
                  .append(enrichedAttribute)
                  .toString());
        }
        if (enrichedAttribute.getAs() != null) {
          compiledAttribute.setAliasedName(enrichedAttribute.getAs());
        }

        if (effectiveMlp == MissingLookupPolicy.STORE_FILL_IN_VALUE
            && enrichedAttribute.getFillInSerialized() == null) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "'fillIn' must be set when missing lookup policy is StoreFillInValue",
                      tenantDesc,
                      streamDesc)
                  .append(", enrichment=")
                  .append(enrichment.getName())
                  .append(", enrichedAttribute=")
                  .append(enrichedAttribute)
                  .toString());
        }

        // Processing of enriched attributes depends on the kind of enrichment.
        switch (compiledEnrichment.getEnrichmentKind()) {
          case SIMPLE_VALUE:
            // Basic validation.
            if ((enrichedAttribute.getValue() == null) || enrichedAttribute.getValue().isEmpty()) {
              throw new ConstraintViolationException(
                  createExceptionMessage(
                          "'value' cannot be missing or empty for this enriched attribute",
                          tenantDesc,
                          streamDesc)
                      .append(", enrichment=")
                      .append(enrichment.getName())
                      .append(", enrichedAttribute=")
                      .append(enrichedAttribute.toString())
                      .toString());
            }
            final String[] valueSplit =
                enrichedAttribute.getValue().split(ENRICHED_ATTRIBUTE_DELIMITER_REGEX);
            if ((valueSplit.length != 2)
                || (Strings.isNullOrEmpty(valueSplit[0]))
                || (Strings.isNullOrEmpty(valueSplit[1]))) {
              throw new ConstraintViolationException(
                  createExceptionMessage(
                          "'value' must be in the form 'contextName'"
                              + ENRICHED_ATTRIBUTE_DELIMITER
                              + "'attribute name'",
                          tenantDesc,
                          streamDesc)
                      .append(", enrichment=")
                      .append(enrichment.getName())
                      .append(", value=")
                      .append(enrichedAttribute.getValue())
                      .toString());
            }

            // Make sure all enriched attributes in this enrichment reference the same context.
            final String attributeContextName = valueSplit[0];
            assert (refContextStream != null);
            if (!attributeContextName.equalsIgnoreCase(refContextStream.getName())) {
              throw new ConstraintViolationException(
                  createExceptionMessage(
                          "All enriched attributes in an enrichment with 'value' must use the same context",
                          tenantDesc,
                          streamDesc)
                      .append(", enrichment=")
                      .append(enrichment.getName())
                      .append(", expected context name=")
                      .append(refContextStream.getName())
                      .append(", enrichedAttribute value=")
                      .append(enrichedAttribute.getValue())
                      .toString());
            }
            // Get which attribute it refers to from the context.
            attributeName = valueSplit[1];
            compiledAttribute.setJoinedAttributeName(attributeName);
            // If 'as' was not provided for this value, use the name of the joined attribute as the
            // name of the enrichedAttribute.
            if (compiledAttribute.getAliasedName() == null) {
              compiledAttribute.setAliasedName(attributeName);
            }
            joinedAttr =
                getAttributeFromJoinedContext(
                        tenantDesc,
                        streamDesc,
                        enrichment.getName(),
                        refContextStream,
                        attributeName)
                    .duplicate();
            // to match cases with the original
            enrichedAttribute.setValue(refContextStream.getName() + "." + joinedAttr.getName());
            break;

          case VALUE_PICK_FIRST:
            // Basic validation.
            if ((enrichedAttribute.getValuePickFirst() == null)
                || enrichedAttribute.getValuePickFirst().isEmpty()) {
              throw new ConstraintViolationException(
                  createExceptionMessage(
                          "'valuePickFirst' cannot be missing or empty for this enriched attribute",
                          tenantDesc,
                          streamDesc)
                      .append(", enrichment=")
                      .append(enrichment.getName())
                      .append(", enrichedAttribute=")
                      .append(enrichedAttribute.toString())
                      .toString());
            }
            if (enrichedAttribute.getAs() == null || enrichedAttribute.getAs().isEmpty()) {
              throw new ConstraintViolationException(
                  createExceptionMessage(
                          "'valuePickFirst' must have a non-empty 'as' property specified",
                          tenantDesc,
                          streamDesc)
                      .append(", enrichment=")
                      .append(enrichment.getName())
                      .append(", enrichedAttribute=")
                      .append(enrichedAttribute.toString())
                      .toString());
            }

            // Process every candidate in the list.
            final int numCandidates = enrichedAttribute.getValuePickFirst().size();
            for (int i = 0; i < numCandidates; i++) {
              // Basic validation.
              final String candidate = enrichedAttribute.getValuePickFirst().get(i);
              final String[] candidateSplit = candidate.split(ENRICHED_ATTRIBUTE_DELIMITER_REGEX);
              if ((candidateSplit.length != 2)
                  || (Strings.isNullOrEmpty(candidateSplit[0]))
                  || (Strings.isNullOrEmpty(candidateSplit[1]))) {
                throw new ConstraintViolationException(
                    createExceptionMessage(
                            "Every candidate in 'valuePickFirst' must be in the form"
                                + "'contextName'"
                                + ENRICHED_ATTRIBUTE_DELIMITER
                                + "'attribute name'",
                            tenantDesc,
                            streamDesc)
                        .append(", enrichment=")
                        .append(enrichment.getName())
                        .append(", valuePickFirst=")
                        .append(enrichedAttribute.getValuePickFirst())
                        .append(", candidate=")
                        .append(candidate)
                        .toString());
              }

              // Get the candidate context.
              final StreamDesc candidateContextStream =
                  getEnrichingContextStream(
                      tenantDesc, streamDesc, candidateSplit[0], enrichment.getName(), foreignKey);
              // Get which attribute it refers to from the context.
              final String candidateAttributeName = candidateSplit[1];
              final AttributeDesc candidateAttr =
                  getAttributeFromJoinedContext(
                          tenantDesc,
                          streamDesc,
                          enrichment.getName(),
                          candidateContextStream,
                          candidateAttributeName)
                      .duplicate();
              if (joinedAttr == null) {
                joinedAttr = candidateAttr;
              } else {
                // All candidates must have the same attribute type.
                final String mismatch = candidateAttr.checkMismatch(joinedAttr);
                if (mismatch != null) {
                  throw new ConstraintViolationException(
                      createExceptionMessage(
                              "All candidates in a 'valuePickFirst' enrichedAttribute must have identical"
                                  + " type including identical membership of enums (allowed values)",
                              tenantDesc,
                              streamDesc)
                          .append(", enrichment=")
                          .append(enrichment.getName())
                          .append(", valuePickFirst=")
                          .append(enrichedAttribute.getValuePickFirst())
                          .append(", candidate=")
                          .append(candidate)
                          .append(", expectedAttributeToMatch=")
                          .append(joinedAttr.getName())
                          .append(", mismatch=")
                          .append(mismatch)
                          .toString());
                }
              }
              // Save this gathered information in the compiled enrichment.
              compiledAttribute.addCandidateContext(candidateContextStream);
              compiledAttribute.addCandidateAttributeName(candidateAttributeName);
            }
            assert (compiledAttribute.getCandidateContexts().size() == numCandidates);
            assert (compiledAttribute.getCandidateAttributeNames().size() == numCandidates);
            break;

          default:
            assert (false);
        }

        final var fillInSerialized = enrichedAttribute.getFillInSerialized();
        if (fillInSerialized != null) {
          try {
            final var fillIn = joinedAttr.getAttributeType().parse(fillInSerialized);
            joinedAttr.setDefaultValue(fillIn);
            compiledAttribute.setFillIn(fillIn);
          } catch (InvalidValueSyntaxException e) {
            throw new InvalidDefaultValueException(
                createExceptionMessage("Invalid default value", tenantDesc, streamDesc)
                    .append(", enrichment=")
                    .append(enrichment.getName())
                    .append(", enrichedAttribute=")
                    .append(enrichedAttribute)
                    .append(", attributeName=")
                    .append(attributeName)
                    .append(", fillIn=")
                    .append(fillInSerialized)
                    .append(", error=")
                    .append(e.getMessage())
                    .toString());
          }
        }

        // If an alias was provided for this enriched attribute, use that name.
        if (enrichedAttribute.getAs() != null) {
          attributeName = enrichedAttribute.getAs();
          assert joinedAttr != null;
          joinedAttr.setName(attributeName);
        }
        // Ensure there is no repeated attribute name or alias.
        assert attributeName != null;
        if (existingAttributeNames.contains(attributeName.toLowerCase())) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "Duplicate attribute names are not allowed", tenantDesc, streamDesc)
                  .append(", enrichment=")
                  .append(enrichment.getName())
                  .append(", enrichedAttribute=")
                  .append(enrichedAttribute.toString())
                  .append(", attributeName=")
                  .append(attributeName)
                  .toString());
        }
        existingAttributeNames.add(attributeName.toLowerCase());

        // Add this attribute to the list of additional attributes for this context.
        streamDesc.addAdditionalAttribute(joinedAttr);
      }
    }
  }

  /**
   * This must be run after attributes and postprocesses have been verified and initialized.
   *
   * @throws ConstraintViolationException thrown to indicate that a constraint error happened
   */
  private void initDataSketches(TenantDesc tenantDesc, StreamDesc streamDesc)
      throws ConstraintViolationException {
    // Currently only signals and contexts are supported for data sketches.
    if ((streamDesc.getType() != StreamType.SIGNAL)
        && (streamDesc.getType() != StreamType.CONTEXT)
        && (streamDesc.getType() != StreamType.METRICS)) {
      return;
    }

    final String sketchesToAdd =
        SharedProperties.getCached(
            DEFAULT_SKETCHES_FOR_ATTRIBUTES_KEY, DEFAULT_SKETCHES_FOR_ATTRIBUTES_DEFAULT);
    final long interval =
        SharedProperties.getCached(
            DEFAULT_SKETCHES_INTERVAL_KEY, DEFAULT_SKETCHES_INTERVAL_DEFAULT);
    final DataSketchDuration defaultDurationType;
    if (streamDesc.getType() == StreamType.CONTEXT) {
      // Contexts allow modifications to data (they are not immutable), and sketches only support
      // adding data. So we need to recalculate the sketches every time for contexts.
      defaultDurationType = DataSketchDuration.ALL_TIME;
    } else {
      try {
        defaultDurationType = DataSketchDuration.fromMillis(interval);
      } catch (IllegalArgumentException e) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "Unsupported sketches interval, check shared property "
                        + DEFAULT_SKETCHES_INTERVAL_KEY,
                    tenantDesc,
                    streamDesc)
                .append(", interval=")
                .append(interval)
                .toString());
      }
    }
    final var attributeList = new ArrayList<>(streamDesc.getAttributes());
    if (streamDesc.getAdditionalAttributes() != null) {
      attributeList.addAll(streamDesc.getAdditionalAttributes());
    }

    // Add default data sketches if configured.
    final var defaultSketches = new ArrayList<DataSketchType>();
    if (!sketchesToAdd.isEmpty()) {
      for (final var sketchTypeName : sketchesToAdd.split(",")) {
        try {
          final var sketchType = DataSketchType.forValue(sketchTypeName);
          defaultSketches.add(sketchType);
        } catch (IllegalArgumentException e) {
          logger.error(
              "Invalid DataSketchType configured in shared properties: {}", sketchTypeName);
        }
      }
    }
    if (!defaultSketches.isEmpty()) {
      for (final var attribute : attributeList) {
        for (final var sketchType : defaultSketches) {
          if (attribute.getAttributeType().isSupportedBySketch(sketchType)) {
            streamDesc.addSketchIfAbsent(attribute.getName(), sketchType, defaultDurationType);
          }
        }
      }
    }

    // Add data sketches from features.
    if (streamDesc.getPostprocesses() == null) {
      return;
    }
    final boolean addMoments =
        SharedProperties.getCached(
            ADD_MOMENTS_SKETCH_TO_ALL_ROLLUPS_KEY, ADD_MOMENTS_SKETCH_TO_ALL_ROLLUPS_DEFAULT);
    for (PostprocessDesc desc : streamDesc.getPostprocesses()) {
      final ViewDesc view = streamDesc.getView(desc.getView());
      for (Rollup rollup : desc.getRollups()) {
        final long rollupInterval = rollup.getInterval().getValueInMillis();
        final DataSketchDuration durationType;
        try {
          durationType = DataSketchDuration.fromMillis(rollupInterval);
        } catch (IllegalArgumentException e) {
          throw new ConstraintViolationException(
              createExceptionMessage(e.getMessage(), tenantDesc, streamDesc)
                  .append(", interval=")
                  .append(rollupInterval)
                  .toString());
        }
        for (final var attributeName : view.getAttributes()) {
          final var attribute = streamDesc.findAnyAttribute(attributeName);
          if (view.getDataSketches() != null) {
            for (final var sketchType : view.getDataSketches()) {
              if (sketchType.isGenericDigestor()) {
                // A generic digestion would replace the rollup, here we keep only data sketches
                // that are independent of signal/view.
                continue;
              }
              if (attribute.getAttributeType().isSupportedBySketch(sketchType)) {
                streamDesc.addSketchIfAbsent(attributeName, sketchType, durationType);
              }
            }
          }
          // Add a moments sketch by default if configured.
          if (addMoments
              && attribute.getAttributeType().isSupportedBySketch(DataSketchType.MOMENTS)) {
            streamDesc.addSketchIfAbsent(attributeName, DataSketchType.MOMENTS, durationType);
          }
        }
        // Special case for count(). If there are no attributes in this view, pick any number type
        // attribute and create a moments sketch for it.
        if (view.getAttributes().isEmpty()) {
          if ((view.getDataSketches() != null) || addMoments) {
            // Find a number type attribute.
            for (final var attribute : attributeList) {
              if (attribute.getAttributeType().isSupportedBySketch(DataSketchType.MOMENTS)) {
                streamDesc.addSketchIfAbsent(
                    attribute.getName(), DataSketchType.MOMENTS, durationType);
                break;
              }
            }
          }
        }
      }
    }
  }

  /**
   * This method finds an attribute description in a stream configuration by specified name.
   *
   * <p>The name match is case insensitive. This method throws ConstraintViolationException if no
   * matching attribute is found. So this method never returns null.
   *
   * @param attributeName Attribute name to find in the context
   * @return Found attribute description
   * @throws ConstraintViolationException When no matching attribute is found.
   */
  protected AttributeDesc getEnrichmentForeignKeyAttribute(
      TenantDesc tenantDesc, StreamDesc streamDesc, String enrichmentName, String attributeName)
      throws ConstraintViolationException {
    for (AttributeDesc desc : streamDesc.getAttributes()) {
      if (desc.getName().equalsIgnoreCase(attributeName)) {
        return desc;
      }
    }
    // If not found in the attributes of the parent stream, look in the additional attributes.
    if (streamDesc.getAdditionalAttributes() != null) {
      for (AttributeDesc desc : streamDesc.getAdditionalAttributes()) {
        if (desc.getName().equalsIgnoreCase(attributeName)) {
          return desc;
        }
      }
    }
    throw new ConstraintViolationException(
        createExceptionMessage(
                "Foreign key must be one of the stream's attributes;", tenantDesc, streamDesc)
            .append(", enrichment=")
            .append(enrichmentName)
            .append(", foreign key=")
            .append(attributeName)
            .toString());
  }

  protected StreamDesc getEnrichingContextStream(
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      String contextName,
      String enrichmentName,
      List<AttributeDesc> foreignKey)
      throws ConstraintViolationException {
    if (Strings.isNullOrEmpty(contextName)) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Context name in enrichedAttribute value field may not be empty",
                  tenantDesc,
                  streamDesc)
              .append(", enrichment=")
              .append(enrichmentName)
              .toString());
    }
    if (contextName.equalsIgnoreCase(streamDesc.getName())) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Context name in enrichedAttribute value must be different from parent stream's name;",
                  tenantDesc,
                  streamDesc)
              .append(", enrichment=")
              .append(enrichmentName)
              .toString());
    }

    final StreamDesc remoteContextDesc = tenantDesc.getDependingStream(contextName, streamDesc);
    if (remoteContextDesc == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("Referring context not found", tenantDesc, streamDesc)
              .append(", enrichment=")
              .append(enrichmentName)
              .append(", remoteContext=")
              .append(contextName)
              .toString());
    }
    if (remoteContextDesc.getType() != StreamType.CONTEXT) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Referring stream is not of type context but "
                      + remoteContextDesc.getType().name().toLowerCase(),
                  tenantDesc,
                  streamDesc)
              .append(", enrichment=")
              .append(enrichmentName)
              .append(", remoteContext=")
              .append(contextName)
              .append(", type=")
              .append(remoteContextDesc.getType().toValue())
              .toString());
    }
    final List<String> remotePrimaryKey = remoteContextDesc.getPrimaryKey();
    if (remotePrimaryKey.size() != foreignKey.size()) {
      final var foreignKeyNames =
          foreignKey.stream().map((key) -> key.getName()).collect(Collectors.toList());
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Foreign key size must be the same with referring context's primary key",
                  tenantDesc,
                  streamDesc)
              .append(", enrichment=")
              .append(enrichmentName)
              .append(", foreignKey=")
              .append(foreignKeyNames)
              .append(", remoteContext=")
              .append(contextName)
              .append(", primaryKey=")
              .append(remotePrimaryKey)
              .toString());
    }
    // Check that the foreign key type matches that of the context's primary key.
    for (int i = 0; i < foreignKey.size(); ++i) {
      final AttributeDesc foreignKeyAttribute = foreignKey.get(i);
      final AttributeDesc remotePrimaryKeyAttribute =
          remoteContextDesc.getPrimaryKeyAttributes().get(i);
      final String mismatch = remotePrimaryKeyAttribute.checkMismatch(foreignKeyAttribute);
      if (mismatch != null) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    String.format(
                        "Type mismatch between foreignKey[%d] and %s.primaryKey[%d]",
                        i, contextName, i),
                    tenantDesc,
                    streamDesc)
                .append(", enrichment=")
                .append(enrichmentName)
                .append(", foreignKey=")
                .append(foreignKeyAttribute.getName())
                .append(", remoteContext=")
                .append(contextName)
                .append(", mismatch=")
                .append(mismatch)
                .toString());
      }
    }
    // Joining with a context implies a dependency on that context. Track the new dependency and
    // ensure that this does not cause a cyclic dependency.
    checkCyclesAndAddDependency(tenantDesc, streamDesc, remoteContextDesc, enrichmentName);

    return remoteContextDesc;
  }

  private void addAuditDependency(TenantDesc tenantDesc, StreamDesc contextDesc)
      throws ConstraintViolationException {

    if (contextDesc.getType() != StreamType.CONTEXT) {
      return;
    }

    if (contextDesc.getAuditEnabled() != Boolean.TRUE) {
      return;
    }

    String auditSignal = CONTEXT_AUDIT_SIGNAL_PREFIX + contextDesc.getName();

    StreamDesc auditSignalStream = tenantDesc.getStream(auditSignal, true);
    auditSignalStream.addTag(StreamTag.CONTEXT_AUDIT);
    try {
      tenantDesc.addDependency(auditSignalStream, contextDesc);
    } catch (DirectedAcyclicGraphException e) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Cyclic dependencies are not allowed. Adding this dependency will cause a cycle",
                  tenantDesc,
                  contextDesc)
              .append(", referencedSignal=")
              .append(auditSignalStream.getName())
              .toString());
    }
    contextDesc.addDependency(auditSignal);
  }

  void checkCyclesAndAddDependency(
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      StreamDesc refContextStream,
      String preprocessOrEnrichmentName)
      throws ConstraintViolationException {

    final var streamToDependOn = refContextStream.getName();

    // #1 Add the dependency to the DAG maintained by the tenant, and look for an exception.
    try {
      tenantDesc.addDependency(refContextStream, streamDesc);
    } catch (DirectedAcyclicGraphException e) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Cyclic dependencies are not allowed. Adding this dependency will cause a cycle",
                  tenantDesc,
                  streamDesc)
              .append(", referencedContext=")
              .append(refContextStream.getName())
              .append(", referenceLocation=")
              .append(preprocessOrEnrichmentName)
              .toString());
    }
    // #2 Successfully added the dependency to the DAG. We can now safely add it to this stream and
    // continue.
    streamDesc.addDependency(streamToDependOn);
  }

  private AttributeDesc getAttributeFromJoinedContext(
      TenantDesc tenantDesc,
      StreamDesc streamDesc,
      String enrichmentName,
      StreamConfig refContextStream,
      String attributeName)
      throws ConstraintViolationException {
    boolean first = true;
    for (AttributeDesc attrDesc : refContextStream.getAttributes()) {
      if (attrDesc.getName().equalsIgnoreCase(attributeName)) {
        if (first) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "Joined attribute name may not be the primary key of the joined context",
                      tenantDesc,
                      streamDesc)
                  .append(", enrichment=")
                  .append(enrichmentName)
                  .append(", context=")
                  .append(refContextStream.getName())
                  .append(", attribute=")
                  .append(attributeName)
                  .toString());
        }
        return attrDesc;
      }
      first = false;
    }
    // If not found in attributes list, look in the additional attributes list.
    if (refContextStream.getAdditionalAttributes() != null) {
      for (AttributeDesc attrDesc : refContextStream.getAdditionalAttributes()) {
        if (attrDesc.getName().equalsIgnoreCase(attributeName)) {
          return attrDesc;
        }
      }
    }

    // If not found yet, it is an error.
    throw new ConstraintViolationException(
        createExceptionMessage(
                "Enrichment attribute name must exist in remote context", tenantDesc, streamDesc)
            .append(", enrichment=")
            .append(enrichmentName)
            .append(", context=")
            .append(refContextStream.getName())
            .append(", attribute=")
            .append(attributeName)
            .toString());
  }

  // TODO(TFOS-1080): See below. Index stream name is <signal>.index.<view>
  private StreamDesc createIndexStream(TenantDesc tenantDesc, StreamDesc signalDesc, ViewDesc view)
      throws ConstraintViolationException {

    StreamDesc indexDesc =
        new StreamDesc(
            AdminUtils.makeIndexStreamName(signalDesc.getName(), view.getName()),
            signalDesc.getVersion(),
            false);
    indexDesc.setType(StreamType.INDEX);
    indexDesc.setParent(tenantDesc);
    indexDesc.setSchemaVersion(view.getSchemaVersion());

    for (String attr : view.getGroupBy()) {
      AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, signalDesc, attr, true, "attribute not found");

      indexDesc.addAttribute(attrDesc);
    }

    indexDesc.addView(view);

    return indexDesc;
  }

  // TODO(TFOS-1080): Concept of ingest view table should be encapsulated in DataEngine. This stream
  // is better to be a CassStream (better to rename to CassTable?) that is tied with the original
  // signal stream. For now, DataEngine needs to be able to resolve and navigate to this view
  // stream.
  //
  // In order to do so, there are constraints in naming view streams that are:
  // (req1) View stream name must not conflict with "user streams".
  // (req2) View stream should be reachable only using the signal stream info.
  //
  // This approach has following requirement, too:
  // (req3) View stream name may not conflict with any other stream names, including other types of
  // sub-stream with the same name.
  // (req4) View streams should not be retrievable by get_tenant() or get_stream() SDK API.
  // (req5) But these streams must be retrievable internally.
  //
  // Doing following is necessary to meet requirements above:
  // - view stream name is <signal>.view.<view>
  // - view config version is exactly the same with signal version
  //
  // Data Engine will find supported ingest views from property 'views'.
  private StreamDesc createViewStream(TenantDesc tenantDesc, StreamDesc signalDesc, ViewDesc view)
      throws ConstraintViolationException {

    StreamDesc viewDesc =
        new StreamDesc(
            AdminUtils.makeViewStreamName(signalDesc.getName(), view.getName()),
            signalDesc.getVersion(),
            false);
    viewDesc.setType(StreamType.VIEW);
    viewDesc.setParent(tenantDesc);
    viewDesc.setSchemaVersion(view.getSchemaVersion());

    for (String attr : view.getGroupBy()) {
      AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, signalDesc, attr, true, "attribute not found");

      viewDesc.addAttribute(attrDesc);
      // This is used for partition key in ingest view table
      viewDesc.addAdditionalAttribute(attrDesc);
    }
    if (viewDesc.getAdditionalAttributes() == null) {
      viewDesc.setAdditionalAttributes(new ArrayList<>());
    }

    for (String attr : view.getAttributes()) {
      AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, signalDesc, attr, true, "attribute not found");
      viewDesc.addAttribute(attrDesc);
    }

    viewDesc.addView(view);

    return viewDesc;
  }

  private StreamDesc createRollupStream(
      TenantDesc tenantDesc, StreamDesc signalDesc, ViewDesc view, Rollup rollup)
      throws ConstraintViolationException {

    final String rollupName = rollup.getName();

    // check name conflict
    if (!inInitialLoading
        && (signalDesc.getName().equalsIgnoreCase(rollupName)
            || tenantDesc.getStream(rollupName, false) != null)) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "rollup name may not conflict with a stream name", tenantDesc, signalDesc)
              .append(", view=")
              .append(view.getName())
              .append(", rollup=")
              .append(rollupName)
              .toString());
    }

    StreamDesc rollupDesc = new StreamDesc(rollupName, signalDesc.getVersion(), false);
    rollupDesc.setType(StreamType.ROLLUP);
    rollupDesc.setParent(tenantDesc);
    rollupDesc.setSchemaName(rollup.getSchemaName());
    rollupDesc.setSchemaVersion(rollup.getSchemaVersion());
    rollupDesc.addView(view.duplicate());
    rollupDesc.setRollupInterval(rollup.getInterval());
    rollupDesc.setRollupHorizon(rollup.getHorizon());
    rollupDesc.setFormatVersion(signalDesc.getFormatVersion());

    for (String attr : view.getGroupBy()) {
      final AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, signalDesc, attr, true, "attribute not found");
      rollupDesc.addAttribute(attrDesc);
    }

    setFeatureAttributes(tenantDesc, signalDesc, view, rollupDesc);

    rollupDesc.addView(view);

    return rollupDesc;
  }

  private void setFeatureAttributes(
      TenantDesc tenantDesc, StreamDesc streamDesc, ViewDesc view, StreamDesc featureStreamDesc)
      throws ConstraintViolationException {
    featureStreamDesc.addAttribute(new AttributeDesc(ROLLUP_STREAM_COUNT_ATTRIBUTE, LONG));

    if (view.hasGenericDigestion()) {
      return;
    }

    // add attributes for moments in case of the default moments rollup

    for (String attr : view.getAttributes()) {
      AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, streamDesc, attr, true, "attribute not found");

      if (!attrDesc.getAttributeType().isAddable()) {
        continue;
      }

      final String sumAttr = attrDesc.getName() + ROLLUP_STREAM_SUM_SUFFIX;
      featureStreamDesc.addAttribute(new AttributeDesc(sumAttr, attrDesc.getAttributeType()));

      final String minAttr = attrDesc.getName() + ROLLUP_STREAM_MIN_SUFFIX;
      featureStreamDesc.addAttribute(new AttributeDesc(minAttr, attrDesc.getAttributeType()));

      final String maxAttr = attrDesc.getName() + ROLLUP_STREAM_MAX_SUFFIX;
      featureStreamDesc.addAttribute(new AttributeDesc(maxAttr, attrDesc.getAttributeType()));
    }
  }

  /**
   * Verify and initialize features for a context.
   *
   * <p>This method must run after pre-process initialization since features depend on join
   * attributes.
   */
  private void initContextFeatures(
      TenantDesc tenantClone,
      StreamDesc contextDesc,
      List<StreamDesc> subStreams,
      boolean isModification)
      throws ConstraintViolationException, NoSuchTenantException {
    if (!contextDesc.isActive() || contextDesc.getFeatures() == null) {
      return;
    }
    if (contextDesc.getType() != StreamType.CONTEXT) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Context features are supported only for CONTEXT streams",
                  tenantClone,
                  contextDesc)
              .toString());
    }

    // build existing features
    final var existingFeatures = new HashMap<String, FeatureDesc>();
    if (isModification) {
      final var existingContext =
          tenants.getTenantDesc(tenantClone.getName(), false).getStream(contextDesc.getName());
      if (existingContext != null && existingContext.getFeatures() != null) {
        existingContext
            .getFeatures()
            .forEach(
                (featureConfig) -> {
                  existingFeatures.put(featureConfig.getName().toLowerCase(), featureConfig);
                });
      }
    }

    Set<String> uniqueFeatureNames = new HashSet<>();
    for (FeatureDesc feature : contextDesc.getFeatures()) {
      if (feature.getName() == null) {
        throw new ConstraintViolationException(
            createExceptionMessage("Feature name may not be null", tenantClone, contextDesc)
                .toString());
      }
      if (!uniqueFeatureNames.add(feature.getName().toLowerCase())) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    "featureName property may not have duplicates", tenantClone, contextDesc)
                .append(", feature=")
                .append(feature.getName())
                .toString());
      }
      FeatureValidator.validateContextFeature(tenantClone, contextDesc, feature);

      final var existingFeature = existingFeatures.get(feature.getName().toLowerCase());
      if (existingFeature != null
          && Objects.equals(feature.getDimensions(), existingFeature.getDimensions())
          && Objects.equals(feature.getAttributes(), existingFeature.getAttributes())) {
        // unchange version if there is no schema change
        feature.setBiosVersion(existingFeature.getBiosVersion());
      } else if (feature.getBiosVersion() == null) {
        feature.setBiosVersion(contextDesc.getSchemaVersion());
      }

      // Create context index descriptor if needed.
      if (feature.getIndexed() == Boolean.TRUE) {
        final StreamDesc indexDesc = createContextIndexStream(tenantClone, contextDesc, feature);
        contextDesc.addSubStreamName(indexDesc.getName());
        subStreams.add(indexDesc);
      }

      // Create context feature descriptor
      final var featureStreamDesc = createContextFeatureStream(tenantClone, contextDesc, feature);
      contextDesc.addSubStreamName(featureStreamDesc.getName());
      subStreams.add(featureStreamDesc);
    }
  }

  // Context index stream name is <context>.index.<feature>
  private StreamDesc createContextIndexStream(
      TenantDesc tenantDesc, StreamDesc contextDesc, FeatureDesc feature)
      throws ConstraintViolationException {

    StreamDesc indexDesc =
        new StreamDesc(
            AdminUtils.makeContextIndexStreamName(contextDesc.getName(), feature.getName()),
            contextDesc.getVersion(),
            false);
    indexDesc.setType(StreamType.CONTEXT_INDEX);
    indexDesc.setParent(tenantDesc);
    indexDesc.setParentStream(contextDesc);
    indexDesc.setSchemaVersion(feature.getBiosVersion());
    Long interval = feature.getTimeIndexInterval();
    if (interval == null) {
      interval = feature.getFeatureInterval();
    }
    indexDesc.setIndexWindowLength(interval);

    final Set<String> primaryKeyNames =
        contextDesc.getPrimaryKey().stream()
            .map((name) -> name.toLowerCase())
            .collect(Collectors.toSet());
    final Set<String> dimensions =
        feature.getDimensions().stream()
            .map((dimension) -> dimension.toLowerCase())
            .collect(Collectors.toSet());
    final boolean contextPrimaryKeyIncluded = dimensions.containsAll(primaryKeyNames);
    for (String dimension : feature.getDimensions()) {
      AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, contextDesc, dimension, true, "attribute not found");
      indexDesc.addAttribute(attrDesc);
      // Attributes included in the dimensions are part of the index key.
      indexDesc.addAdditionalAttribute(attrDesc);
    }
    // If context primary key is not part of the dimensions, add it to index key now.
    // This ensures that the records in the context index are indeed unique and
    // correspond 1:1 to entries in the context table.
    if (!contextPrimaryKeyIncluded) {
      for (var primaryKeyAttributeName : contextDesc.getPrimaryKey()) {
        if (dimensions.contains(primaryKeyAttributeName.toLowerCase())) {
          continue;
        }
        AttributeDesc attrDesc =
            findAttributeReference(
                tenantDesc, contextDesc, primaryKeyAttributeName, true, "attribute not found");
        indexDesc.addAttribute(attrDesc);
        indexDesc.addAdditionalAttribute(attrDesc);
      }
    }

    // Add the attributes asked to be included in the "value / data" part of the index.
    for (String attribute : feature.getAttributes()) {
      // Do not add the context primary key again even if it is specified in the attribute list.
      if (primaryKeyNames.contains(attribute.toLowerCase())) {
        continue;
      }
      AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, contextDesc, attribute, true, "attribute not found");
      indexDesc.addAttribute(attrDesc);
    }

    indexDesc.addFeature(feature);

    return indexDesc;
  }

  private StreamDesc createContextFeatureStream(
      TenantDesc tenantDesc, StreamDesc contextDesc, FeatureDesc feature)
      throws ConstraintViolationException {

    StreamDesc featureStreamDesc =
        new StreamDesc(
            AdminUtils.makeRollupStreamName(contextDesc.getName(), feature.getName()),
            contextDesc.getVersion(),
            false);
    featureStreamDesc.setType(StreamType.CONTEXT_FEATURE);
    featureStreamDesc.setParent(tenantDesc);
    featureStreamDesc.setParentStream(contextDesc);
    featureStreamDesc.setSchemaVersion(feature.getBiosVersion());
    final int intervalMs;
    if (feature.getFeatureInterval() != null) {
      intervalMs = feature.getFeatureInterval().intValue();
    } else {
      intervalMs = 0;
    }
    featureStreamDesc.setRollupInterval(new TimeInterval(intervalMs, TimeunitType.MILLISECOND));

    // Convert the feature to view to have RollupCassStream recognize the schema.
    final var view = new ViewDesc();
    view.setGroupBy(feature.getDimensions() != null ? feature.getDimensions() : List.of());
    view.setAttributes(
        feature.getAttributes().stream()
            .filter(
                (attribute) -> {
                  try {
                    AttributeDesc attributeDesc =
                        findAttributeReference(
                            tenantDesc, contextDesc, attribute, true, "attribute not found");
                    return attributeDesc.getAttributeType().isAddable();
                  } catch (ConstraintViolationException e) {
                    // should not happen
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList()));
    view.setIndexTableEnabled(feature.getIndexed());
    view.setIndexType(feature.getIndexType());
    featureStreamDesc.addView(view);

    // Build attributes
    for (String attr : view.getGroupBy()) {
      final AttributeDesc attrDesc =
          findAttributeReference(tenantDesc, contextDesc, attr, true, "attribute not found");
      featureStreamDesc.addAttribute(attrDesc);
    }
    setFeatureAttributes(tenantDesc, contextDesc, view, featureStreamDesc);

    return featureStreamDesc;
  }

  @Override
  public void reload() {
    for (AdminChangeListener subscriber : subscribers) {
      subscriber.unload();
    }
    initCache();
  }

  @Override
  public void maintenance() {
    tenants.cleanUp();
  }

  @Override
  public String resolveSignalName(String tenantName, String alias)
      throws NoSuchTenantException, NoSuchStreamException {
    final TenantDesc tenantDesc = tenants.getTenantDesc(tenantName, false);
    return tenantDesc.resolveSignalName(alias);
  }

  @Override
  public FeatureAsContextInfo getFeatureAsContextInfo(String tenantName, String contextName) {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(contextName);
    return tenants.getFeatureAsContextInfo(tenantName, contextName);
  }
}
