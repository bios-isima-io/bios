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
package io.isima.bios.admin;

import static io.isima.bios.models.TenantAppendixCategory.EXPORT_TARGETS;
import static io.isima.bios.models.TenantAppendixCategory.IMPORT_DATA_PROCESSORS;
import static io.isima.bios.models.TenantAppendixCategory.IMPORT_DESTINATIONS;
import static io.isima.bios.models.TenantAppendixCategory.IMPORT_FLOW_SPECS;
import static io.isima.bios.models.TenantAppendixCategory.IMPORT_SOURCES;

import io.isima.bios.admin.v1.AdminOption;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.apps.AppsManager;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeModAllowance;
import io.isima.bios.models.AttributeSummary;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.AvailableMetrics;
import io.isima.bios.models.AvailableMetrics.Attribute;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextConfigBuilder;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ImportDataProcessorConfig;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportDestinationStreamType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.MemberStatus;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.PositiveIndicator;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalConfigBuilder;
import io.isima.bios.models.TagsMetadata;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.Unit;
import io.isima.bios.models.UnitDisplayPosition;
import io.isima.bios.models.UnitModifier;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminImpl implements Admin {
  private static final Logger logger = LoggerFactory.getLogger(AdminImpl.class);

  private final io.isima.bios.admin.v1.impl.AdminImpl tfosAdmin;
  private final TenantAppendix tenantAppendix;
  private final AppsManager appsManager;
  private final Validators validators;

  public AdminImpl(
      io.isima.bios.admin.v1.impl.AdminImpl tfosAdmin,
      TenantAppendix tenantAppendix,
      AppsManager appsManager) {
    this.tfosAdmin = tfosAdmin;
    this.tenantAppendix = tenantAppendix;
    this.appsManager = appsManager;
    validators = new Validators();
  }

  @Override
  public List<OperationWarning> createTenant(
      TenantConfig tenant, RequestPhase phase, Long timestamp, boolean isRetry)
      throws TfosException, ApplicationException {
    try {
      validators.validateTenant(tenant);
      final var tfosTenant = Translators.toTfosLeast(tenant);
      tfosAdmin.addTenant(tfosTenant, phase, timestamp);
    } catch (ValidatorException e) {
      throw handleValidatorException(e);
    }
    return Collections.emptyList();
  }

  @Override
  public List<TenantConfig> getTenants(List<String> tenantNames, Set<AdminOption> options)
      throws TfosException {
    final var availableTenantNames = tfosAdmin.getAllTenants();
    final Set<String> specifiedNames;
    if (tenantNames != null) {
      specifiedNames = new LinkedHashSet<>();
      for (String name : tenantNames) {
        specifiedNames.add(name.toLowerCase());
      }
    } else {
      specifiedNames = null;
    }
    final var tenants = new ArrayList<TenantConfig>();
    for (String tenantName : availableTenantNames) {
      if (specifiedNames == null || specifiedNames.remove(tenantName.toLowerCase())) {
        try {
          final var tfosTenant = tfosAdmin.getTenant(tenantName);
          final var tenant = Translators.toBiosLeast(tfosTenant);
          // TODO(Naoki): Provide the functionality by the Translators class
          if (options.contains(AdminOption.DETAIL)) {
            tenant.setDomain(tfosTenant.getDomain());
            tenant.setAppMaster(tfosTenant.getAppMaster());
          }
          tenants.add(tenant);
        } catch (NoSuchTenantException e) {
          logger.warn(
              "Getting tenant failed during operating GetTenantNames,"
                  + " the tenant is likely to be deleted; {}",
              e.getMessage());
        }
      }
    }
    if (specifiedNames != null && !specifiedNames.isEmpty()) {
      throw new NoSuchTenantException(specifiedNames.toString());
    }
    return tenants;
  }

  @Override
  public TenantConfig getTenant(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      List<Permission> permissions)
      throws NoSuchTenantException, InvalidRequestException, ApplicationException {
    final var tenantDesc = tfosAdmin.getTenant(tenantName);
    final var tenantConfig = new TenantConfig();
    tenantConfig.setName(tenantDesc.getName());
    tenantConfig.setVersion(tenantDesc.getVersion());
    if (detail) {
      tenantConfig.setDomain(tenantDesc.getDomain());
    }
    // TODO(Naoki): Make this dynamic when we start supporting tenant status
    tenantConfig.setStatus(MemberStatus.ACTIVE);
    tenantConfig.setAppMaster(tenantDesc.getAppMaster());

    boolean signalReadable = AllowedRoles.isAllowed(AllowedRoles.SIGNAL_READ, permissions);
    boolean contextReadable = AllowedRoles.isAllowed(AllowedRoles.CONTEXT_READ, permissions);

    try {
      if (signalReadable) {
        tenantConfig.setSignals(
            getSignals(tenantName, detail, includeInternal, includeInferredTags, null));
      }
      if (contextReadable) {
        tenantConfig.setContexts(
            getContexts(tenantName, detail, includeInternal, includeInferredTags, null));
      }
    } catch (NoSuchStreamException e) {
      throw new ApplicationException("Unexpected NoSuchStream", e);
    }

    if (detail && signalReadable) {
      populateAppendixes(tenantConfig);
    }

    // TODO(Naoki): Populate users and domains.

    return tenantConfig;
  }

  @Override
  public void deleteTenant(String tenantName, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(phase);
    Objects.requireNonNull(timestamp);
    final var tfosTenant = new io.isima.bios.models.v1.TenantConfig();
    if (BiosConstants.TENANT_SYSTEM.equalsIgnoreCase(tenantName)) {
      throw new InvalidValueException(
          "Tenant " + BiosConstants.TENANT_SYSTEM + " cannot be deleted");
    }
    tfosTenant.setName(tenantName);
    final var biosTenant = this.getTenant(tenantName, false, false, false, List.of());
    tfosAdmin.removeTenant(tfosTenant, phase, timestamp);
    if (phase == RequestPhase.INITIAL) {
      // We deregister the apps for the tenant blindly.
      final var tenantId = new EntityId(biosTenant);
      try {
        appsManager.deregister(tenantId);
      } catch (NoSuchEntityException e) {
        // it's fine to get this since this is a blind deregistration
      }
    }
  }

  @Override
  public SignalConfig createSignal(
      String tenantName, SignalConfig signalConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException {
    return createSignalAllowingInternalAttributes(
        tenantName, signalConfig, phase, timestamp, Set.of());
  }

  @Override
  public SignalConfig createSignalAllowingInternalAttributes(
      String tenantName,
      SignalConfig signalConfig,
      RequestPhase phase,
      Long timestamp,
      Collection<String> internalAttributes)
      throws TfosException, ApplicationException {
    Objects.requireNonNull(tenantName, "parameter 'tenantName' must be set");
    Objects.requireNonNull(signalConfig, "parameter 'signalConfig' must be set");
    Objects.requireNonNull(phase, "parameter 'phase' must be set");
    Objects.requireNonNull(timestamp, "parameter 'timestamp' must be set");

    // TODO(Naoki): Avoid mutating the input object.
    signalConfig.setVersion(timestamp);

    tfosAdmin.addStream(
        tenantName, signalToStream(tenantName, signalConfig, internalAttributes), phase);

    if (phase == RequestPhase.FINAL) {
      try {
        return getSignals(tenantName, true, false, false, List.of(signalConfig.getName())).get(0);
      } catch (NoSuchTenantException | NoSuchStreamException | InvalidRequestException e) {
        logger.warn("Failed to fetch signal after creation", e);
      }
    }
    return signalConfig;
  }

  @Override
  public List<SignalConfig> getSignals(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      Collection<String> signalNames)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException {
    return getSignalsCore(
        tenantName, detail, includeInternal, includeInferredTags, signalNames, true);
  }

  @Override
  public List<SignalConfig> getSignalsIgnoreTfos(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      Collection<String> signalNames)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException {
    return getSignalsCore(
        tenantName, detail, includeInternal, includeInferredTags, signalNames, true);
  }

  public List<SignalConfig> getSignalsCore(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      Collection<String> signalNames,
      boolean ignoreTfos)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException {

    final var streams =
        this.getTfosStreams(
            tenantName, signalNames, includeInternal, StreamType.SIGNAL, StreamType.METRICS);

    // Translate the streams to signals and return
    final var signals = new ArrayList<SignalConfig>();
    for (var streamDesc : streams) {
      try {
        final var signal =
            SignalConfigBuilder.getBuilder(streamDesc)
                .least(!detail)
                .includeInferredTags(includeInferredTags)
                .build();
        signals.add(signal);
      } catch (InvalidRequestException e) {
        if (!ignoreTfos) {
          throw e;
        }
      }
    }
    return signals;
  }

  @Override
  public SignalConfig updateSignal(
      String tenantName,
      String signalName,
      SignalConfig signalConfig,
      RequestPhase phase,
      Long timestamp)
      throws TfosException, ApplicationException {
    return updateSignalAllowingInternalAttributes(
        tenantName, signalName, signalConfig, phase, timestamp, Set.of(), Set.of());
  }

  @Override
  public SignalConfig updateSignalAllowingInternalAttributes(
      String tenantName,
      String signalName,
      SignalConfig signalConfig,
      RequestPhase phase,
      Long timestamp,
      Collection<String> internalAttributes,
      Set<AdminOption> options)
      throws TfosException, ApplicationException {

    Objects.requireNonNull(tenantName, "tenantName");
    Objects.requireNonNull(signalName, "signalName");
    Objects.requireNonNull(signalConfig, "signalConfig");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(timestamp, "timestamp");

    // TODO(Naoki): Implement clone method to SignalConfig
    signalConfig.setVersion(timestamp);

    checkStreamType(tenantName, signalName, StreamType.SIGNAL);
    tfosAdmin.modifyStream(
        tenantName,
        signalName,
        signalToStream(tenantName, signalConfig, internalAttributes),
        phase,
        AttributeModAllowance.CONVERTIBLES_ONLY,
        options);

    // We'll take the signal config from the cache later. Enrichment fill-in values of proper
    // types are available only after the final phase is done. It's premature at this point.
    return null;
  }

  @Override
  public void deleteSignal(String tenantName, String signalName, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ConstraintViolationException,
          ApplicationException,
          InvalidRequestException {

    Objects.requireNonNull(tenantName, "tenantName");
    Objects.requireNonNull(signalName, "signalName");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(timestamp, "timestamp");

    checkStreamType(tenantName, signalName, StreamType.SIGNAL);
    checkFlows(tenantName, signalName, ImportDestinationStreamType.SIGNAL);

    tfosAdmin.removeStream(tenantName, signalName, phase, timestamp);
  }

  @Override
  public AvailableMetrics getAvailableMetrics(String tenant) throws TfosException {
    final var metrics = new AvailableMetrics();

    for (final var metricFunction : MetricFunction.values()) {
      if (metricFunction.getUnitModifier() == UnitModifier.NOT_APPLICABLE) {
        continue;
      }
      final var metric = new AvailableMetrics.Metric();
      metrics.getMetrics().add(metric);
      metric.setMetricFunction(metricFunction);
      metric.setUnitModifier(metricFunction.getUnitModifier());
    }

    final var signals =
        this.getTfosStreams(tenant, null, true, StreamType.SIGNAL, StreamType.METRICS);
    for (final var streamDesc : signals) {
      final var signal = new AvailableMetrics.Signal();
      metrics.getSignals().add(signal);
      signal.setSignalName(streamDesc.getName());
      signal.getMainMetrics().add(MetricFunction.COUNT);
      addAttributes(streamDesc, signal.getAttributes());
    }

    final var contexts = this.getTfosStreams(tenant, null, true, StreamType.CONTEXT);
    for (final var streamDesc : contexts) {
      final var context = new AvailableMetrics.Context();
      metrics.getContexts().add(context);
      context.setContextName(streamDesc.getName());
      context.getMainMetrics().add(MetricFunction.COUNT);
      addAttributes(streamDesc, context.getAttributes());
    }

    return metrics;
  }

  private void addAttributes(final StreamDesc streamDesc, final List<Attribute> attributesList) {
    final var commonMetricsNumeric =
        List.of(
            MetricFunction.SUM,
            MetricFunction.AVG,
            MetricFunction.MEDIAN,
            MetricFunction.DISTINCTCOUNT,
            MetricFunction.MIN,
            MetricFunction.MAX);
    final var remainingMetricsNumeric =
        List.of(
            MetricFunction.STDDEV,
            MetricFunction.VARIANCE,
            MetricFunction.P0_01,
            MetricFunction.P0_1,
            MetricFunction.P1,
            MetricFunction.P10,
            MetricFunction.P25,
            MetricFunction.P50,
            MetricFunction.P75,
            MetricFunction.P90,
            MetricFunction.P99,
            MetricFunction.P99_9,
            MetricFunction.P99_99,
            MetricFunction.SKEWNESS,
            MetricFunction.KURTOSIS,
            MetricFunction.SUM2,
            MetricFunction.SUM3,
            MetricFunction.SUM4,
            MetricFunction.DCLB1,
            MetricFunction.DCUB1,
            MetricFunction.DCLB2,
            MetricFunction.DCUB2,
            MetricFunction.DCLB3,
            MetricFunction.DCUB3,
            MetricFunction.NUMSAMPLES,
            MetricFunction.SAMPLINGFRACTION);
    final var commonMetricsString = List.of(MetricFunction.DISTINCTCOUNT);
    final var remainingMetricsString =
        List.of(
            MetricFunction.DCLB1,
            MetricFunction.DCUB1,
            MetricFunction.DCLB2,
            MetricFunction.DCUB2,
            MetricFunction.DCLB3,
            MetricFunction.DCUB3,
            MetricFunction.NUMSAMPLES,
            MetricFunction.SAMPLINGFRACTION);

    final var allAttributes = new ArrayList<>(streamDesc.getAttributes());
    final var additionalAttributes = streamDesc.getAdditionalAttributes();
    if (additionalAttributes != null) {
      allAttributes.addAll(additionalAttributes);
    }
    for (final var attributeDesc : allAttributes) {
      final var attribute = new AvailableMetrics.Attribute();
      attributesList.add(attribute);
      final var tags = attributeDesc.getEffectiveTags();
      final var metricsAdded = new ArrayList<MetricFunction>();
      final List<MetricFunction> commonMetrics;
      final List<MetricFunction> remainingMetrics;

      attribute.setAttributeName(attributeDesc.getName());
      attribute.setAttributeType(
          attributeDesc.getAttributeType().getBiosAttributeType().stringify());
      attribute.setUnit(tags.getUnit());
      attribute.setUnitDisplayName(tags.getUnitDisplayName());
      attribute.setUnitDisplayPosition(tags.getUnitDisplayPosition());
      attribute.setPositiveIndicator(tags.getPositiveIndicator());
      if ((tags.getFirstSummary() != null)
          && (tags.getFirstSummary().getMetricFunctionForSingleValue() != null)) {
        attribute.getMainMetrics().add(tags.getFirstSummary().getMetricFunctionForSingleValue());
        metricsAdded.add(tags.getFirstSummary().getMetricFunctionForSingleValue());
      }
      if ((tags.getSecondSummary() != null)
          && (tags.getSecondSummary().getMetricFunctionForSingleValue() != null)) {
        attribute.getMainMetrics().add(tags.getSecondSummary().getMetricFunctionForSingleValue());
        metricsAdded.add(tags.getSecondSummary().getMetricFunctionForSingleValue());
      }
      if ((attributeDesc.getAttributeType() == InternalAttributeType.LONG)
          || (attributeDesc.getAttributeType() == InternalAttributeType.DOUBLE)) {
        commonMetrics = commonMetricsNumeric;
        remainingMetrics = remainingMetricsNumeric;
      } else if ((attributeDesc.getAttributeType() == InternalAttributeType.STRING)
          || (attributeDesc.getAttributeType() == InternalAttributeType.ENUM)
          || (attributeDesc.getAttributeType() == InternalAttributeType.BOOLEAN)) {
        commonMetrics = commonMetricsString;
        remainingMetrics = remainingMetricsString;
      } else {
        commonMetrics = Collections.emptyList();
        remainingMetrics = Collections.emptyList();
      }
      for (final var metric : commonMetrics) {
        if (!metricsAdded.contains(metric)) {
          attribute.getCommonMetrics().add(metric);
          metricsAdded.add(metric);
        }
      }
      for (final var metric : remainingMetrics) {
        if (!metricsAdded.contains(metric)) {
          attribute.getRemainingMetrics().add(metric);
          metricsAdded.add(metric);
        }
      }
    }
  }

  @Override
  public ContextConfig createContext(
      String tenantName, ContextConfig contextConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException {

    Objects.requireNonNull(tenantName, "parameter 'tenantName' must be set");
    Objects.requireNonNull(contextConfig, "parameter 'contextConfig' must be set");
    Objects.requireNonNull(phase, "parameter 'phase' must be set");
    Objects.requireNonNull(timestamp, "parameter 'timestamp' must be set");

    // TODO(Naoki): Avoid mutating the input object.
    contextConfig.setVersion(timestamp);

    tfosAdmin.addStream(tenantName, contextToStream(tenantName, contextConfig), phase);

    if (phase == RequestPhase.FINAL) {
      try {
        return getContexts(tenantName, true, true, false, List.of(contextConfig.getName())).get(0);
      } catch (NoSuchTenantException | NoSuchStreamException | InvalidRequestException e) {
        logger.warn("Failed to fetch context after creation", e);
      }
    }
    return contextConfig;
  }

  @Override
  public List<ContextConfig> getContexts(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      List<String> contextNames)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException {
    final var streams =
        getTfosStreams(tenantName, contextNames, includeInternal, StreamType.CONTEXT);

    // Translate the streams to signals and return
    final var contexts = new ArrayList<ContextConfig>();
    for (var streamDesc : streams) {
      contexts.add(
          ContextConfigBuilder.getBuilder(streamDesc)
              .least(!detail)
              .includeInferredTags(includeInferredTags)
              .build());
    }
    return contexts;
  }

  @Override
  public ContextConfig updateContext(
      String tenantName,
      String contextName,
      ContextConfig contextConfig,
      RequestPhase phase,
      Long timestamp,
      Set<AdminOption> options)
      throws TfosException, ApplicationException {

    Objects.requireNonNull(tenantName, "tenantName");
    Objects.requireNonNull(contextName, "contextName");
    Objects.requireNonNull(contextConfig, "contextConfig");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(timestamp, "timestamp");

    // TODO(Naoki): Implement clone method to ContextConfig
    contextConfig.setVersion(timestamp);

    checkStreamType(tenantName, contextName, StreamType.CONTEXT);
    tfosAdmin.modifyStream(
        tenantName,
        contextName,
        contextToStream(tenantName, contextConfig),
        phase,
        AttributeModAllowance.PROHIBITED,
        options);

    if (phase == RequestPhase.INITIAL) {
      return contextConfig;
    } else {
      return getContexts(tenantName, true, true, false, List.of(contextName)).get(0);
    }
  }

  @Override
  public void deleteContext(
      String tenantName, String contextName, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ConstraintViolationException,
          ApplicationException,
          InvalidRequestException {

    Objects.requireNonNull(tenantName, "tenantName");
    Objects.requireNonNull(contextName, "contextName");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(timestamp, "timestamp");

    checkStreamType(tenantName, contextName, StreamType.CONTEXT);
    checkFlows(tenantName, contextName, ImportDestinationStreamType.CONTEXT);
    tfosAdmin.removeStream(tenantName, contextName, phase, timestamp);
  }

  @Override
  public List<BiosStreamConfig> getStreams(String tenantName) throws NoSuchTenantException {
    try {
      final var tfosStreams =
          this.getTfosStreams(tenantName, null, false, StreamType.SIGNAL, StreamType.CONTEXT);
      final var biosStreams = new ArrayList<BiosStreamConfig>(tfosStreams.size());
      for (var streamDesc : tfosStreams) {
        try {
          if (streamDesc.getType() == StreamType.SIGNAL) {
            biosStreams.add(SignalConfigBuilder.getBuilder(streamDesc).build());
          } else {
            biosStreams.add(ContextConfigBuilder.getBuilder(streamDesc).build());
          }
        } catch (InvalidRequestException e) {
          // This shouldn't happen as long as the tenant has only BIOS supported streams.
        }
      }
      return biosStreams;
    } catch (NoSuchStreamException e) {
      // shouldn't happen
      throw new RuntimeException(e);
    }
  }

  /**
   * This method is called in the initial phase by the data engine maintenance job that infers
   * attribute tags. This method then does fan routing to all servers with the final phase. In the
   * final phase each server updates their in-memory stream with the inferred attribute tags.
   */
  @Override
  public void updateInferredTags(
      final String tenant,
      final String stream,
      RequestPhase phase,
      final Map<Short, AttributeTags> attributes)
      throws ApplicationException {

    tfosAdmin.updateInferredTags(tenant, stream, phase, attributes);

    if (phase == RequestPhase.INITIAL) {
      // In the initial phase we need to do fan routing of the final phase here.
      BiosModules.getInferredTagsDistributor().doFanRouting(tenant, stream, attributes);
    }
  }

  @Override
  public TagsMetadata getSupportedTags() {
    final var metadata = new TagsMetadata();

    metadata.setCategories(List.of(AttributeCategory.values()));
    metadata.setUnitDisplayPositions(List.of(UnitDisplayPosition.values()));
    metadata.setPositiveIndicators(List.of(PositiveIndicator.values()));
    metadata.setSummaries(List.of(AttributeSummary.values()));

    final var kindsAndUnits = new HashMap<AttributeKind, List<TagsMetadata.UnitDetails>>();
    metadata.setKindsAndUnits(kindsAndUnits);

    for (final var unit : Unit.values()) {
      final var kind = unit.getKind();
      final var unitList = kindsAndUnits.computeIfAbsent(kind, (k) -> new ArrayList<>());
      if (unit == Unit.OTHER_UNIT) {
        continue;
      }
      final var unitDetails = new TagsMetadata.UnitDetails();
      unitDetails.setUnit(unit);
      unitDetails.setUnitDisplayName(unit.getDisplayName());
      unitDetails.setUnitDisplayPosition(AttributeKind.getUnitDisplayPosition(unit.getKind()));
      unitDetails.setPositiveIndicator(AttributeKind.getPositiveIndicator(unit.getKind()));
      unitDetails.setFirstSummary(Unit.getFirstSummary(AttributeCategory.QUANTITY, unit));
      unitDetails.setSecondSummary(Unit.getSecondSummary(AttributeCategory.QUANTITY, unit));
      unitList.add(unitDetails);
    }

    return metadata;
  }

  // Private methods ///////////////////////////////////////////////////////////////////////

  private List<StreamDesc> getTfosStreams(
      String tenantName, Collection<String> names, boolean includeInternal, StreamType... types)
      throws NoSuchTenantException, NoSuchStreamException {
    final var tenant = tfosAdmin.getTenant(tenantName);

    final boolean myIncludeInternal = includeInternal || (names != null && !names.isEmpty());

    // Prepare target signals filter
    final Map<String, String> targets = names != null ? new LinkedHashMap<>() : null;
    if (targets != null) {
      names.forEach(name -> targets.put(name.toLowerCase(), name));
    }

    List<StreamType> targetTypes = Arrays.asList(types);
    final Predicate<StreamDesc> predicates =
        (stream) -> {
          final var name = stream.getName().toLowerCase();
          final var type = stream.getType();
          final boolean isDesiredType = targetTypes.contains(type);
          final boolean includes = myIncludeInternal || !stream.isInternal();
          final var verdict =
              isDesiredType && includes && (targets == null || targets.containsKey(name));
          return verdict;
        };

    // Filter streams
    final List<StreamDesc> streams = tenant.getStreams(predicates);

    // Check if there are any missing signals in the specification
    if (targets != null && targets.size() != streams.size()) {
      for (var stream : streams) {
        targets.remove(stream.getName().toLowerCase());
      }
      throw new NoSuchStreamException(targets.values().toString());
    }

    return streams;
  }

  /**
   * Method to check whether the specified stream exists of the expected type.
   *
   * <p>Even if the stream exists, the method throws NoSuchStream exception when the expected type
   * mismatches.
   *
   * @param tenantName Target tenant name
   * @param streamName Target stream name
   * @param streamType Target stream type
   * @throws NoSuchTenantException thrown to indicate that the specified tenant does not exist.
   * @throws NoSuchStreamException thrown to indicate that the specified stream does not exist, or
   *     that the stream exists but the expected type mismatches.
   */
  private void checkStreamType(String tenantName, String streamName, StreamType streamType)
      throws NoSuchStreamException, NoSuchTenantException {
    final var streamDesc = tfosAdmin.getStream(tenantName, streamName);
    if (streamDesc.getType() != streamType) {
      throw new NoSuchStreamException(
          String.format(
              "Stream %s.%s found but its type %s does not match expected %s",
              tenantName, streamName, streamDesc.getType().name(), streamType.name()));
    }
  }

  private StreamConfig signalToStream(
      String tenantName, SignalConfig src, Collection<String> internalAttributes)
      throws TfosException {
    try {
      validators.validateSignal(tenantName, src, internalAttributes);
      src.removeInferredInformation();
      final var streamConfig = Translators.toTfos(src);
      return streamConfig;
    } catch (ValidatorException e) {
      throw handleValidatorException(e);
    }
  }

  private StreamConfig contextToStream(String tenantName, ContextConfig src) throws TfosException {
    try {
      validators.validateContext(tenantName, src);
      src.removeInferredInformation();
      final var streamConfig = Translators.toTfos(src);
      return streamConfig;
    } catch (ValidatorException e) {
      throw handleValidatorException(e);
    }
  }

  private TfosException handleValidatorException(ValidatorException e) {
    switch (e.getErrorType()) {
      case CONSTRAINT_VIOLATION:
        return new ConstraintViolationException(e);
      case INVALID_VALUE:
        return new InvalidValueException(e);
      case NOT_IMPLEMENTED:
        return new NotImplementedException(e);
      default:
        return new TfosException(e.getMessage(), e);
    }
  }

  private void populateAppendixes(TenantConfig tenantConfig) throws ApplicationException {
    if (tenantAppendix == null) {
      // for test code that does not set up appendix
      return;
    }
    final EntityId tenantId = new EntityId(tenantConfig);

    tenantConfig.setImportSources(
        tenantAppendix.getEntries(tenantId, IMPORT_SOURCES, ImportSourceConfig.class));

    tenantConfig.setImportDestinations(
        tenantAppendix.getEntries(tenantId, IMPORT_DESTINATIONS, ImportDestinationConfig.class));

    tenantConfig.setImportFlowSpecs(
        tenantAppendix.getEntries(tenantId, IMPORT_FLOW_SPECS, ImportFlowConfig.class));

    tenantConfig.setImportDataProcessors(
        tenantAppendix.getEntries(
            tenantId, IMPORT_DATA_PROCESSORS, ImportDataProcessorConfig.class));

    tenantConfig.setExportDestinations(
        tenantAppendix.getEntries(tenantId, EXPORT_TARGETS, ExportDestinationConfig.class));
  }

  private void checkFlows(
      String tenantName, String streamName, ImportDestinationStreamType streamType)
      throws NoSuchTenantException,
          ApplicationException,
          InvalidRequestException,
          ConstraintViolationException {
    if (tenantAppendix != null) {
      final var tenantConfig = getTenant(tenantName, true, true, false, List.of());
      final var tenantId = new EntityId(tenantConfig);
      final var flows =
          tenantAppendix.getEntries(tenantId, IMPORT_FLOW_SPECS, ImportFlowConfig.class);
      final var dependants = new ArrayList<String>();
      for (var flow : flows) {
        final var destSpec = flow.getDestinationDataSpec();
        if (destSpec != null
            && destSpec.getType() == streamType
            && streamName.equalsIgnoreCase(destSpec.getName())) {
          dependants.add(
              String.format("%s (%s)", flow.getImportFlowName(), flow.getImportFlowId()));
        }
      }
      String message = null;
      if (dependants.size() >= 2) {
        final var flowNames = dependants.stream().collect(Collectors.joining(", ", "[", "]"));
        message =
            String.format(
                "Import flows to the %s exist. Delete them first; flows=%s",
                streamType.name().toLowerCase(), flowNames);
      } else if (dependants.size() == 1) {
        message =
            String.format(
                "An import flow to the %s exists. Delete it first; flow=%s",
                streamType.name().toLowerCase(), dependants.get(0));
      }
      if (message != null) {
        throw new ConstraintViolationException(message);
      }
    }
  }
}
