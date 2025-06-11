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

// import io.isima.bios.admin.v1.StreamDesc;

import io.isima.bios.admin.v1.AdminOption;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.AvailableMetrics;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TagsMetadata;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.v1.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** AdminInternal internal module interface. */
public interface Admin {

  List<OperationWarning> createTenant(
      TenantConfig tenant, RequestPhase phase, Long timestamp, boolean isRetry)
      throws TfosException, ApplicationException;

  List<TenantConfig> getTenants(List<String> tenantNames, Set<AdminOption> options)
      throws TfosException;

  /**
   * Method to return tenant configuration.
   *
   * <p>When the switch detail is turned on, the method checks the user's information and returns
   * permitted properties.
   *
   * @param tenantName Tenant name
   * @param detail Flag to request details of tenant configuration. If false, the method returns
   *     only tenant name and version.
   * @param includeInternal Flag to request internal signals.
   * @param includeInferredTags Flag to include inferred tags.
   * @param permissions Permissions. The method checks these specified permissions and returns only
   *     allowed piece of tenant configuration. The parameter should be empty to allow all.
   * @return Tenant configuration
   * @throws NoSuchTenantException thrown when the target tenant does not exist
   * @throws InvalidRequestException thrown to indicate that the request is invalid
   * @throws ApplicationException thrown to indicate that an unexpected error happened
   */
  TenantConfig getTenant(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      List<Permission> permissions)
      throws NoSuchTenantException, InvalidRequestException, ApplicationException;

  void deleteTenant(String tenantName, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException;

  SignalConfig createSignal(
      String tenant, SignalConfig signalConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException;

  SignalConfig createSignalAllowingInternalAttributes(
      String tenant,
      SignalConfig signalConfig,
      RequestPhase phase,
      Long timestamp,
      Collection<String> internalAttributeNames)
      throws TfosException, ApplicationException;

  /**
   * Gets specified signals.
   *
   * @param tenantName Target tenant name.
   * @param detail Returned signals include detail properties if set true, otherwise the method
   *     returns only the property signalName.
   * @param includeInternal Internal signals are included in the reply when set to true.
   * @param includeInferredTags Flag to include inferred tags in the reply
   * @param signalNames Only the specified signals are returned when specified. If the value is
   *     null, the method returns all signals.
   * @return SignalConfig objects
   * @throws NoSuchTenantException thrown when the specified tenant exists.
   * @throws NoSuchStreamException thrown when any of the specified signals do not exist.
   * @throws InvalidRequestException thrown to indicate that the method is unable to conduct against
   *     specified signal. Possible reason would be that the internal stream configuration has
   *     attribute type that is not supported by SignalConfig, for example.
   */
  // TODO(Naoki): Is it better to make the interface more generic?
  List<SignalConfig> getSignals(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      Collection<String> signalNames)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException;

  /**
   * Get signals, ignores unsupported TFOS streams silently rather than throwing an exception.
   *
   * @param tenantName Target tenant name.
   * @param detail Returned signals include detail properties if set true, otherwise the method
   *     returns only the property signalName.
   * @param includeInternal Internal signals are included in the reply when set to true.
   * @param includeInferredTags Flag to include inferred tags in the reply
   * @param signalNames Only the specified signals are returned when specified. If the value is
   *     null, the method returns all signals.
   * @return SignalConfig objects
   * @throws NoSuchTenantException thrown when the specified tenant exists.
   * @throws NoSuchStreamException thrown when any of the specified signals do not exist.
   * @throws InvalidRequestException thrown to indicate that the method is unable to conduct against
   *     specified signal. Possible reason would be that the internal stream configuration has
   *     attribute type that is not supported by SignalConfig, for example.
   */
  List<SignalConfig> getSignalsIgnoreTfos(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      Collection<String> signalNames)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException;

  SignalConfig updateSignal(
      String tenant,
      String signalName,
      SignalConfig signalConfig,
      RequestPhase currentPhase,
      Long currentTimestamp)
      throws TfosException, ApplicationException;

  SignalConfig updateSignalAllowingInternalAttributes(
      String tenant,
      String signalName,
      SignalConfig signalConfig,
      RequestPhase currentPhase,
      Long currentTimestamp,
      Collection<String> internalAttributes,
      Set<AdminOption> options)
      throws TfosException, ApplicationException;

  void deleteSignal(
      String tenant, String signalName, RequestPhase currentPhase, Long currentTimestamp)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ConstraintViolationException,
          ApplicationException,
          InvalidRequestException;

  AvailableMetrics getAvailableMetrics(String tenant) throws TfosException;

  ContextConfig createContext(
      String tenant, ContextConfig contextConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException;

  List<ContextConfig> getContexts(
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      List<String> contextNames)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException;

  ContextConfig updateContext(
      String tenant,
      String contextName,
      ContextConfig contextConfig,
      RequestPhase currentPhase,
      Long currentTimestamp,
      Set<AdminOption> options)
      throws TfosException, ApplicationException;

  void deleteContext(
      String tenant, String contextName, RequestPhase currentPhase, Long currentTimestamp)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ConstraintViolationException,
          ApplicationException,
          InvalidRequestException;

  /**
   * Gets bios streams from the specified tenant.
   *
   * @param tenantName The tenant
   * @return List of BIOS streams (signals and contexts) in the tenant.
   * @throws NoSuchTenantException Thrown to indicate that the specified tenant does not exists.
   */
  List<BiosStreamConfig> getStreams(String tenantName) throws NoSuchTenantException;

  void updateInferredTags(
      String tenant, String stream, RequestPhase phase, Map<Short, AttributeTags> attributes)
      throws ApplicationException;

  TagsMetadata getSupportedTags();
}
