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
package io.isima.bios.admin.v1;

import io.isima.bios.errors.exception.AdminChangeRequestToSameException;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidDefaultValueException;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.StreamAlreadyExistsException;
import io.isima.bios.errors.exception.TenantAlreadyExistsException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AttributeModAllowance;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AdminInternal {
  /**
   * Method to retrieve all tenant names.
   *
   * @return list Tenant names list. An empty list is returned if there is no tenant available.
   */
  public List<String> getAllTenants();

  /**
   * Method to retrieve tenant configuration. Case is ignored in matching tenant name.
   *
   * @param tenant Tenant name
   * @return Tenant description
   * @throws NoSuchTenantException Tenant does not exist
   */
  TenantDesc getTenant(String tenant) throws NoSuchTenantException;

  /**
   * Method to retrieve tenant configuration. Case is ignored in matching tenant name.
   *
   * <p>Unlike {@link #getTenant(String)}, this method returns null when the target tenant is not
   * found.
   *
   * @param tenant Tenant name
   * @return Tenant description if the target tenant is found. Null otherwise.
   */
  TenantDesc getTenantOrNull(String tenant);

  /**
   * Add new tenant.
   *
   * @param tenantConfig Tenant add request object
   * @param phase AdminInternal write operation execution phase
   * @param timestamp Timestamp to be used as the stream version. The value must be consistent among
   *     execution phases.
   * @throws TenantAlreadyExistsException Stream does not exist
   * @throws ApplicationException When unexpected error happened
   * @throws ConstraintViolationException When input violates constraint check
   * @throws InvalidDefaultValueException When stream configuration has an invalid default value.
   * @throws InvalidEnumException When an entry in an enum attribute is misconfigured.
   * @throws InvalidValueException When an invalid configuration value is specified
   */
  void addTenant(TenantConfig tenantConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException;

  /**
   * Modify existing tenant.
   *
   * <p>The method replaces existing tenant configuration by specified one.
   *
   * @param tenantName Tenant name
   * @param tenantConfig New tenant config
   * @throws NoSuchTenantException There is no existing tenant with the specified name.
   * @throws ApplicationException Unexpected error happened.
   * @throws ConstraintViolationException New tenant config violated constraint check.
   * @throws TenantAlreadyExistsException In case of renaming, a tenant with destination name should
   *     not exist. The method throws this exception if there is one.
   * @throws AdminChangeRequestToSameException When the specified tenantConfig is identical to
   *     existing one.
   * @throws InvalidDefaultValueException When stream configuration has an invalid default value.
   * @throws InvalidEnumException When an entry in an enum attribute is misconfigured.
   * @throws InvalidValueException When an invalid configuration value is specified
   */
  void modifyTenant(
      String tenantName, TenantConfig tenantConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException;

  /**
   * Delete existing tenant.
   *
   * @param tenantConfig tenant config to remove
   * @param phase AdminInternal write operation execution phase
   * @param timestamp Timestamp to be used as the stream version. The value must be consistent among
   *     execution phases.
   * @throws ApplicationException When unexpected error happened
   * @throws ConstraintViolationException When input violates constraint check
   */
  void removeTenant(TenantConfig tenantConfig, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException, ApplicationException, ConstraintViolationException;

  /**
   * Method to retrieve a stream descriptor. Case is ignored in matching tenant and stream name.
   *
   * @param tenant Tenant name
   * @param stream Stream name
   * @return stream Tenant configuration
   * @throws NoSuchStreamException Stream does not exist
   * @throws NoSuchTenantException Tenant does not exist
   */
  StreamDesc getStream(String tenant, String stream)
      throws NoSuchStreamException, NoSuchTenantException;

  /**
   * Retrieve a stream descriptor by tenant name, stream name, and stream version.
   *
   * @param tenant Tenant name
   * @param stream Stream name
   * @param version Stream version
   * @return Found stream descriptor
   * @throws NoSuchStreamException Stream does not exist
   * @throws NoSuchTenantException Tenant does not exist
   */
  default StreamDesc getStream(String tenant, String stream, Long version)
      throws NoSuchStreamException, NoSuchTenantException {
    return null;
  }

  /**
   * Retrieve a stream descriptor by tenant name, stream name, and stream version.
   *
   * @param tenant Tenant name
   * @param stream Stream name
   * @param version Stream version
   * @return Stream descriptor if found. The method returns null if the target stream does not exist
   *     or it has been removed already.
   */
  default StreamDesc getStreamOrNull(String tenant, String stream, Long version) {
    return null;
  }

  /**
   * Retrieve a stream descriptor by tenant name, stream name.
   *
   * @param tenant Tenant name
   * @param stream Stream name
   * @return Stream descriptor if found. The method returns null if the target stream does not exist
   *     or it has been removed already.
   */
  default StreamDesc getStreamOrNull(String tenant, String stream) {
    return null;
  }

  /**
   * Method to list stream names.
   *
   * @param tenant Target tenant to list streams.
   * @param streamType Type of stream
   * @return Stream names that belong to specified tenant.
   * @throws NoSuchTenantException When specified tenant does not exist.
   */
  List<String> listStreams(String tenant, StreamType streamType) throws NoSuchTenantException;

  /**
   * Add new stream config to tenant.
   *
   * @param tenant Tenant name
   * @param streamConfig Stream configuration
   * @param phase AdminInternal write operation execution phase.
   * @param timestamp Timestamp to be used as the stream version. The value must be consistent among
   *     execution phases.
   * @throws NoSuchTenantException Tenant does not exist
   * @throws StreamAlreadyExistsException Stream does not exist
   * @throws ConstraintViolationException When operation is unable to execute due to a failure in
   *     constraint check
   * @throws InvalidDefaultValueException When stream configuration has an invalid default value.
   * @throws InvalidEnumException When an entry in an enum attribute is misconfigured.
   * @throws InvalidValueException When an invalid configuration value is specified
   */
  @Deprecated
  void addStream(String tenant, StreamConfig streamConfig, RequestPhase phase, Long timestamp)
      throws TfosException, ApplicationException;

  /**
   * Add new stream config to tenant.
   *
   * @param tenant Tenant name
   * @param streamConfig Stream configuration
   * @param phase AdminInternal write operation execution phase.
   * @throws NoSuchTenantException Tenant does not exist
   * @throws StreamAlreadyExistsException Stream does not exist
   * @throws ConstraintViolationException When operation is unable to execute due to a failure in
   *     constraint check
   * @throws InvalidDefaultValueException When stream configuration has an invalid default value.
   * @throws InvalidEnumException When an entry in an enum attribute is misconfigured.
   * @throws InvalidValueException When an invalid configuration value is specified
   */
  default void addStream(String tenant, StreamConfig streamConfig, RequestPhase phase)
      throws TfosException, ApplicationException {
    addStream(tenant, streamConfig, phase, streamConfig.getVersion());
  }

  /**
   * Modify existing stream configuration.
   *
   * @param tenant Tenant name
   * @param streamConfig Stream configuration
   * @param phase Request phase
   * @param timestamp Timestamp to be used as the stream version. The value must be consistent among
   *     execution phases.
   * @param attributeModAllowance Attribute type modification allowance
   * @param options Execution options
   * @throws NoSuchTenantException Tenant does not exist
   * @throws NoSuchStreamException Stream does not exist
   * @throws ConstraintViolationException Constraint check failed.
   * @throws StreamAlreadyExistsException In case of renaming, an config entry with destination name
   *     should not exist. If there is, the method throws this exception.
   * @throws ApplicationException Unexpected internal error occurred.
   * @throws AdminChangeRequestToSameException When the specified tenantConfig is identical to
   *     existing one.
   * @throws InvalidDefaultValueException When stream configuration has an invalid default value.
   * @throws InvalidEnumException When an entry in an enum attribute is misconfigured.
   */
  @Deprecated
  default void modifyStream(
      String tenant,
      String stream,
      StreamConfig streamConfig,
      RequestPhase phase,
      Long timestamp,
      AttributeModAllowance attributeModAllowance,
      Set<AdminOption> options)
      throws TfosException, ApplicationException {
    streamConfig.setVersion(timestamp);
    modifyStream(tenant, stream, streamConfig, phase, attributeModAllowance, options);
  }

  /**
   * Modify existing stream configuration.
   *
   * @param tenant Tenant name
   * @param streamConfig Stream configuration
   * @param phase Request phase
   * @param attributeModAllowance Attribute type modification allowance
   * @param options Execution options
   * @throws NoSuchTenantException Tenant does not exist
   * @throws NoSuchStreamException Stream does not exist
   * @throws ConstraintViolationException Constraint check failed.
   * @throws StreamAlreadyExistsException In case of renaming, an config entry with destination name
   *     should not exist. If there is, the method throws this exception.
   * @throws ApplicationException Unexpected internal error occurred.
   * @throws AdminChangeRequestToSameException When the specified tenantConfig is identical to
   *     existing one.
   * @throws InvalidDefaultValueException When stream configuration has an invalid default value.
   * @throws InvalidEnumException When an entry in an enum attribute is misconfigured.
   */
  void modifyStream(
      String tenant,
      String stream,
      StreamConfig streamConfig,
      RequestPhase phase,
      AttributeModAllowance attributeModAllowance,
      Set<AdminOption> options)
      throws TfosException, ApplicationException;

  /**
   * Remove existing stream configuration.
   *
   * @param tenant Tenant name
   * @param stream Stream name
   * @param phase AdminInternal write operation execution phase.
   * @param timestamp Timestamp to be used as the stream version. The value must be consistent among
   *     execution phases.
   * @throws NoSuchTenantException Tenant does not exist
   * @throws NoSuchStreamException Stream does not exist
   * @throws ConstraintViolationException Constraint violation
   * @throws ApplicationException When unexpected error happens
   */
  void removeStream(String tenant, String stream, RequestPhase phase, Long timestamp)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ConstraintViolationException,
          ApplicationException;

  /**
   * Method to retrieve the list of pre-processes that the ingestion processor needs to execute.
   *
   * @param tenant Tenant name
   * @param stream Stream name
   * @return Array list of process stages
   */
  default List<ProcessStage> getPreProcesses(String tenant, String stream)
      throws NoSuchTenantException, NoSuchStreamException {
    return null;
  }

  void updateInferredTags(
      String tenant, String stream, RequestPhase phase, Map<Short, AttributeTags> attributes)
      throws ApplicationException;

  /**
   * Reload AdminInternal entities.
   *
   * <p>The method is called for refreshing in-memory states of the AdminInternal component.
   */
  void reload();

  /** Executes module maintenance. */
  default void maintenance() {}

  /**
   * Method that resolves a stream name by its alias.
   *
   * <p>This method is meant to be used by BI methods. A BI client specifies a stream by its alias.
   * This method provides the mean to find the actual stream name from the specified alias.
   *
   * @param tenantName Tenant name
   * @param alias Signal alias
   * @return Signal name
   * @throws NoSuchTenantException When the target tenant is not found
   * @throws NoSuchStreamException When the target signal is not found
   */
  default String resolveSignalName(String tenantName, String alias)
      throws NoSuchTenantException, NoSuchStreamException {
    return alias;
  }

  /**
   * @return FeatureAsContext information if there is any referring feature, null if there is no
   *     referring feature for the context.
   */
  default FeatureAsContextInfo getFeatureAsContextInfo(String tenantName, String contextName) {
    return null;
  }
}
