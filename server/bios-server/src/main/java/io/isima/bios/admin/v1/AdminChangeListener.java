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

import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.RequestPhase;

/** Interface to subscribe for admin changes. */
public interface AdminChangeListener {

  /**
   * Notification method to be called when a tenant is being created.
   *
   * <p>The implementation of this method must not modify incoming parameter in place. Duplicate the
   * input object when modification is necessary.
   *
   * @param tenantDesc Tenant config object to be added.
   * @param phase AdminInternal operation execution phase; Update persistency for INITIAL. Update
   *     in-memory cache for FINAL.
   * @throws ApplicationException When unexpected error happens.
   */
  // TODO(Naoki): ConstraintViolationException shouldn't be thrown by dependent modules ideally.
  // It rather should be detected before this hook is invoked.
  // This is due to BIOS2 implementation that borrows TFOS AdminImpl for its AdminInternal
  // implementation. The constraint violation may happen there to run BIOS2 specific config
  // validation in this hook.
  void createTenant(TenantDesc tenantDesc, RequestPhase phase)
      throws ApplicationException, ConstraintViolationException;

  /**
   * Notification method to be called when a tenant is being deleted.
   *
   * @param tenantDesc Target tenant config
   * @param phase AdminInternal operation execution phase; Update persistency for INITIAL. Update
   *     in-memory cache for FINAL.
   * @throws NoSuchTenantException When target tenant does not exist.
   * @throws ApplicationException When unexpected error happens.
   */
  void deleteTenant(TenantDesc tenantDesc, RequestPhase phase)
      throws NoSuchTenantException, ApplicationException;

  /**
   * Notification method to be called when a stream is being created.
   *
   * <p>The implementation of this method must not modify incoming parameter in place. Duplicate the
   * input object when modification is necessary.
   *
   * @param tenantName Target tenant name.
   * @param streamDesc Stream config object to be added.
   * @param phase AdminInternal operation execution phase; Update persistency for INITIAL. Update
   *     in-memory cache for FINAL.
   * @throws NoSuchTenantException When target tenant does not exist.
   * @throws ApplicationException When unexpected error happens.
   */
  void createStream(String tenantName, StreamDesc streamDesc, RequestPhase phase)
      throws NoSuchTenantException, ConstraintViolationException, ApplicationException;

  /**
   * Notification method to be called when a stream is being deleted.
   *
   * <p>The implementation of this method must not modify incoming parameter in place. Duplicate the
   * input object when modification is necessary.
   *
   * @param tenantName Target tenant name.
   * @param streamDesc Stream config object to be deleted.
   * @param phase AdminInternal operation execution phase; Update persistency for INITIAL. Update
   *     in-memory cache for FINAL.
   * @throws NoSuchTenantException When target tenant does not exist.
   * @throws NoSuchStreamException When target stream does not exist.
   * @throws ApplicationException When unexpected error happens.
   */
  void deleteStream(String tenantName, StreamDesc streamDesc, RequestPhase phase)
      throws NoSuchTenantException, NoSuchStreamException, ApplicationException;

  /**
   * Method to be called when the AdminInternal component is reloading.
   *
   * <p>The implementing method should flush any in-memory cache of the class.
   */
  void unload();
}
