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

import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.TenantAppendixCategory;
import java.util.List;

/** Interface that serves CRUD operations for tenant appendix entities. */
public interface TenantAppendix {

  /**
   * Create an appendix entry.
   *
   * @param <T> Appendix entity class
   * @param tenantId Tenant ID where the appendix belongs
   * @param category Appendix category
   * @param entryId Appendix entry ID
   * @param newEntry The content of the new entry
   * @throws AlreadyExistsException thrown to indicate that an entry with the specified ID already
   *     exists.
   * @throws ApplicationException thrown to indicate that an unexpected error has happened.
   */
  <T> void createEntry(
      EntityId tenantId, TenantAppendixCategory category, String entryId, T newEntry)
      throws AlreadyExistsException, ApplicationException;

  /**
   * Get appendix entries in the specified tenant and category.
   *
   * @param <T> Appendix entity class.
   * @param tenantId Tenant ID where the appendix belongs
   * @param category Appendix category
   * @param clazz Class of the entities
   * @return List of entries in the category
   * @throws ApplicationException thrown to indicate that an unexpected error has happened.
   */
  <T> List<T> getEntries(EntityId tenantId, TenantAppendixCategory category, Class<T> clazz)
      throws ApplicationException;

  /**
   * Get an appendix entry.
   *
   * @param <T> Appendix entity class.
   * @param tenantId Tenant ID where the appendix belongs
   * @param category Category of the appendix
   * @param entryId The identifier of the entry
   * @param clazz Class of the entry
   * @return Found entry
   * @throws NoSuchEntityException thrown when the specified entry does not exist
   * @throws ApplicationException thrown to indicate that an unexpected error has happened.
   */
  <T> T getEntry(EntityId tenantId, TenantAppendixCategory category, String entryId, Class<T> clazz)
      throws NoSuchEntityException, ApplicationException;

  /**
   * Update an entry.
   *
   * @param <T> Appendix entry class.
   * @param tenantId Tenant ID where the appendix belongs
   * @param category Appendix category
   * @param entryId Appendix entry ID
   * @param newEntry The content of the new entry
   * @throws NoSuchEntityException thrown when the specified entry does not exist
   * @throws ApplicationException thrown to indicate that an unexpected error has happened.
   */
  <T> void updateEntry(
      EntityId tenantId, TenantAppendixCategory category, String entryId, T newEntry)
      throws NoSuchEntityException, ApplicationException;

  /**
   * Delete an entry.
   *
   * @param tenantId Tenant ID where the appendix belongs
   * @param category Appendix category
   * @param entryId Appendix entry ID
   * @throws ApplicationException thrown to indicate that an unexpected error has happened.
   */
  void deleteEntry(EntityId tenantId, TenantAppendixCategory category, String entryId)
      throws NoSuchEntityException, ApplicationException;

  /**
   * Deletes all the entries that belong to the specified tenant from the storage.
   *
   * <p>Underneath in the storage, history of any changes in appendixes is kept in records. Deletion
   * also happens in soft-deleting manner. This method clears all of the history. This operation
   * cannot be undone.
   *
   * @param tenantId Tenant to clear
   * @throws ApplicationException thrown to indicate that an unexpected error has happened
   */
  void hardDelete(EntityId tenantId) throws ApplicationException;

  // TODO(Naoki): Add methods for history management
}
