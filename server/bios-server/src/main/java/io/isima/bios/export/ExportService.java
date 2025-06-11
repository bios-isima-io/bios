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
package io.isima.bios.export;

import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.ExportDestinationConfig;

public interface ExportService {
  ExportDestinationConfig createDestination(String tenantName, ExportDestinationConfig config)
      throws TfosException, ApplicationException;

  ExportDestinationConfig updateDestination(
      String tenantName, String exportDestinationIdId, ExportDestinationConfig config)
      throws TfosException, ApplicationException;

  ExportDestinationConfig getDestination(String tenantName, String exportDestinationId)
      throws ApplicationException,
          NoSuchTenantException,
          InvalidRequestException,
          NoSuchEntityException;

  ExportDestinationConfig getDestination(
      String tenantName, Long tenantVersion, String exportDestinationId)
      throws ApplicationException, NoSuchEntityException;

  void deleteDestination(String tenantName, String exportDestinationName)
      throws NoSuchTenantException,
          ApplicationException,
          InvalidRequestException,
          NoSuchEntityException,
          ConstraintViolationException;

  void startService(String tenantName, String exportTargetName)
      throws TfosException, ApplicationException;

  void stopService(String tenantName, String exportTargetName)
      throws TfosException, ApplicationException;

  void shutdown();
}
