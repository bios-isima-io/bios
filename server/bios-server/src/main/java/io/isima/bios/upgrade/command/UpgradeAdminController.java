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
package io.isima.bios.upgrade.command;

import static io.isima.bios.models.RequestPhase.FINAL;
import static io.isima.bios.models.RequestPhase.INITIAL;
import static io.isima.bios.models.v1.StreamType.METRICS;
import static io.isima.bios.models.v1.StreamType.SIGNAL;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeAdminController {
  private static final String ALL_USER_TENANTS = "_ALL_USER_TENANTS";
  private static final Logger logger = LoggerFactory.getLogger(UpgradeAdminController.class);

  private final AdminInternal admin;

  public UpgradeAdminController(AdminInternal admin) {
    this.admin = admin;
  }

  void deleteSignal(String tenantName, String signalName) throws ApplicationException {
    if (tenantName.equalsIgnoreCase(ALL_USER_TENANTS)) {
      List<String> tenants = admin.getAllTenants();
      for (final var tenant : tenants) {
        if (tenant.equals(BiosConstants.TENANT_SYSTEM)) {
          continue;
        }
        removeSignalInternal(tenant, signalName);
      }
    } else {
      removeSignalInternal(tenantName, signalName);
    }
  }

  private void removeSignalInternal(String tenantName, String signalName)
      throws ApplicationException {
    try {
      logger.info("Deleting signal `{}` in tenant `{}`", signalName, tenantName);
      StreamDesc desc = admin.getStream(tenantName, signalName);
      if (desc.getType() != null
          && (desc.getType().equals(SIGNAL) || desc.getType().equals(METRICS))) {
        final long timestamp = System.currentTimeMillis();
        admin.removeStream(tenantName, signalName, INITIAL, timestamp);
        admin.removeStream(tenantName, signalName, FINAL, timestamp);
      } else {
        logger.warn("Stream `{}` in tenant `{}` is not a signal", signalName, tenantName);
      }
    } catch (NoSuchTenantException e) {
      logger.error("Tenant `{}` not found", tenantName);
      throw new ApplicationException("Unexpected Tenant Not Found exception");
    } catch (NoSuchStreamException e) {
      logger.info("Stream `{}` not found in tenant `{}`", signalName, tenantName);
    } catch (ConstraintViolationException e) {
      logger.error(
          "Unexpected Constraint violation exception {} for Stream `{}` in tenant `{}` ",
          e.getErrorMessage(),
          signalName,
          tenantName);
      throw new ApplicationException("Unexpected Constraint violation exception");
    }
  }
}
