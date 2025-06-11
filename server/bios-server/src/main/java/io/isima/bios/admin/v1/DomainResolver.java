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

import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.repository.auth.OrganizationRepository;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module to resolve domains for tenant names.
 *
 * <p>This modules is used for accepting delegation requests from apps. Some apps are simpler to
 * implement if they use their domain names to identify requesters' realms than keeping
 * bios-generated tenant names on their side.
 *
 * <p>Domain to tenant conversion table is updated when AdminInternal creates, loads, or deletes a
 * tenant using the AdminChangeListener mechanism.
 */
public class DomainResolver implements AdminChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(DomainResolver.class);

  private final OrganizationRepository organizationRepository;

  private final Map<String, String> domainToTenant;

  public DomainResolver(OrganizationRepository organizationRepository) {
    this.organizationRepository = organizationRepository;
    domainToTenant = new ConcurrentHashMap<>();
  }

  public String getTenantByDomain(String domain) {
    Objects.requireNonNull(domain);
    return domainToTenant.get(domain.toLowerCase());
  }

  // AdminChangeListener methods ////////////////////////////////////////////

  @Override
  public void createTenant(TenantDesc tenantDesc, RequestPhase phase) throws ApplicationException {
    if (phase == RequestPhase.FINAL) {
      final var tenantName = tenantDesc.getName();
      if (Set.of("/", "_system").contains(tenantName.toLowerCase())) {
        // do nothing for the system tenant
        return;
      }
      final var state =
          new GenericExecutionState("setupTenant", ExecutorManager.getSidelineExecutor());
      try {
        organizationRepository
            .findByTenantName(tenantName, state)
            .thenComposeAsync(
                (organization) -> {
                  if (organization != null) {
                    return CompletableFuture.completedFuture(organization);
                  }
                  return organizationRepository.setupOrg(
                      generateUniqueId(), tenantName, tenantName, state);
                })
            .thenAccept(
                (organization) -> {
                  domainToTenant.put(organization.getName().toLowerCase(), tenantName);
                  tenantDesc.setDomain(organization.getName());
                })
            .get();
      } catch (ExecutionException e) {
        logger.error("Failed to resolve domain name; tenant={}", tenantName, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted; tenant={}", tenantName, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void deleteTenant(TenantDesc tenantDesc, RequestPhase phase)
      throws NoSuchTenantException, ApplicationException {
    if (phase == RequestPhase.FINAL) {
      for (var entry : domainToTenant.entrySet()) {
        if (entry.getValue().equalsIgnoreCase(tenantDesc.getName())) {
          domainToTenant.remove(entry.getKey());
        }
      }
    }
  }

  @Override
  public void createStream(String tenantName, StreamDesc streamDesc, RequestPhase phase) {
    // do nothing
  }

  @Override
  public void deleteStream(String tenantName, StreamDesc streamDesc, RequestPhase phase) {
    // do nothing
  }

  @Override
  public void unload() {
    // do nothing
  }

  public static long generateUniqueId() {
    final var id = System.currentTimeMillis();
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return id;
  }
}
