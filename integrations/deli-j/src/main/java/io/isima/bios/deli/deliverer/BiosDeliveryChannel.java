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
package io.isima.bios.deli.deliverer;

import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.IntegrationsAuthenticationType;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiosDeliveryChannel extends DeliveryChannel {
  private static final Logger logger = LoggerFactory.getLogger(BiosDeliveryChannel.class);

  private final String host;
  private final int port;
  @Getter
  private final String defaultUser;
  private final String defaultPassword;

  private Map<String, BiosSessionCacheItem> sessionCache;

  protected BiosDeliveryChannel(Configuration configuration,
      ImportDestinationConfig importDestinationConfig) throws InvalidConfigurationException {
    super(configuration, importDestinationConfig);

    final var url = parseUrl(retrieveEndpoint(importDestinationConfig), "endpoint");
    host = url.getHost();
    port = url.getPort() >= 0 ? url.getPort() : 443;

    final var authentication = importDestinationConfig.getAuthentication();
    if (authentication != null) {
      if (authentication.getType() != IntegrationsAuthenticationType.LOGIN) {
        throw new InvalidConfigurationException(
            "destination '%s': Bios destination supports only Login authentication type",
            getName());
      }
      defaultUser = authentication.getUser();
      defaultPassword = authentication.getPassword();
      if (defaultUser == null || defaultPassword == null) {
        throw new InvalidConfigurationException("destination '%s': Properties 'authentication.user'"
            + " and 'authentication.password' must be set", getName());
      }
    } else {
      defaultUser = null;
      defaultPassword = null;
    }

    sessionCache = new ConcurrentHashMap<>();
  }

  @Override
  public Deliverer<?> makeDeliverer(ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    final var destinationSpec = importFlowSpec.getDestinationDataSpec();
    if (destinationSpec == null) {
      throw new InvalidConfigurationException(
          "importFlowSpec '%s': Property 'destinationDataSpec' must be se",
          importFlowSpec.getImportFlowName());
    }
    switch (destinationSpec.getType()) {
      case SIGNAL:
        return new SignalBiosDeliverer(this, configuration, importFlowSpec);
      case CONTEXT:
        return new ContextBiosDeliverer(this, configuration, importFlowSpec);
      default:
        throw new InvalidConfigurationException("importFlowSpec '%s': Unknown destination type %s",
            importFlowSpec.getImportFlowName(), destinationSpec.getType());
    }
  }

  @Override
  public void start() throws BiosClientException {}

  @Override
  public void stop() {
    if (sessionCache.isEmpty()) {
      return;
    }
    synchronized (sessionCache) {
      for (var item : sessionCache.values()) {
        try {
          item.getSession().close();
        } catch (Exception e) {
          logger.warn("Error happened while closing a session", e);
        }
      }
      sessionCache.clear();
    }
  }

  /**
   * Provides the default session.
   *
   * <p>
   * If there is no default session, the method creates one.
   * </p>
   *
   * @return Default session
   * @throws BiosClientException thrown to indicate that an error happened while creating the
   *                             session
   * @IllegalStateException thrown when this method is called although the destination configuration
   *     does not set up default Bios credentials
   */
  public Session getDefaultSession() throws BiosClientException {
    if (defaultUser == null) {
      throw new IllegalStateException("This channel is not configured to use default session");
    }
    return getSession(defaultUser, defaultPassword);
  }

  /**
   * Provides a session for specified user name and password.
   *
   * <p>
   * The class instance caches sessions if
   * </p>
   *
   * @param user     Bios user name
   * @param password Bios password
   * @return Bios session
   * @throws BiosClientException thrown to indicate that an error happened while creating the
   *                             session
   */
  public Session getSession(String user, String password) throws BiosClientException {
    Objects.requireNonNull(user);
    Objects.requireNonNull(password);
    var item = sessionCache.get(user);
    if (item != null) {
      if (item.getPassword().equals(password)) {
        return item.getSession();
      }
      try {
        item.getSession().close();
      } catch (Exception e) {
        logger.warn("Error happened while closing a session", e);
      }
    }
    synchronized (sessionCache) {
      item = sessionCache.get(user);
      if (item == null) {
        final var session = Bios.newSession(host)
            .port(port)
            .user(user)
            .password(password)
            .connect();
        item = new BiosSessionCacheItem(session, password);
        sessionCache.put(user, item);
      }
    }
    return item.getSession();
  }

  /**
   * Delete a session from cache.
   *
   * @param user User name for the deleting item. Put null if it is a default session.
   */
  public synchronized void deleteSession(String user) {
    if (user == null) {
      user = defaultUser;
    }
    if (!sessionCache.containsKey(user)) {
      return;
    }
    synchronized (sessionCache) {
      final var item = sessionCache.get(user);
      if (item == null) {
        return;
      }
      try {
        item.getSession().close();
      } catch (Exception e) {
        logger.warn("Error happened while closing a session", e);
      }
      sessionCache.remove(user);
    }
  }
}
