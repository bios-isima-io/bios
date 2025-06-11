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
package io.isima.bios.sdk;

import io.isima.bios.codec.proto.isql.ProtoBuilderProvider;
import io.isima.bios.models.AppType;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ImportDataProcessorConfig;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.impl.SessionImpl;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface Session extends AutoCloseable {
  /**
   * Executes a single isQL statement synchronously.
   *
   * @throws BiosClientException thrown to indicate an error happens.
   */
  ISqlResponse execute(Statement statement) throws BiosClientException;

  /**
   * Executes a single statement asynchronously.
   *
   * @param statement insert or select statement
   * @return Completable future for execution response
   */
  CompletionStage<ISqlResponse> executeAsync(Statement statement);

  /**
   * Executes one or more select statements synchronously. The queries are sent to the server in a
   * single message and this method returns only after all the queries are executed on the server
   * and the results compiled.
   *
   * <p>Does some client-side pre-validation of the statement.
   *
   * @param statements One or more isQL statement(s).
   * @return query result(s)
   * @throws BiosClientException in case of any error
   */
  List<ISqlResponse> multiExecute(Statement... statements) throws BiosClientException;

  /**
   * Execute one or more select statements asynchronously. The queries are sent to the server on a
   * single message and this message returns only after all the queries are executed on the server
   * and the results compiled.
   *
   * @param statements One or more Isima Sql statement(s) inside a request
   * @return Completable future for execution response
   * @throws BiosClientException in case of any error
   */
  CompletionStage<List<ISqlResponse>> multiExecuteAsync(Statement... statements);

  /**
   * Retrieves tenant configuration for the passed tenantName.
   *
   * @param detail flag which specifies if detailed tenant info is to be retrieved
   * @param includeInternal flag which specifies if internal info need to be included
   */
  TenantConfig getTenant(boolean detail, boolean includeInternal) throws BiosClientException;

  /**
   * Retrieves tenant configuration for the passed tenantName.
   *
   * @param tenantName name of the tenant.
   * @param detail flag which specifies if detailed tenant info is to be retrieved.
   * @param includeInternal flag which specifies if internal info need to be included.
   */
  TenantConfig getTenant(String tenantName, boolean detail, boolean includeInternal)
      throws BiosClientException;

  /**
   * Creates a signal.
   *
   * @param signalConfig Schema of the signal to create.
   * @return Schema of created signal.
   */
  SignalConfig createSignal(SignalConfig signalConfig) throws BiosClientException;

  /**
   * Lists signals in the tenant.
   *
   * <p>Without an option, the method returns specified or all non-internal signals. Each signal
   * configuration contains only name. Options changes this behavior as following:
   *
   * <dl>
   *   <dt>DETAIL
   *   <dd>All signal configuration properties are filled except inferred tags
   *   <dt>INCLUDE_INTERNAL
   *   <dd>Internal signals are included in the results
   *   <dt>INCLUDE_INFERRED_TAGS
   *   <dd>When DETAIL is specified, each signal configuration also includes inferred tags
   * </dl>
   *
   * @param names Collection of names to specify. The method returns all signals when the collection
   *     is empty.
   * @param options Collection of options.
   * @return List of signal configurations.
   */
  List<SignalConfig> getSignals(Collection<String> names, Collection<GetStreamOption> options)
      throws BiosClientException;

  /**
   * Gets signal schema.
   *
   * <p>Supported options are as follows:
   *
   * <dl>
   *   <dt>INCLUDE_INFERRED_TAGS
   *   <dd>When DETAIL is specified, the signal configuration also includes inferred tags
   * </dl>
   *
   * @param signalName Name of the signal to get.
   * @param options Collection of options.
   */
  SignalConfig getSignal(String signalName, Collection<GetStreamOption> options)
      throws BiosClientException;

  /**
   * Updates signal configuration.
   *
   * @param signalName Target signal name.
   * @param signalConfig Schema of the signal to update.
   * @return Schema of updated signal.
   */
  SignalConfig updateSignal(String signalName, SignalConfig signalConfig)
      throws BiosClientException;

  /**
   * Deletes a signal.
   *
   * @param signalName Target signal name.
   */
  void deleteSignal(String signalName) throws BiosClientException;

  /**
   * Creates a context.
   *
   * @param contextConfig Schema of the context to create.
   * @return Schema of created context.
   */
  ContextConfig createContext(ContextConfig contextConfig) throws BiosClientException;

  /**
   * Creates an import source.
   *
   * @param importSourceConfig Import source configuration to create.
   * @return Configuration of the created import source.
   */
  ImportSourceConfig createImportSource(ImportSourceConfig importSourceConfig)
      throws BiosClientException;

  /**
   * Retrieves an import source.
   *
   * @param importSourceId Import source ID to retrieve.
   * @return The import source configuration.
   */
  ImportSourceConfig getImportSource(String importSourceId) throws BiosClientException;

  /**
   * Updates an import source.
   *
   * @param importSourceId Target import source ID.
   * @param importSourceConfig Configuration of the import source to update.
   * @return Configuration of the updated import source.
   */
  ImportSourceConfig updateImportSource(
      String importSourceId, ImportSourceConfig importSourceConfig) throws BiosClientException;

  /**
   * Deletes an import source.
   *
   * @param importSourceId Target import source ID.
   */
  void deleteImportSource(String importSourceId) throws BiosClientException;

  /**
   * Creates an import destination.
   *
   * @param importDestinationConfig Import destination configuration to create.
   * @return Configuration of the created import destination.
   */
  ImportDestinationConfig createImportDestination(ImportDestinationConfig importDestinationConfig)
      throws BiosClientException;

  /**
   * Retrieves an import destination.
   *
   * @param importDestinationId Import destination ID to retrieve.
   * @return The import destination configuration.
   */
  ImportDestinationConfig getImportDestination(String importDestinationId)
      throws BiosClientException;

  /**
   * Updates an import destination.
   *
   * @param importDestinationId Target import destination ID.
   * @param importDestinationConfig Configuration of the import destination to update.
   * @return Configuration of the updated import destination.
   */
  ImportDestinationConfig updateImportDestination(
      String importDestinationId, ImportDestinationConfig importDestinationConfig)
      throws BiosClientException;

  /**
   * Deletes an import destination.
   *
   * @param importDestinationId Target import destination ID.
   */
  void deleteImportDestination(String importDestinationId) throws BiosClientException;

  /**
   * Creates an import flow.
   *
   * @param importFlowSpec Import flow configuration to create.
   * @return Configuration of the created import flow.
   */
  ImportFlowConfig createImportFlowSpec(ImportFlowConfig importFlowSpec) throws BiosClientException;

  /**
   * Retrieves an import flow.
   *
   * @param importFlowId Import flow ID to retrieve.
   * @return The import flow configuration.
   */
  ImportFlowConfig getImportFlowSpec(String importFlowId) throws BiosClientException;

  /**
   * Updates an import flow.
   *
   * @param importFlowId Target import flow ID.
   * @param importFlowSpec Configuration of the import flow to update.
   * @return Configuration of the updated import flow.
   */
  ImportFlowConfig updateImportFlowSpec(String importFlowId, ImportFlowConfig importFlowSpec)
      throws BiosClientException;

  /**
   * Deletes an import flow.
   *
   * @param importFlowId Target import flow ID.
   */
  void deleteImportFlowSpec(String importFlowId) throws BiosClientException;

  /**
   * Creates an import source.
   *
   * @param processorConfig Import data processor configuration to create.
   * @return Configuration of the created import data processor.
   */
  void createImportDataProcessor(ImportDataProcessorConfig processorConfig)
      throws BiosClientException;

  /**
   * Retrieves an import data processor.
   *
   * @param processorName Import processor name to retrieve.
   * @return The import data processor configuration.
   */
  ImportDataProcessorConfig getImportDataProcessor(String processorName) throws BiosClientException;

  /**
   * Updates an import data processor.
   *
   * @param processorName Target import processor name.
   * @param processorConfig Configuration of the import data processor to update.
   * @return Configuration of the updated data processor.
   */
  void updateImportDataProcessor(String processorName, ImportDataProcessorConfig processorConfig)
      throws BiosClientException;

  /**
   * Deletes an import data processor.
   *
   * @param processorName Target import data processor name.
   */
  void deleteImportDataProcessor(String processorName) throws BiosClientException;

  /**
   * Creates an export destination.
   *
   * @param exportDestinationConfig Export destination configuration to create.
   * @return Configuration of the created export destination.
   */
  ExportDestinationConfig createExportDestination(ExportDestinationConfig exportDestinationConfig)
      throws BiosClientException;

  /**
   * Retrieves an export destination.
   *
   * @param exportDestinationId Export destination ID to retrieve.
   * @return The export destination configuration.
   */
  ExportDestinationConfig getExportDestination(String exportDestinationId)
      throws BiosClientException;

  /**
   * Updates an export destination.
   *
   * @param exportDestinationId Target export destination ID.
   * @param exportDestinationConfig Configuration of the export destination to update.
   * @return Configuration of the updated export destination.
   */
  ExportDestinationConfig updateExportDestination(
      String exportDestinationId, ExportDestinationConfig exportDestinationConfig)
      throws BiosClientException;

  /**
   * Deletes an export destination.
   *
   * @param exportDestinationId Target export destination ID.
   */
  void deleteExportDestination(String exportDestinationId) throws BiosClientException;

  /**
   * Lists contexts in the tenant.
   *
   * <p>Without an option, the method returns specified or all non-internal contexts. Each context
   * configuration contains only name. Options changes this behavior as following:
   *
   * <dl>
   *   <dt>DETAIL
   *   <dd>All context configuration properties are filled except inferred tags
   *   <dt>INCLUDE_INTERNAL
   *   <dd>Internal contexts are included in the results
   *   <dt>INCLUDE_INFERRED_TAGS
   *   <dd>When DETAIL is specified, each context configuration also includes inferred tags
   * </dl>
   *
   * @param names Collection of names to specify. The method returns all contexts when the
   *     collection is empty.
   * @param options Collection of options.
   * @return List of context configurations.
   */
  List<ContextConfig> getContexts(Collection<String> names, Collection<GetStreamOption> options)
      throws BiosClientException;

  /**
   * Gets context schema.
   *
   * <p>Supported options are as follows:
   *
   * <dl>
   *   <dt>INCLUDE_INFERRED_TAGS
   *   <dd>When DETAIL is specified, the context configuration also includes inferred tags
   * </dl>
   *
   * @param contextName Name of the context to get.
   * @param options Collection of options.
   */
  ContextConfig getContext(String contextName, Collection<GetStreamOption> options)
      throws BiosClientException;

  /**
   * Updates context configuration.
   *
   * @param contextName Target context name.
   * @param contextConfig Schema of the context to update.
   * @return Schema of updated context.
   */
  ContextConfig updateContext(String contextName, ContextConfig contextConfig)
      throws BiosClientException;

  /**
   * Deletes a context.
   *
   * @param contextName Target context name.
   */
  void deleteContext(String contextName) throws BiosClientException;

  /** Session Starter. */
  public class Starter {

    private final String host;
    private String user;
    private String password;
    private String appName;
    private AppType appType;
    private Integer port;
    private String sslCertFile;

    Starter(String host) {
      this.host = Objects.requireNonNull(host).trim();
      if (host.isBlank()) {
        throw new IllegalArgumentException("Parameter 'host' may not be blank");
      }
      this.sslCertFile = System.getenv("SSL_CERT_FILE");
    }

    /**
     * Specify user email address.
     *
     * @param user User email
     * @return builder
     */
    public Starter user(String user) {
      this.user = Objects.requireNonNull(user).trim();
      return this;
    }

    /**
     * Specify password of the user.
     *
     * @param password password
     * @return builder
     */
    public Starter password(String password) {
      this.password = Objects.requireNonNull(password).trim();
      return this;
    }

    /**
     * Specify the application name for the session.
     *
     * @param appName application name
     * @return builder
     */
    public Starter appName(String appName) {
      this.appName = Objects.requireNonNull(appName).trim();
      return this;
    }

    /**
     * Specify the application type for the session.
     *
     * @param appType application type
     * @return builder
     */
    public Starter appType(AppType appType) {
      this.appType = Objects.requireNonNull(appType);
      return this;
    }

    /**
     * Specify SSL certificate file. If environment variable "SSL_CERT_FILE" is set, it is
     * automatically used by default without needing to call this function.
     *
     * @param sslCertFile Certificate file path.
     * @return builder
     */
    public Starter sslCertFile(String sslCertFile) {
      this.sslCertFile = sslCertFile;
      return this;
    }

    /**
     * Specify a non-default port to connect to on the host. By default, the SSL port 443 is used.
     *
     * @param port Port number.
     * @return builder
     */
    public Starter port(int port) {
      this.port = port;
      return this;
    }

    /**
     * Connects to Bios server and returns a session.
     *
     * @return session
     * @throws BiosClientException in case of error in opening the session
     */
    public Session connect() throws BiosClientException {
      final var session = makeSession();
      session.start(host, port, sslCertFile, user, password, appName, appType);
      return session;
    }

    /**
     * Connects to Bios server and returns a session asynchronously.
     *
     * @return session
     */
    public CompletionStage<Session> connectAsync() {
      final SessionImpl session;
      try {
        session = makeSession();
      } catch (BiosClientException e) {
        return CompletableFuture.failedStage(e);
      }
      return session
          .startAsync(host, port, sslCertFile, user, password, appName, appType)
          .thenApply((none) -> session);
    }

    private SessionImpl makeSession() throws BiosClientException {
      if (user == null || password == null) {
        throw new BiosClientException(
            BiosClientError.BAD_INPUT, "User name and password must be set");
      }
      if (port == null) {
        port = 443;
      }

      ProtoBuilderProvider.configureProtoBuilderProvider();

      return new SessionImpl();
    }
  }
}
