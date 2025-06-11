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
package io.isima.bios.deli.flow;

import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.CdcOperationType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RelationalDataFlow extends DataFlow<Map<String, Object>> {
  private static final Logger logger = LoggerFactory.getLogger(RelationalDataFlow.class);
  protected final String databaseName;
  protected final String tableName;
  protected final Set<CdcOperationType> operationTypes;

  @FunctionalInterface
  private interface IncomingRecordProcessor {
    void process(Map<String, Object> originalData, FlowContext context)
        throws BiosClientException, InterruptedException;
  }

  private final boolean isProxyMode;
  private final IncomingRecordProcessor incomingRecordProcessor;

  public RelationalDataFlow(Configuration configuration, ImportSourceConfig sourceConfig,
      ImportFlowConfig flowConfig) throws InvalidConfigurationException {
    super(configuration, sourceConfig, flowConfig);
    databaseName = sourceConfig.getDatabaseName();
    final var sourceDataSpec = flowConfig.getSourceDataSpec();
    tableName = sourceDataSpec.getTableName();
    if (tableName == null) {
      throw new InvalidConfigurationException("Table name must be set");
    }
    if (sourceDataSpec.getCdcOperationTypes() != null
        && !sourceDataSpec.getCdcOperationTypes().isEmpty()) {
      operationTypes = new HashSet<>(sourceDataSpec.getCdcOperationTypes());
    } else {
      operationTypes = null;
    }

    // We use the special incoming record processor in proxy mode
    isProxyMode = configuration.isProxyMode();
    incomingRecordProcessor = isProxyMode ? this::pipeInput : super::process;
  }

  @Override
  public boolean isForInputData(FlowContext context) {
    boolean yesno = tableName.equalsIgnoreCase(context.getTableName())
        && (operationTypes == null || operationTypes.contains(context.getOperationType()))
        && (!isProxyMode || !context.isSentAlready());
    logger.debug("tableName: {}, context tableName: {}, isProxyMode: {}, isSentAlready: {}",
                tableName, context.getTableName(), isProxyMode, context.isSentAlready());
    logger.debug("Proceed with processing? - {}", yesno);
    return yesno;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> makeImportMessage(Map<String, Object> originalData,
      FlowContext context) {
    return context.getOperationType() == CdcOperationType.DELETE ?
        context.getBefore() : context.getAfter();
  }

  @Override
  public void process(Map<String, Object> originalData, FlowContext context)
      throws BiosClientException, InterruptedException {
    incomingRecordProcessor.process(originalData, context);
  }

  /**
   * Deliver an incoming record without filtering and transformation.
   *
   * <p>This method is used as an incoming record processor in proxy mode.</p>
   */
  private void pipeInput(Map<String, Object> originalData, FlowContext context)
      throws BiosClientException, InterruptedException {
    final var unflattened = makeImportMessage(originalData, context);
    context.setRecord(unflattened);
    deliver(context);
    context.setSentAlready(true);
  }
}
