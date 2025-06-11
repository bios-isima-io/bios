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

import io.isima.bios.deli.deliverer.Deliverer;
import io.isima.bios.deli.deliverer.DeliveryChannel;
import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataFlow<OriginalDataT> {
  private static final Logger logger = LoggerFactory.getLogger(DataFlow.class);

  @Getter
  protected final ImportFlowConfig flowConfig;
  protected final List<ProcessingUnit> processingUnits;
  @Getter
  protected final Deliverer<?> deliverer;

  protected DataFlow(Configuration configuration, ImportSourceConfig importSourceConfig,
      ImportFlowConfig flowConfig) throws InvalidConfigurationException {
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(importSourceConfig);
    Objects.requireNonNull(flowConfig);
    this.flowConfig = flowConfig;
    processingUnits = parsePickupSpecs(configuration, flowConfig);
    deliverer = generateDeliverer(configuration, importSourceConfig, flowConfig);
  }

  private List<ProcessingUnit> parsePickupSpecs(Configuration configuration,
      ImportFlowConfig flowConfig) throws InvalidConfigurationException {
    final var dataPickupSpec = flowConfig.getDataPickupSpec();
    if (dataPickupSpec == null) {
      throw new InvalidConfigurationException("dataPickupSpecs must be specified");
    }
    final var attributeSpecs = dataPickupSpec.getAttributes();
    if (attributeSpecs == null) {
      throw new InvalidConfigurationException("dataPickupSpecs.attributes must be specified");
    }
    final var processingUnits = new ArrayList<ProcessingUnit>();
    for (int i = 0; i < attributeSpecs.size(); ++i) {
      final var attributeSpec = attributeSpecs.get(i);
      processingUnits
          .add(new ProcessingUnit(configuration, flowConfig.getImportFlowName(), i, attributeSpec));
    }

    return Collections.unmodifiableList(processingUnits);
  }

  private Deliverer<?> generateDeliverer(Configuration configuration,
      ImportSourceConfig sourceConfig, ImportFlowConfig flowConfig)
      throws InvalidConfigurationException {
    final var destinationDataSpec = flowConfig.getDestinationDataSpec();
    final String destinationId = destinationDataSpec.getImportDestinationId();
    final DeliveryChannel channel;
    if (configuration.isProxyMode()) {
      // The proxy destination is already registered with import source ID
      channel = DeliveryChannel.getChannel(sourceConfig.getImportSourceId());
      assert channel != null;
    } else {
      final var destinationConfig = configuration.getImportDestinationConfig(destinationId);
      if (destinationConfig == null) {
        throw new InvalidConfigurationException(
            "Import flow %s: Specifying destination %s does not exist", getFlowName(),
            destinationId);
      }
      channel = DeliveryChannel.registerChannel(configuration, destinationConfig);
    }
    return channel.makeDeliverer(flowConfig);
  }

  public final String getFlowName() {
    return flowConfig.getImportFlowName();
  }

  public abstract boolean isForInputData(FlowContext context);

  public abstract Map<String, Object> makeImportMessage(OriginalDataT originalData,
      FlowContext context);

  public void process(OriginalDataT originalData, FlowContext context)
      throws BiosClientException, InterruptedException {
    final var unflattened = makeImportMessage(originalData, context);
    final var inputRecord = searchRecords(unflattened, context);
    final var outputRecord = pickupAttributes(inputRecord, context);
    context.setRecord(outputRecord);
    deliver(context);
  }

  private Map<String, Object> searchRecords(Map<String, Object> originalObject,
      FlowContext context) {
    // TODO Implement filter and flattener
    return originalObject;
  }

  private Map<String, Object> pickupAttributes(Map<String, Object> inputRecord,
      FlowContext context) {
    final var outputRecord = new HashMap<String, Object>();
    for (var processingUnit : processingUnits) {
      processingUnit.process(inputRecord, outputRecord, context);
    }
    return outputRecord;
  }

  protected void deliver(FlowContext context) throws BiosClientException, InterruptedException {
    deliverer.deliver(context);
  }
}
