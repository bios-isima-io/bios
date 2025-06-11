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
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.CdcOperationType;
import io.isima.bios.models.ImportDestinationStreamType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Statement;

public class SignalBiosDeliverer extends BiosDeliverer {

  protected SignalBiosDeliverer(BiosDeliveryChannel channel,
      Configuration configuration, ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    super(ImportDestinationStreamType.SIGNAL, channel, configuration, importFlowSpec);
  }

  @Override
  public Statement buildStatement(FlowContext context) {
    final var operationType = context.getOperationType();
    if (operationType != null && operationType != CdcOperationType.CREATE) {
      return null;
    }
    final var csvText = makeCsvText(context);
    if (csvText.isEmpty()) {
      return null;
    }
    return Bios.isql().insert().into(streamName).csv(csvText).build();
  }
}
