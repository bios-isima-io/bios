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
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.CdcOperationType;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ImportDestinationStreamType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Statement;

public class ContextBiosDeliverer extends BiosDeliverer {

  private final String primaryKeyName;
  private final boolean isPrimaryKeyString;
  private final String defaultStringValue;

  protected ContextBiosDeliverer(BiosDeliveryChannel channel,
      Configuration configuration, ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    super(ImportDestinationStreamType.CONTEXT, channel, configuration, importFlowSpec);
    final var contextConfig = (ContextConfig) streamConfig;
    primaryKeyName = contextConfig.getPrimaryKey().get(0).toLowerCase();
    boolean isPrimaryKeyString = false;
    String defaultStringValue = null;
    for (var attribute : contextConfig.getAttributes()) {
      if (attribute.getName().equalsIgnoreCase(primaryKeyName)) {
        isPrimaryKeyString = attribute.getType() == AttributeType.STRING;
        if (isPrimaryKeyString) {
          defaultStringValue =
              attribute.getDefaultValue() != null ? attribute.getDefaultValue().asString() : null;
        }
        break;
      }
    }
    this.isPrimaryKeyString = isPrimaryKeyString;
    this.defaultStringValue = defaultStringValue;
  }

  @Override
  protected void preprocess(FlowContext context) {
    final var record = context.getRecord();
    if (isPrimaryKeyString && record.get(primaryKeyName) == null
        && "".equals(defaultStringValue)) {
      record.put(primaryKeyName, " ");
    }
  }

  @Override
  public Statement buildStatement(FlowContext context) {
    final var operationType = context.getOperationType();
    if (operationType != null && operationType == CdcOperationType.DELETE) {
      return buildDeleteStatement(context);
    }

    final String csvText = makeCsvText(context);
    if (csvText.isEmpty()) {
      return null;
    }
    return Bios.isql().upsert().intoContext(streamName).csv(csvText).build();
  }

  private Statement buildDeleteStatement(FlowContext context) {
    final var record = context.getRecord();
    final Object value = record.get(primaryKeyName);
    final var key = value != null ? value.toString() : "";
    return Bios.isql().delete().fromContext(streamName).where(Bios.keys().in(key)).build();
  }

}
