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
package io.isima.bios.deli.importer;

import static io.isima.bios.deli.importer.CdcKeywords.C;
import static io.isima.bios.deli.importer.CdcKeywords.D;
import static io.isima.bios.deli.importer.CdcKeywords.DB;
import static io.isima.bios.deli.importer.CdcKeywords.DELTA_CHANGES;
import static io.isima.bios.deli.importer.CdcKeywords.OP;
import static io.isima.bios.deli.importer.CdcKeywords.R;
import static io.isima.bios.deli.importer.CdcKeywords.SOURCE;
import static io.isima.bios.deli.importer.CdcKeywords.TIMESTAMP;
import static io.isima.bios.deli.importer.CdcKeywords.TS_MS;
import static io.isima.bios.deli.importer.CdcKeywords.U;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.models.CdcOperationType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CdcEventHandler {
  private static final Logger logger = LoggerFactory.getLogger(CdcEventHandler.class);

  protected static final ObjectMapper mapper;
  private static final TypeReference<LinkedHashMap<String, Object>> typeRef;
  private static final Map<String, CdcOperationType> operationTypes;

  static {
    mapper = new ObjectMapper();
    typeRef = new TypeReference<>() {};
    operationTypes = new HashMap<>();
    operationTypes.put(C, CdcOperationType.CREATE);
    operationTypes.put(R, CdcOperationType.READ);
    operationTypes.put(U, CdcOperationType.UPDATE);
    operationTypes.put(D, CdcOperationType.DELETE);
  }

  /**
   * Retrieves before data from the source data.
   *
   * <p>The format depends on the type of CDC connector.</p>
   */
  protected abstract Map<String, Object> retrieveBeforeData(Map<String, Object> sourceData);

  /**
   * Retrieves after data from the source data.
   *
   * <p>The format depends on the type of CDC connector.</p>
   */
  protected abstract Map<String, Object> retrieveAfterData(Map<String, Object> sourceData);

  /**
   * Retrieves the table name.
   *
   * <p>The key for retreiving tablename depends on the type of CDC connector.</p>
   */
  protected abstract String getTableName(Map<String, ?> source);

  public Map<String, Object> handle(ChangeEvent<String, String> event, FlowContext context) {
    try {
      if (event.value() == null) {
        logger.debug("NO VALUE: key={}", event.key());
        return null;
      }
      final Map<String, Object> sourceData = mapper.readValue(event.value(), typeRef);
      final String op = (String) sourceData.get(OP);
      final Map<String, ?> source = (Map<String, ?>) sourceData.get(SOURCE);
      final String table = getTableName(source);
      final CdcOperationType cdcOperationType = op != null ? operationTypes.get(op) : null;
      logger.debug("Handle change event: sourceData: {}", sourceData);
      if (cdcOperationType == null) {
        logger.debug("Unsupported operation type; type={}", op);
        return null;
      }
      final String database = (String) source.get(DB);
      final Long timestamp = (Long) sourceData.get(TS_MS);

      context.setDatabase(database);
      context.setTableName(table);
      context.setOperationType(cdcOperationType);
      context.setAdditionalAttribute(TIMESTAMP, timestamp);
      final var before = retrieveBeforeData(sourceData);
      final var after = retrieveAfterData(sourceData);
      // This will happen only for DELETE's. Skip handling if both before and after
      // are null, as we have no data to process. This can happen with mongodb
      if (before == null && after == null) {
        return null;
      }
      context.setBefore(before);
      context.setAfter(after);

      EntityStateChangeCapture entityStateChange = new EntityStateChangeCapture();
      entityStateChange.setOperationType(cdcOperationType);
      entityStateChange.setPreOpState(before);
      entityStateChange.setPostOpState(after);
      context.setAdditionalAttribute(DELTA_CHANGES, mapper.writeValueAsString(entityStateChange));
      logger.debug("Change event handled. Returning sourceData: {}", sourceData);
      return sourceData;
    } catch (Throwable t) {
      logger.error("Error happened during event handling ", t);
      return null;
    }
  }
}
