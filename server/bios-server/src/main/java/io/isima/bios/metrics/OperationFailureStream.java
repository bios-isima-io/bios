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
package io.isima.bios.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.impl.IStreamProvider;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationFailureStream implements IStreamProvider {
  private static final Logger logger = LoggerFactory.getLogger(OperationFailureStream.class);
  private static final String DEFAULT_SCHEMA_JSON_FILE = "operation_failure.json";

  private StreamConfig operationFailureSignalStream;

  public OperationFailureStream() {
    this(DEFAULT_SCHEMA_JSON_FILE);
  }

  OperationFailureStream(String schemaJsonFile) {
    StreamConfig temp = getStreamConfigFromFile(schemaJsonFile);
    if (temp == null) {
      final String msg = "Failed to load client metrics stream config, file :" + schemaJsonFile;
      logger.error(msg);
    }
    operationFailureSignalStream = temp;
  }

  StreamConfig getStreamConfigFromFile(String schemaJsonFile) {
    StreamConfig streamConfig = null;

    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(schemaJsonFile)) {
      ObjectMapper mapper = TfosObjectMapperProvider.get();
      streamConfig = mapper.readValue(inputStream, StreamConfig.class);
    } catch (IOException ex) {
      final String msg = "Failed to client metrics stream config, file :" + schemaJsonFile;
      logger.error(msg, ex);
    }
    return streamConfig;
  }

  @Override
  public StreamConfig getStreamConfig() {
    return operationFailureSignalStream;
  }
}
