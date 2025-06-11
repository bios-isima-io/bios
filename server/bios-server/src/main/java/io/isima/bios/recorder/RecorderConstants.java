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
package io.isima.bios.recorder;

public class RecorderConstants {
  static final int CONTROL_BASE = 0;
  static final int CONTEXT_BASE = 10000;
  static final int SIGNAL_BASE = 15000;

  public static final String DEFAULT_CONTAINER = "_default";

  public static final String BIOS_API_VERSION_1 = "biosV1";

  // operation metrics dimensions
  public static final String ATTR_BIOS_NODE = "node";
  public static final String ATTR_BIOS_TENANT = "tenant";
  public static final String ATTR_BIOS_STREAM = "stream";
  public static final String ATTR_BIOS_REQUEST = "request";
  public static final String ATTR_BIOS_APP_NAME = "appName";
  public static final String ATTR_BIOS_APP_TYPE = "appType";

  public static final String ATTR_OP_SUCCESSFUL_OPERATIONS = "numSuccessfulOperations";

  public static final String ATTR_OP_NUM_VALIDATION_ERRORS = "numValidationErrors";
  public static final String ATTR_OP_NUM_TRANSIENT_ERRORS = "numTransientErrors";

  public static final String ATTR_OP_NUM_WRITES = "numWrites";
  public static final String ATTR_OP_BYTES_WRITTEN = "bytesWritten";

  public static final String ATTR_OP_NUM_READS = "numReads";
  public static final String ATTR_OP_BYTES_READ = "bytesRead";

  public static final String ATTR_OP_LATENCY_MIN = "latencyMin";
  public static final String ATTR_OP_LATENCY_MAX = "latencyMax";
  public static final String ATTR_OP_LATENCY_SUM = "latencySum";

  public static final String ATTR_OP_STORAGE_ACCESSES = "numStorageAccesses";
  public static final String ATTR_OP_STORAGE_LATENCY_MIN = "storageLatencyMin";
  public static final String ATTR_OP_STORAGE_LATENCY_MAX = "storageLatencyMax";
  public static final String ATTR_OP_STORAGE_LATENCY_SUM = "storageLatencySum";

  // optional -- metrics for development/debugging, values do not go to the operations signal
  //
  public static final String ATTR_OP_NUM_DECODES = "numDecodes";
  public static final String ATTR_OP_DECODE_LATENCY_MIN = "decodeLatencyMin";
  public static final String ATTR_OP_DECODE_LATENCY_MAX = "decodeLatencyMax";
  public static final String ATTR_OP_DECODE_LATENCY_SUM = "decodeLatencySum";

  public static final String ATTR_OP_NUM_VALIDATION = "numValidations";
  public static final String ATTR_OP_VALIDATION_LATENCY_MIN = "validationLatencyMin";
  public static final String ATTR_OP_VALIDATION_LATENCY_MAX = "validationLatencyMax";
  public static final String ATTR_OP_VALIDATION_LATENCY_SUM = "validationLatencySum";

  public static final String ATTR_OP_NUM_PRE = "numPreProcesses";
  public static final String ATTR_OP_PRE_LATENCY_MIN = "preProcessLatencyMin";
  public static final String ATTR_OP_PRE_LATENCY_MAX = "preProcessLatencyMax";
  public static final String ATTR_OP_PRE_LATENCY_SUM = "preProcessLatencySum";

  public static final String ATTR_OP_DBP_COUNT = "dbPrepareSuccessCount";
  public static final String ATTR_OP_DBP_LATENCY_MIN = "dbPrepareLatencyMin";
  public static final String ATTR_OP_DBP_LATENCY_MAX = "dbPrepareLatencyMax";
  public static final String ATTR_OP_DBP_LATENCY_SUM = "dbPrepareLatencySum";

  public static final String ATTR_OP_DB_ERRORS = "dbErrors";

  public static final String ATTR_OP_NUM_POST_PROCESSES = "numPostProcesses";
  public static final String ATTR_OP_POST_LATENCY_MIN = "postProcessLatencyMin";
  public static final String ATTR_OP_POST_LATENCY_MAX = "postProcessLatencyMax";
  public static final String ATTR_OP_POST_LATENCY_SUM = "postProcessLatencySum";

  public static final String ATTR_OP_NUM_ENCODES = "numEncodes";
  public static final String ATTR_OP_ENCODE_LATENCY_MIN = "encodeLatencyMin";
  public static final String ATTR_OP_ENCODE_LATENCY_MAX = "encodeLatencyMax";
  public static final String ATTR_OP_ENCODE_LATENCY_SUM = "encodeLatencySum";
}
