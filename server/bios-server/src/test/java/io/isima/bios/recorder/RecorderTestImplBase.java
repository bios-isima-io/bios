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

import com.google.common.base.Ticker;
import io.isima.bios.models.AppType;
import java.util.ArrayList;
import java.util.List;

class RecorderTestImplBase extends RecorderTestBase {
  OperationMetricGroupRecorder createTestRecorder() {
    return new OperationMetricGroupRecorder(
        TENANT, SIGNAL, "recorderTest", AppType.INTERNAL, TEST_OP);
  }

  List<OperationMetricGroupRecorder> createSignalTestRecorder(
      String tenantName, String signalName) {
    List<OperationMetricGroupRecorder> recorders = new ArrayList<>();
    for (var req : SignalRequestType.values()) {
      recorders.add(
          new OperationMetricGroupRecorder(
              tenantName, signalName, "recorderTest", AppType.INTERNAL, req));
    }
    return recorders;
  }

  List<OperationMetricGroupRecorder> createControlTestRecorder(
      String tenantName, String streamName) {
    List<OperationMetricGroupRecorder> recorders = new ArrayList<>();
    for (var req : ControlRequestType.values()) {
      recorders.add(
          new OperationMetricGroupRecorder(
              tenantName, streamName, "recorderTest", AppType.INTERNAL, req));
    }
    return recorders;
  }

  OperationsMeasurementRegistry createTestRegistryWithTestTicker(Ticker testTicker) {
    return new OperationsMeasurementRegistry("sys", testTicker, 10, 2, null, 1, 100);
  }
}
