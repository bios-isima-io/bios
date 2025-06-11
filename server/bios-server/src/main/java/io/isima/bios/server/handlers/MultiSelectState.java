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
package io.isima.bios.server.handlers;

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.proto.DataProto.SelectRequest;
import java.util.Objects;
import java.util.concurrent.Executor;
import lombok.Getter;

@Getter
public class MultiSelectState extends ExecutionState {

  private SelectRequest request;
  private BiosVersion clientVersion;

  public MultiSelectState(Executor executor) {
    super("MultiSelect", executor);
  }

  public void setInitialParams(
      String tenantName, SelectRequest request, BiosVersion clientVersion) {
    Objects.requireNonNull(tenantName);
    this.tenantName = tenantName;
    this.request = request;
    this.clientVersion = clientVersion;
  }
}
