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
package io.isima.bios.models;

import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.models.isql.ISqlResponseType;
import java.util.List;

/**
 * ISql response type that does not carry data particularly.
 *
 * <p>This type is used when the request statement does not return any data.
 */
public class VoidISqlResponse implements ISqlResponse {
  public static final VoidISqlResponse INSTANCE = new VoidISqlResponse();

  private VoidISqlResponse() {}

  @Override
  public ISqlResponseType getResponseType() {
    return ISqlResponseType.VOID;
  }

  @Override
  public List<? extends Record> getRecords() {
    return List.of();
  }

  @Override
  public List<DataWindow> getDataWindows() {
    return List.of();
  }
}
