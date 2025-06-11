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

import static io.isima.bios.recorder.RecorderConstants.CONTEXT_BASE;

/** All the supported context data operations that are tracked by operational metrics. */
public enum ContextRequestType implements RequestType {
  LIST_CONTEXT_ENTRIES(10),
  SELECT_CONTEXT(12),
  UPDATE(101),
  UPDATE_INITIAL(101),
  UPDATE_FINAL(101),
  REPLACE_CONTEXT_ATTRIBUTES(100),
  REPLACE_CONTEXT_ATTRIBUTES_INITIAL(100),
  REPLACE_CONTEXT_ATTRIBUTES_FINAL(100),
  UPSERT(102),
  UPSERT_INITIAL(102),
  UPSERT_FINAL(102),
  DELETE(103),
  DELETE_INITIAL(103),
  DELETE_FINAL(103);

  private int priority;

  ContextRequestType(int priority) {
    this.priority = priority;
  }

  @Override
  public String getRequestName() {
    return this.name();
  }

  @Override
  public int getRequestNumber() {
    return CONTEXT_BASE + this.ordinal();
  }

  @Override
  public boolean isControl() {
    return false;
  }

  @Override
  public int priority() {
    return priority;
  }

  @Override
  public boolean shouldRateLimit() {
    return false;
  }
}
