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
package io.isima.bios.data.impl.models;

import io.isima.bios.errors.exception.DataEngineException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * This contains the result of evaluating a metric function. In most cases the result is a single
 * object such as a Double or a Long. In some cases such as samplecounts(), the result is a list of
 * rows, each with one or more attributes.
 */
@Getter
public class FunctionResult {
  private final boolean isSimple;
  private final Object simpleValue;
  private final ComplexValue complexValue;

  public FunctionResult(Object simpleValue) {
    if (simpleValue == null) {
      throw new DataEngineException("Unexpected: Got null value for FunctionResult.");
    }
    isSimple = true;
    this.simpleValue = simpleValue;
    complexValue = null;
  }

  public FunctionResult(List<String> outputNames) {
    isSimple = false;
    simpleValue = null;
    complexValue = new ComplexValue(outputNames);
  }

  @Getter
  public static class ComplexValue {
    private final List<String> outputNames;
    private final List<List<Object>> rows;

    public ComplexValue(List<String> outputNames) {
      this.outputNames = outputNames;
      rows = new ArrayList<>();
    }
  }
}
