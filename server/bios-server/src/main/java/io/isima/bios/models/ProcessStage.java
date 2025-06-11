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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.isima.bios.common.IngestState;
import io.isima.bios.preprocess.PreProcessor;
import lombok.ToString;

/**
 * Class to represent a stage of pre-processes. One instance of this class corresponds to an entry
 * of pre-process rules in Tenant Configuration JSON. Actual behavior is defined using a function
 * object {@link #process}. Note that this function object must be stateless. Any state necessary
 * for pre-processing must be stored in variable table of IngestState object. That can be accessible
 * while applying the process via {@link IngestState#getVariables()}
 */
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessStage extends Duplicatable {
  /** Name of the stage. This corresponds to rule name in tenant configuration. */
  private String name;

  /**
   * Function to process at this stage.
   *
   * <p>The function takes an IngestState object as the argument and should return the same. The
   * function must be stateless since this processor is reused concurrently. Use {@link
   * IngestState#getVariables()} to store processing variables.
   */
  private PreProcessor process;

  public ProcessStage() {}

  @Override
  public ProcessStage duplicate() {
    ProcessStage clone = new ProcessStage();
    clone.name = name;
    clone.process = process;
    return clone;
  }

  /**
   * Constructor with stage name and function to process.
   *
   * @param name Name of the stage.
   * @param process Function to process.
   */
  public ProcessStage(String name, PreProcessor process) {
    if (name == null || process == null) {
      throw new NullPointerException("All paramters must be non-null.");
    }
    this.name = name;
    this.process = process;
  }

  /**
   * Get name of the stage. The name corresponds to rule name in tenant configuration.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get function to process at this stage.
   *
   * <p>The function takes a ProcessState object and should return the same. The function should
   * throw TfoServiceException for an error during the process.
   *
   * @return the process
   */
  public PreProcessor getProcess() {
    return process;
  }

  public void setProcess(PreProcessor process) {
    this.process = process;
  }
}
