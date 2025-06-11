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
package io.isima.bios.errors.exception;

import io.isima.bios.errors.GenericError;

public class BiosAppsDeploymentException extends TfosException {
  private static final long serialVersionUID = 7094549036341779857L;

  public BiosAppsDeploymentException() {
    super(GenericError.BIOS_APPS_DEPLOYMENT_FAILED);
  }

  public BiosAppsDeploymentException(String message) {
    super(GenericError.BIOS_APPS_DEPLOYMENT_FAILED, message);
  }

  public BiosAppsDeploymentException(Throwable t) {
    super(GenericError.BIOS_APPS_DEPLOYMENT_FAILED, t.getMessage(), t);
  }

  public BiosAppsDeploymentException(String message, Throwable t) {
    super(GenericError.BIOS_APPS_DEPLOYMENT_FAILED, message, t);
  }
}
