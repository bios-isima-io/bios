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
package io.isima.bios.upgrade.command;

import io.isima.bios.exceptions.ApplicationException;

/**
 * Encapsulates commands to execute.
 *
 * <p>Leverages the command design pattern so as to allow easy plug-in of undo command and rollback
 * features in the upgrade process, if they are needed in the future.
 */
public interface UpgradeOperation {
  void execute() throws ApplicationException;
}
