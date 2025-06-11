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
package io.isima.bios.vigilantt.exceptions;

/** This exception will be thrown when a notifier fails to deliver the alert. */
public class NotificationFailedException extends Exception {

  private static final long serialVersionUID = 8106468676285188451L;

  public NotificationFailedException() {}

  public NotificationFailedException(String message) {
    super(message);
  }
}
