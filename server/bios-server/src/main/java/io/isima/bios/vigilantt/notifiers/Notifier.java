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
package io.isima.bios.vigilantt.notifiers;

import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.models.NotificationContents;

/** Interface with a single method to trigger a notification, used for alerts. */
public interface Notifier {

  /**
   * Send a notification.
   *
   * @param event Event to notify
   * @param kind Type of notification
   * @param retryCredit Credit left for retrying in case of notification failure
   */
  void sendNotification(NotificationContents event, String kind, int retryCredit)
      throws NotificationFailedException;
}
