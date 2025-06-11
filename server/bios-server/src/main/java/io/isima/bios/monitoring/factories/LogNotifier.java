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
package io.isima.bios.monitoring.factories;

import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.models.NotificationContents;
import io.isima.bios.vigilantt.notifiers.Notifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dummy implementation for notifier which logs the anomalous event as WARNING log. */
public class LogNotifier implements Notifier {
  private final Logger logger = LoggerFactory.getLogger(LogNotifier.class);

  @Override
  public void sendNotification(NotificationContents event, String kind, int numRetries)
      throws NotificationFailedException {
    logger.warn("{} event {}", kind, event);
  }
}
