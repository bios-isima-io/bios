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
package io.isima.bios.data;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.models.Event;
import java.util.List;

/**
 * Publishes raw and rolled up data out to interested parties. The data engine will invoke this
 * interface, if registered. This interface ensures layering rules are not broken between old TFOS
 * and new BIOS code in the server.
 *
 * <p>Currently all the logic to check whether data needs to be exported is abstracted out of the
 * engine. The engine (specifically DataEngine Maintenance) job is to simply invoke, if registered.
 */
public interface DataPublisher {
  void publishRawData(StreamDesc streamDesc, List<Event> rawEvents);

  // TODO: publish rolledup data in the future
  void publishRollupData(StreamDesc rollupStreamDesc, List<Event> rolledupEvents);
}
