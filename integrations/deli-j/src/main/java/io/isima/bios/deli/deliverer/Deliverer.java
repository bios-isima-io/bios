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
package io.isima.bios.deli.deliverer;

import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.Objects;

public abstract class Deliverer<ChannelT extends DeliveryChannel> {

  protected ChannelT channel;

  protected Deliverer(ChannelT channel) {
    Objects.requireNonNull(channel);
    this.channel = channel;
  }

  public String getDestinationName() {
    return channel.getName();
  }

  public abstract void deliver(FlowContext context) throws BiosClientException,
      InterruptedException;
}
