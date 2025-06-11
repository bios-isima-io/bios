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

import io.isima.bios.models.AppType;
import java.util.Objects;
import java.util.StringJoiner;
import lombok.Getter;

/** Key that uniquely identifies an entry. */
@Getter
public class OperationsMetricsDimensions implements Comparable<OperationsMetricsDimensions> {
  private final String tenantName;
  private final String streamName;
  private final String appName;
  private final AppType appType;
  private final RequestType requestType;
  private final int destinationNumber;

  public OperationsMetricsDimensions(
      String tenantName,
      String streamName,
      String appName,
      AppType appType,
      RequestType requestType) {
    this.tenantName = Objects.requireNonNull(tenantName);
    this.streamName = streamName;
    this.appName = appName;
    this.appType = appType;
    this.requestType = Objects.requireNonNull(requestType);
    this.destinationNumber = 0;
  }

  private OperationsMetricsDimensions(int destinationNumber, OperationsMetricsDimensions other) {
    this.tenantName = other.tenantName;
    this.streamName = other.streamName;
    this.appName = other.appName;
    this.appType = other.appType;
    this.requestType = other.requestType;
    this.destinationNumber = destinationNumber;
  }

  @Override
  public String toString() {
    final var joiner = new StringJoiner(":");
    joiner.add(tenantName);
    joiner.add(streamName);
    joiner.add(appName);
    joiner.add(appType.name());
    joiner.add(requestType.toString());
    joiner.add(Integer.toString(destinationNumber));
    return joiner.toString();
  }

  OperationsMetricsDimensions newDestination(int destinationNumber) {
    return new OperationsMetricsDimensions(destinationNumber, this);
  }

  @Override
  public int compareTo(OperationsMetricsDimensions o) {
    int ret = Integer.compare(requestType.priority(), o.getRequestType().priority());
    if (ret == 0) {
      ret = appName.compareTo(o.appName);
      if (ret == 0) {
        ret = appType.compareTo(o.appType);
        if (ret == 0) {
          ret = Integer.compare(this.destinationNumber, o.getDestinationNumber());
          if (ret == 0) {
            ret = streamName.compareTo(o.getStreamName());
            if (ret == 0) {
              ret = tenantName.compareTo(o.getTenantName());
            }
          }
        }
      }
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OperationsMetricsDimensions)) {
      return false;
    }
    OperationsMetricsDimensions that = (OperationsMetricsDimensions) o;
    return tenantName.equals(that.tenantName)
        && streamName.equals(that.streamName)
        && appName.equals(that.appName)
        && appType.equals(that.appType)
        && requestType.equals(that.requestType)
        && destinationNumber == that.destinationNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantName, streamName, appName, appType, requestType, destinationNumber);
  }
}
