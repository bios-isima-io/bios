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

import io.isima.bios.models.v1.StreamType;
import java.util.Objects;
import lombok.Getter;

@Getter
public class StreamId {
  private String tenant;
  private String stream;
  private Long version;
  private StreamType type;

  public StreamId(String tenant, String stream, Long version) {
    this.tenant = tenant;
    this.stream = stream;
    this.version = version;
  }

  public StreamId(String tenant, String stream, Long version, StreamType type) {
    this.tenant = tenant;
    this.stream = stream;
    this.version = version;
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamId)) {
      return false;
    }
    StreamId other = (StreamId) o;

    return this.tenant.equals(other.tenant)
        && this.stream.equals(other.stream)
        && this.version.equals(other.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, stream, version);
  }

  @Override
  public String toString() {
    String out = tenant + "/" + stream + "/" + version;
    if (type != null) {
      out += "(" + type + ")";
    }
    return out;
  }
}
