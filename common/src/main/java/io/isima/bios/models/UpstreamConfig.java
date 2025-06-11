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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class UpstreamConfig {

  @Valid @NotNull @NotEmpty private List<Upstream> upstreams;

  private LbPolicy lbPolicy = LbPolicy.ROUND_ROBIN;
  private long version;
  private long epoch;

  public UpstreamConfig() {}

  public List<Upstream> getUpstreams() {
    return upstreams;
  }

  public void setUpstreams(List<Upstream> upstreams) {
    this.upstreams = upstreams;
  }

  public LbPolicy getLbPolicy() {
    return lbPolicy;
  }

  public void setLbPolicy(LbPolicy lbPolicy) {
    this.lbPolicy = lbPolicy;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  @Override
  public String toString() {
    return "UpstreamConfig: [Upstreams = " + upstreams + ", lbPolicy = " + lbPolicy + "]";
  }
}
