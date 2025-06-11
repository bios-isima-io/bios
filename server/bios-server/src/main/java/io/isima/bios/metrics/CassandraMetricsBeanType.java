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
package io.isima.bios.metrics;

public enum CassandraMetricsBeanType {
  DB_STORAGE_LOAD("org.apache.cassandra.metrics:type=Storage,name=Load"),
  MEMORY("java.lang:type=Memory"),
  OS("java.lang:type=OperatingSystem"),
  KS_LIVE_DISK("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=LiveDiskSpaceUsed"),
  KS_READ_LATENCY("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=ReadLatency"),
  KS_TOTAL_DISK("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=TotalDiskSpaceUsed"),
  KS_WRITE_LATENCY("org.apache.cassandra.metrics:type=Keyspace,keyspace=%s,name=WriteLatency");

  private String beanType;

  CassandraMetricsBeanType(String beanType) {
    this.setBeanType(beanType);
  }

  public String getBeanType() {
    return String.format(beanType, "");
  }

  public String getFormattedBeanType(final String arg) {
    return String.format(beanType, arg);
  }

  private void setBeanType(String beanType) {
    this.beanType = beanType;
  }
}
