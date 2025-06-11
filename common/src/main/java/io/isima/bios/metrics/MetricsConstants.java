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

import io.isima.bios.common.Constants;

public class MetricsConstants {
  private static final String PREFIX_INTERNAL = Constants.PREFIX_INTERNAL_NAME;

  public static final String STREAM_PREFIX = PREFIX_INTERNAL + "system_";
  public static final String ATTR_TFOS_NODE = "biosNode";
  public static final String ATTR_BIOS_VERSION = "tfos_version";
  public static final String ATTR_TENANT = "tenant";
  public static final String ATTR_STREAM = "stream";

  public static final String BIOS_SYSTEM_OP_METRICS_SIGNAL = PREFIX_INTERNAL + "operations";
  public static final String BIOS_TENANT_OP_METRICS_SIGNAL = PREFIX_INTERNAL + "usage";

  // This is for opm tenant view when created as default (without file)
  // need this as some tests asserts the difference for tenant opms when it is created
  // from a file or from code.
  public static final String BIOS_OPM_TENANT_VIEW = "_opm_stream_op_default_view";
  public static final String BIOS_OPM_TENANT_ROLLUP = "_opm_stream_op_default_rollup";

  // physical memory
  public static final String ATTR_TFOS_PHYSICAL_MEMORY = "biosphymemAvail";
  public static final String ATTR_TFOS_PHYSICAL_MEMORY_USED = "biosphymemUsed";
  public static final String ATTR_DB_PHYSICAL_MEMORY = "dbphymemAvail";
  public static final String ATTR_DB_PHYSICAL_MEMORY_USED = "dbphymemUsed";

  // cpu related
  public static final String ATTR_TFOS_CPU_COUNT = "bioscpuCount";
  public static final String ATTR_TFOS_CPU_LOAD = "bioscpuLoad";
  public static final String ATTR_DB_CPU_LOAD = "dbcpuLoad";

  // disk related ( need to fix this )
  public static final String ATTR_ROOT_DISK_USAGE = "root_disk_usage";
  public static final String ATTR_DATA_DISK_USAGE = "data_disk_usage";

  // casssandra DB metric
  public static final String ATTR_DB_STORAGE_USAGE = "dbstorageUsed";

  // memory (heap, off-heap) used
  public static final String ATTR_TFOS_ALLOCATED_MEM = "biosmemAllocated";
  public static final String ATTR_TFOS_MEM_USAGE = "biosmemUsed";
  public static final String ATTR_TFOS_MEM_USAGE_NONHEAP = "biosnonheapUsed";
  public static final String ATTR_DB_ALLOCATED_MEM = "dbmemAllocated";
  public static final String ATTR_DB_MEM_USAGE = "dbmemUsed";
  public static final String ATTR_DB_MEM_USAGE_NONHEAP = "dbmemnonheapUsed";

  // ( need to fix this )
  public static final String ATTR_OPEN_FD_COUNT = "openfdCount";
  public static final String ATTR_MAX_FD_COUNT = "maxfdCount";
  public static final String ATTR_TENANT_DISK_USAGE = "tenantdiskUsage";
}
