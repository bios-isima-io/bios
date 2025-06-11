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
package io.isima.bios.monitoring.store;

import io.isima.bios.common.MonitoringSystemConfigs;

/**
 * This class has list of rules which will be applied for monitoring system metrics TODO -
 * Externalize these rules in a json file
 */
public class BootstrappedRules {
  public static BootStrappedRule[] bootstrappedRules =
      new BootStrappedRule[] {
        /*
        Rule for high cpu load when one execution task is waiting for CPU for
        last one minute on an average
        cpu_overload = (cpu_load - num_cores) - number of tasks waiting for cpu
        cpu_load - number of executing + queued tasks in system over last one minute
        num_process - number of cores in system
        https://www.tecmint.com/understand-linux-load-averages-and-monitor-performance
         */
        new BootStrappedRule(
            "High Cpu Load",
            "((cpu_load - num_cores) > "
                + MonitoringSystemConfigs.getCpuLoadFactorThreshold()
                + ")"),
        /*
        Rule for high JVM memory usage
         memory_usage = (used_memory * 100) / total_memory > 70
         used_memory - the amount of used memory in JVM
         total_memory - the total amount of memory allocated to JVM
        */
        new BootStrappedRule(
            "High Memory Usage",
            "(((jvm_used_memory * 100) / jvm_total_memory) > "
                + MonitoringSystemConfigs.getMemoryUsageThreshold()
                + ")"),

        /*
         Rule for high JVM heap memory usage
         heap_memory_usage - (heap_used_memory * 100) / heap_total_memory > 70
         heap_used_memory - the amount of heap used memory in JVM
         heap_total_memory - the total amount of heap memory allocated to JVM
        */
        new BootStrappedRule(
            "High Heap Memory Usage",
            "(((heap_used_memory * 100) / heap_total_memory) > "
                + MonitoringSystemConfigs.getHeapMemoryUsageThreshold()
                + ")"),

        /*
        Rule for root disk usage higher than 80%
        root_disk_usage - Disk usage for root partition "/"
         */
        new BootStrappedRule(
            "High Root Disk Usage",
            "(root_disk_usage > " + MonitoringSystemConfigs.getRootDiskUsageThreshold() + ")"),

        /*
        Rule for data disk usage higher than 80%
        data_disk_usage - Disk usage for data partitions "/var/lib/db/data*"
         */
        new BootStrappedRule(
            "High Data Disk Usage",
            "(data_disk_usage > " + MonitoringSystemConfigs.getDataDiskUsageThreshold() + ")"),

        /*
        Rule for high heap memory usage in DB
        db_memory_usage - heap memory in use in DB JVM
        db_memory - allocated heap memory for DB JVM
         */
        new BootStrappedRule(
            "High heap memory usage in DB",
            "(((db_memory_usage * 100) / db_memory) > "
                + MonitoringSystemConfigs.getDbHeapMemoryUsageThreshold()
                + ")"),

        /*
        Rule for high memory usage in OS where DB is installed
        db_physical_memory_used - memory usage in OS where DB resides
        db_physical_memory - total memory in OS where DB resides
         */
        new BootStrappedRule(
            "High memory usage in DB OS",
            "(((db_physical_memory_used * 100) / db_physical_memory) > "
                + MonitoringSystemConfigs.getDbHeapMemoryUsageThreshold()
                + ")"),

        /*
        Rule for high open file descriptors in DB
        open_fd_count - number of open file descriptors
        max_fd_count = maximum number of allowed open file descriptors
         */
        new BootStrappedRule(
            "Too many open file descriptors",
            "(((open_fd_count * 100) / max_fd_count) > "
                + MonitoringSystemConfigs.getOpenFileDescriptorThreshold()
                + ")")
      };
}
