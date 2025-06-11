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
package io.isima.bios.common;

public class MonitoringSystemConfigs {
  private static final String CPU_LOAD_FACTOR_THRESHOLD =
      "com.tieredfractals.tfos.cpu_load_factor_threshold";
  private static final String MEMORY_USAGE_THRESHOLD =
      "com.tieredfractals.tfos.memory_usage_threshold";
  private static final String HEAP_MEMORY_USAGE_THRESHOLD =
      "com.tieredfractals.tfos.heap_memory_usage_threshold";
  private static final String ROOT_DISK_USAGE_THRESHOLD =
      "com.tieredfractals.tfos.root_disk_usage_threshold";
  private static final String DATA_DISK_USAGE_THRESHOLD =
      "com.tieredfractals.tfos.data_disk_usage_threshold";
  private static final String DB_HEAP_MEMORY_USAGE_THRESHOLD =
      "com.tieredfractals.tfos.db_heap_memory_usage_threshold";
  private static final String DB_OS_MEMORY_USAGE_THRESHOLD =
      "com.tieredfractals.tfos.db_os_memory_usage_threshold";
  private static final String DB_OPEN_FILE_DESCRIPTOR_THRESHOLD =
      "com.tieredfractals.tfos.db_open_file_descriptor_threshold";
  private static final String MONITORING_NOTIFICATION_URL =
      "com.tieredfractals.tfos.monitoring_notification_url";
  private static final String MONITORING_NOTIFICATION_RETRY_COUNT =
      "com.tieredfractals.tfos.monitoring_notification_retry_count";
  private static final String MONITORING_NOTIFICATIONS_ENABLED =
      "io.isima.bios.alerts.monitoringNotificationsEnabled";
  private static final String ALERT_NOTIFICATIONS_ENABLED =
      "io.isima.bios.alerts.notificationsEnabled";
  private static final String ANOMALY_NOTIFICATIONS_ENABLED =
      "io.isima.bios.alerts.anomalyNotificationsEnabled";

  private static final BiosConfigBase tfosConfig = BiosConfigBase.getInstance();

  public static int getCpuLoadFactorThreshold() {
    return tfosConfig.getInt(CPU_LOAD_FACTOR_THRESHOLD, 1);
  }

  public static int getMemoryUsageThreshold() {
    return tfosConfig.getInt(MEMORY_USAGE_THRESHOLD, 80);
  }

  public static int getHeapMemoryUsageThreshold() {
    return tfosConfig.getInt(HEAP_MEMORY_USAGE_THRESHOLD, 80);
  }

  public static int getRootDiskUsageThreshold() {
    return tfosConfig.getInt(ROOT_DISK_USAGE_THRESHOLD, 80);
  }

  public static int getDataDiskUsageThreshold() {
    return tfosConfig.getInt(DATA_DISK_USAGE_THRESHOLD, 80);
  }

  public static int getDbHeapMemoryUsageThreshold() {
    return tfosConfig.getInt(DB_HEAP_MEMORY_USAGE_THRESHOLD, 80);
  }

  public static int getDbOsMemoryUsageThreshold() {
    return tfosConfig.getInt(DB_OS_MEMORY_USAGE_THRESHOLD, 80);
  }

  public static int getOpenFileDescriptorThreshold() {
    return tfosConfig.getInt(DB_OPEN_FILE_DESCRIPTOR_THRESHOLD, 80);
  }

  public static String getMonitoringNotificationUrl() {
    return tfosConfig.getString(
        MONITORING_NOTIFICATION_URL, "https://webhook.site/1eb5341c-1b9f-4329-9699-1fe29c353eb7");
  }

  public static Integer getMonitoringNotificationRetryCount() {
    return tfosConfig.getInt(MONITORING_NOTIFICATION_RETRY_COUNT, 5);
  }

  public static Boolean getMonitoringNotificationsEnabled() {
    return tfosConfig.getBoolean(MONITORING_NOTIFICATIONS_ENABLED, false);
  }

  public static Boolean getAlertNotificationsEnabled() {
    return tfosConfig.getBoolean(ALERT_NOTIFICATIONS_ENABLED, true);
  }

  public static Boolean getAnomalyNotificationsEnabled() {
    return tfosConfig.getBoolean(ANOMALY_NOTIFICATIONS_ENABLED, false);
  }
}
