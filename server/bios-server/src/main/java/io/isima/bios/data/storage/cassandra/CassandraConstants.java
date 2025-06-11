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
package io.isima.bios.data.storage.cassandra;

/** Class to provide constants related to Cassandra. */
public class CassandraConstants {
  /** Cassandra default superuser user name. */
  public static final String DEFAULT_USERNAME = "cassandra";

  /** Cassandra default superuser password. */
  public static final String DEFAULT_PASSWORD = "cassandra";

  /** Text type name. */
  public static final String TYPE_TEXT = "text";

  /** Varchar type name. */
  public static final String TYPE_VARCHAR = "varchar";

  /** Varint type name. */
  public static final String TYPE_VARINT = "varint";

  /** Int type name. */
  public static final String TYPE_INT = "int";

  /** Long type name. */
  public static final String TYPE_LONG = "bigint";

  /** Double type name. */
  public static final String TYPE_DOUBLE = "double";

  /** Inet type name. */
  public static final String TYPE_INET = "inet";

  /** Date type name. */
  public static final String TYPE_DATE = "date";

  /** Timestamp type name. */
  public static final String TYPE_TIMESTAMP = "timestamp";

  /** UUID type name. */
  public static final String TYPE_UUID = "uuid";

  /** Boolean type name. */
  public static final String TYPE_BOOLEAN = "boolean";

  /** Blob type name. */
  public static final String TYPE_BLOB = "blob";

  // keyspace tfos_auth //////////////////////////////////////////////////

  /** Keyspace name for auth config. */
  public static final String KEYSPACE_AUTH = "tfos_auth";

  public static final String TABLE_AUTH = "USERS";

  // keyspace tfos_admin //////////////////////////////////////////////////

  public static final String KEYSPACE_ADMIN = "tfos_admin";
  public static final String KEYSPACE_BI = "tfos_bi_meta";

  public static final String TABLE_ADMIN = "admin";
  public static final String TABLE_ADMIN_ARCHIVED = "admin_archived";
  public static final String TABLE_INSIGHTS = "insights";
  public static final String TABLE_POSTPROCESS_RECORDS = "postprocess_records";
  public static final String TABLE_DATA_SKETCH_RECORDS = "sketch_records";
  public static final String TABLE_INFERENCE_RECORDS = "inference_records";
  public static final String TABLE_PROPERTIES = "config";
  public static final String TABLE_REPORTS = "reports";
  public static final String TABLE_ROLLUP_LOCKS = "rollup_locks";
  public static final String TABLE_APPENDIX = "tenant_appendix";
  public static final String TABLE_RESOURCE_ALLOCATION = "resource_allocation";

  // keyspace tfos_bi_meta where TFOS user tables exist //////////////////
  public static final String KEYSPACE_BI_META = "tfos_bi_meta";
  public static final String TABLE_TFOS_USERS = "users";
  public static final String TABLE_TFOS_ORGANIZATIONS = "bi_organizations";
  public static final String TABLE_TFOS_DOMAINS = "email_domains";

  /** Delete tenant and tenant configuration. */
  public static final String FORMAT_DELETE_KEYSPACE = "DROP KEYSPACE IF EXISTS %s";

  public static final String FORMAT_DROP_TABLE = "DROP TABLE IF EXISTS %s.%s";

  public static final String FORMAT_DROP_TABLE_QUALIFIED_NAME = "DROP TABLE IF EXISTS %s";

  public static final String LIST_KEYSPACES = "SELECT * from system_schema.keyspaces";

  public static final String LIST_TABLES =
      "select keyspace_name, table_name from system_schema.tables";

  /** Format for statment used for creating a role. Params are role name and password. */
  public static final String FORMAT_CREATE_ROLE =
      "CREATE ROLE IF NOT EXISTS %s WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = '%s'";

  /** Statement used for dropping a role. Append the role name you like to drop. */
  public static final String DROP_ROLE = "DROP ROLE IF EXISTS ";

  /**
   * Maximum error succession during a maintenance. This parameter is meant to be referred by
   * maintenance modules.
   */
  public static final int MAX_ERROR_SUCCESSION = 10;

  public static final String KEYSPACE_DATA_PREFIX = "tfos_d_";

  /** Prefix for events table name. */
  public static final String PREFIX_EVENTS_TABLE = "evt_";

  /** Prefix for events table name. */
  public static final String PREFIX_METRICS_TABLE = "met_";

  /** Prefix for rollup table name. */
  public static final String PREFIX_ROLLUP_TABLE = "rlp_";

  /** Prefix for view table name. */
  public static final String PREFIX_VIEW_TABLE = "ivw_";

  /** Prefix for index table name. */
  public static final String PREFIX_INDEX_TABLE = "idx_";

  /** Prefix for context table name. */
  public static final String PREFIX_CONTEXT_TABLE = "cx2_";

  /** Prefix for context table name. */
  public static final String PREFIX_CONTEXT_TABLE_OLD = "ctx_";

  /** Prefix for events column name. */
  public static final String PREFIX_EVENTS_COLUMN = "evt_";

  /** Prefix for rollup events column name. */
  public static final String PREFIX_ROLLUP_EVENTS_COLUMN = "rlp_";

  /** Prefix for context index table name. */
  public static final String PREFIX_CONTEXT_INDEX_TABLE = "cix_";

  public static final String TABLE_SKETCH_SUMMARY = "sketch_summary";
  public static final String TABLE_SKETCH_SUMMARY_CONTEXT = "sketch_summary_context";
  public static final String TABLE_SKETCH_BLOB = "sketch_blob";
  public static final String TABLE_SKETCH_BLOB_CONTEXT = "sketch_blob_context";

  /**
   * Format used for building a statement to create a keyspace for a tenant.
   *
   * <p>parameters:
   *
   * <dl>
   *   <li>String: keyspace name
   *   <li>String: replication strategy class ('SimpleStrategy' or 'NetworkTopologyStrategy')
   *   <li>String: RF property name ('replication_factor' for SimpleStrategy; dc name for
   *       NetworkTopologyStrategy)
   *   <li>int: replication factor
   * </dl>
   */
  public static final String FORMAT_CREATE_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s " + "AND durable_writes = true";

  /** Reserved column name for event ID. */
  public static final String COL_EVENT_ID = "event_id";

  /** Reserved column name for rollup time. */
  public static final String COL_ROLLUP_TIME = "rollup_time";

  /** Reserved column name for ingestion timestamp. */
  public static final String COL_INGEST_TIMESTAMP = "ingest_timestamp";

  /** Reserved column name for time index. */
  public static final String COL_TIME_INDEX = "time_index";

  // Used as the partition key when we want a single partition for the whole table.
  // It is populated with a single constant value for all rows, so it contains no real data.
  public static final String COL_DUMMY_PARTITION_KEY = "dummy";

  public static final String COL_TENANT = "tenant";

  public static final String COL_STREAM = "stream";

  public static final String COL_VERSION = "version";

  public static final String COL_CONFIG = "config";

  public static final String COL_DELETED = "is_deleted";
}
