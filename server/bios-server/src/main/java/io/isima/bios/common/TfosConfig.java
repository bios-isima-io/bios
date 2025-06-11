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

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

/** TFOS configuration value provider. */
public class TfosConfig {

  public static final String OPERATION_TIMEOUT_MILLIS = "io.isima.bios.data.operationTimeoutMillis";

  public static final String SIGNAL_DEFAULT_TIME_TO_LIVE =
      "io.isima.bios.data.signal.defaultTimeToLive";

  private static final String FEATURE_RECORDS_DEFAULT_TIME_TO_LIVE =
      "io.isima.bios.features.records.defaultTtl";

  private static final String EVENTS_GC_GRACE_SECONDS = "io.isima.bios.data.signal.gcGraceSeconds";

  public static final String CONTEXT_JOIN_MAXIMUM_CHAIN_LENGTH =
      "io.isima.bios.data.signal.maxEnrichChainLength";

  private static final String MAINTENANCE_DB_CLEANUP_GRACE_TIME =
      "io.isima.bios.db.cleanup.grace.time.minutes";

  private static final String EVENTS_COMPACTION_CONFIG =
      "io.isima.bios.data.signal.compactionConfig";

  private static final String EVENTS_BLOOM_FILTER_FP_CHANCE =
      "io.isima.bios.data.bloomFilterFpChance";

  public static final String CONTEXT_ENTRY_CACHE_MAX_SIZE =
      "io.isima.bios.data.contextEntryCacheMaxSize";

  public static final String LOCK_LOGGING_TARGET = "io.isima.bios.maintenance.lock.logging.target";

  public static final String CONTEXT_MAINTENANCE_CLEANUP_MARGIN =
      "io.isima.bios.data.maintenance.context.cleanupMargin";

  public static final String ROLLUP_ENABLED = "io.isima.bios.data.rollup.enabled";
  public static final String ROLLUP_INTERVAL_SECONDS = "io.isima.bios.data.rollup.interval";
  public static final String INDEXING_DEFAULT_INTERVAL_SECONDS =
      "io.isima.bios.data.indexing.defaultInterval";
  private static final String ROLLUP_LOCK_DEFAULT_TIME_TO_LIVE = "io.isima.bios.lock.defaultTtl";
  private static final String ROLLUP_PROCESSOR_NUM_THREADS = "io.isima.bios.data.rollup.numThreads";

  private static final String NUM_CONTEXT_TASK_SLOTS = "io.isima.bios.data.numContextTaskSlots";

  public static final String METRICS_REPORT_INTERVAL_SECONDS =
      "io.isima.bios.metrics.reportInterval";
  public static final String METRICS_DESTINATION = "io.isima.bios.metrics.destination";
  public static final String METRICS_ENABLED = "io.isima.bios.metrics.enabled";
  public static final String DB_METRICS_ENABLED = "io.isima.bios.db.metrics.enabled";
  public static final String METRICS_CONFIG_READ_FROM_FILE =
      "io.isima.bios.db.metrics.config.readFromFile";

  public static final String ANOMALY_DETECTION_THRESHOLD =
      "io.isima.bios.anomaly.detector.threshold";
  public static final String ANOMALY_DETECTOR_WINDOW_LENGTH =
      "io.isima.bios.anomaly.detector.window.length";
  public static final String ANOMALY_NOTIFICATION_WEBHOOK_URL =
      "io.isima.bios.anomaly.detector.webhook.url";

  // time indexing
  public static final String TIME_INDEX_WIDTH = "io.isima.bios.data.timeIndexWidth";
  public static final String PARTITION_SIZE = "io.isima.bios.data.partitionSize";
  public static final String OPERATIONS_PER_SEC = "io.isima.bios.data.ops";
  public static final String AVERAGE_EVENT_SIZE = "io.isima.bios.data.averageEventSize";

  public static final String SUMMARIZE_HORIZONTAL_LIMIT =
      "io.isima.bios.data.summarize.horizontal-limit";

  public static final String SELECT_MAX_NUM_DATA_POINTS = "io.isima.bios.select.maxNumDataPoints";

  // authentication related configuration keys
  public static final String AUTH_EXPIRATION_TIME = "io.isima.bios.auth.timeout";
  public static final int MINIMUM_AUTH_EXPIRATION = 15000; // 15 seconds
  public static final String AUTH_TOKEN_SECRET = "io.isima.bios.auth.token.secret";
  public static final String PASSWORD_RESET_TOKEN_SECRET =
      "io.isima.bios.passwordReset.token.secret";
  public static final String APPROVAL_TOKEN_SECRET = "io.isima.bios.approval.token.secret";
  public static final String VERIFICATION_TOKEN_SECRET = "io.isima.bios.verification.token.secret";
  public static final String FINALIZATION_TOKEN_SECRET = "io.isima.bios.finalization.token.secret";

  // Initial users file location.
  public static final String INITIAL_USERS_FILE = "io.isima.bios.initialUsersFile";

  // sign-up related configuration keys
  public static final String SIGNUP_MAIL_CONTENT = "io.isima.bios.signup.content";
  public static final String REGISTRATION_COMPLETE_MAIL_CONTENT =
      "io.isima.bios.registration.complete.content";
  public static final String SIGNUP_APPROVAL_MAIL_CONTENT = "io.isima.bios.signup.content";
  public static final String SIGNUP_VERIFY_URL_FORMAT = "io.isima.bios.signup.verifyUrlFormat";
  public static final String INVITE_VERIFY_URL_FORMAT = "io.isima.bios.signup.inviteUrlFormat";
  public static final String SIGNUP_APPROVAL_URL_TEMPLATE =
      "io.isima.bios.signup.approval.url.template";
  public static final String BIOS_DOMAIN_NAME = "io.isima.bios.domain.name";
  public static final String FORGOT_PASSWORD_URL_FORMAT = "io.isima.bios.forgotPassword.urlFormat";
  public static final String FORGOT_PASSWORD_MAIL_CONTENT = "io.isima.bios.forgotPasswordContent";

  public static final String SIGNUP_INVALID_EMAIL_DOMAIN = "io.isima.bios.signup.invalid.domain";
  public static final String SIGNUP_EXPIRATION_TIME = "io.isima.bios.signup.timeout";
  public static final String SIGNUP_APPROVAL_EXPIRATION_TIME =
      "io.isima.bios.signup.approval.timeout";

  public static final String SIGNUP_MAIL_BCC = "io.isima.bios.signup.mail.bcc";
  public static final String SIGNUP_APPROVAL_MAIL_BCC = "io.isima.bios.signup.approval.mail.bcc";

  // Default user and org id
  public static final Long DEFAULT_SUPERADMIN_ID = 1L;
  public static final Long DEFAULT_ORG_ID = 1L;

  // Default user names and passwords
  public static final String DEFAULT_SUPERADMIN_USERNAME = "superadmin";
  // on prem configuration
  public static final String ON_PREM_USER_MANAGEMENT_ENABLED = "io.isima.bios.on.prem.enabled";
  public static final String ON_PREM_USER_MANAGEMENT_ADMIN_PASSWORD =
      "io.isima.bios.o.prem.user.creation.admin.password";
  // mail related configuration keys
  public static final String MAIL_DISABLED = "io.isima.bios.mail.disabled";
  public static final String MAIL_PROVIDER = "io.isima.bios.mail.provider";
  public static final String MAIL_AUTH_ENABLE = "io.isima.bios.mail.auth.enable";
  public static final String MAIL_TLS_ENABLE = "io.isima.bios.mail.tls.enable";
  public static final String MAIL_SECURE_CONNECTION_DISABLED =
      "io.isima.bios.mail.secureConnection.disabled";
  public static final String MAIL_HOST = "io.isima.bios.mail.host";
  public static final String MAIL_SSL_PORT = "io.isima.bios.mail.sslport";
  public static final String MAIL_TLS_PORT = "io.isima.bios.mail.tlsport";
  public static final String MAIL_NON_SECURE_PORT = "io.isima.bios.mail.nonsecureport";
  public static final String MAIL_FROM_ADDRESS = "io.isima.bios.mail.fromaddress";
  public static final String MAIL_USERNAME = "io.isima.bios.mail.username";
  public static final String MAIL_PASSWORD = "io.isima.bios.mail.password";

  public static final String JUPYTER_HUB_URL = "io.isima.bios.jupyterhub.url";
  public static final String JUPYTER_HUB_ADMIN_TOKEN = "io.isima.bios.jupyterhub.adminToken";

  public static final String AWS_ACCESS_KEY = "<AWS_ACCESS_KEY>";
  public static final String AWS_SECRET_KEY = "<AWS_SECRET_KEY>";

  public static final String CASSANDRA_INGEST_CONCURRENCY = "io.isima.bios.db.ingest.concurrency";

  public static final String CASSANDRA_INGEST_BATCH_SIZE = "io.isima.bios.db.ingest.batchSize";

  public static final String HEALTH_CHECK_INTERVAL_SECONDS = "io.isima.bios.healthcheck.interval";
  public static final String HEALTH_CHECK_ENABLBED = "io.isima.bios.healthcheck.enabled";

  private static final String SIGNUP_APPROVAL_MAIL_SUBJECT_TEMPLATE =
      "io.isima.bios.signup.approval.mail.subject.template";
  private static final String SIGNUP_APPROVAL_ADMIN_EMAIL_ID =
      "io.isima.bios.signup.approval.admin.email.id";
  private static final String SIGNUP_APPROVAL_COMPETITIOR_ADMIN_EMAIL_ID =
      "io.isima.bios.signup.approval.competitior.admin.email.id";
  private static final String SIGNUP_APPROVAL_MAIL_DEFAULT_SUBJECT_TEMPLATE_USER =
      "Signup Approval For New User with Email : %s";
  private static final String SIGNUP_APPROVAL_MAIL_DEFAULT_SUBJECT_TEMPLATE_TENANT =
      "Signup Approval For New Tenant with Email : %s";
  private static final String SIGNUP_ATTEMPT_NOTIFICATION_DEFAULT_SUBJECT =
      "Existing user re-attempted signup for user : %s";
  private static final String SIGNUP_REJECTED_MAIL_DEFAULT_SUBJECT =
      "Signup from public email domain : %s";
  private static final String SIGNUP_REJCTED_MAIL_DEFAULT_BODY =
      "Email address = %s signup request is rejected";
  private static final String SIGNUP_WARNING_MAIL_DEFAULT_SUBJECT =
      "Signup request from potential competitior or partner : %s";
  private static final String SIGNUP_WARNING_MAIL_DEFAULT_BODY =
      "To approve the email : %s go to tenant-approvals-competitor group";

  // CA certificates file
  public static final String SSL_CERT_FILE = "io.isima.bios.client.ssl.cafile";
  public static final String SSL_CERT_SELF_SIGNED = "io.isima.bios.client.ssl.selfSigned";
  public static final String JUPYTERHUB_CERT_SELF_SIGNED =
      "io.isima.bios.jupyterhub.ssl.selfSigned";
  public static final String WEBHOOK_CERT_SELF_SIGNED = "io.isima.bios.notification.ssl.selfSigned";

  // Blocked ip addresses file
  public static final String BLOCKED_IP_ADDRESSES_FILE = "io.isima.bios.blocked.ip.addresses.file";

  /**
   * CORS Access-Control-Allow-Origin origin white list (comma separated). Set asterisk ("*") to
   * allow-all.
   */
  public static final String CORS_ALLOW_ORIGIN_WHITELIST =
      "io.isima.bios.server.corsOriginWhitelist";

  /** Cassandra operation retry count. */
  public static final int CASSANDRA_OP_RETRY_COUNT = 3;

  /** Sleep milliseconds before retrying Cassandra keyspace or table creation. */
  public static final long CASSANDRA_CREATE_RETRY_SLEEP_MILLIS = 5000;

  /** Sleep milliseconds before retrying Cassandra operation. */
  public static final long CASSANDRA_OP_RETRY_SLEEP_MILLIS = 1000;

  /** Backoff milliseconds before retrying timed out Cassandra operation. */
  public static final long CASSANDRA_TIMEOUT_BACKOFF_MILLIS = 20000;

  /**
   * Turns on test mode -- the key enables special features for test such as test service, online
   * session expiry configuration, etc.
   */
  public static final String TEST_MODE = "io.isima.bios.test.enabled";

  public static final String TEST_SIGNUP = "io.isima.bios.test.signup.enabled";

  public static final String INGEST_ERRORS_SIGNAL = "io.bios.ingest.internal.errors";

  public static final String DB_AUTH_USER = "io.isima.bios.db.auth.user";

  public static final String DB_AUTH_PASSWORD = "io.isima.bios.db.auth.password";

  public static final String MYSQL_SOURCE_TRUSTSTORE_PASSWORD =
      "io.isima.bios.integrations.mysql.truststore.password";

  //
  // Utilities ///////////////////////////////////////////////////////////////////////
  //
  protected static BiosConfigBase getInstance() {
    return BiosConfigBase.getInstance();
  }

  public static void setProperties(Properties properties) {
    BiosConfigBase.setProperties(properties);
  }

  //
  // End utilities ///////////////////////////////////////////////////////////////////////

  public static boolean mailDisabled() {
    return getInstance().getBoolean(MAIL_DISABLED, false);
  }

  public static boolean onPremUserManagementEnabled() {
    return getInstance().getBoolean(ON_PREM_USER_MANAGEMENT_ENABLED, false);
  }

  public static String onPremUserManagementAdminPassword() {
    return getInstance().getString(ON_PREM_USER_MANAGEMENT_ADMIN_PASSWORD, "");
  }

  public static String mailProvider() {
    return getInstance().getString(MAIL_PROVIDER, "aws");
  }

  public static boolean mailAuthEnabled() {
    return getInstance().getBoolean(MAIL_AUTH_ENABLE, true);
  }

  public static boolean mailTlsEnabled() {
    return getInstance().getBoolean(MAIL_TLS_ENABLE, true);
  }

  public static boolean mailSecureConnectionDisabled() {
    return getInstance().getBoolean(MAIL_SECURE_CONNECTION_DISABLED, false);
  }

  public static String mailServiceHost() {
    return getInstance().getString(MAIL_HOST, "email-smtp.us-east-1.amazonaws.com");
  }

  public static String getJupyterHubUrl() {
    return getInstance().getString(JUPYTER_HUB_URL, "https://" + biosDomainName());
  }

  public static String getJupyterHubAdminToken() {
    return getInstance().getString(JUPYTER_HUB_ADMIN_TOKEN, "token TOKEN");
  }

  public static boolean ingestErrorsSignal() {
    return getInstance().getBoolean(INGEST_ERRORS_SIGNAL, false);
  }

  /** Return AWS Access key. */
  public static String getAwsAccessKey() {
    return getInstance().getString(AWS_ACCESS_KEY, "<AWS_ACCESS_KEY>");
  }

  /** Return AWS Secret key. */
  public static String getAwsSecretKey() {
    return getInstance().getString(AWS_SECRET_KEY, "<AWS_SECRET_KEY>");
  }

  public static String mailServiceFromAddress() {
    return getInstance().getString(MAIL_FROM_ADDRESS, "bios@isima.io");
  }

  public static String mailServiceUserName() {
    return getInstance().getString(MAIL_USERNAME, "<AWS_SES_USER>");
  }

  public static String mailServicePassword() {
    return getInstance().getString(MAIL_PASSWORD, "<AWS_SES_PASSWORD>");
  }

  public static int mailSmtpSslPort() {
    return getInstance().getInt(MAIL_SSL_PORT, 465);
  }

  public static int mailSmtpTlsPort() {
    return getInstance().getInt(MAIL_TLS_PORT, 587);
  }

  public static int mailSmtpNonSecurePort() {
    return getInstance().getInt(MAIL_NON_SECURE_PORT, 25);
  }

  public static int authExpirationTimeMillis() {
    return Math.max(
        getInstance().getInt(AUTH_EXPIRATION_TIME, 10 * 60 * 1000), MINIMUM_AUTH_EXPIRATION);
  }

  public static String authTokenSecret() {
    return getInstance().getString(AUTH_TOKEN_SECRET, "");
  }

  public static String passwordResetTokenSecret() {
    return getInstance().getString(PASSWORD_RESET_TOKEN_SECRET, "");
  }

  public static String approvalTokenSecret() {
    return getInstance().getString(APPROVAL_TOKEN_SECRET, "");
  }

  public static String verificationTokenSecret() {
    return getInstance().getString(VERIFICATION_TOKEN_SECRET, "");
  }

  public static String finalizationTokenSecret() {
    return getInstance().getString(FINALIZATION_TOKEN_SECRET, "");
  }

  public static String initialUsersFile() {
    return getInstance().getPath(INITIAL_USERS_FILE, null);
  }

  public static List<String> signupMailBccList() {
    return cleanupList(getInstance().getStringArray(SIGNUP_MAIL_BCC, ",", ""));
  }

  public static List<String> signupApprovalMailBccList() {
    return cleanupList(getInstance().getStringArray(SIGNUP_APPROVAL_MAIL_BCC, ",", ""));
  }

  private static List<String> cleanupList(String[] src) {
    return Stream.of(src).filter(StringUtils::isNotBlank).collect(Collectors.toList());
  }

  public static String signupApprovalAdminMailId() {
    return getInstance().getString(SIGNUP_APPROVAL_ADMIN_EMAIL_ID, "tenant-approvals-eng@isima.io");
  }

  public static String signupApprovalCompetitorAdminMailId() {
    return getInstance()
        .getString(
            SIGNUP_APPROVAL_COMPETITIOR_ADMIN_EMAIL_ID, "tenant-approvals-competitor@isima.io");
  }

  public static String signupApprovalMailSubjectTemplateUser() {
    return getInstance()
        .getString(
            SIGNUP_APPROVAL_MAIL_SUBJECT_TEMPLATE,
            SIGNUP_APPROVAL_MAIL_DEFAULT_SUBJECT_TEMPLATE_USER);
  }

  public static String signupApprovalMailSubjectTemplateTenant() {
    return SIGNUP_APPROVAL_MAIL_DEFAULT_SUBJECT_TEMPLATE_TENANT;
  }

  public static String signupAttemptNotificationSubjectTemplate() {
    return SIGNUP_ATTEMPT_NOTIFICATION_DEFAULT_SUBJECT;
  }

  public static String signupRejectedAdminSubjectTemplate() {
    return SIGNUP_REJECTED_MAIL_DEFAULT_SUBJECT;
  }

  public static String signupRejectedAdminBodyTemplate() {
    return SIGNUP_REJCTED_MAIL_DEFAULT_BODY;
  }

  public static String signupWarningAdminSubjectTemplate() {
    return SIGNUP_WARNING_MAIL_DEFAULT_SUBJECT;
  }

  public static String signupWarningAdminBodyTemplate() {
    return SIGNUP_WARNING_MAIL_DEFAULT_BODY;
  }

  public static int signupApprovalExpirationTimeMillis() {
    return getInstance().getInt(SIGNUP_APPROVAL_EXPIRATION_TIME, 2 * 24 * 60 * 60 * 1000);
  }

  public static int signupExpirationTimeMillis() {
    return getInstance().getInt(SIGNUP_EXPIRATION_TIME, 4 * 24 * 60 * 60 * 1000);
  }

  public static String[] signupInvalidEmailDomains() {
    return getInstance().getStringArray(SIGNUP_INVALID_EMAIL_DOMAIN, ",", "gmail.com, yahoo.com");
  }

  public static String signupVerifyBaseUrl() {
    return getInstance()
        .getString(SIGNUP_VERIFY_URL_FORMAT, "https://localhost/verifyuser?token=%s");
  }

  public static String invitationVerifyBaseUrl() {
    return getInstance().getString(INVITE_VERIFY_URL_FORMAT, "https://localhost/invite?token=%s");
  }

  public static String biosDomainName() {
    return getInstance().getString(BIOS_DOMAIN_NAME, "localhost");
  }

  public static String adminApprovalBaseUrlTemplate() {
    return getInstance()
        .getString(SIGNUP_APPROVAL_URL_TEMPLATE, "https://%s/bios/v1/signup/approve");
  }

  private static String DEFAULT_APPROVAL_MAIL_CONTENT =
      "<html>\n"
          + "<head></head>\n"
          + "<body>\n"
          + "  <h1>User Signup Approval Form </h1>\n"
          + "<p>\n"
          + "service cluster = %s\n"
          + "%s\n"
          + "</p>\n"
          + "  <form name=\"userApprovalForm\" method=\"post\" action=\"%s\" "
          + "enctype=\"multipart/form-data\" target=\"_blank\">"
          + "  Choose action for user :\n"
          + " <input type=\"hidden\" name=\"token\" value=\"%s\" />\n"
          + " <input type=\"hidden\" name=\"action\" value=\"Approve\" />\n"
          + " <input type=\"submit\" value=\"Approve\">\n"
          + "</form>\n"
          + "</body>\n"
          + "</html>";

  public static String signupMailContent() {
    return getInstance()
        .getString(SIGNUP_MAIL_CONTENT, MAIL_HEADER + DEFAULT_SIGNUP_MAIL_CONTENT + MAIL_FOOTER);
  }

  public static String registrationCompleteMailContent() {
    return getInstance()
        .getString(
            REGISTRATION_COMPLETE_MAIL_CONTENT,
            MAIL_HEADER + DEFAULT_REGISTRATION_COMPLETE_MAIL_CONTENT + MAIL_FOOTER);
  }

  public static String alreadySignupMailContent() {
    return MAIL_HEADER + DEFAULT_ALREADY_SIGNUP_MAIL_CONTENT + MAIL_FOOTER;
  }

  public static String signupApprovalMailContent() {
    return getInstance().getString(SIGNUP_APPROVAL_MAIL_CONTENT, DEFAULT_APPROVAL_MAIL_CONTENT);
  }

  public static String signupApprovalNotificationMailContent() {
    return "Action taken = %s\n";
  }

  public static String signupAttemptNotificationMailContent() {
    return "Signup attempt taken by the registered user %s for tenant %s from bios domain %s\n";
  }

  public static String forgotPasswordBaseUrl() {
    return getInstance()
        .getString(FORGOT_PASSWORD_URL_FORMAT, "https://localhost/passwordreset?token=%s");
  }

  public static String forgotPasswordMailContent() {
    return getInstance()
        .getString(
            FORGOT_PASSWORD_MAIL_CONTENT,
            MAIL_HEADER + DEFAULT_FORGOT_PASSWORD_MAIL_CONTENT + MAIL_FOOTER);
  }

  public static final boolean AUTH_CHECK_ENABLED = true;

  /** Maximum time to allow for ingest/extract requests before returning. */
  public static int operationTimeoutMillis() {
    return getInstance().getInt(OPERATION_TIMEOUT_MILLIS, 10000);
  }

  /** Default time to live seconds of a signal table. (default: 60 days) */
  public static int signalDefaultTimeToLive() {
    return getInstance().getInt(SIGNAL_DEFAULT_TIME_TO_LIVE, 60 * 24 * 3600);
  }

  /** Default time to live seconds of a feature record table. (default: 90 days) */
  public static int featureRecordsDefaultTimeToLive() {
    return getInstance().getInt(FEATURE_RECORDS_DEFAULT_TIME_TO_LIVE, 90 * 24 * 3600);
  }

  /**
   * Default maintenance grace time in minutes during which candidate entities for deletion(tables
   * and databases) are kept in DB before cleanup.
   */
  public static int getMaintenanceGraceTimeInMinutesForDatabaseCleanup() {
    return getInstance().getInt(MAINTENANCE_DB_CLEANUP_GRACE_TIME, 10);
  }

  /**
   * Default GC grace seconds for events table.
   *
   * <p>default: 259200 (3 days)
   */
  public static int eventsGcGraceSeconds() {
    return getInstance().getInt(EVENTS_GC_GRACE_SECONDS, 259200);
  }

  public static int contextJoinMaximumChainLength() {
    return getInstance().getInt(CONTEXT_JOIN_MAXIMUM_CHAIN_LENGTH, 2);
  }

  /** Compaction configuration for events table. */
  public static String eventsCompactionConfig() {
    return getInstance()
        .getString(
            EVENTS_COMPACTION_CONFIG,
            "{"
                + "'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
                + " 'compaction_window_size': '6',"
                + " 'compaction_window_unit': 'HOURS',"
                + " 'tombstone_compaction_interval': '43200',"
                + " 'tombstone_threshold': '0.8',"
                + " 'unchecked_tombstone_compaction': 'true'"
                + "}");
  }

  /**
   * Table config bloom_filter_fp_chance.
   *
   * <p>default: 0.1 (10%)
   */
  public static String eventsBloomFilterFpChance() {
    return getInstance().getString(EVENTS_BLOOM_FILTER_FP_CHANCE, "0.01");
  }

  /** Maximum number of context entry cache items. */
  public static int contextEntryCacheMaxSize() {
    return getInstance().getInt(CONTEXT_ENTRY_CACHE_MAX_SIZE, 524288);
  }

  /**
   * Margin milliseconds for context entry cleanup milliseconds, default: 60000 (ms). Deleted and
   * overwritten context entries in DB are deleted by context table maintenance, but they are
   * deleted only after this margin milliseconds since inserted.
   */
  public static long contextMaintenanceCleanupMargin() {
    return getInstance().getLong(CONTEXT_MAINTENANCE_CLEANUP_MARGIN, 60000L);
  }

  /**
   * Specifies prefix of lock logging target. Set an empty string in order to log any lock
   * activities. Do not set anything to disable log.
   */
  public static String getLockLoggingTarget() {
    return getInstance().getString(LOCK_LOGGING_TARGET, null);
  }

  /** Rollup enabled. */
  public static boolean rollupEnabled() {
    return getInstance().getBoolean(ROLLUP_ENABLED, true);
  }

  /** Rollup interval in seconds. */
  public static int rollupIntervalSeconds() {
    return getInstance().getInt(ROLLUP_INTERVAL_SECONDS, 6);
  }

  /** Number of rollup processor threads. */
  public static int rollupThreadCount() {
    return getInstance().getInt(ROLLUP_PROCESSOR_NUM_THREADS, 8);
  }

  /**
   * Number of concurrent tasks for context feature calculation.
   *
   * <p>default=3
   */
  public static int numContextTaskSlots() {
    return getInstance().getInt(NUM_CONTEXT_TASK_SLOTS, 3);
  }

  /** Default indexing interval when not specified. */
  public static int indexingDefaultIntervalSeconds() {
    return getInstance().getInt(INDEXING_DEFAULT_INTERVAL_SECONDS, 300);
  }

  /** Rollup lock entry default TTL in seconds. */
  public static int rollupLockDefaultTimeToLiveSeconds() {
    return getInstance().getInt(ROLLUP_LOCK_DEFAULT_TIME_TO_LIVE, 60);
  }

  /** Metrics report period in seconds. */
  public static int metricsReportIntervalSeconds() {
    return getInstance().getInt(METRICS_REPORT_INTERVAL_SECONDS, 30);
  }

  /** Metrics report enabled(true/false) . */
  public static Boolean metricsReportEnabled() {
    return getInstance().getBoolean(METRICS_ENABLED, true);
  }

  /**
   * return boolean value whether capturing of db metrics is enabled or not, if this property is not
   * set, it return true.
   */
  public static Boolean dbMetricsEnabled() {
    return getInstance().getBoolean(DB_METRICS_ENABLED, true);
  }

  /**
   * return boolean value whether to read operational metrics config from file or not, if this
   * property is not set, it return true.
   */
  public static boolean readOperationalMetricsConfigFromFile() {
    return getInstance().getBoolean(METRICS_CONFIG_READ_FROM_FILE, true);
  }

  /**
   * Default time_index width in millisecond. Don't change this configuration once service has
   * started. Changing value voids entries in existing tables.
   */
  public static long timeIndexWidthMillis() {
    return getInstance().getLong(TIME_INDEX_WIDTH, 0L);
  }

  /** Target data table partition size in bytes. */
  public static long partitionSize() {
    return getInstance().getLong(PARTITION_SIZE, 400L * 1024 * 1024);
  }

  /** Estimated maximum operation per second. */
  // TODO(Naoki): Make this a stream configuration parameter
  public static long operationsPerSec() {
    return getInstance().getLong(OPERATIONS_PER_SEC, 1000);
  }

  /** Estimated average event size in bytes. */
  public static long averageEventSize() {
    return getInstance().getLong(AVERAGE_EVENT_SIZE, 1024);
  }

  public static int summarizeHorizontalLimit() {
    return getInstance().getInt(SUMMARIZE_HORIZONTAL_LIMIT, 2048);
  }

  /** Maximum number of data points to generate during a select operation. */
  public static int selectMaxNumDataPoints() {
    return getInstance().getInt(SELECT_MAX_NUM_DATA_POINTS, 500 * 1000);
  }

  /**
   * Anomaly detection threshold - alert values outside this threshold will be qualified as anomaly.
   */
  public static Double getAnomalyDetectionThreshold() {
    return getInstance().getDouble(ANOMALY_DETECTION_THRESHOLD, 0.2);
  }

  /** Anomaly detector window length. */
  public static int getAnomalyDetectorWindowLength() {
    return getInstance().getInt(ANOMALY_DETECTOR_WINDOW_LENGTH, 10);
  }

  /** Anomaly notification url. */
  public static String getAnomalyNotificationUrl() {
    return getInstance()
        .getString(
            ANOMALY_NOTIFICATION_WEBHOOK_URL,
            "https://webhook.site/1eb5341c-1b9f-4329-9699-1fe29c353eb7");
  }

  /** Parallelism for ingest operation to Cassandra. */
  public static int getDbIngestConcurrency() {
    return getInstance().getInt(CASSANDRA_INGEST_CONCURRENCY, 8192);
  }

  public static int getDbIngestBatchSize() {
    return getInstance().getInt(CASSANDRA_INGEST_BATCH_SIZE, 1024);
  }

  /** SSL certificate file path used for fan routing (default=null). */
  public static String cafile() {
    return getInstance().getPath(SSL_CERT_FILE, null);
  }

  public static boolean useSelfSignedCertForFanRouting() {
    return getInstance().getBoolean(SSL_CERT_SELF_SIGNED, false);
  }

  public static boolean useSelfSignedCertForJupyterHub() {
    return getInstance().getBoolean(JUPYTERHUB_CERT_SELF_SIGNED, false);
  }

  public static boolean useSelfSignedCertForNotifications() {
    return getInstance().getBoolean(WEBHOOK_CERT_SELF_SIGNED, false);
  }

  /** Blocked ip addresses file */
  public static String getBlockedIpAddressesFile() {
    return getInstance().getPath(BLOCKED_IP_ADDRESSES_FILE, null);
  }

  public static String getDbAuthUser() {
    return getInstance().getString(DB_AUTH_USER, "");
  }

  public static String getDbAuthPassword() {
    return getInstance().getString(DB_AUTH_PASSWORD, "");
  }

  public static int healthCheckIntervalSeconds() {
    return getInstance().getInt(HEALTH_CHECK_INTERVAL_SECONDS, 60);
  }

  public static String mysqlSourceTrustStorePassword() {
    return getInstance().getString(MYSQL_SOURCE_TRUSTSTORE_PASSWORD, "");
  }

  private static final String MAIL_HEADER =
      "<!DOCTYPE html>\n"
          + "<html lang=\"en\">\n"
          + "<head>\n"
          + "  <meta charset=\"UTF-8\">\n"
          + "  <style>\n"
          + "    body {\n"
          + "      font-style: normal;\n"
          + "      font-weight: 500;\n"
          + "      font-size: 14px;\n"
          + "      line-height: 24px;\n"
          + "      text-align: center;\n"
          + "      margin: 0;\n"
          + "      padding: 0;\n"
          + "      font-family: HelveticaNeue, 'Helvetica Neue', Helvetica, Arial, sans-serif;\n"
          + "      background-color: #e5e5e5;\n"
          + "    }\n"
          + "\n"
          + "    .container {\n"
          + "      width: 525px;\n"
          + "      margin: 1rem auto;\n"
          + "      font-style: normal;\n"
          + "      font-weight: 500;\n"
          + "      font-size: 14px;\n"
          + "      line-height: 24px;\n"
          + "      text-align: center;\n"
          + "      padding: 0;\n"
          + "      font-family: HelveticaNeue, 'Helvetica Neue', Helvetica, Arial, sans-serif;\n"
          + "      background-color: #fff;\n"
          + "    }\n"
          + "\n"
          + "    .container > .section {\n"
          + "      margin: 2rem 4rem;\n"
          + "    }\n"
          + "\n"
          + "    header, footer {\n"
          + "      height: 44px;\n"
          + "      text-align: center;\n"
          + "      padding: 20px 0;\n"
          + "    }\n"
          + "\n"
          + "    header {\n"
          + "      border-bottom: 1px solid #111111;\n"
          + "    }\n"
          + "\n"
          + "    .button {\n"
          + "      height: 32px;\n"
          + "      background: #941100;\n"
          + "      border-radius: 4px;\n"
          + "      min-width: 8rem;\n"
          + "      border: none;\n"
          + "      box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.25);\n"
          + "      color: #f1f1f1 !important;\n"
          + "      padding: 6px 1rem 1px;\n"
          + "      cursor: pointer;\n"
          + "      display: inline-block;\n"
          + "      text-decoration: none;\n"
          + "    }\n"
          + "\n"
          + "    .button:active {\n"
          + "      box-shadow: 0 1px 0 rgba(148, 17, 0, .6);\n"
          + "      top: 1px;\n"
          + "    }\n"
          + "\n"
          + "    .button:visited {\n"
          + "      color: #f1f1f1;\n"
          + "    }\n"
          + "footer {\n"
          + "  border-top: 1px solid #111111;\n"
          + "}\n"
          + "footer > a {\n"
          + "  margin-right: 16px;\n"
          + "  display: inline-block;\n"
          + "}\n"
          + "footer > a > img {\n"
          + "  margin-top: 16px;\n"
          + "  position: relative;\n"
          + "  top: 2px;\n"
          + "}\n"
          + "footer > span {\n"
          + "  display: inline-block;\n"
          + "  margin-top: -3px;\n"
          + "  color: #706E6B;\n"
          + "  vertical-align: center;\n"
          + "}"
          + "footer > span > a {\n"
          + "  text-decoration: none;\n"
          + "  color: #706E6B !important;\n"
          + "}\n"
          + "footer > span > a:hover {\n"
          + "  text-decoration: none;\n"
          + "  color: #941100 !important;\n"
          + "}"
          + "    .logo-os {\n"
          + "      color: #941100;\n"
          + "      font-style: normal;\n"
          + "    }\n"
          + "  </style>\n"
          + "</head>\n"
          + "<body>\n"
          + "  <div class=\"container\">\n"
          + "    <header>\n"
          + "      <img height=\"30px\" title=\"logo\" src=\"https://isima-images.s3.ap-south-1.amazonaws.com/logo_static_v2.png\">\n"
          + "    </header>\n";

  private static final String MAIL_FOOTER =
      "<footer>\n"
          + "  <a href=\"https://twitter.com/isimaio\"><img height=\"16px\" title=\"twitter\""
          + " src=\"https://isima-images.s3.ap-south-1.amazonaws.com/twitter_v1.png\"></a>\n"
          + "  <a href=\"https://www.linkedin.com/company/isimalabs/about\"><img height=\"16px\" "
          + "title=\"linkedin\" src=\"https://isima-images.s3.ap-south-1.amazonaws.com/linkedin_v1.png\"></a>\n"
          + "  <span><a href=\"https://www.isima.io/legal/\">Legal</a>&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;"
          + "Copyright © 2024 Isima Inc. All rights reserved.</span>\n"
          + "</footer>"
          + "  </div>\n"
          + "</body>\n"
          + "</html>";

  private static final String DEFAULT_SIGNUP_MAIL_CONTENT =
      "<div class=\"section\">\n"
          + " <img height=\"110px\" title=\"twitter\" src=\"https://isima-images.s3.ap-south-1.amazonaws.com/congrats.png\"> \n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  <b>Hello,</b>\n"
          + "  <br />\n"
          + "  Welcome to Isima\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  You are ready to experience the power of"
          + " <b>bi(<i class=\"logo-os\">OS</i>)</b> from Isima.\n"
          + "  <br />\n"
          + "  <br />\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  <a href=\"%s\" class=\"button\">Verify email address</a>\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  The experience may make you a data super-hero!\n"
          + "</div>";

  private static final String DEFAULT_REGISTRATION_COMPLETE_MAIL_CONTENT =
      "<div class=\"section\">\n"
          + " <img height=\"110px\" title=\"twitter\" src=\"https://isima-images.s3.ap-south-1.amazonaws.com/congrats.png\"> \n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  <b>Hello,</b>\n"
          + "  <br />\n"
          + "  <br />\n"
          + "  You have successfully registered with Isima.\n"
          + "  <br />\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  You are ready to experience the power of"
          + " <b>bi(<i class=\"logo-os\">OS</i>)</b> from Isima.\n"
          + "  <br />\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  <a href=\"%s\" class=\"button\">Login</a>\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  The experience may make you a data super-hero!\n"
          + "</div>";

  private static final String DEFAULT_ALREADY_SIGNUP_MAIL_CONTENT =
      "<div class=\"section\">\n"
          + "  Dear %s,\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  Your email is already registered with <b>bi(<i class=\"logo-os\">OS</i>)</b>.\n"
          + "  <br />\n"
          + "  You can reset your password if you are facing trouble logging in to the service.\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  <a href=\"%s\" class=\"button\">Reset Password</a>\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  If you did not request to change your password, please disregard this email.\n"
          + "</div>";
  private static final String DEFAULT_FORGOT_PASSWORD_MAIL_CONTENT =
      "<div class=\"section\">\n"
          + "  Dear %s,\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  We've received a request to change your password to sign-in"
          + " to <b>bi(<i class=\"logo-os\">OS</i>)</b>.\n"
          + "  <br />\n"
          + " If you did not request to change your password, please disregard this email.\n"
          + "</div>\n"
          + "<div class=\"section\">\n"
          + "  <a href=\"%s\" class=\"button\">Reset Password</a>\n"
          + "</div>";

  /** Flag to indicate that the execution is in test mode (default=false). */
  public static boolean isTestMode() {
    return getInstance().getBoolean(TEST_MODE, false);
  }

  public static boolean testSignup() {
    return getInstance().getBoolean(TEST_SIGNUP, false);
  }
}
