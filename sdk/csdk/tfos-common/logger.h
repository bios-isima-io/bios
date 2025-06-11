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
#ifndef TFOS_CSDK_LOGGER_H_
#define TFOS_CSDK_LOGGER_H_

#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "tfoscsdk/csdk.h"

namespace tfos::csdk {

using logMetricsKey =
    // stream, operation Id, server_endpoint
    std::tuple<std::string, CSdkOperationId, std::string>;

struct Accumulator {
  uint64_t count = 0;
  uint64_t sum = 0;
  uint64_t min = 0;
  uint64_t max = 0;

  Accumulator() = default;
  void Push(uint64_t in);
};

struct LogMetrics {
  uint64_t success_count = 0;
  uint64_t failure_count = 0;
  Accumulator latency_accum;
  Accumulator latency_internal_accum;
  uint64_t num_reads = 0;
  uint64_t num_writes = 0;
  uint64_t num_qos_retry_considered = 0;
  uint64_t num_qos_retry_sent = 0;
  uint64_t num_qos_retry_response_used = 0;

  std::set<std::thread::id> thread_ids;

  void Push(bool is_success, uint64_t latency_us, uint64_t latency_internal_us,
            uint64_t num_records_read, uint64_t num_records_written,
            uint64_t num_qos_retry_considered_p, uint64_t num_qos_retry_sent_p,
            uint64_t num_qos_retry_response_used_p);
};

class Logger {
 private:
  std::string signal_name_;
  pid_t pid_;
  uint64_t flush_interval_;
  std::string app_name_;
  std::string app_type_;

  uint64_t last_flush_time_;

  mutable std::mutex metrics_lock_;
  std::map<logMetricsKey, LogMetrics*> metrics_;

 public:
  Logger();
  ~Logger();

  const std::string& GetSignalName() const { return signal_name_; }
  void SetAppName(const std::string& app_name) { app_name_ = Escape(app_name); }
  void SetAppType(const std::string& app_type) { app_type_ = Escape(app_type); }
  const std::string& GetAppType() const { return app_type_; }

  void Log(bool is_success, const std::string& stream, CSdkOperationId op_id,
           const std::string& server_endpoint, uint64_t latency_us, uint64_t latency_internal_us,
           uint64_t num_reads, uint64_t num_writes, uint64_t num_qos_retry_considered,
           uint64_t num_qos_retry_sent, uint64_t num_qos_retry_response_used);

  std::string* Flush();
  std::string Escape(const std::string& src);
};

}  // namespace tfos::csdk

#endif  // TFOS_CSDK_LOGGER_H_
