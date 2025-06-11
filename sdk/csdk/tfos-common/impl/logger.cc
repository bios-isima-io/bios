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
#include "tfos-common/logger.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "_bios/data.pb.h"
#include "sole/sole.hpp"
#include "tfos-common/utils.h"
#include "tfoscsdk/csdk.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

void Accumulator::Push(uint64_t in) {
  if (count == 0) {
    min = in;
    max = in;
  } else {
    max = std::max(max, in);
    min = std::min(min, in);
  }
  ++count;
  sum += in;
}

void LogMetrics::Push(bool is_success, uint64_t latency_us, uint64_t latency_internal_us,
                      uint64_t num_records_read, uint64_t num_records_written,
                      uint64_t num_qos_retry_considered_p, uint64_t num_qos_retry_sent_p,
                      uint64_t num_qos_retry_response_used_p) {
  if (is_success) {
    latency_accum.Push(latency_us);
    latency_internal_accum.Push(latency_internal_us);
    num_reads += num_records_read;
    num_writes += num_records_written;
    num_qos_retry_considered += num_qos_retry_considered_p;
    num_qos_retry_sent += num_qos_retry_sent_p;
    num_qos_retry_response_used += num_qos_retry_response_used_p;
    ++success_count;
  } else {
    ++failure_count;
  }

  thread_ids.emplace(std::this_thread::get_id());
}

Logger::Logger() {
  signal_name_ = "_clientMetrics";
  flush_interval_ = 30000;
  pid_ = getpid();
  last_flush_time_ = Utils::CurrentTimeMillis();
}

Logger::~Logger() {
  for (auto it = metrics_.begin(); it != metrics_.end(); it++) {
    delete (it->second);
  }
  metrics_.clear();
}

void Logger::Log(bool is_success, const std::string& stream, CSdkOperationId op_id,
                 const std::string& server_endpoint, uint64_t latency_us,
                 uint64_t latency_internal_us, uint64_t num_reads, uint64_t num_writes,
                 uint64_t num_qos_retry_considered, uint64_t num_qos_retry_sent,
                 uint64_t num_qos_retry_response_used) {
  assert(latency_internal_us <= latency_us);
  std::unique_lock lock(metrics_lock_);
  auto it = metrics_.find(std::make_tuple(stream, op_id, server_endpoint));
  if (it == metrics_.end()) {
    metrics_[std::make_tuple(stream, op_id, server_endpoint)] = new LogMetrics();
  }

  metrics_[std::make_tuple(stream, op_id, server_endpoint)]->Push(
      is_success, latency_us, latency_internal_us, num_reads, num_writes, num_qos_retry_considered,
      num_qos_retry_sent, num_qos_retry_response_used);
}

std::string* Logger::Flush() {
  auto now = Utils::CurrentTimeMillis();
  if (now - last_flush_time_ < flush_interval_) {
    return nullptr;
  }
  last_flush_time_ = now;

  if (!metrics_.size()) {
    return nullptr;
  }

  std::vector<std::string> log_events;
  char buf[2048];
  {
    std::scoped_lock lock(metrics_lock_);
    for (auto it = metrics_.begin(); it != metrics_.end(); it++) {
      auto stream = std::get<0>(it->first);
      boost::algorithm::to_lower(stream);
      int bytes = snprintf(
          buf, sizeof(buf),
          "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\","
          "%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld\n",
          Escape(stream).c_str(),
          Escape(std::string(Utils::GetActualOperationName(std::get<1>(it->first)))).c_str(),
          app_name_.c_str(), app_type_.c_str(), Escape(std::get<2>(it->first)).c_str(),
          it->second->success_count, it->second->failure_count, it->second->latency_accum.sum,
          it->second->latency_accum.min, it->second->latency_accum.max,
          it->second->latency_internal_accum.sum, it->second->latency_internal_accum.min,
          it->second->latency_internal_accum.max, it->second->num_reads, it->second->num_writes,
          it->second->thread_ids.size(), it->second->num_qos_retry_considered,
          it->second->num_qos_retry_sent, it->second->num_qos_retry_response_used);
      if (bytes < 2048) {
        log_events.emplace_back(buf, bytes);
      } else {
        assert(false);
      }
      delete it->second;
    }
    metrics_.clear();
  }

  com::isima::bios::models::proto::InsertBulkRequest request;
  request.set_signal(signal_name_);
  request.set_content_rep(com::isima::bios::models::proto::ContentRepresentation::CSV);
  uint8_t event_id[16];
  for (size_t i = 0; i < log_events.size(); ++i) {
    const auto& log_event = log_events.at(i);
    auto uuid = sole::uuid1();
    auto* record = request.add_record();
    record->add_string_values(log_event);
    *reinterpret_cast<uint64_t*>(&event_id[0]) = bswap_64(uuid.ab);
    *reinterpret_cast<uint64_t*>(&event_id[8]) = bswap_64(uuid.cd);
    record->set_event_id(event_id, 16);
  }
  std::stringstream ss;
  request.SerializeToOstream(&ss);
  return new std::string{ss.str()};
}

std::string Logger::Escape(const std::string& src) {
  std::string out;
  out.reserve(src.size() * 2);
  for (size_t i = 0; i < src.size(); ++i) {
    char ch = src.at(i);
    if (ch == '"') {
      out += '"';
      out += '"';
    } else {
      out += ch;
    }
  }
  return out;
}

}  // namespace tfos::csdk
