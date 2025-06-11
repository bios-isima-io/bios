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
#include "tfos-common/event_loop.h"

#include <sys/time.h>

#include <condition_variable>
#include <memory>
#include <mutex>

#include "gtest/gtest.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

TEST(EventLoopTest, EventLoopStartStop) {
  auto *event_loop = new EventLoop();
  event_loop->Shutdown();
}

static void MarkExecuted(void *args) {
  auto *executed = reinterpret_cast<bool *>(args);
  TEST_LOG("MarkExecuted");
  *executed = true;
}

TEST(EventLoopTest, EventLoopStopWithDelayedTask) {
  auto *event_loop = new EventLoop();
  bool executed = false;
  event_loop->ScheduleTask(MarkExecuted, &executed, 2000);

  event_loop->Shutdown();

  EXPECT_TRUE(executed);
}

struct evloop_test_state {
  std::mutex *mutex;
  std::condition_variable *cv;
  int *count;
  int src_value;
  int result_value;
};

static void TestTask(void *arg) {
  auto *state = reinterpret_cast<struct evloop_test_state *>(arg);
  state->result_value = state->src_value;
  std::unique_lock<std::mutex> lock(*state->mutex);
  --*state->count;
  TEST_LOG("TestTask count=%d", *state->count);
  state->cv->notify_one();
}

TEST(EventLoopTest, PutTasks) {
  EventLoop *event_loop = new EventLoop();

  std::mutex mutex;
  std::condition_variable cv;

  int count = 2;

  struct evloop_test_state first = {
      .mutex = &mutex,
      .cv = &cv,
      .count = &count,
      .src_value = 123,
      .result_value = 0,
  };

  struct evloop_test_state second = {
      .mutex = &mutex,
      .cv = &cv,
      .count = &count,
      .src_value = 551,
      .result_value = 0,
  };

  event_loop->PutTask(TestTask, &first);
  event_loop->PutTask(TestTask, &second);

  while (count > 0) {
    std::unique_lock<std::mutex> lock(mutex);
    if (count > 0) {
      cv.wait(lock);
    }
  }

  EXPECT_EQ(first.result_value, 123);
  EXPECT_EQ(second.result_value, 551);

  event_loop->Shutdown();
}

struct DelayedTaskTestParams {
  struct timeval done;
  std::mutex *mutex;
  std::condition_variable *cv;
};

static void CollectTime(void *args) {
  auto *params = reinterpret_cast<DelayedTaskTestParams *>(args);
  gettimeofday(&params->done, nullptr);
  std::unique_lock<std::mutex> lock(*params->mutex);
  params->cv->notify_one();
}

TEST(EventLoopTest, TaskWithDelay) {
  EventLoop *event_loop = new EventLoop();

  std::mutex mutex;
  std::condition_variable cv;

  DelayedTaskTestParams params = {
      .done = {0},
      .mutex = &mutex,
      .cv = &cv,
  };

  struct timeval start;
  gettimeofday(&start, nullptr);

  uint64_t millis = 1200;
  event_loop->ScheduleTask(CollectTime, &params, millis);

  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock);
  }

  int64_t diff =
      (params.done.tv_sec - start.tv_sec) * 1000 + (params.done.tv_usec - start.tv_usec) / 1000;

  EXPECT_GE(diff, millis);
  EXPECT_LT(diff, millis + 100);

  event_loop->Shutdown();
}

}  // namespace tfos::csdk
