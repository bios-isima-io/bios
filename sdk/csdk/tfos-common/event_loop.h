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

/**
 * Physical pipe implementation
 */
#ifndef TFOS_PHYSICAL_EVENT_LOOP_H_
#define TFOS_PHYSICAL_EVENT_LOOP_H_

#include <event.h>
#include <event2/event.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/lockfree/queue.hpp>
#include <list>
#include <unordered_set>

namespace tfos::csdk {

/**
 * Task used for dispatching a job to EventLoop.
 */
struct Task {
  void (*run)(void *);
  void *args;
  // special task runner used for a scheduled task on shutdown.
  // Set nullptr if no special shutdown handling is required.
  void (*on_shutting_down)(void *) = nullptr;
};

class DelayTaskParams;

class EventLoop final {
 private:
  struct event_base *evbase_;
  int notify_post_fd_;
  int notify_pend_fd_;
  struct event *notify_event_;
  pthread_t thread_id_;
  pid_t pid_;

  bool shutdown_;
  boost::lockfree::queue<Task> *task_queue_;
  int delay_tasks_count_;
  std::unordered_set<DelayTaskParams *> scheduled_events_;

 public:
  EventLoop();
  ~EventLoop();

  void Shutdown();

  bool IsValid();

  /**
   * Put a task to be executed by the EventLoop.
   */
  void PutTask(const Task &task);

  inline void PutTask(void (*run)(void *), void *args) { PutTask({.run = run, .args = args}); }

  /**
   * Put a task to be executed by the EventLoop with delay.
   */
  void ScheduleTask(const Task &task, uint64_t millis);

  inline void ScheduleTask(void (*run)(void *), void *args, uint64_t millis) {
    return ScheduleTask({.run = run, .args = args, .on_shutting_down = nullptr}, millis);
  }

  inline void ScheduleTask(void (*run)(void *), void (*on_shutting_down)(void *), void *args,
                           uint64_t millis) {
    return ScheduleTask({.run = run, .args = args, .on_shutting_down = on_shutting_down}, millis);
  }

  /**
   * Puts the final task of the EventLoop.
   * The method also reschedules all pending delayed tasks to run immediately,
   * expecting the tasks would cancel operations if necessary by checking the
   * shutdown status. You must not schedule any more task after calling this
   * method.
   */
  void PutFinalTask(const Task &task);

  inline void PutFinalTask(void (*run)(void *), void *args) {
    PutFinalTask({.run = run, .args = args});
  }

  inline struct event_base *event_base() const { return evbase_; }
  inline pthread_t thread_id() const { return thread_id_; }
  inline pid_t pid() const { return pid_; }

 private:
  void Notify();
  void Exit();

  static void *ThreadMain(void *ptr);
  static void HandleNotification(evutil_socket_t fd, int16_t what, void *arg);

  static void ExecuteDelayedTask(evutil_socket_t fd, int16_t what, void *args);
  static void ScheduleDelayedTask(void *args);
  static void ScheduleFinalTask(void *args);
};

}  // namespace tfos::csdk
#endif  // TFOS_PHYSICAL_EVENT_LOOP_H_
