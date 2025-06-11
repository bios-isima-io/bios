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

#if defined(__linux__)
#include <sys/eventfd.h>
#endif
#include <sys/types.h>
#include <unistd.h>

#include "tfos-common/utils.h"
#include "tfoscsdk/log.h"

namespace tfos::csdk {

// EventLoop implementation
// //////////////////////////////////////////////////////////////////////
EventLoop::EventLoop() : task_queue_(new boost::lockfree::queue<Task>(256)) {
  shutdown_ = false;
  pid_ = getpid();
#if defined(EVENT_DEBUG_ENABLED)
  event_enable_debug_logging(EVENT_DBG_ALL);
#else
#ifdef DEBUG_LOG_ENABLED
#undef DEBUG_LOG_ENABLED
#endif
#undef DEBUG_LOG
#define DEBUG_LOG(format, ...)
#endif
  evbase_ = event_base_new();
#if defined(__linux__)
  notify_post_fd_ = eventfd(0, EFD_NONBLOCK);
  notify_pend_fd_ = notify_post_fd_;
#else  // macos
  // this may be slow but it should function as expected
  int fields[2];
  pipe(fields);
  notify_post_fd_ = fields[1];
  notify_pend_fd_ = fields[0];
#endif
  delay_tasks_count_ = 0;
  notify_event_ = event_new(evbase_, notify_pend_fd_, EV_PERSIST | EV_TIMEOUT | EV_READ | EV_ET,
                            HandleNotification, this);
  struct timeval tv = {
      .tv_sec = 3,
      .tv_usec = 0,
  };
  event_add(notify_event_, &tv);
  pthread_create(&thread_id_, nullptr, ThreadMain, this);
}

void EventLoop::Shutdown() {
  DEBUG_LOG("EventLoop::shutdown() %p -- Enter thread=%lx", this, thread_id_);
  shutdown_ = true;
  Notify();

  pthread_join(thread_id_, nullptr);

  delete this;
}

bool EventLoop::IsValid() { return getpid() == pid_; }

EventLoop::~EventLoop() {
  DEBUG_LOG("EventLoop::~EventLoop() %p -- Enter", this);
  event_free(notify_event_);
  event_base_free(evbase_);
  if (notify_pend_fd_ != notify_post_fd_) {
    close(notify_pend_fd_);
  }
  close(notify_post_fd_);
  DEBUG_LOG("EventLoop::~EventLoop() %p -- Exit", this);
  delete task_queue_;
}

void EventLoop::PutTask(const Task &task) {
  assert(task.run != nullptr);
  DEBUG_LOG("%p queueing task %p to queue %p pid=%d", this, task.args, task_queue_, getpid());
  bool queue_success = task_queue_->push(task);
  assert(queue_success);
  (void)queue_success;  // to suppress compiler warning
  Notify();
}

struct DelayTaskParams {
  struct timeval delay;
#ifdef DEBUG_LOG_ENABLED
  uint64_t millis;
#endif
  Task task;
  EventLoop *event_loop;
  struct event *ev;
};

void EventLoop::ExecuteDelayedTask(evutil_socket_t fd, int16_t what, void *args) {
  auto *params = reinterpret_cast<DelayTaskParams *>(args);
  auto event_loop = params->event_loop;
  DEBUG_LOG("%p EventLoop::ExecuteDelayTask task args=%p counter=%d", event_loop, params->task.args,
            event_loop->delay_tasks_count_);
  --event_loop->delay_tasks_count_;
  params->task.run(params->task.args);
  event_loop->scheduled_events_.erase(params);
  event_free(params->ev);
  if (event_loop->shutdown_ && event_loop->delay_tasks_count_ == 0) {
    event_loop->Exit();
  }
  DEBUG_LOG("%p EventLoop Deleting params %p", event_loop, params);
  delete params;
}

void EventLoop::ScheduleDelayedTask(void *args) {
  auto *params = reinterpret_cast<DelayTaskParams *>(args);
  params->ev = evtimer_new(params->event_loop->event_base(), ExecuteDelayedTask, params);
  ++params->event_loop->delay_tasks_count_;
  params->event_loop->scheduled_events_.insert(params);
  evtimer_add(params->ev, &params->delay);

#ifdef DEBUG_LOG_ENABLED
  DEBUG_LOG("%p EventLoop::ScheduleDelayTask task=%p args=%p delay=%ld counter=%d",
            &params->event_loop, &params->task, params->task.args, params->millis,
            params->event_loop->delay_tasks_count_);
#endif
}

void EventLoop::ScheduleTask(const Task &task, uint64_t millis) {
  DelayTaskParams *params = new DelayTaskParams();
  DEBUG_LOG("%p EventLoop::ScheduleTask created params %p", this, params);
  params->delay.tv_sec = millis / 1000;
  params->delay.tv_usec = millis % 1000 * 1000;
#ifdef DEBUG_LOG_ENABLED
  params->millis = millis;
#endif
  params->task = task;
  params->event_loop = this;
  params->ev = nullptr;
  DEBUG_LOG(
      "%p EventLoop::ScheduleTask scheduling the task %p via "
      "ScheduleDelayTask %p",
      this, &task, params);
  PutTask(ScheduleDelayedTask, params);
}

/**
 * Strategy:
 *  - Cancel all pending delayed tasks
 *  - Reschedule these tasks to run immediately
 *  - Register the final task as (only one left) delayed task.
 */
void EventLoop::ScheduleFinalTask(void *args) {
  auto *params = reinterpret_cast<DelayTaskParams *>(args);
  auto *event_loop = params->event_loop;
  // Execute pending tasks immediately without waiting for the scheduled time.
  // This would break the scheduled execution order,
  // but we assume each task can handle shutdown status properly.
  for (auto pending : event_loop->scheduled_events_) {
    --event_loop->delay_tasks_count_;
    event_del(pending->ev);
    event_free(pending->ev);
    if (pending->task.on_shutting_down) {
      pending->task.run = pending->task.on_shutting_down;
    }
    event_loop->PutTask(pending->task);
    delete pending;
  }
  event_loop->scheduled_events_.clear();
  ScheduleDelayedTask(params);
}

void EventLoop::PutFinalTask(const Task &task) {
  auto *params = new DelayTaskParams();
  DEBUG_LOG("%p EventLoop::PutFinalTask final params created %p", this, params);
  params->delay.tv_sec = 0;
  params->delay.tv_usec = 10000;  // 10 msec
  params->task = task;
  params->event_loop = this;
  params->ev = nullptr;
  PutTask(ScheduleFinalTask, params);
}

void EventLoop::Notify() { eventfd_write(notify_post_fd_, 151); }

void EventLoop::Exit() {
  DEBUG_LOG("EventLoop: shutting down %p count=%d", this, delay_tasks_count_);
  event_base_loopexit(evbase_, nullptr);
}

void *EventLoop::ThreadMain(void *ptr) {
  auto *event_loop = reinterpret_cast<EventLoop *>(ptr);
  DEBUG_LOG("EventLoop::ThreadMain started loop %p pid=%d", event_loop, getpid());
  auto *evbase = event_loop->event_base();
  event_base_loop(evbase, 0);
  DEBUG_LOG("EventLoop: The loop terminated");

  return nullptr;
}

void EventLoop::HandleNotification(evutil_socket_t fd, int16_t what, void *arg) {
  auto *event_loop = reinterpret_cast<EventLoop *>(arg);
  if (what & EV_READ) {
    DEBUG_LOG("%p notification received pid=%d", event_loop, getpid());
    eventfd_t value;
    eventfd_read(fd, &value);
    DEBUG_LOG("%p notification received; value=%ld", event_loop, value);
  }

  Task task;
#ifdef DEBUG_LOG_ENABLED
  int count = 0;
#endif
  while (event_loop->task_queue_->pop(task)) {
    DEBUG_LOG("%p queue=%p task[%d] run=%p args=%p", event_loop, event_loop->task_queue_, count,
              task.run, task.args);
    task.run(task.args);
#ifdef DEBUG_LOG_ENABLED
    ++count;
#endif
  }

  if (event_loop->shutdown_ && event_loop->delay_tasks_count_ == 0) {
    event_loop->Exit();
  }

#ifdef DEBUG_LOG_ENABLED
  if ((what & EV_READ) || count > 0) {
    DEBUG_LOG("%p EventLoop::HandleNotification handled %d tasks from queue %p", event_loop, count,
              event_loop->task_queue_);
  }
#endif
}

}  // namespace tfos::csdk
