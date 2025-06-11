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
#ifndef TFOS_CSDK_INGEST_BULK_CTX_H_
#define TFOS_CSDK_INGEST_BULK_CTX_H_

namespace tfos::csdk {

class Session;

/**
 * Class to maintain the context of an IngestBulk execution.
 *
 * The class methods are not thread safe assuming the operation runs
 * synchronously in the first implementation. We'll enhance it later.
 *
 * Also, the methods assume sequencial execution, so the class does not track
 * event indexes individually. We need enhancement here, too, for concurrent
 * operations support.
 */
class IngestBulkCtx final {
 private:
  Session *const session_;
  const int num_events_;
  void (*const ask_cb_)(long, int, int, void *);
  void *const cb_args_;
  CSdkOperationId op_id_;

  int batch_size_;
  int index_;

 public:
  IngestBulkCtx(int num_events, void (*ask_cb)(long, int, int, void *), void *cb_args,
                Session *session, CSdkOperationId op_id)
      : session_(session),
        num_events_(num_events),
        ask_cb_(ask_cb),
        cb_args_(cb_args),
        op_id_(op_id) {
    batch_size_ = num_events_ < 1000 ? num_events_ : 1000;
    index_ = 0;
  }

  // getters
  Session *session() { return session_; }
  int num_events() { return num_events_; }
  void (*ask_cb())(long, int, int, void *) { return ask_cb_; }
  void *cb_args() { return cb_args_; }
  CSdkOperationId op_id() { return op_id_; }

  inline bool WouldAskMore() { return (index_ < num_events_); }

  void AskMore() {
    if (!WouldAskMore()) {
      // no more to ask. do nothing
      return;
    }
    int to_index = index_ + batch_size_;
    to_index = to_index < num_events_ ? to_index : num_events_;
    // TODO(Naoki): Hack, this is fundamentally dangerous. Improve later.
    int64_t ctx_id = reinterpret_cast<int64_t>(this);
    ask_cb_(ctx_id, index_, to_index, cb_args_);
  }

  void MarkDone(int from_index, int to_index) {
    (void)from_index;
    index_ = to_index;
  }

  void SqueezeBatchSize() { batch_size_ = (batch_size_ + 1) / 2; }
};

}  // namespace tfos::csdk

#endif  // TFOS_CSDK_INGEST_BULK_CTX_H_
