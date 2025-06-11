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
package io.isima.bios.req;

/**
 * Allow mutating aggregates.
 *
 * <p>NOTE: Ideally we should not allow mutating model objects as it is counter intuitive and can
 * also have unintended side effects (e.g when executorservice.submit is replaced with lock free
 * queues and static thread pools for perf reasons).
 *
 * <p>Also, Protobuf does not support mutating objects except through builders. So the mutation does
 * not update the underlying protobuf structures. This is OK currently because the mutations need
 * not be reflected beyond the mutating threads and is a temporary change. Currently this is done to
 * support temporary mutations done in several places in the server. This may be removed in the
 * future.
 */
public interface MutableAggregate extends Aggregate {
  void setBy(String by);
}
