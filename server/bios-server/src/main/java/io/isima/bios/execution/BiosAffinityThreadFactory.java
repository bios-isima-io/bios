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
package io.isima.bios.execution;

import java.util.concurrent.ThreadFactory;
import net.openhft.affinity.AffinityLock;

/** This is a ThreadFactory which generates threads with CPUs assigned out of given CPU pull. */
public class BiosAffinityThreadFactory implements ThreadFactory {
  private final String name;
  private final int priority;
  private final boolean daemon;
  private final int[] cpuIds;
  private int id = 0;

  public BiosAffinityThreadFactory(String name, int priority, int... cpuIds) {
    this(name, priority, false, cpuIds);
  }

  public BiosAffinityThreadFactory(String name, int priority, boolean daemon, int... cpuIds) {
    this.name = name;
    this.priority = priority;
    this.daemon = daemon;
    this.cpuIds = cpuIds;
  }

  @Override
  public synchronized Thread newThread(final Runnable r) {
    int cpuId = cpuIds[id % cpuIds.length];
    String name2 = name + '-' + id + "-cpu" + cpuId;
    id++;
    Thread t =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try (AffinityLock ignored = AffinityLock.acquireLock(cpuId)) {
                  r.run();
                }
              }
            },
            name2);
    t.setDaemon(daemon);
    return t;
  }
}
