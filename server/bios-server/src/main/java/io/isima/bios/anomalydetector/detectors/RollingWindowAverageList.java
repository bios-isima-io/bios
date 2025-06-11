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
package io.isima.bios.anomalydetector.detectors;

import java.util.LinkedList;

/**
 * Class RollingWindowAverageList is a wrapper on LinkedList. It provides below guarantees 1) Number
 * of elements is atmost k (an integer which is passed in constructor) 2) Exposes a function to
 * return the average of elements currently in the list
 */
public class RollingWindowAverageList extends LinkedList<Double> {

  private final int capacity;
  private double rollingWindowSum;

  public RollingWindowAverageList(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity should be greater than 0");
    }
    this.capacity = capacity;
    this.rollingWindowSum = 0;
  }

  public synchronized double getRollingWindowAverage() {
    int listSize = super.size();
    if (listSize > 0 && listSize < capacity) {
      return rollingWindowSum / listSize;
    } else {
      return rollingWindowSum / capacity;
    }
  }

  @Override
  public synchronized boolean add(Double elem) {
    int listSize = super.size();
    while (listSize > capacity) {
      super.poll();
      listSize -= 1;
    }

    if (listSize < capacity) {
      rollingWindowSum = rollingWindowSum + elem;
    } else {
      Double outgoingElement = super.poll();
      rollingWindowSum = rollingWindowSum + elem - outgoingElement;
    }
    super.add(elem);
    return true;
  }

  @Override
  public void addFirst(Double elem) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  @Override
  public void addLast(Double elem) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  @Override
  public Double remove() {
    throw new UnsupportedOperationException("This operation is not supported");
  }
}
