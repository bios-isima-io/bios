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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ActivityRecorder {
  final List<String> records = new ArrayList<>();

  public static void log(ActivityRecorder recorder, String format, Object... params) {
    if (recorder != null) {
      recorder.log(format, params);
    }
  }

  public void log(String format, Object... params) {
    records.add(String.format(format, params));
  }

  public String dump() {
    return records.stream().collect(Collectors.joining("\n"));
  }
}
