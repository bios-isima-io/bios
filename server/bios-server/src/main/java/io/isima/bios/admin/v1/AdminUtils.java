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
package io.isima.bios.admin.v1;

import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;

/** Class to provide AdminInternal related utility methods. */
public class AdminUtils {

  public static String makeViewStreamName(StreamConfig signalConf, ViewDesc viewDesc) {
    if (signalConf.getType() != StreamType.SIGNAL && signalConf.getType() != StreamType.METRICS) {
      throw new IllegalArgumentException("stream type must be SIGNAL or METRICS");
    }
    return signalConf.getName() + AdminConstants.VIEW_STREAM_PREFIX + viewDesc.getName();
  }

  public static String makeViewStreamName(String streamName, String featureName) {
    return streamName + AdminConstants.VIEW_STREAM_PREFIX + featureName;
  }

  public static String makeIndexStreamName(String streamName, String featureName) {
    return streamName + AdminConstants.INDEX_STREAM_PREFIX + featureName;
  }

  public static String makeRollupStreamName(String streamName, String featureName) {
    return streamName + AdminConstants.ROLLUP_STREAM_PREFIX + featureName;
  }

  public static String makeContextIndexStreamName(String streamName, String featureName) {
    return streamName + AdminConstants.CONTEXT_INDEX_STREAM_PREFIX + featureName;
  }
}
