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
package io.isima.bios.vigilantt.classifiers;

import java.util.ArrayList;
import java.util.List;

/**
 * This class encapsulates the information identified for an event isAnomaly indicates whether the
 * event was an anomaly violatedRules contains the names of the rules which were violated
 */
public class ClassificationInfo {
  private boolean isAnomaly;
  private List<String> violatedRules = new ArrayList<>();

  public ClassificationInfo(boolean isAnomaly) {
    this.isAnomaly = isAnomaly;
  }

  public ClassificationInfo(boolean isAnomaly, List<String> violatedRules) {
    this.isAnomaly = isAnomaly;
    this.violatedRules = violatedRules;
  }

  public boolean getIsAnomaly() {
    return isAnomaly;
  }

  public void setIsAnomaly(Boolean isAnomaly) {
    this.isAnomaly = isAnomaly;
  }

  public List<String> getViolatedRules() {
    return violatedRules;
  }

  public void setViolatedRules(List<String> violatedRules) {
    this.violatedRules = violatedRules;
  }
}
