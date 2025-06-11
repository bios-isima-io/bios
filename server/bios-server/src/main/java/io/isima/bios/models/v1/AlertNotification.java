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
package io.isima.bios.models.v1;

import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.EventJson;
import java.util.List;

public class AlertNotification extends EventJson {

  private AlertConfig alert;

  private EventJson event;

  private String signalName;

  private String rollupName;

  private List<String> viewDimensions;

  public AlertNotification() {}

  public AlertNotification(AlertConfig alert, EventJson event) {
    this.alert = alert;
    this.event = event;
  }

  public AlertNotification(
      AlertConfig alert,
      EventJson event,
      String signalName,
      String rollupName,
      List<String> viewDimensions) {
    this.alert = alert;
    this.event = event;
    this.signalName = signalName;
    this.rollupName = rollupName;
    this.viewDimensions = viewDimensions;
  }

  public AlertConfig getAlert() {
    return alert;
  }

  public EventJson getEvent() {
    return event;
  }

  public void setAlert(AlertConfig alert) {
    this.alert = alert;
  }

  public void setEvent(EventJson event) {
    this.event = event;
  }

  public void setViewDimensions(List<String> viewDimensions) {
    this.viewDimensions = viewDimensions;
  }

  public List<String> getViewDimensions() {
    return viewDimensions;
  }

  public String getSignalName() {
    return signalName;
  }

  public void setSignalName(String signalName) {
    this.signalName = signalName;
  }

  public void setRollupName(String rollupName) {
    this.rollupName = rollupName;
  }

  public String getRollupName() {
    return rollupName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("alert=").append(alert);
    sb.append(", signalName=").append(signalName);
    sb.append(", rollupName=").append(rollupName);
    sb.append(", viewDimensions=").append(viewDimensions.toArray());
    sb.append(", event=").append(event);
    return sb.toString();
  }
}
