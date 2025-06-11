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
package io.isima.bios.models;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum SignupApprovalAction {
  APPROVE("Approve"),
  Unknown("Unknown");
  private String signupAction;

  public String getSignupAction() {
    return signupAction;
  }

  SignupApprovalAction(String signupAction) {
    this.signupAction = signupAction;
  }

  public static SignupApprovalAction getSignupApprovalAction(String action) {
    Map<String, SignupApprovalAction> map = new HashMap<>();
    for (SignupApprovalAction e : EnumSet.allOf(SignupApprovalAction.class)) {
      map.put(e.signupAction, e);
    }
    return map.getOrDefault(action, Unknown);
  }
}
