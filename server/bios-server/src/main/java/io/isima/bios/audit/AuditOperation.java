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
package io.isima.bios.audit;

public enum AuditOperation {
  ADD_TENANT("Add Tenant"),
  MODIFY_TENANT("Modify Tenant"),
  DELETE_TENANT("Delete Tenant"),
  GET_TENANT("Get Tenant"),
  GET_TENANT_LIST("Get Tenant List"),
  GET_STREAM_LIST("Get Stream List"),
  GET_SIGNAL_LIST("Get Signal List"),
  GET_CONTEXT_LIST("Get Context List"),
  GET_STREAM_CONFIG("Get Stream Config"),
  GET_SIGNAL_CONFIG("Get Signal Config"),
  GET_CONTEXT_CONFIG("Get Context Config"),
  GET_STREAM_VERSION("Get Stream version"),
  ADD_STREAM_CONFIG("Add Stream Config"),
  ADD_SIGNAL_CONFIG("Add Signal Config"),
  ADD_CONTEXT_CONFIG("Add Context Config"),
  UPDATE_STREAM_CONFIG("Update Stream Config"),
  UPDATE_SIGNAL_CONFIG("Update Signal Config"),
  UPDATE_CONTEXT_CONFIG("Update Context Config"),
  DELETE_STREAM_CONFIG("Delete Stream Config"),
  DELETE_SIGNAL_CONFIG("Delete Signal Config"),
  DELETE_CONTEXT_CONFIG("Delete Context Config"),
  GET_STREAM_ATTRIBUTE("Get Stream Attribute"),
  GET_STREAM_ATTRIBUTES("Get Stream Attributes"),
  ADD_STREAM_ATTRIBUTE("Add Stream Attribute"),
  UPDATE_STREAM_ATTRIBUTES("Update Stream Attributes"),
  DELETE_STREAM_ATTRIBUTE("Delete Stream Attribute"),
  GET_STREAM_PREPROCESS("Get Stream Preprocess"),
  GET_STREAM_PREPROCESSES("Get Stream Preprocesses"),
  ADD_STREAM_PREPROCESS("Add Stream Preprocess"),
  MODIFY_STREAM_PREPROCESS("Modify Stream Preprocess"),
  DELETE_STREAM_PREPROCESS("Delete Stream Preprocess"),
  ADD_ENDPOINTS("Add Endpoints"),
  REMOVE_ENDPOINT("Remove Endpoints"),
  LOGIN("Login"),
  LOGOUT("Logout"),
  CHANGE_PASSWORD("Change Password"),
  CREATE_USER("Create User"),
  MODIFY_USER("Modify User"),
  DELETE_USER("Delete User"),
  DROP_TABLE("Drop Table"),
  DROP_KEYSPACE("Drop Keyspace"),
  INITIATE_SIGNUP("Initiate Signup"),
  APPROVE_USER("Approve User"),
  VERIFY_SIGNUP_TOKEN("Verify Signup Token"),
  COMPLETE_SIGNUP("Complete Signup"),
  INITIATE_SERVICE_REGISTRATION("Initiate Service Registration"),
  INVITE_USER("Invite User"),
  INITIATE_PASSWORD_RESET("Initiate Password Reset"),
  RESET_PASSWORD("Reset Password"),
  ;

  final String name;

  private AuditOperation(final String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }
}
