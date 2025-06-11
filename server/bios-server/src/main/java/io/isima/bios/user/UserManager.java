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
package io.isima.bios.user;

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.UserConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.auth.User;
import java.util.concurrent.CompletableFuture;

public interface UserManager {

  void systemUsersBootstrap();

  CompletableFuture<User> createUserAsync(UserConfig user, ExecutionState state);

  CompletableFuture<UserConfig> modifyUserAsync(
      Long userId, UserConfig userConfig, UserContext requestor, ExecutionState state);

  CompletableFuture<Void> deleteUserAsync(
      String email, Long userId, UserContext requestor, ExecutionState state);
}
