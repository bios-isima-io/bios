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
package io.isima.bios.service;

import io.isima.bios.execution.ExecutionState;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Response;

public class HttpRemoteCallState<RequestT, ResponseT> implements Callback {

  protected final HttpFanRouter<RequestT, ResponseT> fanRouter;
  @Getter protected final String endpoint;
  @Getter protected final OkHttpClient client;
  @Getter protected final CompletableFuture<ResponseT> future;
  @Getter protected final Optional<ExecutionState> state;

  protected HttpRemoteCallState(
      HttpFanRouter<RequestT, ResponseT> fanRouter,
      String endpoint,
      OkHttpClient client,
      ExecutionState state) {
    this.fanRouter = fanRouter;
    this.endpoint = endpoint;
    this.client = client;
    this.future = new CompletableFuture<>();
    this.state = Optional.ofNullable(state);
  }

  @Override
  public void onResponse(Call call, Response response) throws IOException {
    try {
      state.ifPresent((st) -> st.addHistory(")(handleResponse"));
      final ResponseT reply = fanRouter.handleWebResponse(response);
      future.complete(reply);
      state.ifPresent(
          (st) -> {
            st.addHistory(")");
          });
    } catch (Throwable t) {
      future.completeExceptionally(t);
    }
  }

  @Override
  public void onFailure(Call call, IOException e) {
    future.completeExceptionally(e);
  }

  @Override
  public String toString() {
    return new StringBuilder("{")
        .append("endpoint=")
        .append(endpoint)
        .append(", fanRouter=")
        .append(fanRouter)
        .append("}")
        .toString();
  }
}
