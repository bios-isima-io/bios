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
package io.isima.bios.req;

import java.util.concurrent.atomic.AtomicReference;

public abstract class BuilderProvider {
  private static final AtomicReference<BuilderProvider> configuredProvider =
      new AtomicReference<>();

  protected abstract ExtractRequest.Builder getExtractRequestBuilder(long startTime, long delta);

  protected abstract Aggregate.Builder getAggregateBuilder();

  protected abstract View.Builder getViewBuilder();

  static BuilderProvider getBuilderProvider() {
    return configuredProvider.get();
  }

  public static void reset() {
    configuredProvider.set(new DefaultBuilderProvider());
  }

  private static class DefaultBuilderProvider extends BuilderProvider {
    @Override
    protected ExtractRequest.Builder getExtractRequestBuilder(long startTime, long delta) {
      throw new IllegalStateException("No Extract Request builder configured yet");
    }

    @Override
    protected Aggregate.Builder getAggregateBuilder() {
      throw new IllegalStateException("No Aggregate builder configured yet");
    }

    @Override
    protected View.Builder getViewBuilder() {
      throw new IllegalStateException("No View builder configured yet");
    }
  }
}
