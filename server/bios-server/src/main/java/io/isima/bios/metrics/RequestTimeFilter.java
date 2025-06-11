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
package io.isima.bios.metrics;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;

/**
 * Filter for request latency computation.
 *
 * <p>TODO(ramesh): move this filter when TFOS API is removed. Keeping this in TFOS for now to avoid
 * cross dependency.
 */
@PreMatching
@Provider
public class RequestTimeFilter implements ContainerRequestFilter, ContainerResponseFilter {
  public static final String PROPERTY_OPERATION_TRACER = "bios.operationTracer";

  @Override
  public void filter(ContainerRequestContext containerRequestContext) {
    // start the request timer as early as possible, we can attach it later to the correct
    // recorder
    final OperationMetricsTracer tracer =
        new OperationMetricsTracer(containerRequestContext.getLength());
    containerRequestContext.setProperty(PROPERTY_OPERATION_TRACER, tracer);
  }

  @Override
  public void filter(
      ContainerRequestContext containerRequestContext,
      ContainerResponseContext containerResponseContext) {
    // before encoding
    final Object reqTracer = containerRequestContext.getProperty(PROPERTY_OPERATION_TRACER);
    if (reqTracer instanceof OperationMetricsTracer) {
      ((OperationMetricsTracer) reqTracer)
          .startEncoding(containerResponseContext.getStatus() < 400);
      if (!containerResponseContext.hasEntity()) {
        // nothing to encode..stop now
        ((OperationMetricsTracer) reqTracer).stop(0);
      }
    }
  }
}
