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

import io.isima.bios.common.IngestState;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.ServiceException;
import io.isima.bios.errors.exception.TfosException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtil {
  private static final Logger logger = LoggerFactory.getLogger(MetricsUtil.class);

  public static Consumer<Throwable> createErrorHandler(
      IngestState state, CompletableFuture<IngestResponse> insertFuture) {
    return new Consumer<Throwable>() {
      @Override
      public void accept(Throwable t) {
        if (t instanceof TimeoutException) {
          TfosException ex = new TfosException(GenericError.TIMEOUT);
          logger.warn(
              "{} failed. error={} status={} tenant={} stream={}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state.getTenantName(),
              state.getStreamName());
          insertFuture.completeExceptionally(ex);
        } else if (t instanceof TfosException) {
          TfosException ex = (TfosException) t;
          logger.warn(
              "{} failed. error={} status={} tenant={} stream={}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state.getTenantName(),
              state.getStreamName());
          insertFuture.completeExceptionally(ex);
        } else if (t.getCause() instanceof TfosException) {
          TfosException ex = (TfosException) t.getCause();
          logger.error(
              "{} failed. error={} status={} tenant={} stream={}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state.getTenantName(),
              state.getStreamName());
          insertFuture.completeExceptionally(ex);
        } else if (t.getCause() instanceof ServiceException) {
          ServiceException ex = (ServiceException) t.getCause();
          logger.warn(
              "{} failed. error={} status={} tenant={} stream={}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getResponse().getStatus(),
              state.getTenantName(),
              state.getStreamName());
          insertFuture.completeExceptionally(ex);
        } else {
          // This causes Internal Server Error
          TfosException ex = new TfosException(GenericError.APPLICATION_ERROR);
          logger.error(
              "{} error. error={} status=500 tenant={} stream={}",
              state.getExecutionName(),
              t.getMessage(),
              state.getTenantName(),
              state.getStreamName());
          insertFuture.completeExceptionally(ex);
        }
      }
    };
  }

  public static <T> Consumer<T> createAcceptor(IngestState state, CompletableFuture<T> future) {
    return new Consumer<T>() {
      @Override
      public void accept(T output) {
        state.addHistory("}");
        logger.debug(
            "Metrics report was done successfully; tenant={}, stream={}, event={}",
            state.getTenantName(),
            state.getStreamName(),
            state.getEvent());
        state.markDone();
        future.complete(output);
      }
    };
  }

  public static Long getMBeanLong(
      MBeanServerConnection mbsc,
      CassandraMetricsBeanType type,
      CassandraMetricsBeanAttribute attribute)
      throws AttributeNotFoundException,
          InstanceNotFoundException,
          MalformedObjectNameException,
          MBeanException,
          ReflectionException,
          IOException {
    final Object value =
        mbsc.getAttribute(new ObjectName(type.getBeanType()), attribute.getBeanAttr());
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else {
      try {
        return Long.valueOf(value.toString());
      } catch (NumberFormatException e) {
        // give up
      }
    }
    return 0L;
  }

  public static Double getMBeanDouble(
      MBeanServerConnection mbsc,
      CassandraMetricsBeanType type,
      CassandraMetricsBeanAttribute attribute)
      throws AttributeNotFoundException,
          InstanceNotFoundException,
          MalformedObjectNameException,
          MBeanException,
          ReflectionException,
          IOException {
    final Object value =
        mbsc.getAttribute(new ObjectName(type.getBeanType()), attribute.getBeanAttr());
    if (value instanceof Number) {
      return Double.valueOf(((Number) value).doubleValue());
    } else {
      try {
        return Double.valueOf(value.toString());
      } catch (NumberFormatException e) {
        // give up
      }
    }
    return Double.valueOf(0.0);
  }

  public static Object fetchCompositeData(CompositeData data, String key, Object defaultValue) {
    if (data == null) {
      return defaultValue;
    }
    final Object value = data.get(key);
    return value != null ? value : defaultValue;
  }
}
