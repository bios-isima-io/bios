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

import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.LONG;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.impl.IMetricsStreamProvider;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.TimeInterval;
import io.isima.bios.models.TimeunitType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.recorder.RecorderConstants;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsStreamProvider implements IMetricsStreamProvider {

  private static final Logger logger = LoggerFactory.getLogger(MetricsStreamProvider.class);

  private static final String DEFAULT_SCHEMA_JSON_FILE = "usage.json";

  private StreamConfig operationalMetricsStream;

  public MetricsStreamProvider() {
    this(DEFAULT_SCHEMA_JSON_FILE);
  }

  /** Generic ctor, only test methods should be able to call this directly. */
  MetricsStreamProvider(String schemaJsonFile) {
    boolean readFromFile = TfosConfig.readOperationalMetricsConfigFromFile();
    StreamConfig temp = null;
    if (readFromFile) {
      temp = getStreamConfigFromFile(schemaJsonFile);
    }
    if (temp == null) {
      temp = getDefaultStreamConfig();
    }
    operationalMetricsStream = temp;
  }

  @Override
  public StreamConfig getOperationalMetricsStreamConfig() {
    return operationalMetricsStream;
  }

  StreamConfig getStreamConfigFromFile(String schemaJsonFile) {
    StreamConfig streamConfig = null;

    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(schemaJsonFile)) {
      ObjectMapper mapper = TfosObjectMapperProvider.get();
      streamConfig = mapper.readValue(inputStream, StreamConfig.class);
    } catch (IOException ex) {
      final String msg =
          "Failed to load operational metrics stream config, file :" + schemaJsonFile;
      logger.error(msg, ex);
    }

    return streamConfig;
  }

  StreamConfig getDefaultStreamConfig() {
    StreamConfig streamConfig = new StreamConfig(MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL);
    streamConfig.setType(StreamType.SIGNAL);
    streamConfig.addAttribute(new AttributeDesc(RecorderConstants.ATTR_BIOS_STREAM, STRING));
    streamConfig.addAttribute(new AttributeDesc(RecorderConstants.ATTR_BIOS_REQUEST, STRING));
    streamConfig.addAttribute(new AttributeDesc(RecorderConstants.ATTR_BIOS_APP_NAME, STRING));
    streamConfig.addAttribute(
        new AttributeDesc(RecorderConstants.ATTR_BIOS_APP_TYPE, ENUM)
            .setEnum(List.of("Unknown", "Realtime", "Batch", "Adhoc")));
    streamConfig.addAttribute(
        new AttributeDesc(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS, LONG));
    streamConfig.addAttribute(new AttributeDesc(RecorderConstants.ATTR_OP_NUM_WRITES, LONG));
    streamConfig.addAttribute(
        new AttributeDesc(RecorderConstants.ATTR_OP_NUM_VALIDATION_ERRORS, LONG));
    streamConfig.addAttribute(new AttributeDesc(RecorderConstants.ATTR_OP_NUM_READS, LONG));

    // defining views
    ViewDesc view = new ViewDesc();
    view.setName(MetricsConstants.BIOS_OPM_TENANT_VIEW);
    view.setGroupBy(
        Arrays.asList(MetricsConstants.ATTR_STREAM, RecorderConstants.ATTR_BIOS_REQUEST));
    view.setAttributes(
        List.of(
            RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS,
            RecorderConstants.ATTR_OP_NUM_WRITES,
            RecorderConstants.ATTR_OP_NUM_VALIDATION_ERRORS,
            RecorderConstants.ATTR_OP_NUM_READS));

    List<ViewDesc> views = new ArrayList<>();
    views.add(view);

    // defining rollup
    Rollup rollup = new Rollup();
    rollup.setName(MetricsConstants.BIOS_OPM_TENANT_ROLLUP);
    rollup.setInterval(new TimeInterval(5, TimeunitType.MINUTE));
    rollup.setHorizon(new TimeInterval(5, TimeunitType.MINUTE));

    List<Rollup> rollups = new ArrayList<>();
    rollups.add(rollup);

    // defining post-processes
    PostprocessDesc postprocess = new PostprocessDesc();
    postprocess.setView(MetricsConstants.BIOS_OPM_TENANT_VIEW);
    postprocess.setRollups(rollups);

    List<PostprocessDesc> postProcesses = new ArrayList<>();
    postProcesses.add(postprocess);

    // adding views and post-processes to stream config
    streamConfig.setViews(views);
    streamConfig.setPostprocesses(postProcesses);
    return streamConfig;
  }
}
