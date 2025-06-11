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
package io.isima.bios.load.model;

import static org.junit.Assert.assertEquals;

import com.strobel.assembler.Collection;
import io.isima.bios.data.service.Config;
import io.isima.bios.data.service.DataStore;
import io.isima.bios.data.service.GaussianLoad;
import io.isima.bios.data.service.LoadClient;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

public class LoadTest {

  static LoadClient loadClient;
  static Config config;
  static DataStore dataStore;
  static Logger logger = org.slf4j.LoggerFactory.getLogger(LoadTest.class);


  @Before
  public void setUp() throws Exception {
    config = new Config(
        System.getProperty("user.dir") + "/scripts/load-test-script/load-profiles");
    dataStore = new DataStore(config);
  }

  @Test
  public void testGenerateEventData() throws Exception {
    for (String stream : config.getStreamNames()) {
      logger.info(stream);
      final String event = dataStore.generateEvent(stream).replaceAll("/\\,", "&");
      assertEquals(config.getStream(stream).attributes.size(), event.split(",").length);
    }
  }

  @Test
  public void testAddExistingContextKey() throws Exception {
    for (String stream : config.getStreamNames()) {
      String event = dataStore.generateEvent(stream).replaceAll("/\\,", "&");
      assertEquals(config.getStream(stream).attributes.size(), event.split(",").length);
    }
  }

  // @Test
  public void testNewLoad() throws BiosClientException {
    ApplicationUtils.initSystemProperty();
    loadClient = new LoadClient(config);
    GaussianLoad gaussianLoad = new GaussianLoad(null, config, dataStore);
    ExecutorService executor =
        gaussianLoad.initExecutors(config.getStreamNames().size(),
            ApplicationUtils.getGaussianTimeWindow());
    Collection<Runnable> ingestTasks = gaussianLoad.getExecutorThreads();
    for (Runnable runnable : ingestTasks) {
      executor.submit(runnable);
    }
  }

  // @Test
  public void writeDataToCsv() throws IOException {
    String[] configsToWrite = {"subsInfoContext", "dataUsageSignal", "callHistorySignal",
        "dailyAddsChurnsSignal", "rechargeSignal"};
    // String[] configsToWrite = {"subsInfoContext"};

    String filePath = "/isima/schemas/telco-schema/telco-schema/load-schema/";
    for (String stream : configsToWrite) {
      List<AttributeDesc> attrs = config.getStream(stream).getAttributes();
      StringJoiner fileHeader = new StringJoiner(",");
      for (AttributeDesc attr : attrs) {
        fileHeader.add(attr.getName());
      }
      File csvOutputFile = new File(filePath + stream + ".csv");
      try (PrintWriter pw = new PrintWriter(new FileWriter(csvOutputFile, true))) {
        pw.println(fileHeader.toString());
        pw.close();
      }
    }
    for (int idx = 0; idx < 100; idx++) {
      for (String stream : configsToWrite) {
        File csvOutputFile = new File(filePath + stream + ".csv");
        try (PrintWriter pw = new PrintWriter(new FileWriter(csvOutputFile, true))) {
          pw.println(dataStore.generateEvent(stream).replaceAll("/\\,", "&"));
          pw.close();
        }
      }
    }
  }
}
