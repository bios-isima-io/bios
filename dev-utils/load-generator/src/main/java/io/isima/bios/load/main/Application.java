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
package io.isima.bios.load.main;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.strobel.assembler.Collection;
import io.isima.bios.data.distribution.CloneDistribution;
import io.isima.bios.data.service.Config;
import io.isima.bios.data.service.DataStore;
import io.isima.bios.data.service.GaussianLoad;
import io.isima.bios.data.service.JmeterTest;
import io.isima.bios.data.service.LoadClient;
import io.isima.bios.load.model.StreamType;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.load.utils.DbUtils;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.jmeter.DeleteContextTest;
import io.isima.bios.sdk.jmeter.ExtractTest;
import io.isima.bios.sdk.jmeter.IngestTest;
import io.isima.bios.sdk.jmeter.PutContextTest;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Application {

  private final static Logger logger = Logger.getLogger(Application.class);

  public static void main(String[] args)
      throws IOException, BiosClientException, InterruptedException {
    if (args.length < 1) {
      System.err.println(
          "Usage: java -cp <class_paths> " + Application.class.getName() + "<property_file>");
      System.exit(1);
    }
    final String propertiesFile = args[0];
    final String configPath = Paths.get(propertiesFile).getParent().toString();
    final String logConfFile =
        System.getProperty("log4j.configurationFile", configPath + "/log4j.properties");
    logger.info("---------------------------------------------------");
    logger.info("Working Directory  : " + System.getProperty("user.dir"));
    logger.info("Configuration file : " + propertiesFile);
    logger.info("---------------------------------------------------");
    // Repeating the messages is ugly, but this is supposed to go to stdout console where info level
    // log is turned off in default log config
    System.out.println("---------------------------------------------------");
    System.out.println("Working Directory  : " + System.getProperty("user.dir"));
    System.out.println("Configuration file : " + propertiesFile);
    System.out.println("See " + logConfFile + " for logging configuration");
    System.out.println("---------------------------------------------------");
    PropertyConfigurator.configure(logConfFile);
    File resourceFile = new File(propertiesFile);
    try (FileReader reader = new FileReader(resourceFile)) {
      Properties props = new Properties();
      props.load(reader);
      props.forEach((key, value) -> {
        System.setProperty(String.valueOf(key), String.valueOf(value));
      });
    } catch (IOException ex) {
      logger.error(ex.getMessage(), ex);
      throw ex;
    }

    Config config = new Config(ApplicationUtils.getStreamJsonPath());
    LoadClient loadClient = new LoadClient(config);

    switch (ApplicationUtils.getLoadPattern()) {
      case CONSTANT:
        startJmeterLoad(config, loadClient);
        break;
      case GAUSSIAN:
        startGaussianLoad(config);
        break;
      case EXPONENTAIL:
        startGaussianLoad(config);
        break;
      case CLONE:
        startLoadCloning(config);
        break;
    }
  }

  // this will start load using normal distribution
  private static void startGaussianLoad(final Config config) throws BiosClientException {
    DataStore data = new DataStore(config);
    Session userSession =
        DbUtils.initSession(ApplicationUtils.getHost(), ApplicationUtils.getPort(),
            ApplicationUtils.getEmail(), ApplicationUtils.getPassword());
    int threadPoolSize = 0;
    for (String stream : config.getStreamNames()) {
      if (config.getStream(stream).getType().equals(StreamType.CONTEXT)) {
        threadPoolSize += 4; // for put/delete/get/update
      } else {
        threadPoolSize += 2; // for ingest and extract
      }
    }
    logger.debug("Size threadpool : " + threadPoolSize);
    GaussianLoad gaussianLoad = new GaussianLoad(userSession, config, data);
    ScheduledExecutorService executor =
        gaussianLoad.initExecutors(threadPoolSize, ApplicationUtils.getGaussianTimeWindow());
    Collection<Runnable> loadTasks = gaussianLoad.getExecutorThreads();
    for (Runnable runnable : loadTasks) {
      executor.scheduleAtFixedRate(runnable, 1, ApplicationUtils.getGaussianTimeWindow(),
          TimeUnit.MINUTES);
    }
  }

  // this will start load using normal distribution
  private static void startLoadCloning(final Config config)
      throws BiosClientException, InterruptedException {
    DataStore data = new DataStore(config);
    Session userSession =
        DbUtils.initSession(ApplicationUtils.getCloneSourceHost(), ApplicationUtils.getCloneSourcePort(),
            ApplicationUtils.getCloneSourceEmail(), ApplicationUtils.getCloneSourcePassword());
    Session adminSession =
        DbUtils.initSession(ApplicationUtils.getCloneSourceHost(), ApplicationUtils.getCloneSourcePort(),
            ApplicationUtils.getSystemAdminUser(), ApplicationUtils.getSystemAdminPassword());
    int threadPoolSize = 0;
    for (String stream : config.getStreamNames()) {
      if (config.getStream(stream).getType().equals(StreamType.CONTEXT)) {
        threadPoolSize += 4; // for put/delete/get/update
      } else {
        threadPoolSize += 2; // for ingest and extract
      }
    }
    logger.debug("Size threadpool : " + threadPoolSize);
    final long window5min = 1000 * 60 * 5;
    long startTime = System.currentTimeMillis();
    long next5thMin = Math.round((double) startTime / window5min) * window5min + window5min;
    // sleep till next 5th min
    long waitTime = next5thMin - System.currentTimeMillis();
    if (waitTime > 0) {
      logger.info(String.format("Waiting for %d millis before starting load", waitTime));
      Thread.sleep(waitTime);
    }

    CloneDistribution cd = new CloneDistribution(userSession, adminSession, config, data);
    ScheduledExecutorService executor = cd.initExecutors(threadPoolSize);
    Collection<Runnable> loadTasks = cd.getDistributionTasks();
    for (Runnable runnable : loadTasks) {
      executor.scheduleAtFixedRate(runnable, 30, 300, TimeUnit.SECONDS);
    }
  }

  // this will start load using jmeter
  private static void startJmeterLoad(final Config config, final LoadClient loadClient)
      throws JsonParseException, JsonMappingException,
      IOException, FileNotFoundException {
    DataStore dataStore = new DataStore(config);

    JMeterUtils.loadJMeterProperties(ApplicationUtils.getJmeterPropFile());
    JMeterUtils.setJMeterHome(ApplicationUtils.getJmeterHome());
    JMeterUtils.initLocale();
    JMeterUtils.setProperty("summariser.interval", "30");


    HashTree testPlan = JmeterTest.createPlanTree(config);
    IngestTest.init(config, loadClient, dataStore);
    ExtractTest.init(config, loadClient, dataStore);
    PutContextTest.init(config, loadClient, dataStore);
    DeleteContextTest.init(config, loadClient, dataStore);

    StandardJMeterEngine jmeter = new StandardJMeterEngine();
    jmeter.configure(testPlan);
    jmeter.run();
  }
}
