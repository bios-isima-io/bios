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
package io.isima.bios.data.service;

import io.isima.bios.load.model.StreamType;
import io.isima.bios.load.utils.ApplicationUtils;
import io.isima.bios.sdk.jmeter.DeleteContextTest;
import io.isima.bios.sdk.jmeter.ExtractTest;
import io.isima.bios.sdk.jmeter.IngestTest;
import io.isima.bios.sdk.jmeter.PutContextTest;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.gui.ArgumentsPanel;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.control.RandomController;
import org.apache.jmeter.control.RandomOrderController;
import org.apache.jmeter.control.gui.LoopControlPanel;
import org.apache.jmeter.control.gui.RandomControlGui;
import org.apache.jmeter.control.gui.RandomOrderControllerGui;
import org.apache.jmeter.control.gui.TestPlanGui;
import org.apache.jmeter.protocol.java.control.gui.JavaTestSamplerGui;
import org.apache.jmeter.protocol.java.sampler.JavaSampler;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.reporters.gui.SummariserGui;
import org.apache.jmeter.save.SaveService;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.threads.gui.ThreadGroupGui;
import org.apache.jmeter.timers.ConstantTimer;
import org.apache.jmeter.timers.gui.ConstantTimerGui;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.visualizers.SummaryReport;
import org.apache.jorphan.collections.HashTree;

public class JmeterTest {

  public static HashTree createPlanTree(final Config config)
      throws IOException {

    Set<String> streams = config.getStreamNames();
    List<JavaSampler> ingestSamplers = new ArrayList<JavaSampler>();
    List<JavaSampler> extractSamplers = new ArrayList<JavaSampler>();
    List<JavaSampler> putContextSamplers = new ArrayList<JavaSampler>();
    List<JavaSampler> deleteContextSamplers = new ArrayList<JavaSampler>();
    for (String stream : streams) {
      if (config.getStream(stream).getType().equals(StreamType.SIGNAL)) {
        ingestSamplers.add(getSampler(stream, IngestTest.class));
        extractSamplers.add(getSampler(stream, ExtractTest.class));
      } else if (config.getStream(stream).getType().equals(StreamType.CONTEXT)) {
        putContextSamplers.add(getSampler(stream, PutContextTest.class));
        deleteContextSamplers.add(getSampler(stream, DeleteContextTest.class));
      }
    }

    // Loop Controller
    TestElement ingestLoopController = new LoopController();
    ((LoopController) ingestLoopController).setLoops(ApplicationUtils.getTestLoopCount());
    for (JavaSampler sampler : ingestSamplers) {
      ingestLoopController.addTestElement(sampler);
    }
    ((LoopController) ingestLoopController).setFirst(true);
    ingestLoopController.setProperty(TestElement.TEST_CLASS, LoopController.class.getName());
    ingestLoopController.setProperty(TestElement.GUI_CLASS, LoopControlPanel.class.getName());
    ((LoopController) ingestLoopController).initialize();

    // Extract Loop Controller
    TestElement extractLoopController = new LoopController();
    ((LoopController) extractLoopController).setLoops(ApplicationUtils.getTestLoopCount());

    TestElement randomController = new RandomController();
    for (JavaSampler sampler : extractSamplers) {
      randomController.addTestElement(sampler);
    }
    randomController.setProperty(TestElement.TEST_CLASS, RandomController.class.getName());
    randomController.setProperty(TestElement.GUI_CLASS, RandomOrderControllerGui.class.getName());

    extractLoopController.addTestElement(randomController);
    ((LoopController) extractLoopController).setFirst(true);
    extractLoopController.setProperty(TestElement.TEST_CLASS, LoopController.class.getName());
    extractLoopController.setProperty(TestElement.GUI_CLASS, LoopControlPanel.class.getName());
    ((LoopController) extractLoopController).initialize();

    // Thread Group
    ThreadGroup threadGroup =
        createThreadGroup(ingestLoopController, ApplicationUtils.INGEST_THREAD_NAME,
            ApplicationUtils.getIngestThreadNumber(), 60 * 5);
    ThreadGroup extractThreadGroup =
        createThreadGroup(extractLoopController, ApplicationUtils.EXTRACT_THREAD_NAME,
            ApplicationUtils.getExtractThreadNumber(), 60 * 5);

    // controllers for thread group
    RandomOrderController ingestController = new RandomOrderController();
    ingestController.setName("controller");
    ingestController.setProperty(TestElement.TEST_CLASS, RandomOrderController.class.getName());
    ingestController.setProperty(TestElement.GUI_CLASS, RandomOrderControllerGui.class.getName());
    ingestController.initialize();

    RandomController extractController = new RandomController();
    extractController.setName("controller");
    extractController.setProperty(TestElement.TEST_CLASS, RandomController.class.getName());
    extractController.setProperty(TestElement.GUI_CLASS, RandomControlGui.class.getName());
    extractController.initialize();

    RandomOrderController putContextController = (RandomOrderController) ingestController.clone();
    putContextController.initialize();

    Map<ThreadGroup, List<JavaSampler>> thSamplerMap =
        new HashMap<ThreadGroup, List<JavaSampler>>();
    thSamplerMap.put(threadGroup, ingestSamplers);
    thSamplerMap.put(extractThreadGroup, extractSamplers);

    // ingest sampler map
    Map<ThreadGroup, Map<String, Object>> map = new HashMap<ThreadGroup, Map<String, Object>>();
    Map<String, Object> ingestControllerMap = new HashMap<String, Object>();
    ingestControllerMap.put("samplers", ingestSamplers);
    ingestControllerMap.put("controller", ingestController);

    // extract sampler map
    Map<String, Object> extractControllerMap = new HashMap<String, Object>();
    extractControllerMap.put("samplers", extractSamplers);
    extractControllerMap.put("controller", extractController);

    Map<String, Object> putContextControllerMap = new HashMap<String, Object>();
    putContextControllerMap.put("samplers", putContextSamplers);
    putContextControllerMap.put("controller", putContextController);

    ThreadGroup contextThreadGroup = (ThreadGroup) threadGroup.clone();
    contextThreadGroup.setName(ApplicationUtils.CONTEXT_THREAD_NAME);
    contextThreadGroup.setNumThreads(ApplicationUtils.getContextThreadNumber());
    contextThreadGroup.initialize();

    if (ingestSamplers.size() > 0 && ApplicationUtils.getIngestThreadNumber() > 0) {
      map.put(threadGroup, ingestControllerMap);
    }
    if (extractSamplers.size() > 0 && ApplicationUtils.getExtractThreadNumber() > 0) {
      map.put(extractThreadGroup, extractControllerMap);
    }
    if (putContextSamplers.size() > 0 && ApplicationUtils.getContextThreadNumber() > 0) {
      map.put(contextThreadGroup, putContextControllerMap);
    }

    HashTree testPlanTree = new HashTree();
    configureTestPlan(createNewTestPlan(ApplicationUtils.getJmeterConfigFileName()), testPlanTree,
        map);

    Summariser summer = null;
    String summariserName = JMeterUtils.getPropDefault("summariser.name", "summary");
    if (summariserName.length() > 0) {
      summer = new Summariser(summariserName);
      summer.setName("Summarize results");
      summer.setProperty(TestElement.TEST_CLASS, Summariser.class.getName());
      summer.setProperty(TestElement.GUI_CLASS, SummariserGui.class.getName());
    }

    // Constant timer
    ConstantTimer constantTimer = new ConstantTimer();
    constantTimer.setName("constant wait timer");
    constantTimer.setDelay(ApplicationUtils.getJmeterConstantTimer());
    constantTimer.setProperty(TestElement.TEST_CLASS, ConstantTimer.class.getName());
    constantTimer.setProperty(TestElement.GUI_CLASS, ConstantTimerGui.class.getName());
    testPlanTree.add(testPlanTree.getArray()[0], constantTimer);

    // Store execution results into a .jtl file
    ResultCollector logger = new ResultCollector(summer);
    logger.setFilename(ApplicationUtils.getTestResultFileName());
    logger.setName("Summary Report");
    logger.setErrorLogging(true);
    // logger.setSuccessOnlyLogging(false);
    logger.setProperty(TestElement.TEST_CLASS, ResultCollector.class.getName());
    logger.setProperty(TestElement.GUI_CLASS, SummaryReport.class.getName());

    testPlanTree.add(testPlanTree.getArray()[0], logger);

    // save test to jmx file
    SaveService.saveTree(testPlanTree,
        new FileOutputStream(ApplicationUtils.getTestPlanFileName()));

    return testPlanTree;
  }

  private static JavaSampler getSampler(String streamName, Class<?> samplerClass) {
    JavaSampler javaSampler = new JavaSampler();
    javaSampler.setName(streamName);
    javaSampler.setClassname(samplerClass.getCanonicalName());

    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument("email", ApplicationUtils.getEmail());
    defaultParameters.addArgument("password", ApplicationUtils.getPassword());
    defaultParameters.addArgument("streams_json", ApplicationUtils.getStreamJsonPath());
    defaultParameters.addArgument("host", ApplicationUtils.getHost());
    defaultParameters.addArgument("port", String.valueOf(ApplicationUtils.getPort()));
    defaultParameters.addArgument("ssl", String.valueOf(ApplicationUtils.getSSLFlag()));
    defaultParameters.addArgument("stream", streamName);

    javaSampler.setArguments(defaultParameters);
    javaSampler.setProperty(TestElement.TEST_CLASS, JavaSampler.class.getName());
    javaSampler.setProperty(TestElement.GUI_CLASS, JavaTestSamplerGui.class.getName());
    return javaSampler;
  }

  private static void configureTestPlan(TestPlan testPlan, HashTree testPlanTree,
      Map<ThreadGroup, Map<String, Object>> threadGroupSamplerMap) {

    // Construct Test Plan from previously initialized elements
    testPlanTree.add(testPlan);
    for (ThreadGroup threadGroup : threadGroupSamplerMap.keySet()) {
      HashTree planTree = testPlanTree.add(testPlan);
      HashTree threadTree = planTree.add(threadGroup);
      HashTree controllerTree =
          threadTree.add(threadGroupSamplerMap.get(threadGroup).get("controller"));
      controllerTree
          .add((List<JavaSampler>) threadGroupSamplerMap.get(threadGroup).get("samplers"));
      if (threadGroupSamplerMap.get(threadGroup).containsKey("timer")) {
        controllerTree.add(threadGroupSamplerMap.get(threadGroup).get("timer"));
      }
    }
  }

  // creates a new test plan
  private static TestPlan createNewTestPlan(String testPlanName) {
    TestPlan testPlan = new TestPlan(testPlanName);
    testPlan.setProperty(TestElement.TEST_CLASS, TestPlan.class.getName());
    testPlan.setProperty(TestElement.GUI_CLASS, TestPlanGui.class.getName());
    testPlan.setUserDefinedVariables((Arguments) new ArgumentsPanel().createTestElement());
    return testPlan;
  }

  private static ThreadGroup createThreadGroup(TestElement loopController, String threadGroupName,
      int numThread, int rampUp) {
    ThreadGroup threadGroup = new ThreadGroup();
    threadGroup.setNumThreads(numThread);
    threadGroup.setRampUp(rampUp);
    threadGroup.setName(threadGroupName);
    threadGroup.setSamplerController(((LoopController) loopController));
    threadGroup.setProperty(TestElement.TEST_CLASS, ThreadGroup.class.getName());
    threadGroup.setProperty(TestElement.GUI_CLASS, ThreadGroupGui.class.getName());
    return threadGroup;
  }
}
