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
package io.isima.bios.control;

import static io.isima.bios.models.v1.InternalAttributeType.INT;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test case verifies constraint checks with control plane operations regarding the Join
 * functionality.
 */
public class JoinConfigConstraintTest {

  private static AdminInternal tfosAdmin;
  private static long timestamp;

  @BeforeClass
  public static void setUpClass() throws FileReadException {
    tfosAdmin = new AdminImpl(null, new MetricsStreamProvider());
    timestamp = System.currentTimeMillis();
  }

  @After
  public void tearDown()
      throws NoSuchTenantException, ApplicationException, ConstraintViolationException {
    for (String tenant : tfosAdmin.getAllTenants()) {
      tfosAdmin.removeTenant(new TenantConfig(tenant), RequestPhase.FINAL, ++timestamp);
    }
  }

  @Test
  public void addTenant() throws TfosException, ApplicationException {
    // setup
    final String tenant = "test";
    final String signalStreamName = "signal";
    final String keyAttr = "mykey";
    final String signalSecond = "signal_second";
    final String signalThird = "signal_third";
    final String contextStreamName = "lookup";
    final String secondContextStreamName = "lookup2";
    final String thirdContextStreamName = "lookup3";

    TenantConfig tenantConfig = new TenantConfig(tenant);

    // setup signal stream
    StreamConfig signalStream = new StreamConfig(signalStreamName);
    tenantConfig.addStream(signalStream);
    signalStream.addAttribute(new AttributeDesc(keyAttr, STRING));
    signalStream.addAttribute(new AttributeDesc(signalSecond, STRING));
    signalStream.addAttribute(new AttributeDesc(signalThird, INT));
    //
    PreprocessDesc join = new PreprocessDesc("join");
    signalStream.addPreprocess(join);
    join.setCondition(keyAttr);
    join.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    //
    ActionDesc action = new ActionDesc();
    join.addAction(action);
    action.setActionType(ActionType.MERGE);
    action.setContext(contextStreamName);
    action.setAttribute("second");

    // setup the second context stream (will setup the first later)
    StreamConfig secondContextStream =
        new StreamConfig(secondContextStreamName, StreamType.CONTEXT);
    tenantConfig.addStream(secondContextStream);
    secondContextStream.addAttribute(new AttributeDesc("whatever", INT));
    secondContextStream.addAttribute(new AttributeDesc("converted", INT));
    secondContextStream.addAttribute(new AttributeDesc("conflicting", STRING));
    secondContextStream.addAttribute(new AttributeDesc("unique_in_second", INT));

    // context invalid context stream specification test /////////////

    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
      fail("exception is expected: target context stream is missing");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }
    try {
      tfosAdmin.getTenant(tenant);
      fail("exception is expected: the tenant should not be found after add failure");
    } catch (NoSuchTenantException e) {
      // do nothing
    }

    action.setContext(signalStreamName);
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
      fail("exception is expected: target stream must not be itself");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    action.setContext(contextStreamName);
    // context stream test /////////////

    // setup context stream
    StreamConfig contextStream = new StreamConfig(contextStreamName, StreamType.CONTEXT);
    contextStream.addAttribute(new AttributeDesc("first", STRING));
    contextStream.addAttribute(new AttributeDesc("second", STRING));
    contextStream.addAttribute(new AttributeDesc("conflicting", STRING));
    tenantConfig.addStream(contextStream);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // check immutability
    assertNull(signalStream.getAdditionalAttributes());

    // stream type test /////////////

    // wrong context stream type
    contextStream.setType(StreamType.SIGNAL);
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
      fail("exception is expected: context stream must be of type CONTEXT");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    // wrong signal stream type
    contextStream.setType(StreamType.CONTEXT);
    signalStream.setType(StreamType.CONTEXT);
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
      fail("exception is expected: signal stream must be of type SIGNAL");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    // break; fix invalid part and make sure the addTenant succeeds
    signalStream.setType(StreamType.SIGNAL);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // condition test /////////////

    // invalid condition attribute: missing
    join.setCondition(null);
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected: condition attribute is missing");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    // invalid condition attribute: no such attribute
    join.setCondition("nosuchattr");
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected: condition attribute does not match any signal attributes");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    // valid: condition attribute matches the second signal attribute
    join.setCondition(signalSecond);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // invalid condition attribute: the type is different from primary key
    join.setCondition(signalThird);
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected: condition attribute is int type but primary key is string");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    // done condition parameter test. fix the problem
    join.setCondition(keyAttr);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // two conditions test ///////////////////
    //
    PreprocessDesc secondJoin = new PreprocessDesc("join2");
    signalStream.addPreprocess(secondJoin);
    secondJoin.setCondition(signalThird);
    secondJoin.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    //
    ActionDesc secondAction = new ActionDesc();
    secondJoin.addAction(secondAction);
    secondAction.setActionType(ActionType.MERGE);
    secondAction.setContext(secondContextStreamName);
    secondAction.setAttribute("converted");

    // test valid one first.
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // duplicate attributes are not allowed even against different context
    action.setAttribute("conflicting");
    secondAction.setAttribute("conflicting");
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected: duplicate attributes are not allowed");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    // break; fix the problem and verify addTenant succeeds
    secondAction.setAttribute("unique_in_second");
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // three contexts and two conditions ////////////////////

    StreamConfig thirdContextStream = new StreamConfig(thirdContextStreamName);
    tenantConfig.addStream(thirdContextStream);
    thirdContextStream.setType(StreamType.CONTEXT);
    thirdContextStream.addAttribute(new AttributeDesc("dict", STRING));
    thirdContextStream.addAttribute(new AttributeDesc("theword", STRING));
    //
    PreprocessDesc thirdJoin = new PreprocessDesc();
    signalStream.addPreprocess(thirdJoin);
    thirdJoin.setCondition(keyAttr);
    thirdJoin.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    //
    ActionDesc thirdAction = new ActionDesc();
    thirdJoin.addAction(thirdAction);
    thirdAction.setActionType(ActionType.MERGE);
    thirdAction.setContext(thirdContextStreamName);
    thirdAction.setAttribute("theword");

    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected: preprocess name must be set");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    thirdJoin.setName("join");
    try {
      tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected: duplicate process names are not allowed");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    thirdJoin.setName("join3");

    // this is valid
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    // check interpreter output
    List<ProcessStage> stages = tfosAdmin.getStream(tenant, signalStreamName).getPreprocessStages();
    assertEquals(3, stages.size());
  }

  @Test
  public void addAndRemoveStream() throws TfosException, ApplicationException {
    final String tenant = "test";
    final String signalStreamName = "signal";
    final String keyAttr = "mykey";
    final String contextStreamName = "lookup";
    TenantConfig tenantConfig = new TenantConfig(tenant);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, ++timestamp);

    StreamConfig signalStream = new StreamConfig(signalStreamName);
    tenantConfig.addStream(signalStream);
    signalStream.addAttribute(new AttributeDesc(keyAttr, STRING));
    PreprocessDesc join = new PreprocessDesc("join");
    signalStream.addPreprocess(join);
    join.setCondition(keyAttr);
    join.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    ActionDesc action = new ActionDesc();
    join.addAction(action);
    action.setActionType(ActionType.MERGE);
    action.setContext(contextStreamName);
    action.setAttribute("second");
    try {
      tfosAdmin.addStream(tenant, signalStream, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }

    StreamConfig contextStream = new StreamConfig(contextStreamName);
    contextStream.setType(StreamType.CONTEXT);
    contextStream.addAttribute(new AttributeDesc("first", STRING));
    contextStream.addAttribute(new AttributeDesc("second", STRING));
    tfosAdmin.addStream(tenant, contextStream, RequestPhase.FINAL, ++timestamp);
    tfosAdmin.addStream(tenant, signalStream, RequestPhase.FINAL, ++timestamp);

    try {
      tfosAdmin.removeStream(tenant, contextStreamName, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      System.out.println(e.getErrorMessage());
    }
  }
}
