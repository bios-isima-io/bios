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
package io.isima.bios.admin;

import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MaterializedAs;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.PostStorageStageConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalFeatureConfig;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SignalValidatorFeaturesTest {
  private static Validators validators;

  private SignalConfig simpleFeature;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    validators = new Validators();
  }

  @Before
  public void setup() throws Exception {
    simpleFeature = new SignalConfig();
    simpleFeature.setName("simpleFeature");
    simpleFeature.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("first", AttributeType.STRING));
    attributes.add(new AttributeConfig("second", AttributeType.INTEGER));
    attributes.add(new AttributeConfig("third", AttributeType.STRING));
    attributes.add(new AttributeConfig("fourth", AttributeType.INTEGER));
    simpleFeature.setAttributes(attributes);
    final var postStorage = new PostStorageStageConfig();
    simpleFeature.setPostStorageStage(postStorage);
    postStorage.setFeatures(new ArrayList<>());
    final var feature = new SignalFeatureConfig();
    feature.setName("featureOne");
    feature.setDimensions(List.of());
    feature.setFeatureInterval(60000L);
    feature.setMaterializedAs(MaterializedAs.ACCUMULATING_COUNT);
    postStorage.getFeatures().add(feature);
  }

  @Test
  public void testSimpleFeature() throws Exception {
    validators.validateSignal("testTenant", simpleFeature);
  }

  // Regression BB-1245
  @Test
  public void testFeatureNamesConflict() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[1].featureName:"
            + " The name conflicts with another feature entry;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne");

    final var feature = new SignalFeatureConfig();
    simpleFeature.getPostStorageStage().getFeatures().add(feature);
    feature.setName("featureOne");
    feature.setDimensions(List.of());
    feature.setFeatureInterval(60000L);
    validators.validateSignal("testTenant", simpleFeature);
  }

  // Regression BB-1246
  @Test
  public void testNegativeRollupInterval() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[0].featureInterval:"
            + " featureInterval must be chosen from among:"
            + " 50/100/500 ms, 1/5/15/30 second, and 1/5/15/30 minute;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne,"
            + " featureInterval=-300000");

    final var feature = simpleFeature.getPostStorageStage().getFeatures().get(0);

    feature.setFeatureInterval(-300000L);
    validators.validateSignal("testTenant", simpleFeature);
  }

  @Test
  public void testInvalidRollupInterval() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[0].featureInterval:"
            + " featureInterval must be chosen from among:"
            + " 50/100/500 ms, 1/5/15/30 second, and 1/5/15/30 minute;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne,"
            + " featureInterval=123456");

    final var feature = simpleFeature.getPostStorageStage().getFeatures().get(0);

    feature.setFeatureInterval(123456L);
    validators.validateSignal("testTenant", simpleFeature);
  }

  @Test
  public void testInvalidRollupInterval2() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[0].featureInterval:"
            + " featureInterval must be chosen from among:"
            + " 50/100/500 ms, 1/5/15/30 second, and 1/5/15/30 minute;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne,"
            + " featureInterval=3600000");

    final var feature = simpleFeature.getPostStorageStage().getFeatures().get(0);

    feature.setFeatureInterval(1000L * 60 * 60);
    validators.validateSignal("testTenant", simpleFeature);
  }

  @Test
  public void testDataSketchesWithDimensions() throws Exception {
    thrown.expect(NotImplementedValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[0].dataSketches:"
            + " Data sketch Moments with group-by dimensions are not supported yet;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne");

    final var feature = simpleFeature.getPostStorageStage().getFeatures().get(0);

    feature.setDimensions(List.of("first"));
    feature.setDataSketches(List.of(DataSketchType.MOMENTS));
    feature.setAttributes(List.of("fourth"));
    validators.validateSignal("testTenant", simpleFeature);
  }

  @Test
  public void testDataSketchesWithoutAttributes() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[0].dataSketches:"
            + " Data sketches require at least one attribute to be specified;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne");

    final var feature = simpleFeature.getPostStorageStage().getFeatures().get(0);

    feature.setDataSketches(List.of(DataSketchType.MOMENTS));
    validators.validateSignal("testTenant", simpleFeature);
  }

  @Test
  public void testRepeatedDataSketches() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage(
        "signalConfig.postStorageStage.features[0].dataSketches:"
            + " Data sketches must not be repeated; {Moments} was repeated;"
            + " tenantName=testTenant, signalName=simpleFeature, featureName=featureOne");

    final var feature = simpleFeature.getPostStorageStage().getFeatures().get(0);

    feature.setAttributes(List.of("first", "second"));
    feature.setDataSketches(List.of(DataSketchType.MOMENTS, DataSketchType.MOMENTS));
    validators.validateSignal("testTenant", simpleFeature);
  }
}
