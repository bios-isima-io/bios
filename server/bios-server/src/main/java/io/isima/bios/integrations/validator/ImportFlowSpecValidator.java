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
package io.isima.bios.integrations.validator;

import io.isima.bios.admin.ValidationState;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.MultipleValidatorViolationsException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.DataPickupSpec;
import io.isima.bios.models.DestinationDataSpec;
import io.isima.bios.models.ImportDestinationStreamType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.Method;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.SourceDataSpec;
import io.isima.bios.models.TenantConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ImportFlowSpecValidator {

  protected static final String IMPORT_FLOW_SPEC = "importFlowSpec";
  protected static final String SOURCE_DATA_SPEC = "sourceDataSpec";
  protected static final String DESTINATION_DATA_SPEC = "destinationDataSpec";
  protected static final String DATA_PICKUP_SPEC = "dataPickupSpec";

  protected static final String IMPORT_FLOW_NAME = "importFlowName";
  protected static final String IMPORT_FLOW_ID = "importFlowId";
  protected static final String TYPE = "type";

  protected final TenantConfig tenantConfig;
  protected final ImportFlowConfig config;
  protected final ValidationState state;

  protected final List<ValidatorException> errors;
  protected ImportDestinationStreamType destinationType;
  protected BiosStreamConfig streamConfig;

  protected boolean used;

  public static ImportFlowSpecValidator newValidator(
      TenantConfig tenantConfig, ImportFlowConfig config) {
    return new ImportFlowSpecValidator(tenantConfig, config);
  }

  protected ImportFlowSpecValidator(TenantConfig tenantConfig, ImportFlowConfig config) {
    this.tenantConfig = tenantConfig;
    this.config = config;
    state = new ValidationState(IMPORT_FLOW_SPEC);
    errors = new ArrayList<>();
    used = false;
  }

  public void validate() throws ValidatorException {
    if (used) {
      throw new IllegalStateException("This validator is used already. Create a new instance");
    }
    used = true;

    if (config.getImportFlowName() == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(IMPORT_FLOW_NAME + " must be set")));
    } else {
      state.putContext(IMPORT_FLOW_NAME, config.getImportFlowName());
    }

    if (config.getImportFlowId() == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(IMPORT_FLOW_ID + " must be set")));
    } else {
      state.putContext(IMPORT_FLOW_ID, config.getImportFlowId());
    }

    final var sourceDataSpec = config.getSourceDataSpec();
    if (sourceDataSpec == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(SOURCE_DATA_SPEC + " must be set")));
    } else {
      validateSourceDataSpec(sourceDataSpec, tenantConfig, state.next(SOURCE_DATA_SPEC), errors);
    }

    final var destinationDataSpec = config.getDestinationDataSpec();
    if (destinationDataSpec == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(DESTINATION_DATA_SPEC + " must be set")));
      checkErrors(errors); // we know we throw here
    }

    validateDestinationDataSpec(
        destinationDataSpec, tenantConfig, state.next(DESTINATION_DATA_SPEC), errors);

    // TODO: Check importDestinationId

    state.putContext(destinationType.name().toLowerCase(), streamConfig.getName());

    final var streamAttributes = new LinkedHashMap<String, AttributeConfig>();
    streamConfig
        .getAttributes()
        .forEach((attribute) -> streamAttributes.put(attribute.getName().toLowerCase(), attribute));

    state.getNames("streamNames");

    final var dataPickupSpec = config.getDataPickupSpec();
    if (dataPickupSpec == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage("%s must be set", DATA_PICKUP_SPEC)));
      checkErrors(errors); // we throw here
    }
    final var dataPickupState = state.next(DATA_PICKUP_SPEC);

    validateDeletionSpec(dataPickupSpec.getDeletionSpec());

    final var outputNames = new HashSet<String>();

    for (int i = 0; i < dataPickupSpec.getAttributes().size(); ++i) {
      final var attributeSpec = dataPickupSpec.getAttributes().get(i);
      final var currentState = dataPickupState.nextWithIndex("attributes", i);

      final var name = attributeSpec.getSourceAttributeName();
      final var names = attributeSpec.getSourceAttributeNames();
      final var transforms = attributeSpec.getTransforms();
      final var processes = attributeSpec.getProcesses();
      final var as = attributeSpec.getAs();
      if (name == null && names == null) {
        errors.add(
            new ConstraintViolationValidatorException(
                currentState.buildMessage(
                    "Property sourceAttributeName or sourceAttributeNames must be set")));
        continue;
      }
      if (name != null) {
        currentState.putContext("sourceAttributeName", name);
      }
      if (names != null) {
        currentState.putContext("sourceAttributeNames", names.toString());
      }

      // check disallowed combinations
      if (name != null && names != null) {
        errors.add(
            new ConstraintViolationValidatorException(
                currentState.buildMessage(
                    "Only one of 'sourceAttributeName' or 'sourceAttributeNames' can be set")));
        continue;
      }
      if (as != null && transforms != null) {
        errors.add(
            new ConstraintViolationValidatorException(
                currentState.buildMessage("Both of 'as' and 'transforms' must not be set")));
        continue;
      }

      String out = null;

      // check the constraints for sourceAttributeName
      if (name != null) {
        out = name;
      }

      if (names != null) {
        if (names.isEmpty()) {
          errors.add(
              new ConstraintViolationValidatorException(
                  currentState.buildMessage(
                      "Property 'sourceAttributeNames' may not be empty if set")));
        }
        currentState.putContext("names", names.toString());
        if (processes == null && transforms == null) {
          errors.add(
              new ConstraintViolationValidatorException(
                  currentState.buildMessage(
                      "Either property 'transforms' or 'processes' must be set"
                          + " when 'sourceAttributeNames' is set")));
        }
      }

      if (processes != null) {}

      // check the constraints for transforms
      if (transforms != null) {
        if (transforms.isEmpty()) {
          errors.add(
              new ConstraintViolationValidatorException(
                  currentState.buildMessage("Property 'transforms' may not be empty if set")));
        }
        for (int j = 0; j < transforms.size(); ++j) {
          final var tstate = currentState.nextWithIndex("transforms", j);
          final var transform = transforms.get(j);
          final var rule = transform.getRule();
          if (rule == null) {
            errors.add(
                new ConstraintViolationValidatorException(
                    tstate.buildMessage("Property 'rule' must be set")));
          }
          final var transformOut = transform.getAs();
          if (transformOut == null) {
            errors.add(
                new ConstraintViolationValidatorException(
                    tstate.buildMessage("Property 'as' must be set")));
            continue;
          }
          if (outputNames.contains(transformOut.toLowerCase())) {
            errors.add(
                new ConstraintViolationValidatorException(
                    tstate.buildMessage("Duplicate destination attribute name: %s", transformOut)));
          } else if (streamAttributes.remove(transformOut.toLowerCase()) == null) {
            errors.add(
                new ConstraintViolationValidatorException(
                    tstate.buildMessage(
                        "Destination attribute '%s' is missing in the attributes of %s '%s'",
                        transformOut,
                        destinationType.name().toLowerCase(),
                        streamConfig.getName())));
          }
          outputNames.add(transformOut.toLowerCase());
        }
      } else {
        if (as != null) {
          out = as;
        }
        if (out == null) {
          errors.add(
              new ConstraintViolationValidatorException(
                  currentState.buildMessage("Property 'as' must be set")));
        } else {
          if (outputNames.contains(out.toLowerCase())) {
            errors.add(
                new ConstraintViolationValidatorException(
                    currentState.buildMessage("Duplicate destination attribute name: %s", out)));
          } else if (streamAttributes.remove(out.toLowerCase()) == null) {
            errors.add(
                new ConstraintViolationValidatorException(
                    currentState.buildMessage(
                        "Destination attribute '%s' is missing in the attributes of %s '%s'",
                        out, destinationType.name().toLowerCase(), streamConfig.getName())));
          }
          outputNames.add(out.toLowerCase());
        }
      }
    }

    // Remaining entries in streamAttributes here are signal/context attributes that are not
    // covered by the flow. Their missing attribute policy must be StoreDefaultValue.
    streamAttributes
        .values()
        .forEach(
            (attribute) -> {
              var missingAttributePolicy = attribute.getMissingAttributePolicy();
              if (missingAttributePolicy == null) {
                missingAttributePolicy = streamConfig.getMissingAttributePolicy();
              }
              if (missingAttributePolicy != MissingAttributePolicy.STORE_DEFAULT_VALUE) {
                errors.add(
                    new ConstraintViolationValidatorException(
                        state.buildMessage(
                            "Attribute '%s' of %s '%s' is not covered by the flow,"
                                + " missingAttributePolicy must be 'StoreDefaultValue' but is '%s'",
                            attribute.getName(),
                            destinationType.name().toLowerCase(),
                            streamConfig.getName(),
                            missingAttributePolicy.stringify())));
              }
            });

    // check corrected errors and throw
    checkErrors(errors);
  }

  private void validateSourceDataSpec(
      SourceDataSpec sourceDataSpec,
      TenantConfig tenantConfig,
      ValidationState state,
      List<ValidatorException> errors) {
    final var importSourceId = sourceDataSpec.getImportSourceId();
    if (importSourceId == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage("importSourceId must be set")));
    } else {
      ImportSourceConfig sourceConfig = null;
      for (var importSourceConfig : tenantConfig.getImportSources()) {
        if (importSourceId.equals(importSourceConfig.getImportSourceId())) {
          sourceConfig = importSourceConfig;
          break;
        }
      }
      if (sourceConfig != null) {
        state.putContext("importSourceId", importSourceId);
        ImportSourceType sourceType = sourceConfig.getType();
        if (sourceType != null && sourceType.equals(ImportSourceType.REST_CLIENT)) {
          Method method = sourceConfig.getMethod();
          if (method == null) {
            errors.add(
                new ConstraintViolationValidatorException(
                    state.buildMessage("method must be set")));
          } else if (method.equals(Method.GET)) {
            if (sourceDataSpec.getBodyParams() != null) {
              errors.add(
                  new ConstraintViolationValidatorException(
                      state.buildMessage("bodyParams must be not be set for http GET method")));
            }
          }
        }
      } else {
        errors.add(
            new ConstraintViolationValidatorException(
                state.buildMessage(
                    "importSourceId '%s' not found in existing importSources", importSourceId)));
      }
    }
    if (sourceDataSpec.getPayloadType() == null) {
      errors.add(
          new ConstraintViolationValidatorException(state.buildMessage("payloadType must be set")));
    }
  }

  private void validateDestinationDataSpec(
      DestinationDataSpec destinationDataSpec,
      TenantConfig tenantConfig,
      ValidationState state,
      List<ValidatorException> errors)
      throws ValidatorException {
    final var importDestinationId = destinationDataSpec.getImportDestinationId();
    if (importDestinationId == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage("importDestinationId must be set")));
    } else {
      boolean found = false;
      for (var importDestinationConfig : tenantConfig.getImportDestinations()) {
        if (importDestinationId.equals(importDestinationConfig.getImportDestinationId())) {
          found = true;
          break;
        }
      }
      if (found) {
        state.putContext("importDestinationId", importDestinationId);
      } else {
        errors.add(
            new ConstraintViolationValidatorException(
                state.buildMessage(
                    "importDestinationId '%s' not found in existing importDestinations",
                    importDestinationId)));
      }
    }

    destinationType = destinationDataSpec.getType();
    if (destinationType == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage("property 'type' must be set")));
      checkErrors(errors); // we know we throw here
    }

    final var streamName = destinationDataSpec.getName();
    if (streamName == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage("Property 'name' must be set")));
      checkErrors(errors); // we stop here
    } else {
      switch (destinationType) {
        case SIGNAL:
          streamConfig = findStream(tenantConfig.getSignals(), streamName);
          break;
        case CONTEXT:
          streamConfig = findStream(tenantConfig.getContexts(), streamName);
          break;
        default:
          // shouldn't happen
      }
    }

    if (streamConfig == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(
                  "Target %s %s is missing", destinationType.name().toLowerCase(), streamName)));
      checkErrors(errors); // we know we throw here
    }
  }

  private void validateDeletionSpec(DataPickupSpec.DeletionSpec deletionSpec) {
    if (deletionSpec == null) {
      // Do nothing if not set. Deltion spec is optional.
      return;
    }
    final var destinationType = config.getDestinationDataSpec().getType();
    if (destinationType != ImportDestinationStreamType.CONTEXT) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(
                  String.format(
                      "deletionSpec may not be set for a destination of type %s",
                      destinationType.stringify()))));
    }
  }

  private void checkErrors(List<ValidatorException> errors) throws ValidatorException {
    if (errors.size() > 1) {
      throw new MultipleValidatorViolationsException(errors);
    } else if (errors.size() == 1) {
      throw errors.get(0);
    }
  }

  private <T extends BiosStreamConfig> BiosStreamConfig findStream(
      List<T> streams, String streamName) {

    final var found =
        streams.stream()
            .filter((signal) -> signal.getName().equalsIgnoreCase(streamName))
            .collect(Collectors.toList());

    if (found.size() == 0) {
      return null;
    }

    return found.get(0);
  }
}
