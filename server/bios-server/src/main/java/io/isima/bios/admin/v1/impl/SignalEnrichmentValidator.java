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
package io.isima.bios.admin.v1.impl;

import static io.isima.bios.admin.v1.impl.AdminImplUtils.createExceptionMessage;
import static io.isima.bios.models.v1.InternalAttributeType.BLOB;
import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.DOUBLE;
import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.LONG;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import com.google.common.base.Strings;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidDefaultValueException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.preprocess.JoinProcessor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates enrichment configuration of a signal.
 *
 * <p>The validator checks if the configuration meets the constraint. It also may do minor
 * correction to the testing signal in place, such as fixing cases of names.
 */
public class SignalEnrichmentValidator {
  private final AdminImpl admin;
  private final TenantDesc tenantDesc;
  private final StreamDesc streamDesc;

  // Compiled pre-processors
  // key is <remoteContext>.<foreignKey>
  final Map<String, ProcessStage> processes = new LinkedHashMap<>();

  // for conflict detection
  final Set<String> preprocessNames = new HashSet<>();
  final Set<String> allAttributeNames = new HashSet<>();

  // enrichment chain length management
  final Map<String, Integer> attributeChainLengths = new HashMap<>();

  // foreign key
  private List<String> foreignKey;
  private List<AttributeDesc> foreignKeyAttributes;

  // primary keys of remote contexts
  private Map<String, Set<String>> primaryKeys = new HashMap<>();

  //
  private MissingAttributePolicyV1 preProcessPolicy;

  /**
   * Creates a validator instance.
   *
   * <p>The constructor sets up internal objects necessary for validation but does not run any
   * validation. So exceptions would not occur during the construction.
   *
   * @param admin AdminInternal component
   * @param tenantDesc parent tenant
   * @param signalDesc signal to test
   */
  public SignalEnrichmentValidator(AdminImpl admin, TenantDesc tenantDesc, StreamDesc signalDesc) {
    this.admin = admin;
    this.tenantDesc = tenantDesc;
    this.streamDesc = signalDesc;
    signalDesc
        .getAttributes()
        .forEach((attributeDesc) -> allAttributeNames.add(attributeDesc.getName().toLowerCase()));
  }

  public void validate() throws ConstraintViolationException, InvalidDefaultValueException {
    final List<PreprocessDesc> preprocesses = streamDesc.getPreprocesses();
    if (preprocesses == null) {
      return;
    }

    if (streamDesc.getType() != StreamType.SIGNAL) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Preprocess is supported only by stream type " + StreamType.SIGNAL.name(),
                  tenantDesc,
                  streamDesc)
              .toString());
    }

    final MissingAttributePolicyV1 mlp = streamDesc.getMissingLookupPolicy();
    for (var preprocess : preprocesses) {
      validatePreprocess(mlp, preprocess);
    }

    // Set up pre-compiled pre-processing
    List<ProcessStage> stages = new ArrayList<>(processes.values());
    streamDesc.setPreprocessStages(stages);
  }

  private void validatePreprocess(MissingAttributePolicyV1 mlp, PreprocessDesc process)
      throws ConstraintViolationException, InvalidDefaultValueException {
    if (Strings.isNullOrEmpty(process.getName()) || process.getName().isBlank()) {
      throw new ConstraintViolationException(
          createExceptionMessage("Preprocess name must be set", tenantDesc, streamDesc).toString());
    }
    if (preprocessNames.contains(process.getName().toLowerCase())) {
      throw new ConstraintViolationException(
          createExceptionMessage("Preprocess name may not duplicate", tenantDesc, streamDesc)
              .append(", preprocess=")
              .append(process.getName())
              .toString());
    }
    preprocessNames.add(process.getName().toLowerCase());

    preProcessPolicy = process.getMissingLookupPolicy();
    if (preProcessPolicy == null) {
      preProcessPolicy = mlp;
    }
    if (preProcessPolicy == null) {
      final StringBuilder sb =
          createExceptionMessage(
                  "Property 'missingLookupPolicy' is required to configure enrichment.",
                  tenantDesc,
                  streamDesc)
              .append(", preprocess=")
              .append(process.getName());
      throw new ConstraintViolationException(sb.toString());
    }

    int nextChainLength = validateForeignKey(process);
    validateActions(process, nextChainLength);
  }

  private int validateForeignKey(PreprocessDesc process) throws ConstraintViolationException {

    foreignKey =
        process.getCondition() != null ? List.of(process.getCondition()) : process.getForeignKey();

    foreignKeyAttributes =
        getForeignKeyAttributes(tenantDesc, streamDesc, process.getName(), foreignKey);

    // normalize the foreign key
    final var foreignKey =
        foreignKeyAttributes.stream().map((key) -> key.getName()).collect(Collectors.toList());

    // Verify the context join chain length
    int nextChainLength = 0;
    for (var keyComponent : foreignKey) {
      nextChainLength =
          Math.max(
              nextChainLength,
              attributeChainLengths.computeIfAbsent(keyComponent.toLowerCase(), (k) -> 0) + 1);
      final int chainMax = TfosConfig.contextJoinMaximumChainLength();
      if (chainMax > 0) {
        if (nextChainLength > chainMax) {
          StringBuilder sb =
              createExceptionMessage(
                      String.format("Enrichment chain length exceeds allowed maximum %d", chainMax),
                      tenantDesc,
                      streamDesc)
                  .append(", preprocess=")
                  .append(process.getName())
                  .append(", attribute=")
                  .append(keyComponent);
          throw new ConstraintViolationException(sb.toString());
        }
      }
    }

    // (correction) Set back the foreign key
    process.setCondition(null);
    process.setForeignKey(foreignKey);
    return nextChainLength;
  }

  /**
   * This method finds an attribute description in a signal configuration by specified name.
   *
   * <p>The name match is case insensitive. This method throws ConstraintViolationException if no
   * matching attribute is found. So this method never returns null.
   *
   * @param foreignKey Attribute name to find in the context
   * @return Found attribute description
   * @throws ConstraintViolationException When no matching attribute is found.
   */
  protected List<AttributeDesc> getForeignKeyAttributes(
      TenantDesc tenantDesc, StreamDesc signal, String preprocess, List<String> foreignKey)
      throws ConstraintViolationException {

    if (foreignKey == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("Property 'foreignKey' is not set", tenantDesc, signal)
              .append(", preprocess=")
              .append(preprocess)
              .toString());
    }

    final var foreignKeyAttributes = new ArrayList<AttributeDesc>();
    for (int i = 0; i < foreignKey.size(); ++i) {
      final var keyComponent = foreignKey.get(i);
      if (Strings.isNullOrEmpty(keyComponent)) {
        throw new ConstraintViolationException(
            createExceptionMessage(
                    String.format("foreignKey[%d] is not set", i), tenantDesc, signal)
                .append(", preprocess=")
                .append(preprocess)
                .toString());
      }
      AttributeDesc desc = signal.findAttribute(keyComponent);
      if (desc == null) {
        desc = signal.findAdditionalAttribute(keyComponent);
      }
      if (desc != null) {
        foreignKeyAttributes.add(desc);
      } else {
        String message =
            "Foreign key attribute must be one of the signal attributes;"
                + " tenant="
                + tenantDesc.getName()
                + ", signal="
                + signal.getName()
                + ", enrichment="
                + preprocess
                + ", attribute="
                + foreignKey;
        throw new ConstraintViolationException(message);
      }
    }
    return foreignKeyAttributes;
  }

  protected StreamDesc getRemoteContext(
      TenantDesc tenantDesc, StreamDesc signalDesc, String contextName, String preprocessName)
      throws ConstraintViolationException {
    if (Strings.isNullOrEmpty(contextName)) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Merging context name in action may not be empty", tenantDesc, signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .toString());
    }
    if (contextName.equalsIgnoreCase(signalDesc.getName())) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Merging context name in action must be different from the parent stream's name;",
                  tenantDesc,
                  signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .toString());
    }

    final StreamDesc remoteContextDesc = tenantDesc.getDependingStream(contextName, signalDesc);
    if (remoteContextDesc == null) {
      throw new ConstraintViolationException(
          createExceptionMessage("Merging context not found", tenantDesc, signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .append(", context=")
              .append(contextName)
              .toString());
    }
    if (remoteContextDesc.getType() != StreamType.CONTEXT) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  String.format(
                      "Remote context is of wrong type of stream (%s)",
                      remoteContextDesc.getType().name().toLowerCase()),
                  tenantDesc,
                  signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .append(", context=")
              .append(contextName)
              .toString());
    }

    // Joining with a context implies a dependency on that context. Track the new dependency and
    // ensure that this does not cause a cyclic dependency.
    admin.checkCyclesAndAddDependency(tenantDesc, signalDesc, remoteContextDesc, preprocessName);

    // Make all lower-case primary key attribute names. Theis is used for later validation.
    primaryKeys.computeIfAbsent(
        remoteContextDesc.getName(),
        (k) ->
            remoteContextDesc.getPrimaryKey().stream()
                .map((name) -> name.toLowerCase())
                .collect(Collectors.toSet()));

    return remoteContextDesc;
  }

  protected AttributeDesc getEnrichingAttributeFromContext(
      TenantDesc tenantDesc,
      StreamDesc signalDesc,
      String preprocessName,
      StreamDesc remoteContextDesc,
      ActionDesc action)
      throws ConstraintViolationException {
    String attributeName = action.getAttribute();
    if (Strings.isNullOrEmpty(attributeName)) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Enrichment attribute name may not be empty", tenantDesc, signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .toString());
    }
    if (action.getAs() != null && action.getAs().isEmpty()) {
      throw new ConstraintViolationException(
          createExceptionMessage("The 'as' property may not be empty", tenantDesc, signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .append(", attribute=")
              .append(attributeName)
              .toString());
    }
    final Set<String> primaryKeyNames = primaryKeys.get(remoteContextDesc.getName());
    if (primaryKeyNames.contains(attributeName.toLowerCase())) {
      throw new ConstraintViolationException(
          createExceptionMessage(
                  "Enrichment attribute may not be part of the referring context's primary key",
                  tenantDesc,
                  signalDesc)
              .append(", preprocess=")
              .append(preprocessName)
              .append(", attribute=")
              .append(attributeName)
              .append(", context=")
              .append(remoteContextDesc.getName())
              .toString());
    }
    // First look for the attribute in the context's attribute list.
    boolean first = true;
    var attrDesc = remoteContextDesc.findAttribute(attributeName);

    // If not found in attributes list, look in the enrichments of the context.
    if (attrDesc == null) {
      attrDesc = remoteContextDesc.findAdditionalAttribute(attributeName);
    }

    if (attrDesc != null) {
      // align cases to the original
      action.setAttribute(attrDesc.getName());
      return attrDesc;
    }

    // If not found yet, it is an error.
    throw new ConstraintViolationException(
        createExceptionMessage(
                "Enriching attribute name must exist in referring context", tenantDesc, signalDesc)
            .append(", preprocess=")
            .append(preprocessName)
            .append(", attribute=")
            .append(attributeName)
            .append(", context=")
            .append(remoteContextDesc.getName())
            .toString());
  }

  private void validateActions(PreprocessDesc process, int nextChainLength)
      throws ConstraintViolationException, InvalidDefaultValueException {
    final var actions = process.getActions();
    if (actions == null || actions.isEmpty()) {
      StringBuilder sb =
          createExceptionMessage("Preprocess actions may not be empty", tenantDesc, streamDesc)
              .append(", preprocess=")
              .append(process.getName());
      throw new ConstraintViolationException(sb.toString());
    }
    for (ActionDesc action : process.getActions()) {
      if (action.getActionType() == ActionType.MERGE) {
        String contextName = action.getContext();
        StreamDesc remoteContextDesc =
            getRemoteContext(tenantDesc, streamDesc, contextName, process.getName());
        // normalize the context name cases
        action.setContext(remoteContextDesc.getName());
        if (remoteContextDesc.getPrimaryKey().size() != foreignKeyAttributes.size()) {
          StringBuilder sb =
              createExceptionMessage(
                      "Foreign key size and primary key size of the remote context must be the same",
                      tenantDesc,
                      streamDesc)
                  .append(", preprocess=")
                  .append(process.getName())
                  .append(", foreignKey=")
                  .append(foreignKey)
                  .append(", remotePrimaryKey=")
                  .append(remoteContextDesc.getPrimaryKey());
          throw new ConstraintViolationException(sb.toString());
        }
        for (int i = 0; i < foreignKeyAttributes.size(); ++i) {
          final AttributeDesc foreignKeyAttribute = foreignKeyAttributes.get(i);
          final String remotePrimaryKeyName = remoteContextDesc.getPrimaryKey().get(i);
          final AttributeDesc remotePrimaryKeyAttribute =
              remoteContextDesc.findAttribute(remotePrimaryKeyName);
          final String mismatch = remotePrimaryKeyAttribute.checkMismatch(foreignKeyAttribute);
          if (mismatch != null) {
            StringBuilder sb =
                createExceptionMessage(
                        String.format(
                            "Type of foreignKey[%d] must match the type of the context's primary key",
                            i),
                        tenantDesc,
                        streamDesc)
                    .append(", enrichment=")
                    .append(process.getName())
                    .append(", foreignKey=")
                    .append(foreignKeyAttribute.getName())
                    .append(", context=")
                    .append(remoteContextDesc.getName())
                    .append(", primaryKey=")
                    .append(remotePrimaryKeyName)
                    .append(", mismatch=")
                    .append(mismatch);
            throw new ConstraintViolationException(sb.toString());
          }

          if (foreignKeyAttribute.getMissingValuePolicy() == MissingAttributePolicyV1.USE_DEFAULT) {
            final InternalAttributeType condAttrType = foreignKeyAttribute.getAttributeType();
            final Object def = foreignKeyAttribute.getInternalDefaultValue();
            if ((condAttrType == STRING && ((String) def).isEmpty())
                || (condAttrType == BLOB && ((ByteBuffer) def).remaining() == 0)) {
              StringBuilder sb =
                  createExceptionMessage(
                          String.format(
                              "Default value of %s type foreignKey[%d] may not be empty",
                              condAttrType.name().toLowerCase(), i),
                          tenantDesc,
                          streamDesc)
                      .append(", preprocess=")
                      .append(process.getName())
                      .append(", attribute=")
                      .append(foreignKeyAttribute.getName())
                      .append(", context=")
                      .append(remoteContextDesc.getName());
              throw new ConstraintViolationException(sb.toString());
            }
          }
        }

        String attribute = action.getAttribute();
        AttributeDesc enrichingAttribute =
            getEnrichingAttributeFromContext(
                    tenantDesc, streamDesc, process.getName(), remoteContextDesc, action)
                .duplicate();
        // If an alias was provided for this merging attribute, update its name.
        String enrichingAttributeName = enrichingAttribute.getName();
        if (action.getAs() != null) {
          enrichingAttributeName = action.getAs();
          enrichingAttribute.setName(enrichingAttributeName);
        }
        // Align cases of the attribute name to simplify further operation. Original name in
        // action may have different cases than the context attribute. This is especially
        // relevant to MergeProcess operation that is invoked on ingestion.
        // The function looks up
        // context attributes against attribute map by name.

        if (allAttributeNames.contains(enrichingAttributeName.toLowerCase())) {
          throw new ConstraintViolationException(
              createExceptionMessage(
                      "Enriched attribute name must not conflict with other attributes",
                      tenantDesc,
                      streamDesc)
                  .append(", attribute=")
                  .append(enrichingAttributeName)
                  .toString());
        }

        // Add the new entry for this attribute to the depth tracker
        attributeChainLengths.put(enrichingAttributeName.toLowerCase(), nextChainLength);

        final MissingAttributePolicyV1 policy =
            action.getMissingLookupPolicy() != null
                ? action.getMissingLookupPolicy()
                : preProcessPolicy;
        if (policy == null) {
          throw new ConstraintViolationException(
              "Every merging attribute must have 'missingLookupPolicy' property value: "
                  + attribute);
        }
        if (policy == MissingAttributePolicyV1.USE_DEFAULT && action.getDefaultValue() == null) {
          throw new ConstraintViolationException(
              "Enrichment attribute with missingLookupPolicy 'StoreFillInValue' must have "
                  + "'default' property; attribute="
                  + attribute);
        }
        action.setMissingLookupPolicy(policy);
        // Convert default value in actionDesc to proper type. The default value
        // may be specified by a string, but we want to hold it in action with defined type.
        if (action.getDefaultValue() != null) {
          if (enrichingAttribute.getAttributeType() == ENUM) {
            String error = null;
            if (action.getDefaultValue() instanceof String) {
              final String normalized = ((String) action.getDefaultValue());
              if (normalized.isEmpty()) {
                error = "Enum default value may not be an empty string";
              }
            } else {
              error = "Enum default value must be a string";
            }
            if (error != null) {
              throw new InvalidDefaultValueException(
                  createExceptionMessage(error, tenantDesc, streamDesc)
                      .append(", preprocess=")
                      .append(process.getName())
                      .append(", attribute=")
                      .append(enrichingAttribute.getName())
                      .append(", value=")
                      .append(action.getDefaultValue())
                      .toString());
            }
          }
          try {
            final Object defaultValue =
                Attributes.parseDefaultValue(action.getDefaultValue(), enrichingAttribute);
            action.setInternalDefaultValue(defaultValue);
            if (Set.of(LONG, DOUBLE, BOOLEAN).contains(enrichingAttribute.getAttributeType())) {
              action.setDefaultValue(defaultValue);
            }
          } catch (InvalidValueSyntaxException e) {
            String message =
                e.getMessage()
                    + ", tenant="
                    + tenantDesc.getName()
                    + ", stream="
                    + streamDesc.getName()
                    + ", preprocess="
                    + process.getName();
            throw new InvalidDefaultValueException(message);
          }
        }

        allAttributeNames.add(enrichingAttributeName.toLowerCase());
        enrichingAttribute.setDefaultValue(action.getDefaultValue());
        enrichingAttribute.setInternalDefaultValue(action.getInternalDefaultValue());

        String key = (remoteContextDesc.getName() + "." + foreignKey).toLowerCase();
        ProcessStage stage = processes.get(key);
        if (stage == null) {
          stage = new ProcessStage(key, new JoinProcessor(foreignKeyAttributes, remoteContextDesc));
          processes.put(key, stage);
        }
        ((JoinProcessor) stage.getProcess()).addAction(action);

        if (streamDesc.getAdditionalAttributes() == null
            || streamDesc.getAdditionalAttributes().stream()
                .noneMatch(attr -> enrichingAttribute.getName().equalsIgnoreCase(attr.getName()))) {
          streamDesc.addAdditionalAttribute(enrichingAttribute);
        }
      }
    }
  }
}
