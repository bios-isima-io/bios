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
package io.isima.bios.service.handler;

import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import com.fasterxml.uuid.Generators;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.common.ContextQueryValidator;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.ReplaceContextAttributesSpec;
import io.isima.bios.data.UpdateContextEntrySpec;
import io.isima.bios.dto.MultiContextEntriesSpec;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.dto.ReplaceContextAttributesRequest;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.UpdateContextEntryRequest;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextEntryRecord;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ObjectListEventValue;
import io.isima.bios.models.isql.DerivedQueryComponents;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.preprocess.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that provides utility methods for Data Service. */
public class DataServiceUtils {
  private static final Logger logger = LoggerFactory.getLogger(DataServiceUtils.class);
  private static final int MAX_ALLOWED_QUERIES = 64;

  private static final String INSERT_BULK = "INSERT_BULK";
  private static final String SELECT_CONTEXT = "SELECT_CONTEXT";

  /**
   * Cleanse client metrics attributes.
   *
   * <p>The method replaces the attribute 'stream' by the original properly-capitalized name if
   * specified. Also, values 'INSERT_BULK' and 'CONTEXT_SELECT' in attribute 'request' are renamed
   * to 'INSERT' and 'SELECT' for each if the inserting signal is '_query'.
   *
   * @param metricsSignalDesc Description of the metrics signal where the event goes
   * @param tfosAdmin AdminInternal module
   * @param event Client metrics event to insert
   */
  public static void cleanseClientMetricsData(
      StreamDesc metricsSignalDesc, AdminInternal tfosAdmin, Event event) {
    final boolean isClientMetrics = metricsSignalDesc.isClientMetrics();
    final boolean isAllClientMetrics = metricsSignalDesc.isAllClientMetrics();

    if (!isClientMetrics) {
      // do nothing if the signal is not for client metrics
      return;
    }

    final String tenantName;
    if (isAllClientMetrics) {
      tenantName = (String) event.get("tenant");
    } else {
      tenantName = metricsSignalDesc.getParent().getName();
      var request = event.get("request");
      if (INSERT_BULK.equals(request)) {
        request = "INSERT";
      } else if (SELECT_CONTEXT.equals(request)) {
        request = "SELECT";
      }
      event.set("request", request);
    }

    var streamName = (String) event.get("stream");
    if (tenantName != null && streamName != null && !streamName.isBlank()) {
      try {
        final var streamDesc = tfosAdmin.getStream(tenantName, streamName);
        streamName = streamDesc.getName();
        event.set("stream", streamName);
      } catch (NoSuchTenantException | NoSuchStreamException e) {
        // It's OK. This may be a metrics report for a non-existing stream (where errors were count)
      }
    }
  }

  public static void validateNumMultiSelectQueries(String tenant, int numQueries)
      throws TfosException {
    if (numQueries <= 0 || numQueries > MAX_ALLOWED_QUERIES) {
      String errorMessage =
          numQueries <= 0
              ? "Select query list cannot be empty"
              : "Number of queries may not exceed " + MAX_ALLOWED_QUERIES;

      throw new TfosException(GenericError.INVALID_REQUEST, errorMessage);
    }
  }

  /**
   * Validates a select statement.
   *
   * @param validator The query validator
   * @param statement Query statement
   * @param index Index of multi-select
   * @param derived Derived query components
   * @throws TfosException thrown to indicate validation errors
   */
  public static void validateQuery(
      QueryValidator validator,
      SelectStatement statement,
      int index,
      DerivedQueryComponents derived)
      throws TfosException {
    try {
      validator.validate(statement, index, derived);
    } catch (ConstraintViolationValidatorException e) {
      throw new ConstraintViolationException(e.getMessage(), e);
    } catch (InvalidValueValidatorException e) {
      throw new InvalidValueException(e.getMessage(), e);
    } catch (NotImplementedValidatorException e) {
      throw new NotImplementedException(e.getMessage(), e);
    }
  }

  /**
   * Validates a context select statement.
   *
   * @param contextDesc context descriptor
   * @param request The select context request
   * @throws TfosException thrown to indicate validation errors
   */
  public static void validateQuery(
      StreamDesc contextDesc, SelectContextRequest request, ContextQueryState queryState)
      throws TfosException, ApplicationException {
    ContextQueryValidator validator = new ContextQueryValidator(contextDesc);
    try {
      validator.validate(request, queryState);
    } catch (InvalidValueValidatorException e) {
      throw new InvalidValueException(e.getMessage(), e);
    } catch (ConstraintViolationValidatorException e) {
      throw new ConstraintViolationException(e.getMessage(), e);
    } catch (NotImplementedValidatorException e) {
      throw new NotImplementedException(e.getMessage(), e);
    }
  }

  /**
   * Builds context entries from a PutContextEntriesRequest.
   *
   * @param contextDesc Context description
   * @param request Request to handle
   * @param timestamp Upsert timestamp
   * @return List of built context entries
   * @throws TfosException
   */
  public static List<Event> buildContextEntries(
      StreamDesc contextDesc, PutContextEntriesRequest request, Long timestamp)
      throws TfosException {

    if (request.getContentRepresentation() != ContentRepresentation.CSV) {
      throw new NotImplementedException(
          "PutContext supports only ContentRepresentation " + ContentRepresentation.CSV.name());
    }
    final List<Event> entries = new ArrayList<>();
    final var partitionKeyType = contextDesc.getAttributes().get(0).getAttributeType();
    final String partitionKeyName = contextDesc.getAttributes().get(0).getName();
    for (String entryText : request.getEntries()) {
      Event entry = Utils.createContextEvent(timestamp, entryText, contextDesc, false);
      // entry sanity check
      validateIncomingEvent(contextDesc, entry);
      final Object keyValue = entry.getAttributes().get(partitionKeyName);
      if (partitionKeyType == STRING) {
        if (keyValue.toString().isEmpty()) {
          throw new InvalidValueException(
              "First primary key element of type String may not be empty");
        }
      } else if (partitionKeyType == InternalAttributeType.BLOB) {
        if (!((ByteBuffer) keyValue).hasRemaining()) {
          throw new InvalidValueException(
              "First primary key element of type Blob may not be empty");
        }
      }
      entries.add(entry);
    }
    return entries;
  }

  public static List<List<Object>> buildContextPrimaryKeys(
      StreamDesc contextDesc, MultiContextEntriesSpec contextEntriesSpec) throws TfosException {
    if (contextEntriesSpec.getContentRepresentation() != ContentRepresentation.UNTYPED) {
      throw new NotImplementedException(
          "GetContext supports only ContentRepresentation " + ContentRepresentation.UNTYPED.name());
    }
    final List<AttributeDesc> primaryKeyDescs = contextDesc.getPrimaryKeyAttributes();
    final List<List<Object>> primaryKeys = contextEntriesSpec.getPrimaryKeys();
    if (primaryKeys == null) {
      throw new InvalidRequestException("Request property 'primaryKeys' is missing");
    }
    List<List<Object>> convertedKeys = new ArrayList<>();
    for (int i = 0; i < primaryKeys.size(); ++i) {
      List<Object> primaryKey = primaryKeys.get(i);
      final List<Object> convertedKey = buildPrimaryKey(primaryKey, primaryKeyDescs, i);
      convertedKeys.add(convertedKey);
    }
    return convertedKeys;
  }

  private static List<Object> buildPrimaryKey(
      List<Object> primaryKey, List<AttributeDesc> primaryKeyDescs, int index)
      throws TfosException {
    if (!isValidPrimaryKey(primaryKey)) {
      throw new InvalidRequestException("%s is null", makePrimaryKeyName(index));
    }
    if (primaryKey.size() != primaryKeyDescs.size()) {
      throw new InvalidRequestException(
          "Size of %s is incorrect (should be %d but %d)",
          makePrimaryKeyName(index), primaryKeyDescs.size(), primaryKey.size());
    }
    final var values = new Object[primaryKey.size()];
    for (int x = 0; x < values.length; ++x) {
      final var keyElement = primaryKey.get(x);
      if (keyElement == null) {
        throw new InvalidRequestException("%s element includes null", makePrimaryKeyName(index));
      }
      final var keyDesc = primaryKeyDescs.get(x);
      if (keyDesc.getAttributeType() == STRING && keyElement.toString().isEmpty()) {
        throw new InvalidRequestException(
            "%s: String primary key element must not be empty", makePrimaryKeyName(index));
      }
      values[x] = Attributes.convertValue(keyElement.toString(), keyDesc);
    }
    return Arrays.asList(values);
  }

  private static String makePrimaryKeyName(int index) {
    return index < 0 ? "Primary key" : String.format("primaryKeys[%d]", index);
  }

  /**
   * Checks whether a composite primary key is valid.
   *
   * <p>The method checks if the key is null, empty, or contains null. In such a case, the key is
   * considered to be invalid.
   *
   * @param primaryKey A composite primary key
   * @return whether the key is valid
   */
  public static boolean isValidPrimaryKey(List<Object> primaryKey) {
    if (primaryKey == null || primaryKey.isEmpty()) {
      return false;
    }
    for (var element : primaryKey) {
      if (element == null) {
        return false;
      }
    }
    return true;
  }

  public static List<ContextEntryRecord> keysToContextRecords(List<List<Object>> keys) {
    final var records = new ArrayList<ContextEntryRecord>();
    for (List<Object> key : keys) {
      final var record = new ContextEntryRecord();
      record.setAttributes(key);
      records.add(record);
    }
    return records;
  }

  public static List<ContextEntryRecord> countToContextRecord(long count) {
    final var record = new ContextEntryRecord();
    final List<Object> attributes = List.of(count);
    record.setAttributes(attributes);
    return List.of(record);
  }

  public static List<ContextEntryRecord> entriesToContextRecords(
      StreamDesc contextDesc, List<Event> entries) {
    final var records = new ArrayList<ContextEntryRecord>();
    for (Event entry : entries) {
      if (entry == null) {
        continue;
      }
      final var record = new ContextEntryRecord();
      record.setTimestamp(entry.getIngestTimestamp().getTime());

      if (entry instanceof ObjectListEventValue.ObjectListEvent) {
        record.setAttributes(((ObjectListEventValue.ObjectListEvent) entry).getValueList());
      } else {
        int size = contextDesc.getAttributes().size();
        if (contextDesc.getAdditionalAttributes() != null) {
          size += contextDesc.getAdditionalAttributes().size();
        }
        final var attributes = new ArrayList<>(size);
        for (var attr : contextDesc.getAttributes()) {
          attributes.add(entry.get(attr.getName()));
        }
        if (contextDesc.getAdditionalAttributes() != null) {
          for (var attr : contextDesc.getAdditionalAttributes()) {
            attributes.add(entry.get(attr.getName()));
          }
        }
        record.setAttributes(attributes);
      }

      records.add(record);
    }
    return records;
  }

  public static UpdateContextEntrySpec buildContextEntryUpdateSpec(
      StreamDesc contextDesc, UpdateContextEntryRequest request) throws TfosException {
    if (request.getPrimaryKey() == null || request.getPrimaryKey().isEmpty()) {
      throw new InvalidRequestException("Primary key must be set");
    }

    final var requestedKey = request.getPrimaryKey();
    final var key = buildPrimaryKey(requestedKey, contextDesc.getPrimaryKeyAttributes(), -1);

    if (request.getAttributes() == null) {
      throw new InvalidRequestException("Attributes must be set");
    }

    Map<String, Object> newAttributes = new HashMap<>();
    for (var attribute : request.getAttributes()) {
      if (attribute == null) {
        throw new InvalidRequestException("Attributes must not include null");
      }
      final String attributeName = attribute.getName();
      final Object attributeValue = attribute.getValue();
      final AttributeDesc attributeDesc = findAttribute(contextDesc, attributeName);
      newAttributes.put(
          attributeName, Attributes.convertValue(attributeValue.toString(), attributeDesc));
    }

    return new UpdateContextEntrySpec(key, newAttributes);
  }

  public static ReplaceContextAttributesSpec buildContextAttributeReplacementSpec(
      StreamDesc contextDesc, ReplaceContextAttributesRequest request)
      throws InvalidRequestException, InvalidEnumException, InvalidValueSyntaxException {
    final String attributeName = request.getAttributeName();
    if (attributeName == null) {
      throw new InvalidRequestException("Request property 'attributeName' must be set");
    }

    AttributeDesc attrDesc = findAttribute(contextDesc, attributeName);

    final Object oldValueSrc = request.getOldValue();
    if (oldValueSrc == null) {
      throw new InvalidRequestException("Request property 'oldValue' must be set");
    }
    final Object newValueSrc = request.getNewValue();
    if (newValueSrc == null) {
      throw new InvalidRequestException("Request property 'newValue' must be set");
    }
    final Object oldValue = Attributes.convertValue(oldValueSrc.toString(), attrDesc);
    final Object newValue = Attributes.convertValue(newValueSrc.toString(), attrDesc);

    return new ReplaceContextAttributesSpec(attrDesc.getName(), oldValue, newValue);
  }

  private static AttributeDesc findAttribute(StreamDesc contextDesc, String attributeName)
      throws InvalidRequestException {
    for (int i = 0; i < contextDesc.getAttributes().size(); ++i) {
      AttributeDesc d = contextDesc.getAttributes().get(i);
      if (d.getName().equalsIgnoreCase(attributeName)) {
        if (i == 0) {
          throw new InvalidRequestException("Primary key may not be modified; " + attributeName);
        }
        return d;
      }
    }
    throw new InvalidRequestException("No such attribute: " + attributeName);
  }

  /**
   * Inserts operation failure events based on the given throwable.
   *
   * @param tenantName Name of the tenant where the operation failed
   * @param streamName Name of the stream where the operation failed
   * @param request Reuqest name
   * @param appType Client application type
   * @param appName Client application name
   * @param throwable Occurred error
   * @param dataEngine Data Engine to be used for insertion
   * @param tfosAdmin AdminInternal to be used for getting the latest failure signals
   */
  public static void insertOperationFailureEvents(
      String tenantName,
      String streamName,
      String request,
      String appType,
      String appName,
      Throwable throwable,
      DataEngine dataEngine,
      AdminInternal tfosAdmin,
      ExecutionState state) {
    try {
      final var event = new EventJson();
      final var allEvent = new EventJson();
      event.setEventId(Generators.timeBasedGenerator().generate());
      allEvent.setEventId(Generators.timeBasedGenerator().generate());

      long timestamp = System.currentTimeMillis();
      event.set("timestamp", timestamp);
      allEvent.set("timestamp", timestamp);
      event.setIngestTimestamp(new Date(timestamp));
      allEvent.setIngestTimestamp(new Date(timestamp));

      var tenantToReport = tenantName;
      if (state.isDelegate() && state.getUserContext() != null) {
        tenantToReport = state.getUserContext().getTenant();
      }
      allEvent.set("tenant", tenantToReport);

      event.set("stream", streamName);
      allEvent.set("stream", streamName);

      event.set("request", request);
      allEvent.set("request", request);

      Throwable toRecord = throwable;
      while (toRecord instanceof ExecutionException
          || (toRecord instanceof RuntimeException && toRecord.getCause() != null)) {
        toRecord = toRecord.getCause();
      }
      if (toRecord instanceof TfosException) {
        final var ex = (TfosException) toRecord;
        event.set("errorName", ex.getErrorName());
        allEvent.set("errorName", ex.getErrorName());

        event.set("errorMessage", ex.getErrorMessage());
        allEvent.set("errorMessage", ex.getErrorMessage());
      } else {
        event.set("errorName", "INTERNAL_SERVER_ERROR");
        allEvent.set("errorName", "INTERNAL_SERVER_ERROR");

        event.set("errorMessage", "");
        allEvent.set("errorMessage", throwable.toString());
      }

      event.set("appType", appType);
      allEvent.set("appType", appType);

      event.set("appName", appName);
      allEvent.set("appName", appName);

      final var allOpFailureStreamDesc =
          tfosAdmin.getStream(
              BiosConstants.TENANT_SYSTEM, BiosConstants.STREAM_ALL_OPERATION_FAILURE);

      StreamDesc opFailureStreamDesc = null;
      if (!tenantToReport.equals(BiosConstants.TENANT_SYSTEM)) {
        opFailureStreamDesc =
            tfosAdmin.getStreamOrNull(tenantToReport, BiosConstants.STREAM_OPERATION_FAILURE);
      }

      final var executor = state.getExecutor();
      final var stateForAll = new GenericExecutionState("InsertOperationFailureEvents", executor);
      dataEngine.insertEventIgnoreErrors(allOpFailureStreamDesc, allEvent, stateForAll);
      if (opFailureStreamDesc != null) {
        final var stateForTenant =
            new GenericExecutionState("InsertOperationFailureEvents", executor);
        dataEngine.insertEventIgnoreErrors(opFailureStreamDesc, event, stateForTenant);
      }

    } catch (Throwable tt) {
      logger.error("Error happened while inserting an operationFailure record", tt);
    }
  }

  /**
   * Checks the size of an incoming event.
   *
   * <p>The method throws {@link InvalidValueException} if total size of string attributes exceeds
   * 32 KB
   *
   * @param streamDesc Stream description for the event
   * @param event The event to validate
   * @throws InvalidValueException thrown to incidate that the event is malformed
   */
  public static void validateIncomingEvent(StreamDesc streamDesc, Event event)
      throws InvalidValueException {
    final int maxAttributeLength = 32768;
    long totalAttributeLength = 0;
    for (var attributeDesc : streamDesc.getAttributes()) {
      final int length;
      if (attributeDesc.getAttributeType() == STRING) {
        final String value = (String) event.get(attributeDesc.getName());
        length = value.length();
      } else if (attributeDesc.getAttributeType() == InternalAttributeType.BLOB) {
        final ByteBuffer value = (ByteBuffer) event.get(attributeDesc.getName());
        length = value.remaining();
      } else {
        length = 0;
      }
      if (length >= maxAttributeLength) {
        // Throw an exception immediately to inform the user which attribute is malformed
        final var signalOrContext =
            streamDesc.getType() == StreamType.SIGNAL ? "signal" : "context";
        throw new InvalidValueException(
            String.format(
                "Attribute too long; length=%d, attribute=%s, %s=%s, tenant=%s",
                length,
                attributeDesc.getName(),
                signalOrContext,
                streamDesc.getName(),
                streamDesc.getParent().getName()));
      }
      totalAttributeLength += length;
    }
    if (totalAttributeLength >= maxAttributeLength) {
      final var signalOrContext = streamDesc.getType() == StreamType.SIGNAL ? "signal" : "context";
      throw new InvalidValueException(
          String.format(
              "Total attribute length too long; totalLength=%d, %s=%s, tenant=%s",
              totalAttributeLength,
              signalOrContext,
              streamDesc.getName(),
              streamDesc.getParent().getName()));
    }
  }
}
