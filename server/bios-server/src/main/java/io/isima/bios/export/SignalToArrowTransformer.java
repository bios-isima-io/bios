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
package io.isima.bios.export;

import static io.isima.bios.export.Vectorizer.BlobVectorizer;
import static io.isima.bios.export.Vectorizer.BooleanVectorizer;
import static io.isima.bios.export.Vectorizer.DoubleVectorizer;
import static io.isima.bios.export.Vectorizer.LongVectorizer;
import static io.isima.bios.export.Vectorizer.StringVectorizer;
import static io.isima.bios.export.Vectorizer.TimeStampVectorizer;
import static io.isima.bios.export.Vectorizer.UUIDVectorizer;
import static io.isima.bios.export.Vectorizer.UnknownVectorizer;
import static io.isima.bios.models.v1.InternalAttributeType.BLOB;
import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.DOUBLE;
import static io.isima.bios.models.v1.InternalAttributeType.LONG;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;
import static io.isima.bios.models.v1.InternalAttributeType.TIMESTAMP;
import static io.isima.bios.models.v1.InternalAttributeType.UUID;

import com.google.common.collect.ImmutableList;
import io.isima.bios.models.Event;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalToArrowTransformer {
  private static final Logger logger = LoggerFactory.getLogger(SignalToArrowTransformer.class);
  private static final String EVENT_ID = "eventId";
  private static final String EVENT_TIMESTAMP = "eventTimeStamp";

  private static final Vectorizer<?> EMPTY_VECTORIZER = new UnknownVectorizer();
  private static final Map<InternalAttributeType, Supplier<ArrowType>> bios2ArrowTypeMap;

  static {
    bios2ArrowTypeMap =
        Map.of(
            LONG, Types.MinorType.BIGINT::getType,
            DOUBLE, Types.MinorType.FLOAT8::getType,
            BOOLEAN, Types.MinorType.BIT::getType,
            STRING, Types.MinorType.VARCHAR::getType,
            BLOB, Types.MinorType.VARBINARY::getType,
            TIMESTAMP, Types.MinorType.TIMESTAMPMILLI::getType,
            UUID, () -> new ArrowType.FixedSizeBinary(16));
  }

  private final VectorSchemaRoot schemaRoot;
  private final Map<String, Vectorizer<?>> vectorizerMap;
  private final SchemaMapping schemaMapping;
  private final ArrowDataPipe dataPipe;

  public SignalToArrowTransformer(StreamConfig signalConfig) {
    Map<String, InternalAttributeType> allAttributes = getAllRawAttributes(signalConfig);
    allAttributes.put(EVENT_ID, UUID);
    allAttributes.put(EVENT_TIMESTAMP, TIMESTAMP);
    schemaRoot = buildSchemaRoot(allAttributes);
    vectorizerMap = buildVectorizers(allAttributes);
    this.schemaMapping = SchemaConverter.fromArrow(schemaRoot);
    this.dataPipe = new ArrowDataPipe(schemaMapping);
  }

  public ArrowDataPipe getDataPipe() {
    return dataPipe;
  }

  private Map<String, InternalAttributeType> getAllRawAttributes(StreamConfig signalConfig) {
    Map<String, InternalAttributeType> rawAttrMap = new HashMap<>();
    signalConfig.getAttributes().forEach((a) -> rawAttrMap.put(a.getName(), a.getAttributeType()));
    final var additional = signalConfig.getAdditionalAttributes();
    if (additional != null && !additional.isEmpty()) {
      additional.forEach((a) -> rawAttrMap.put(a.getName(), a.getAttributeType()));
    }
    return rawAttrMap;
  }

  private VectorSchemaRoot buildSchemaRoot(Map<String, InternalAttributeType> allAttributes) {
    ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
    allAttributes.forEach((key, value) -> fieldBuilder.add(descToField(key, value)));
    Schema signalSchema = new Schema(fieldBuilder.build(), null);
    RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    return VectorSchemaRoot.create(signalSchema, allocator);
  }

  private Map<String, Vectorizer<?>> buildVectorizers(
      Map<String, InternalAttributeType> allAttributes) {
    return allAttributes.entrySet().stream()
        .map(
            (e) ->
                new AbstractMap.SimpleEntry<>(
                    e.getKey(), buildVectorizer(schemaRoot.getVector(e.getKey()), e.getValue())))
        .collect(
            Collectors.toUnmodifiableMap(
                AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
  }

  public void transformEvents(List<Event> events) {
    if (events == null || events.isEmpty()) {
      return;
    }
    vectorizerMap.values().forEach((v) -> v.preAllocate(events.size()));
    int index = 0;
    long firstTimeStamp = 0L;
    for (var event : events) {
      if (firstTimeStamp == 0L) {
        firstTimeStamp = event.getIngestTimestamp().getTime();
      }
      vectorizeEvent(index++, event);
    }
    schemaRoot.setRowCount(events.size());
    dataPipe.addToBatch(schemaRoot, firstTimeStamp);
  }

  private Field descToField(String attributeName, InternalAttributeType type) {
    final var arrowType = bios2ArrowTypeMap.get(type);
    if (arrowType == null) {
      throw new RuntimeException("Unexpected old tfos type in signal");
    }
    return new Field(attributeName, FieldType.nullable(arrowType.get()), null);
  }

  private Vectorizer<?> buildVectorizer(FieldVector fv, InternalAttributeType attributeType) {
    switch (attributeType) {
      case LONG:
        return new LongVectorizer((BigIntVector) fv);

      case DOUBLE:
        return new DoubleVectorizer((Float8Vector) fv);

      case BOOLEAN:
        return new BooleanVectorizer((BitVector) fv);

      case STRING:
      case ENUM:
        return new StringVectorizer((VarCharVector) fv);

      case BLOB:
        return new BlobVectorizer((VarBinaryVector) fv);

      case TIMESTAMP:
        return new TimeStampVectorizer((TimeStampMilliVector) fv);

      case UUID:
        return new UUIDVectorizer((FixedSizeBinaryVector) fv);

      default:
        return EMPTY_VECTORIZER;
    }
  }

  private void vectorizeEvent(int index, Event event) {
    for (final var vector : schemaRoot.getFieldVectors()) {
      final var name = vector.getField().getName();
      Vectorizer<?> v = vectorizerMap.getOrDefault(name, EMPTY_VECTORIZER);
      typeSafeVectorize(v, index, vector, event);
    }
  }

  private <T> void typeSafeVectorize(Vectorizer<T> v, int index, FieldVector vector, Event event) {
    final var name = vector.getField().getName();
    final Object item = event.get(name);
    if (item != null && v.getType().isAssignableFrom(item.getClass())) {
      @SuppressWarnings("unchecked")
      final T val = (T) item;

      v.vectorize(index, val);
      return;
    }
    if (v instanceof UUIDVectorizer && name.equalsIgnoreCase(EVENT_ID)) {
      ((UUIDVectorizer) v).vectorize(index, event.getEventId());
      return;
    }
    if (v instanceof TimeStampVectorizer && name.equalsIgnoreCase(EVENT_TIMESTAMP)) {
      ((TimeStampVectorizer) v).vectorize(index, event.getIngestTimestamp().getTime());
      return;
    } else {
      logger.warn(
          "Unknown and unexpected arrow field type. Unable to vectorize  {}",
          vector.getField().getName());
    }
  }
}
