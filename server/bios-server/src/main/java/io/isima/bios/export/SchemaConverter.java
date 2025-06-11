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

import static org.apache.parquet.schema.OriginalType.INT_64;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import io.isima.bios.export.SchemaMapping.TypeMapping;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class SchemaConverter {

  public static SchemaMapping fromArrow(VectorSchemaRoot schemaRoot) {
    final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    final var schema = schemaRoot.getSchema();
    final var readableSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    List<TypeMapping> mappedAttributes = convertFromArrow(schema.getFields());
    MessageType parquetSchema = convertToParquet(mappedAttributes);
    return new SchemaMapping(readableSchemaRoot, parquetSchema, mappedAttributes);
  }

  private static List<TypeMapping> convertFromArrow(List<Field> fields) {
    // currently there are only primitive types
    List<TypeMapping> result = new ArrayList<>(fields.size());
    for (final var field : fields) {
      result.add(convertFieldFromArrow(field));
    }
    return result;
  }

  private static TypeMapping convertFieldFromArrow(Field field) {
    return field.getType().accept(new PrimitiveOnlyVisitor(field));
  }

  private static MessageType convertToParquet(List<TypeMapping> mappedAttributes) {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (final var type : mappedAttributes) {
      builder.addField(type.getParquetType());
    }
    return builder.named("root");
  }

  private static final class PrimitiveOnlyVisitor implements ArrowTypeVisitor<TypeMapping> {
    private final Field arrowType;
    private final String attrName;

    PrimitiveOnlyVisitor(Field field) {
      this.arrowType = field;
      this.attrName = field.getName();
    }

    @Override
    public TypeMapping visit(ArrowType.Null a) {
      return primitiveMapping(BINARY);
    }

    @Override
    public TypeMapping visit(ArrowType.Struct struct) {
      throw new IllegalStateException("Unexpected type. Composite types not supported yet");
    }

    @Override
    public TypeMapping visit(ArrowType.List list) {
      throw new IllegalStateException("Unexpected type. List not supported yet");
    }

    @Override
    public TypeMapping visit(ArrowType.LargeList largeList) {
      throw new IllegalStateException("Unexpected type. List not supported yet");
    }

    @Override
    public TypeMapping visit(ArrowType.FixedSizeList fixedSizeList) {
      throw new IllegalStateException("Unexpected type. List not supported yet");
    }

    @Override
    public TypeMapping visit(ArrowType.Union union) {
      throw new IllegalStateException("Unexpected type. Composite types not supported yet");
    }

    @Override
    public TypeMapping visit(ArrowType.Map map) {
      throw new IllegalStateException("Unexpected type. Map type not supported yet");
    }

    @Override
    public TypeMapping visit(ArrowType.Int anInt) {
      if (anInt.getBitWidth() == 64) {
        return primitiveMapping(INT64, INT_64);
      }
      throw new IllegalStateException("Unexpected type. Only 64 bit integers supported");
    }

    @Override
    public TypeMapping visit(ArrowType.FloatingPoint floatingPoint) {
      if (floatingPoint.getPrecision().equals(FloatingPointPrecision.DOUBLE)) {
        return primitiveMapping(DOUBLE);
      }
      throw new IllegalStateException(
          "Unexpected type. Only double precision supported " + floatingPoint.getPrecision());
    }

    @Override
    public TypeMapping visit(ArrowType.Utf8 utf8) {
      return primitiveMapping(BINARY, OriginalType.UTF8);
    }

    @Override
    public TypeMapping visit(ArrowType.LargeUtf8 largeUtf8) {
      throw new IllegalStateException("Unexpected type. Only double precision supported");
    }

    @Override
    public TypeMapping visit(ArrowType.Binary binary) {
      return primitiveMapping(BINARY);
    }

    @Override
    public TypeMapping visit(ArrowType.LargeBinary largeBinary) {
      return primitiveMapping(BINARY);
    }

    @Override
    public TypeMapping visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
      return primitiveMapping(FIXED_LEN_BYTE_ARRAY, fixedSizeBinary.getByteWidth());
    }

    @Override
    public TypeMapping visit(ArrowType.Bool bool) {
      return primitiveMapping(BOOLEAN);
    }

    @Override
    public TypeMapping visit(ArrowType.Decimal decimal) {
      throw new IllegalStateException("Unexpected type. Only double precision supported");
    }

    @Override
    public TypeMapping visit(ArrowType.Date date) {
      throw new IllegalStateException("Unexpected type. Date type not supported");
    }

    @Override
    public TypeMapping visit(ArrowType.Time time) {
      throw new IllegalStateException("Unexpected type. Time type not supported");
    }

    @Override
    public TypeMapping visit(ArrowType.Timestamp timestamp) {
      return primitiveMapping(INT64, TIMESTAMP_MILLIS);
    }

    @Override
    public TypeMapping visit(ArrowType.Interval interval) {
      throw new IllegalStateException("Unexpected type. Interval type not supported");
    }

    @Override
    public TypeMapping visit(ArrowType.Duration duration) {
      throw new IllegalStateException("Unexpected type. Duration type not supported");
    }

    private TypeMapping primitiveMapping(PrimitiveTypeName typeName) {
      Type parquetType = Types.optional(typeName).named(attrName);
      return new SchemaMapping.PrimitiveTypeMapping(arrowType, parquetType);
    }

    private TypeMapping primitiveMapping(PrimitiveTypeName typeName, OriginalType otype) {
      Type parquetType = Types.optional(typeName).as(otype).named(attrName);
      return new SchemaMapping.PrimitiveTypeMapping(arrowType, parquetType);
    }

    private TypeMapping primitiveMapping(PrimitiveTypeName typeName, int width) {
      Type parquetType = Types.optional(typeName).length(width).named(attrName);
      return new SchemaMapping.PrimitiveTypeMapping(arrowType, parquetType);
    }
  }
}
