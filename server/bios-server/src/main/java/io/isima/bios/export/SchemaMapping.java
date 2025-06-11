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

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class SchemaMapping {
  private final VectorSchemaRoot schemaRoot;
  private final Schema arrowSchema;
  private final MessageType parquetSchema;
  private final List<TypeMapping> types;

  public SchemaMapping(
      VectorSchemaRoot schemaRoot, MessageType parquetSchema, List<TypeMapping> types) {
    this.schemaRoot = schemaRoot;
    this.arrowSchema = schemaRoot.getSchema();
    this.parquetSchema = parquetSchema;
    this.types = types;
  }

  public VectorSchemaRoot getSchemaRoot() {
    return schemaRoot;
  }

  public Schema getArrowSchema() {
    return arrowSchema;
  }

  public MessageType getParquetSchema() {
    return parquetSchema;
  }

  public List<TypeMapping> getTypes() {
    return types;
  }

  public abstract static class TypeMapping {
    private final Field arrowType;
    private final Type parquetType;

    TypeMapping(Field arrowField, Type parquetField) {
      this.arrowType = arrowField;
      this.parquetType = parquetField;
    }

    public Field getArrowType() {
      return arrowType;
    }

    public Type getParquetType() {
      return parquetType;
    }
  }

  static final class PrimitiveTypeMapping extends TypeMapping {
    PrimitiveTypeMapping(Field arrowField, Type parquetField) {
      super(arrowField, parquetField);
    }
  }
}
