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
package io.isima.bios.codec.proto.isql;

import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.proto.DataProto;

/** Builds an independent protobuf order by clause. */
public class ProtoOrderByBuilder implements OrderBy.OrderByFinalSpecifier {
  private final DataProto.OrderBy.Builder orderByBuilder;

  /** Protobuf Order By Builder. */
  public ProtoOrderByBuilder(String by) {
    this.orderByBuilder = DataProto.OrderBy.newBuilder();
    this.orderByBuilder.setBy(by);
    this.orderByBuilder.setCaseSensitive(false);
  }

  @Override
  public OrderBy.OrderByFinalSpecifier asc() {
    this.orderByBuilder.setReverse(false);
    return this;
  }

  @Override
  public OrderBy.OrderByFinalSpecifier desc() {
    this.orderByBuilder.setReverse(true);
    return this;
  }

  @Override
  public OrderBy.OrderByFinalSpecifier caseSensitive() {
    this.orderByBuilder.setCaseSensitive(true);
    return this;
  }

  DataProto.OrderBy buildProto() {
    return orderByBuilder.build();
  }
}
