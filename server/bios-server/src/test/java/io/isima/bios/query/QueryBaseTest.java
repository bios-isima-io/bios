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
package io.isima.bios.query;

import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import io.isima.bios.codec.proto.wrappers.ProtoSelectRequest;
import io.isima.bios.models.MultiSelectRequest;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;

public class QueryBaseTest {
  protected static final String TEST_SIGNAL = "signal1";
  protected static final String TEST_TENANT = "tenant1";
  protected static final String ATTRIBUTE_NAME_PREFIX = "attr";
  protected static final String ADDITIONAL_NAME_PREFIX = "additional";

  protected StreamConfig buildStreamConfig(
      long version, int numAttributes, int numAdditionalAttributes) {
    StreamConfig config = new StreamConfig(TEST_SIGNAL, version).setType(StreamType.SIGNAL);
    for (int i = 0; i < numAttributes; i++) {
      config.addAttribute(new AttributeDesc(ATTRIBUTE_NAME_PREFIX + i, STRING));
    }
    for (int i = 0; i < numAdditionalAttributes; i++) {
      config.addAdditionalAttribute(new AttributeDesc(ADDITIONAL_NAME_PREFIX + i, STRING));
    }
    return config;
  }

  protected MultiSelectRequest buildQuery(
      int numAttributes, int numAdditionalAttributes, int numGroups, boolean orderBy) {
    final DataProto.SelectRequest.Builder builder = DataProto.SelectRequest.newBuilder();
    final DataProto.SelectQuery.Builder queryBuilder = DataProto.SelectQuery.newBuilder();
    queryBuilder.setStartTime(10).setEndTime(100);
    queryBuilder.setFrom(TEST_SIGNAL);
    if (numAttributes > 0 || numAdditionalAttributes > 0) {
      DataProto.AttributeList.Builder attrBuilder = DataProto.AttributeList.newBuilder();
      for (int i = 0; i < numAttributes; i++) {
        attrBuilder.addAttributes(ATTRIBUTE_NAME_PREFIX + i);
      }
      for (int i = 0; i < numAdditionalAttributes; i++) {
        attrBuilder.addAttributes(ADDITIONAL_NAME_PREFIX + i);
      }
      queryBuilder.setAttributes(attrBuilder.build());
    }
    if (numGroups > 0) {
      DataProto.Dimensions.Builder dimensionBuilder = DataProto.Dimensions.newBuilder();
      for (int i = 0; i < numGroups; i++) {
        dimensionBuilder.addDimensions(ATTRIBUTE_NAME_PREFIX + i);
      }
      queryBuilder.setGroupBy(dimensionBuilder.build());
    }
    if (orderBy) {
      DataProto.OrderBy.Builder orderByBuilder = DataProto.OrderBy.newBuilder();
      orderByBuilder.setBy(ATTRIBUTE_NAME_PREFIX + 0);
    }
    builder.addQueries(queryBuilder.build());
    return new ProtoSelectRequest(builder.build());
  }
}
