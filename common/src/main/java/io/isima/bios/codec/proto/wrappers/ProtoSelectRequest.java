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
package io.isima.bios.codec.proto.wrappers;

import io.isima.bios.models.MultiSelectRequest;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.proto.DataProto;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrapper class for protobuf select request that adapts to the protobuf auto generated classes.
 *
 * <p>When protobuf auto generated classes support implementing user specified interfaces (which is
 * in their roadmap), these wrapper classes can be removed. To avoid too much object copying these
 * classes directly interfaces with underlying de-serialized proto message {@link
 * com.google.protobuf.Message} classes.
 */
public class ProtoSelectRequest implements MultiSelectRequest {

  private final List<ProtoSelectQuery> selectQueries;

  /**
   * Construct a wrapper to protobuf.
   *
   * @param request actual protobuf representation of select request
   */
  public ProtoSelectRequest(DataProto.SelectRequest request) {
    selectQueries =
        request.getQueriesList().stream().map(ProtoSelectQuery::new).collect(Collectors.toList());
  }

  @Override
  public List<? extends SelectStatement> queries() {
    return selectQueries;
  }
}
