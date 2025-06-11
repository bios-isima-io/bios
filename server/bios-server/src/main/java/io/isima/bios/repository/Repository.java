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
package io.isima.bios.repository;

import com.datastax.driver.core.Row;
import io.isima.bios.common.FilterCriteria;
import java.util.Objects;
import java.util.function.BiPredicate;

public abstract class Repository {
  protected BiPredicate<Row, FilterCriteria> byUser =
      (row, reqParam) -> {
        return Objects.equals(row.getLong("user_id"), reqParam.getUserId());
      };

  protected BiPredicate<Row, FilterCriteria> bySearchTerm =
      (row, reqParam) -> {
        return row.getString("name").toLowerCase().contains(reqParam.getSearchTerm().toLowerCase());
      };

  protected BiPredicate<Row, Long> byGroup =
      (row, groupId) -> {
        return row.getList("groups", Long.class).contains(groupId);
      };

  protected BiPredicate<Row, FilterCriteria> byFavorite =
      (row, reqParam) -> {
        return reqParam.getFavoriteIds().contains(row.getLong("id"));
      };

  protected BiPredicate<Row, FilterCriteria> byDisabled =
      (row, reqParam) -> {
        return !row.getBool("enable");
      };

  protected BiPredicate<Row, FilterCriteria> byEnabled =
      (row, reqParam) -> {
        return row.getBool("enable");
      };

  protected BiPredicate<Row, FilterCriteria> byUnarchived =
      (row, reqParam) -> {
        return row.getBool("is_archived") == false;
      };

  protected BiPredicate<Row, FilterCriteria> byPublished =
      (row, reqParam) -> {
        return row.getBool("is_published") == true;
      };
}
