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
package io.isima.bios.integrations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.ImportPayloadType;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.SslMode;
import io.isima.bios.models.SubjectSchema;
import io.isima.bios.models.SubjectType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

@Slf4j
public class MongoSourceSchemaFinder extends SourceSchemaFinder {
  private final ObjectMapper mapper;

  public MongoSourceSchemaFinder(String tenantName, ImportSourceConfig config) {
    super(tenantName, config);
    mapper = BiosObjectMapperProvider.get();
  }

  @Override
  public ImportSourceSchema find(Integer timeoutSeconds) throws TfosException {
    final var schema = new ImportSourceSchema();
    schema.setImportSourceName(config.getImportSourceName());
    schema.setImportSourceId(config.getImportSourceId());

    final var protocol =
        (config.getUseDnsSeedList() != null && config.getUseDnsSeedList())
            ? "mongodb+srv://"
            : "mongodb://";
    final var sslEnabled = config.getSsl() != null && config.getSsl().getMode() != SslMode.DISABLED;
    String queryParams = sslEnabled ? "ssl=true" : "ssl=false";
    if (config.getAuthSource() != null && !config.getAuthSource().isEmpty()) {
      queryParams += "&authSource=" + config.getAuthSource();
    }
    if (!config.getReplicaSet().isEmpty()) {
      queryParams += "&replicaSet=" + config.getReplicaSet();
    }
    final var connectionString =
        new StringBuilder(protocol)
            .append(config.getAuthentication().getUser())
            .append(":")
            .append(config.getAuthentication().getPassword())
            .append("@")
            .append(String.join(",", config.getEndpoints()))
            .append("/?" + queryParams)
            .toString();

    try (final var client = MongoClients.create(connectionString)) {
      final var db = client.getDatabase(config.getDatabaseName());
      final var collections = db.listCollectionNames();

      final var warnings = new ArrayList<String>();
      schema.setSubjects(new ArrayList<>());
      for (final var collection : collections) {
        final List<Document> documents =
            db.getCollection(collection).find().limit(10).into(new ArrayList<>());
        for (final Document document : documents) {
          try {
            schema.getSubjects().add(parseRecord(collection, document.toJson()));
          } catch (Exception e) {
            warnings.add(
                String.format(
                    "Data from collection %s could not be parsed: %s", collection, e.getMessage()));
          }
        }
      }
      if (!warnings.isEmpty()) {
        schema.setWarningMessages(warnings);
      }
      if (schema.getSubjects().isEmpty()) {
        warnings.add(
            String.format("No collections found in database: %s", config.getDatabaseName()));
        schema.setWarningMessages(warnings);
      }
    }
    return schema;
  }

  private SubjectSchema parseRecord(String collection, String json) throws Exception {
    final var subject = new SubjectSchema();
    subject.setSubjectName(collection);
    subject.setSubjectType(SubjectType.TOPIC);
    subject.setSourceAttributes(new ArrayList<>());

    final Object object = mapper.readValue(json, Object.class);
    subject.setPayloadType(ImportPayloadType.JSON);
    parseJsonObject(object, subject.getSourceAttributes());
    return subject;
  }
}
