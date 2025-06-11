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
import com.google.common.io.CharSource;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.integrations.validator.IntegrationError;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ImportPayloadType;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.SourceAttributeSchema;
import io.isima.bios.models.SubjectSchema;
import io.isima.bios.models.SubjectType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class KafkaSourceSchemaFinder extends SourceSchemaFinder {
  private final CSVFormat csvFormat;

  private final ObjectMapper mapper;

  public KafkaSourceSchemaFinder(String tenantName, ImportSourceConfig config) {
    super(tenantName, config);
    mapper = BiosObjectMapperProvider.get();
    csvFormat = CSVFormat.RFC4180;
  }

  @Override
  public ImportSourceSchema find(Integer timeoutSeconds) throws TfosException {
    // default timeout is 60s
    if (timeoutSeconds == null) {
      timeoutSeconds = 60;
    }

    // configure Kafka consumer
    final var props = new Properties();
    props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.getBootstrapServers().stream().collect(Collectors.joining(",")));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    final var authentication = config.getAuthentication();
    if (authentication != null) {
      switch (authentication.getType()) {
        case SASL_PLAINTEXT:
          props.put(
              CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
          props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
          final var saslJaasConfig =
              String.format(
                  "%s required username='%s' password='%s';",
                  PlainLoginModule.class.getName(),
                  authentication.getUser(),
                  authentication.getPassword());
          props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
          break;
        default:
          throw new NotImplementedException(
              "Kafka import source authentication type " + authentication.getType());
      }
    }

    // initialize the schema
    final var schema = new ImportSourceSchema();
    schema.setImportSourceId(config.getImportSourceId());
    schema.setImportSourceName(config.getImportSourceName());
    schema.setSubjects(new ArrayList<>());
    final var warnings = new ArrayList<String>();

    // Hack to enable Kafka PlainLoginModule. Wildfly/JBOSS server sets
    // javax.security.auth.spi.LoginModule for authenticate module. Kafka client
    // adds its own login module org.apache.kafka.common.security.plain.PlainLoginModule to the
    // available login module on startup, but it does not seem to work from servlet,
    // then LoginException happens for login module not found.
    // In order to workaround this problem.  We replace the context class loader
    // by loader of this class that has the Kafka login modules.
    final var currentClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

    // start kafka consumer
    try (final Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
      final var allTopics = consumer.listTopics();
      logger.debug("TOPICS={}", allTopics.keySet());

      final Set<String> topics =
          allTopics.keySet().stream()
              .filter((topic) -> !topic.startsWith("__"))
              .collect(Collectors.toCollection(HashSet::new));

      consumer.subscribe(topics);

      final long timeLimit = System.currentTimeMillis() + timeoutSeconds * 1000;
      while (!topics.isEmpty() && System.currentTimeMillis() < timeLimit) {
        final var records = consumer.poll(Duration.ofSeconds(1L));
        for (var record : records) {
          logger.debug("RECORD: {}", record);
          try {
            schema.getSubjects().add(parseRecord(record));
          } catch (TfosException e) {
            final var topic = record.topic();
            logger.warn(
                "Exception happened while parsing Kafka import source;"
                    + " tenant={}, id={}, name={}, topic={}, error={}",
                tenantName,
                config.getImportSourceId(),
                config.getImportSourceName(),
                topic,
                e.getMessage());
            warnings.add(
                String.format("Data from topic %s could not be parsed: %s", topic, e.getMessage()));
          }
          topics.remove(record.topic());
        }
      }
      if (!topics.isEmpty()) {
        warnings.add(
            String.format(
                "Potential subjects by topics %s could not be resolved due to lack of input data",
                topics));
      }
      if (!warnings.isEmpty()) {
        schema.setWarningMessages(warnings);
      }
    } catch (SaslAuthenticationException e) {
      logger.warn("Kafka authentication error; error={}", e.getMessage());
      throw new TfosException(IntegrationError.SOURCE_SIGN_IN_FAILURE, e.getMessage());
    } catch (TimeoutException e) {
      logger.warn("Kafka operation timeout; error={}", e.getMessage());
      throw new TfosException(IntegrationError.SOURCE_CONNECTION_ERROR, e.getMessage());
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
    return schema;
  }

  private SubjectSchema parseRecord(ConsumerRecord<Long, String> record) throws TfosException {
    final var subject = new SubjectSchema();
    subject.setSubjectName(record.topic());
    subject.setSubjectType(SubjectType.TOPIC);
    subject.setSourceAttributes(new ArrayList<>());

    // resolve data type
    try {
      final var object = mapper.readValue(record.value(), Object.class);
      subject.setPayloadType(ImportPayloadType.JSON);
      if (parseJsonObject(object, subject.getSourceAttributes())) {
        return subject;
      }
      // flat JSON cannot be parsed. would treat the input as a CSV
    } catch (IOException e) {
    }

    // If the source is not JSON, we assume it is CSV
    // TODO(Naoki): Move this part to the base class for reusing
    try {
      final Reader reader = CharSource.wrap(record.value()).openStream();
      final CSVParser csvParser = csvFormat.parse(reader);
      final CSVRecord elements = csvParser.getRecords().get(0);
      for (int i = 0; i < elements.size(); ++i) {
        final String valueString = elements.get(i);
        final var attribute = new SourceAttributeSchema();
        attribute.setSourceAttributeName("column" + (i + 1));
        attribute.setIsNullable(valueString.isEmpty());
        attribute.setSuggestedType(determineType(valueString));
        attribute.setExampleValue(valueString.trim());
        subject.getSourceAttributes().add(attribute);
      }
      subject.setPayloadType(ImportPayloadType.CSV);
    } catch (IOException e) {
      logger.warn("Error in handling CSV values from Kafka source", e);
      throw new TfosException(IntegrationError.IMPORT_SOURCE_UNABLE_TO_PARSE, e.getMessage());
    }

    return subject;
  }

  private AttributeType determineType(String valueString) {
    if (valueString.isBlank()) {
      return AttributeType.STRING;
    }
    final var normalized = valueString.trim().toLowerCase();
    if (normalized.equals("true") || normalized.equals("false")) {
      return AttributeType.BOOLEAN;
    }
    try {
      Long.parseLong(normalized);
      return AttributeType.INTEGER;
    } catch (NumberFormatException e) {
    }
    try {
      Double.parseDouble(normalized);
      return AttributeType.DECIMAL;
    } catch (NumberFormatException e) {
      return AttributeType.STRING;
    }
  }
}
