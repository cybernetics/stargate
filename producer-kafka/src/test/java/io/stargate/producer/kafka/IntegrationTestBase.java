/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka;

import static io.stargate.producer.kafka.configuration.ConfigLoader.CDC_TOPIC_PREFIX_NAME;
import static io.stargate.producer.kafka.configuration.ConfigLoader.METRICS_ENABLED_SETTING_NAME;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT;

import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.stargate.producer.kafka.configuration.ConfigLoader;
import io.stargate.producer.kafka.schema.EmbeddedSchemaRegistryServer;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

public class IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);

  protected static KafkaContainer kafkaContainer;
  protected static EmbeddedSchemaRegistryServer schemaRegistry;

  protected static final String TOPIC_PREFIX = "topicPrefix";
  protected static final String PARTITION_KEY_VALUE = "pk_value";
  protected static final Integer CLUSTERING_KEY_VALUE = 1;

  @BeforeAll
  public static void setup() throws Exception {
    Network network = Network.newNetwork();
    kafkaContainer = new KafkaContainer().withNetwork(network).withEmbeddedZookeeper();
    kafkaContainer.start();
    try (ServerSocket serverSocket = new ServerSocket(0)) {

      schemaRegistry =
          new EmbeddedSchemaRegistryServer(
              String.format("http://localhost:%s", serverSocket.getLocalPort()),
              String.format("localhost:%s", ZOOKEEPER_PORT),
              kafkaContainer.getBootstrapServers());
    }
    schemaRegistry.startSchemaRegistry();
  }

  @AfterAll
  public static void cleanup() {
    kafkaContainer.stop();
    schemaRegistry.close();
  }

  protected TableMetadata mockTableMetadata() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getKeyspace()).thenReturn("keyspaceName");
    when(tableMetadata.getName()).thenReturn("tableName" + UUID.randomUUID().toString());
    return tableMetadata;
  }

  protected String createTopicName(TableMetadata tableMetadata) {
    return String.format(
        "%s.%s.%s", TOPIC_PREFIX, tableMetadata.getKeyspace(), tableMetadata.getName());
  }

  @NotNull
  protected Map<String, Object> createKafkaProducerSettings(Map<String, Object> overrides) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CDC_TOPIC_PREFIX_NAME, TOPIC_PREFIX);
    // metrics disabled in tests by default
    properties.put(METRICS_ENABLED_SETTING_NAME, false);

    properties.put(
        withCDCPrefixPrefix(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
        kafkaContainer.getBootstrapServers());
    // lower the max.block to allow faster failure scenario testing
    properties.put(withCDCPrefixPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), "2000");

    properties.put(
        withCDCPrefixPrefix("schema.registry.url"), schemaRegistry.getSchemaRegistryUrl());

    for (Map.Entry<String, Object> override : overrides.entrySet()) {
      properties.put(override.getKey(), override.getValue());
    }
    return properties;
  }

  @NotNull
  protected Map<String, Object> createKafkaProducerSettings() {
    return createKafkaProducerSettings(Collections.emptyMap());
  }

  @NotNull
  private String withCDCPrefixPrefix(String settingName) {
    return String.format("%s.%s", ConfigLoader.CDC_KAFKA_PRODUCER_SETTING_PREFIX, settingName);
  }

  protected void verifyReceivedByKafka(
      GenericRecord expectedKey, GenericRecord expectedValue, String topicName) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("schema.registry.url", schemaRegistry.getSchemaRegistryUrl());

    KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topicName));

    try {
      await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () -> {
                ConsumerRecords<GenericRecord, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                  LOG.info(
                      "Retrieved {} records: {}",
                      records.count(),
                      IteratorUtils.toList(records.iterator()));
                }
                return Streams.stream(records)
                    .anyMatch(r -> r.key().equals(expectedKey) && r.value().equals(expectedValue));
              });
    } finally {
      consumer.close();
    }
  }
}