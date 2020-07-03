package io.nataman.learn.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Log4j2
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1"
    },
    topics = {
      "orchestrator.posting.request",
      "posting.booking.request",
      "lks-correlation-store-changelog"
    })
class StreamTest {
  private static final String POSTING_REQUEST_TOPIC = "orchestrator.posting.request";
  private static final String BOOKING_REQUEST_TOPIC = "posting.booking.request";
  private static final String CHANGE_LOG_TOPIC = "lks-correlation-store-changelog";

  @SuppressWarnings("SpringJavaAutowiredMembersInspection")
  @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;

  private BlockingQueue<ConsumerRecord<String, String>> bookingRequestQueue =
      new LinkedBlockingQueue<>();
  private KafkaMessageListenerContainer<String, String> bookingRequestContainer;
  private BlockingQueue<ConsumerRecord<String, String>> changeLogQueue =
      new LinkedBlockingQueue<>();
  private KafkaMessageListenerContainer<String, String> changeLogContainer;

  private ConfigurableApplicationContext context;
  private ObjectMapper objectMapper = new ObjectMapper();
  private Producer<String, String> producer;

  private KafkaMessageListenerContainer<String, String> createContainer(
      String topicName, BlockingQueue<ConsumerRecord<String, String>> queue) {
    var consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    var cf = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
    ContainerProperties containerProperties = new ContainerProperties(topicName);
    var container = new KafkaMessageListenerContainer<>(cf, containerProperties);
    container.setupMessageListener((MessageListener<String, String>) queue::add);
    container.start();
    //ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    return container;
  }

  private Producer<String, String> createProducer() {
    var producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new DefaultKafkaProducerFactory<String, String>(producerProps).createProducer();
  }

  @BeforeEach
  void start() {
    producer = createProducer();
    bookingRequestContainer = createContainer(BOOKING_REQUEST_TOPIC, bookingRequestQueue);
    changeLogContainer = createContainer(CHANGE_LOG_TOPIC, changeLogQueue);
    // spring boot app
    context =
        new SpringApplicationBuilder(KafkaStreamsApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "spring.cloud.stream.kafka.streams.binder.configuration.application.server="
                    + embeddedKafkaBroker.getBrokersAsString(),
                "spring.cloud.stream.kafka.streams.binder.brokers="
                    + embeddedKafkaBroker.getBrokersAsString())
            .run();
  }

  @AfterEach
  void stop() {
    context.close();
    bookingRequestContainer.stop();
    changeLogContainer.stop();
  }

  @SneakyThrows
  private void sendPostingRequest(String paymentId, String corrId) {
    var event =
        PostingRequestedEvent.builder()
            .paymentUID(paymentId)
            .correlationId(corrId)
            .postingRequest("Posting request for " + paymentId)
            .build();
    var future =
        producer.send(
            new ProducerRecord<>(
                POSTING_REQUEST_TOPIC, paymentId, objectMapper.writeValueAsString(event)));
    var result = future.get(1, TimeUnit.SECONDS);
    assertThat(result).isNotNull();
    log.debug(
        "metadata: {} {}-{}-{}",
        Instant.ofEpochMilli(result.timestamp()),
        result.topic(),
        result.partition(),
        result.offset());
  }

  @SneakyThrows
  private void dumpRecords(
      String topicName, BlockingQueue<ConsumerRecord<String, String>> queue, int expectedCount) {
    var record = queue.poll(1, TimeUnit.SECONDS);
    assertThat(record).isNotNull();
    log.debug("\nTOPIC: {}\n{}: {}", topicName, record.key(), record.value());
  }

  @SneakyThrows
  @Test
  void contextLoaded() {
    sendPostingRequest("payment-1", "corr-1");
    dumpRecords(CHANGE_LOG_TOPIC, changeLogQueue, 1);
    dumpRecords(BOOKING_REQUEST_TOPIC, bookingRequestQueue, 1);
  }
}
