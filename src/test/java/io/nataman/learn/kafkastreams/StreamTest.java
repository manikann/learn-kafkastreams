package io.nataman.learn.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.CheckedConsumer;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SuppressWarnings("SameParameterValue")
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
      "posting-service-correlation-store-changelog",
      "booking.posting.response",
      "posting.orchestrator.response"
    })
class StreamTest {
  private static final String POSTING_REQUEST_TOPIC = "orchestrator.posting.request";
  private static final String POSTING_RESPONSE_TOPIC = "posting.orchestrator.response";
  private static final String BOOKING_REQUEST_TOPIC = "posting.booking.request";
  private static final String BOOKING_RESPONSE_TOPIC = "booking.posting.response";
  private static final String CHANGE_LOG_TOPIC = "posting-service-correlation-store-changelog";

  private final BlockingQueue<ConsumerRecord<String, String>> bookingRequestQueue =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<ConsumerRecord<String, String>> postingResponseQueue =
      new LinkedBlockingQueue<>();
  private final BlockingQueue<ConsumerRecord<String, String>> changeLogQueue =
      new LinkedBlockingQueue<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @SuppressWarnings("SpringJavaAutowiredMembersInspection")
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  private KafkaMessageListenerContainer<String, String> bookingRequestListener;
  private KafkaMessageListenerContainer<String, String> postingResponseListener;
  private KafkaMessageListenerContainer<String, String> changeLogContainer;
  private ConfigurableApplicationContext context;
  private Producer<String, String> producer;

  private KafkaMessageListenerContainer<String, String> createMessageListener(
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
    // ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
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
    // request sink
    bookingRequestListener = createMessageListener(BOOKING_REQUEST_TOPIC, bookingRequestQueue);
    // response sink
    postingResponseListener = createMessageListener(POSTING_RESPONSE_TOPIC, postingResponseQueue);
    // changelog sink
    changeLogContainer = createMessageListener(CHANGE_LOG_TOPIC, changeLogQueue);
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
    bookingRequestListener.stop();
    postingResponseListener.stop();
    changeLogContainer.stop();
  }

  @SneakyThrows
  private void sendPostingRequest(String paymentId, String corrId) {
    var event =
        PostingRequestedEvent.builder()
            .correlationId(corrId)
            .payload("Posting request for " + paymentId)
            .build();
    var future =
        producer.send(
            new ProducerRecord<>(
                POSTING_REQUEST_TOPIC, paymentId, objectMapper.writeValueAsString(event)));
    var result = future.get(1, TimeUnit.SECONDS);
    assertThat(result).isNotNull();
    log.debug(
        "sendPostingRequest: metadata: {} {}-{}-{}",
        Instant.ofEpochMilli(result.timestamp()),
        result.topic(),
        result.partition(),
        result.offset());
  }

  @SneakyThrows
  private void dumpRecords(
      String topicName, BlockingQueue<ConsumerRecord<String, String>> queue, int expectedCount) {
    CheckedConsumer<Integer> readQueue =
        i -> {
          var record = queue.poll(2, TimeUnit.SECONDS);
          assertThat(record).isNotNull();
          log.debug("\nTOPIC: {}\n{}: {}", topicName, record.key(), record.value());
        };

    Stream.iterate(0, n -> n++).limit(expectedCount).forEach(readQueue.unchecked());
  }

  @SneakyThrows
  @Test
  void contextLoaded() {
    sendPostingRequest("payment-1", "corr-1");
    dumpRecords(CHANGE_LOG_TOPIC, changeLogQueue, 1);
    dumpRecords(BOOKING_REQUEST_TOPIC, bookingRequestQueue, 1);
  }
}
