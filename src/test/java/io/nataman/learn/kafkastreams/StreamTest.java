package io.nataman.learn.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
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
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SuppressWarnings("SameParameterValue")
@Log4j2
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1",
      "metadata.max.age.ms=1000",
      "auto.commit.interval.ms=100",
      "auto.create.topics.enable=false",
      "auto.leader.rebalance.enable=false",
      "linger.ms=100"
    },
    topics = {
      "orchestrator.posting.request",
      "posting.booking.request",
      "booking.posting.response",
      "posting.orchestrator.response",
      "posting-booking-correlation-log"
    })
class StreamTest {
  private static final String POSTING_REQUEST_TOPIC = "orchestrator.posting.request";
  private static final String POSTING_RESPONSE_TOPIC = "posting.orchestrator.response";
  private static final String BOOKING_REQUEST_TOPIC = "posting.booking.request";
  private static final String BOOKING_RESPONSE_TOPIC = "booking.posting.response";
  private static final String CORRELATION_LOG_TOPIC = "posting-booking-correlation-log";

  private final LinkedTransferQueue<ConsumerRecord<String, String>> bookingRequestQueue =
      new LinkedTransferQueue<>();
  private final LinkedTransferQueue<ConsumerRecord<String, String>> postingResponseQueue =
      new LinkedTransferQueue<>();
  private final LinkedTransferQueue<ConsumerRecord<String, String>> correlationLogQueue =
      new LinkedTransferQueue<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @SuppressWarnings("SpringJavaAutowiredMembersInspection")
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  private KafkaMessageListenerContainer<String, String> bookingRequestListener;
  private KafkaMessageListenerContainer<String, String> postingResponseListener;
  private KafkaMessageListenerContainer<String, String> correlationLogListener;
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

  @SneakyThrows
  @BeforeEach
  void start() {
    producer = createProducer();
    // request sink
    bookingRequestListener = createMessageListener(BOOKING_REQUEST_TOPIC, bookingRequestQueue);
    // response sink
    postingResponseListener = createMessageListener(POSTING_RESPONSE_TOPIC, postingResponseQueue);
    // changelog sink
    correlationLogListener = createMessageListener(CORRELATION_LOG_TOPIC, correlationLogQueue);
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
    bookingRequestListener.stop();
    postingResponseListener.stop();
    correlationLogListener.stop();
    context.close();
  }

  @SneakyThrows
  private <T> T getObjectFromQueue(
      LinkedTransferQueue<ConsumerRecord<String, String>> queue, Class<T> targetType) {
    var record = Objects.requireNonNull(queue.peek()).value();
    return objectMapper.readValue(record, targetType);
  }

  @SneakyThrows
  private void assertSizeAndLogRecord(
      String topicName,
      LinkedTransferQueue<ConsumerRecord<String, String>> queue,
      int minExpectedCount) {
    await()
        .atMost(2, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(200))
        .until(
            () -> {
              log.debug(
                  "topicName: {}, expected={} vs actual={}",
                  topicName,
                  minExpectedCount,
                  queue.size());
              return queue.size() >= minExpectedCount;
            });
    queue.forEach(
        record ->
            log.debug(
                "\n-------  TOPIC: {}-{}-{}--------\n  Time: {}\n   Key: {}\n Value: {}\nHeader: {}\n",
                topicName,
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                record.key(),
                record.value(),
                record.headers()));
  }

  @SneakyThrows
  private void sendPostingRequestEvent(String paymentId, String corrId) {
    var event =
        PostingRequestedEvent.builder()
            .paymentUID(paymentId)
            .correlationId(corrId)
            .payload("Posting request for " + paymentId)
            .build();
    send(POSTING_REQUEST_TOPIC, paymentId, event);
  }

  private void sendBookingResponse(final String bookingRequestId) {
    var response =
        BookingResponse.builder()
            .bookingRequestId(bookingRequestId)
            .status(Status.SUCCESS)
            .payload("Booking processed for " + bookingRequestId)
            .build();
    send(BOOKING_RESPONSE_TOPIC, bookingRequestId, response);
  }

  @SneakyThrows
  private void send(String topic, String key, Object value) {
    var future =
        producer.send(new ProducerRecord<>(topic, key, objectMapper.writeValueAsString(value)));
    var result = future.get(1, TimeUnit.SECONDS);
    assertThat(result).isNotNull();
    log.debug(
        "\n------------ TEST DATA {}-{}-{} ----\n    Time: {}\n     Key: {}\n   Value: {}\n",
        result.topic(),
        result.partition(),
        result.offset(),
        Instant.ofEpochMilli(result.timestamp()),
        key,
        value);
  }

  @SneakyThrows
  @Test
  void contextLoaded() {
    sendPostingRequestEvent("payment-1", "corr-1");
    assertSizeAndLogRecord(CORRELATION_LOG_TOPIC, correlationLogQueue, 1);
    assertSizeAndLogRecord(BOOKING_REQUEST_TOPIC, bookingRequestQueue, 1);
    var bookingRequest = getObjectFromQueue(bookingRequestQueue, BookingRequest.class);
    sendBookingResponse(bookingRequest.getBookingRequestId());
    assertSizeAndLogRecord(POSTING_RESPONSE_TOPIC, postingResponseQueue, 1);
    assertSizeAndLogRecord(CORRELATION_LOG_TOPIC, correlationLogQueue, 2);
  }
}
