package io.nataman.learn.kafkastreams;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.RETRIES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(
    webEnvironment = WebEnvironment.NONE,
    properties = {"spring.cloud.stream.kafka.binder.brokers=localhost:9192"})
@DirtiesContext
@EmbeddedKafka(
    ports = 9192,
    partitions = 1,
    brokerProperties = {
      "transaction.state.log.replication.factor=1",
      "transaction.state.log.min.isr=1",
      "metadata.max.age.ms=100",
      "auto.commit.interval.ms=100",
      "auto.create.topics.enable=true",
      "auto.leader.rebalance.enable=false",
      "linger.ms=100"
    },
    topics = {
      "orchestrator.posting.request",
      "posting.booking.request",
      "booking.posting.response",
      "posting.orchestrator.response",
      "posting.booking.repartition",
      "postingservice-correlation-store-changelog"
    },
    controlledShutdown = true)
@ActiveProfiles("test")
@Log4j2
class EmbeddedKafkaStreamTest {
  static final String POSTING_REQUEST_TOPIC = "orchestrator.posting.request";
  static final String POSTING_RESPONSE_TOPIC = "posting.orchestrator.response";
  static final String BOOKING_REQUEST_TOPIC = "posting.booking.request";
  static final String BOOKING_RESPONSE_TOPIC = "booking.posting.response";

  private final LinkedTransferQueue<ConsumerRecord<String, String>> bookingRequestQueue =
      new LinkedTransferQueue<>();
  private final LinkedTransferQueue<ConsumerRecord<String, String>> postingResponseQueue =
      new LinkedTransferQueue<>();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  private KafkaMessageListenerContainer<String, String> bookingRequestListener;
  private KafkaMessageListenerContainer<String, String> postingResponseListener;
  private Producer<String, String> producer;
  private ProducerFactory<String, String> producerFactory;

  private KafkaMessageListenerContainer<String, String> createMessageListener(
      String topicName, BlockingQueue<ConsumerRecord<String, String>> queue) {
    var consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
    consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(AUTO_OFFSET_RESET_CONFIG, EARLIEST.toString().toLowerCase());
    consumerProps.put(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase());
    var cf = new DefaultKafkaConsumerFactory<String, String>(consumerProps);
    ContainerProperties containerProperties = new ContainerProperties(topicName);
    var container = new KafkaMessageListenerContainer<>(cf, containerProperties);
    container.setupMessageListener((MessageListener<String, String>) queue::add);
    container.start();
    ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    return container;
  }

  private ProducerFactory<String, String> createProducer() {
    var producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
    producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(RETRIES_CONFIG, 3);
    producerProps.put(ENABLE_IDEMPOTENCE_CONFIG, true);
    return new DefaultKafkaProducerFactory<>(producerProps);
  }

  @SneakyThrows
  @BeforeEach
  void start() {
    producerFactory = createProducer();
    producer = producerFactory.createProducer();
    // request sink
    bookingRequestListener = createMessageListener(BOOKING_REQUEST_TOPIC, bookingRequestQueue);
    // response sink
    postingResponseListener = createMessageListener(POSTING_RESPONSE_TOPIC, postingResponseQueue);
  }

  @AfterEach
  void stop() {
    bookingRequestListener.stop();
    postingResponseListener.stop();
    producer.close();
    producerFactory.reset();
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
        .pollInterval(Duration.ofMillis(100))
        .until(() -> queue.size() >= minExpectedCount);
    queue.forEach(
        record ->
            log.debug(
                "\n-------  TOPIC: {}-{}-{} --------\n  Time: {}\n   Key: {}\n Value: {}\nHeader: {}\n",
                topicName,
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                record.key(),
                record.value(),
                record.headers()));
  }

  @SneakyThrows
  private PostingRequestedEvent sendPostingRequestEvent(String paymentId, String corrId) {
    var event =
        PostingRequestedEvent.builder()
            .paymentUID(paymentId)
            .correlationId(corrId)
            .payload("Posting request for " + paymentId)
            .build();
    send(POSTING_REQUEST_TOPIC, paymentId, event);
    return event;
  }

  private BookingResponse sendBookingResponse(final String bookingRequestId) {
    var response =
        BookingResponse.builder()
            .bookingRequestId(bookingRequestId)
            .status(Status.SUCCESS)
            .payload("Booking processed for " + bookingRequestId)
            .build();
    send(BOOKING_RESPONSE_TOPIC, bookingRequestId, response);
    return response;
  }

  @SneakyThrows
  private void send(String topic, String key, Object value) {
    var future =
        producer.send(new ProducerRecord<>(topic, key, objectMapper.writeValueAsString(value)));
    var result = future.get(1, TimeUnit.SECONDS);
    assertThat(result).isNotNull();
    log.debug(
        "\n------------ TEST DATA {}-{}-{} -----------\n    Time: {}\n     Key: {}\n   Value: {}\n",
        result.topic(),
        result.partition(),
        result.offset(),
        Instant.ofEpochMilli(result.timestamp()),
        key,
        value);
  }

  @SneakyThrows
  @Test
  void givenPostingRequest_whenBookingResponse_thenPostingConfirmedEvent() {
    // given
    var postingRequestedEvent = sendPostingRequestEvent("payment-1", "corr-1");

    assertSizeAndLogRecord(BOOKING_REQUEST_TOPIC, bookingRequestQueue, 1);

    var bookingRequest = getObjectFromQueue(bookingRequestQueue, BookingRequest.class);

    assertThat(bookingRequest.getBookingRequestId())
        .isEqualTo(postingRequestedEvent.getPaymentUID() + "-b");

    // when
    sendBookingResponse(bookingRequest.getBookingRequestId());

    // then
    assertSizeAndLogRecord(POSTING_RESPONSE_TOPIC, postingResponseQueue, 1);

    var postingConfirmedEvent =
        getObjectFromQueue(postingResponseQueue, PostingConfirmedEvent.class);
    assertThat(postingConfirmedEvent.getCorrelationId())
        .isEqualTo(postingRequestedEvent.getCorrelationId());
  }
}
