package io.nataman.learn.kafkastreams;

import java.util.function.BiConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Log4j2
@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
class StreamConfiguration {

  static final String CORRELATION_STATE_STORE_NAME = "correlation-store";
  static final String POSTING_REQUEST_REPARTITION_TOPIC = "posting.booking.repartition";
  static final String BOOKING_REQUEST_TOPIC = "posting.booking.request";
  static final String POSTING_RESPONSE_TOPIC = "posting.orchestrator.response";

  private final Transformer<String, PostingRequestedEvent, KeyValue<String, BookingRequest>>
      bookingRequestTransformer;
  private final Transformer<String, BookingResponse, KeyValue<String, PostingConfirmedEvent>>
      bookingResponseTransformer;

  @Bean
  public StoreBuilder<KeyValueStore<String, ValueAndTimestamp<PostingRequestedEvent>>>
      correlationStateStore() {
    return Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(CORRELATION_STATE_STORE_NAME),
            Serdes.String(),
            new ValueAndTimestampSerde<>(new JsonSerde<>(PostingRequestedEvent.class)))
        .withCachingEnabled();
  }

  @Bean
  public BiConsumer<KStream<String, PostingRequestedEvent>, KStream<String, BookingResponse>>
      process() {
    return (postingRequestStream, bookingResponseStream) -> {
      configureBookingRequestStream(postingRequestStream);
      configureBookingResponseStream(bookingResponseStream);
    };
  }

  private void configureBookingRequestStream(
      final KStream<String, PostingRequestedEvent> postingRequestStream) {
    // Posting Request -> Booking Request stream
    postingRequestStream
        .peek(
            (key, value) -> log.debug("requestStream: source {}:{}", key, value),
            Named.as("peek-request-source"))
        // generate and change the key to booking id
        .selectKey((key, value) -> value.getPaymentUID() + "-b")
        // repartition the stream to be based on booking id
        .through(
            POSTING_REQUEST_REPARTITION_TOPIC,
            Produced.<String, PostingRequestedEvent>as("posting.booking.repartition.processor")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(PostingRequestedEvent.class)))
        // now transform the posting request into booking request and store the correlation info
        .transform(
            () -> bookingRequestTransformer,
            Named.as("booking-request-transformer"),
            CORRELATION_STATE_STORE_NAME)
        // send the request to booking request topic
        .peek(
            (key, value) -> log.debug("requestStream: sink {}:{}", key, value),
            Named.as("peek-request-sink"))
        .to(
            BOOKING_REQUEST_TOPIC,
            Produced.<String, BookingRequest>as("booking-request-sink")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(BookingRequest.class)));
  }

  private void configureBookingResponseStream(
      final KStream<String, BookingResponse> bookingResponseStream) {
    // booking response to posting response stream
    bookingResponseStream
        .peek(
            (key, value) -> log.debug("responseStream: source {}:{}", key, value),
            Named.as("peek-response-source"))
        // correlate and transform the response to confirmed event
        .transform(
            () -> bookingResponseTransformer,
            Named.as("booking-response-transformer"),
            CORRELATION_STATE_STORE_NAME)
        .peek(
            (key, value) -> log.debug("responseStream: sink {}:{}", key, value),
            Named.as("peek-response-sink"))
        .to(
            POSTING_RESPONSE_TOPIC,
            Produced.<String, PostingConfirmedEvent>as("posting-response-sink")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(PostingConfirmedEvent.class)));
  }
}
