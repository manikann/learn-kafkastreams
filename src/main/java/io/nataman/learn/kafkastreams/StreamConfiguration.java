package io.nataman.learn.kafkastreams;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Log4j2
@Configuration(proxyBeanMethods = false)
class StreamConfiguration {

  static final String CORRELATION_TOPIC = "posting-booking-correlation-log";
  static final String CORRELATION_STATE_STORE_NAME = "correlation-store";
  static final Duration TIMEOUT_DURATION = Duration.ofSeconds(5);

  // this has to be public, for spring to interrogate this method
  @Bean
  public Function<KStream<String, PostingRequestedEvent>, KStream<String, BookingRequest>>
      requestToBooking(
          final KeyValueMapper<
                  String, PostingRequestedEvent, KeyValue<PostingRequestedEvent, BookingRequest>>
              bookingRequestMapper,
          final KeyValueMapper<
                  PostingRequestedEvent, BookingRequest, KeyValue<String, CorrelationEntry>>
              requestCorrelationMapper) {
    return input -> {
      var bookingRequestStream =
          input.map(bookingRequestMapper, Named.as("booking-request-mapper"));
      bookingRequestStream
          .map(requestCorrelationMapper, Named.as("request-correlation-mapper"))
          .to(CORRELATION_TOPIC, Produced.as("correlation-sink"));
      return bookingRequestStream.selectKey(
          (key, value) -> value.getBookingRequestId(), Named.as("booking-id-key"));
    };
  }

  @Bean
  public BiFunction<
          KStream<String, BookingResponse>,
          KStream<String, CorrelationEntry>,
          KStream<String, PostingConfirmedEvent>>
      responseFromBooking(
          final KeyValueMapper<
                  String, Tuple2<BookingResponse, CorrelationEntry>, KeyValue<String, Object>>
              correlationDeleteMapper,
          final KeyValueMapper<
                  String,
                  Tuple2<BookingResponse, CorrelationEntry>,
                  KeyValue<String, PostingConfirmedEvent>>
              bookingResponseMapper) {
    return (bookingResponseStream, correlationEntryStream) -> {
      // store config
      var correlationStoreMaterialized =
          Materialized.<String, CorrelationEntry>as(
                  Stores.inMemoryKeyValueStore(CORRELATION_STATE_STORE_NAME))
              .withKeySerde(Serdes.String())
              .withValueSerde(new JsonSerde<>(CorrelationEntry.class))
              .withCachingEnabled()
              .withLoggingDisabled();

      // create in-memory key/value table
      var correlationEntryTable =
          correlationEntryStream.toTable(
              Named.as("correlation-table"), correlationStoreMaterialized);

      // join the response and correlation table
      var correlatedStream =
          bookingResponseStream.join(
              correlationEntryTable, Tuple::of, Joined.as("correlated-stream"));

      // delete the correlated entry
      correlatedStream
          .map(correlationDeleteMapper, Named.as("correlation-delete-mapper"))
          .to(CORRELATION_TOPIC, Produced.as("correlation-delete-sink"));

      // send response to posting
      return correlatedStream.map(bookingResponseMapper, Named.as("booking-response-mapper"));
    };
  }

  @Bean
  public BiConsumer<KStream<String, CorrelationEntry>, KStream<String, BookingResponse>>
      timeoutMonitoring() {
    return (correlationEntryStream, bookingResponseStream) -> {
      var windowSize = TIMEOUT_DURATION.multipliedBy(2);
      var retentionSize = windowSize.plus(Duration.ofSeconds(1));

      var correlationEntryWindowStore =
          Stores.inMemoryWindowStore("timeout-correlation-store", retentionSize, windowSize, true);

      var bookingResponseWindowStore =
          Stores.inMemoryWindowStore("timeout-booking-response", retentionSize, windowSize, true);

      var alertStreamJoined =
          StreamJoined.with(
                  Serdes.String(),
                  new JsonSerde<>(CorrelationEntry.class),
                  new JsonSerde<>(BookingResponse.class))
              .withName("timeout-join")
              .withStoreName("alert-store")
              .withThisStoreSupplier(correlationEntryWindowStore)
              .withOtherStoreSupplier(bookingResponseWindowStore);

      // alert steam
      correlationEntryStream
          .leftJoin(
              bookingResponseStream,
              Tuple::of,
              JoinWindows.of(TIMEOUT_DURATION).grace(Duration.ofSeconds(1)),
              alertStreamJoined)
          .filter((key, value) -> value._2 == null, Named.as("timeout-filter"))
          .mapValues((readOnlyKey, value) -> value._1, Named.as("timeout-mapper"))
          .foreach(
              (key, value) ->
                  log.warn(
                      "Timed out; no response received for paymentUID: {} since {}, correlationEntry: {}",
                      key,
                      Instant.ofEpochMilli(value.getEntryTime()),
                      value),
              Named.as("alert-action"));
    };
  }
}
