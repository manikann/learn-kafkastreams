package io.nataman.learn.kafkastreams;

import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Log4j2
@Configuration(proxyBeanMethods = false)
class StreamConfiguration {

  static final String CORRELATION_TOPIC = "posting-booking-correlation-log";
  static final String CORRELATION_STATE_STORE_NAME = "correlation-store";

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
          .to(CORRELATION_TOPIC, Produced.as("store-correlation-sink"));
      return bookingRequestStream.selectKey(
          (key, value) -> value.getBookingRequestId(), Named.as("select-key-booking-id"));
    };
  }

  @Bean
  public BiFunction<
          KStream<String, BookingResponse>,
          KStream<String, CorrelationEntry>,
          KStream<String, PostingConfirmedEvent>>
      responseFromBooking(
          final KeyValueMapper<
                  String, KeyValue<CorrelationEntry, BookingResponse>, KeyValue<String, Object>>
              responseCorrelationMapper,
          final KeyValueMapper<
                  String,
                  KeyValue<CorrelationEntry, BookingResponse>,
                  KeyValue<String, PostingConfirmedEvent>>
              bookingResponseMapper) {
    return (bookingResponseStream, correlationEntryStream) -> {
      var correlationStore =
          Materialized.<String, CorrelationEntry>as(
                  Stores.inMemoryKeyValueStore(CORRELATION_STATE_STORE_NAME))
              .withKeySerde(Serdes.String())
              .withValueSerde(new JsonSerde<>(CorrelationEntry.class))
              .withCachingEnabled()
              .withLoggingDisabled();

      var correlationEntryTable =
          correlationEntryStream.toTable(Named.as("correlation-store"), correlationStore);

      var correlatedStream =
          bookingResponseStream.join(
              correlationEntryTable,
              (bookingResponse, correlationEntry) ->
                  KeyValue.pair(correlationEntry, bookingResponse),
              Joined.as("correlate-booking-response"));

      // delete the correlated entry
      correlatedStream
          .map(responseCorrelationMapper, Named.as("response-correlation-mapper"))
          .to(CORRELATION_TOPIC, Produced.as("purge-correlation-sink"));

      // send response to posting
      return correlatedStream.map(bookingResponseMapper, Named.as("booking-response-mapper"));
    };
  }
}
