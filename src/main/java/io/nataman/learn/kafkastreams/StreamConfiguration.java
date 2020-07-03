package io.nataman.learn.kafkastreams;

import java.time.Instant;
import java.util.function.Function;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Log4j2
@Configuration(proxyBeanMethods = false)
public class StreamConfiguration {

  public static final String CORRELATION_STATE_STORE_NAME = "correlation-store";

  // this has to be public, for spring to interrogate this method
  @Bean
  public Function<KStream<String, PostingRequestedEvent>, KStream<String, BookingRequestedEvent>>
      sendRequestToBooking(
          ValueTransformer<PostingRequestedEvent, BookingRequestedEvent> requestTransformer) {
    return input -> input.transformValues(() -> requestTransformer, CORRELATION_STATE_STORE_NAME);
  }

  @Bean
  StoreBuilder<KeyValueStore<String, CorrelationEntry>> correlationStore() {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(CORRELATION_STATE_STORE_NAME),
        Serdes.String(),
        new JsonSerde<>());
  }
}

@Value
@Builder
class PostingRequestedEvent {
  @Default long eventTime = Instant.now().toEpochMilli();
  String paymentUID;
  String correlationId;
  String postingRequest;
}

@Value
@Builder
class BookingRequestedEvent {
  @Default long eventTime = Instant.now().toEpochMilli();
  String paymentUID;
  String bookingRequestId;
  String bookingRequest;
}

@Value
@Builder
class CorrelationEntry {
  String paymentUID;
  String correlationId;
}
