package io.nataman.learn.kafkastreams;

import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Log4j2
@Configuration(proxyBeanMethods = false)
class StreamConfiguration {

  static final String CORRELATION_STATE_STORE_NAME = "correlation-store";
  static final String REQUEST_TRANSFORMER_PROCESSOR = "requestTransformer";
  static final String RESPONSE_TRANSFORMER_PROCESSOR = "responseTransformer";

  // this has to be public, for spring to interrogate this method
  @Bean
  public Function<KStream<String, PostingRequestedEvent>, KStream<String, BookingRequest>>
      requestToBooking(
          Transformer<String, PostingRequestedEvent, KeyValue<String, BookingRequest>>
              requestTransformer) {
    return input ->
        input.transform(
            () -> requestTransformer,
            Named.as(REQUEST_TRANSFORMER_PROCESSOR),
            CORRELATION_STATE_STORE_NAME);
  }

  // this has to be public, for spring to interrogate this method
  @Bean
  public Function<KStream<String, BookingResponse>, KStream<String, PostingConfirmedEvent>>
      responseFromBooking(
          Transformer<String, BookingResponse, KeyValue<String, PostingConfirmedEvent>>
              responseTransformer) {
    return input ->
        input.transform(
            () -> responseTransformer,
            Named.as(RESPONSE_TRANSFORMER_PROCESSOR),
            CORRELATION_STATE_STORE_NAME);
  }

  @Bean
  StoreBuilder<KeyValueStore<String, CorrelationEntry>> correlationStore() {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(CORRELATION_STATE_STORE_NAME),
        Serdes.String(),
        new JsonSerde<>());
  }
}
