package io.nataman.learn.kafkastreams;

import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PRODUCER_PREFIX;
import static org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER;
import static org.springframework.kafka.support.serializer.JsonDeserializer.REMOVE_TYPE_INFO_HEADERS;
import static org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES;
import static org.springframework.kafka.support.serializer.JsonDeserializer.USE_TYPE_INFO_HEADERS;
import static org.springframework.kafka.support.serializer.JsonSerializer.ADD_TYPE_INFO_HEADERS;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.springframework.cloud.stream.binder.kafka.streams.SendToDlqAndContinue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.kafka.support.serializer.JsonSerde;

@Log4j2
@Configuration(proxyBeanMethods = false)
class StreamInfrastructureConfiguration {

  private static final Map<String, Object> STREAM_CONFIG_OVERRIDE =
      ImmutableMap.<String, Object>builder()
          .put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE)
          .put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
          .put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class)
          .put(PRODUCER_PREFIX + ADD_TYPE_INFO_HEADERS, false)
          .put(
              PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
              AuditProducerInterceptor.class.getName())
          .put(
              CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
              AuditConsumerInterceptor.class.getName())
          .put(CONSUMER_PREFIX + REMOVE_TYPE_INFO_HEADERS, true)
          .put(CONSUMER_PREFIX + USE_TYPE_INFO_HEADERS, false)
          .put(CONSUMER_PREFIX + TRUSTED_PACKAGES, KafkaStreamsApplication.class.getPackageName())
          .put(
              CONSUMER_PREFIX + DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
              RecoveringDeserializationExceptionHandler.class)
          .build();

  @Bean
  KafkaStreamsInfrastructureCustomizer streamsInfrastructureCustomizer() {
    return new KafkaStreamsInfrastructureCustomizer() {
      @Override
      public void configureTopology(final Topology topology) {
        log.debug("topology: {}", topology.describe());
      }
    };
  }

  @Bean
  StreamsBuilderFactoryBeanCustomizer customizer(
      KafkaStreamsInfrastructureCustomizer infrastructureCustomizer,
      StateRestoreListener stateRestoreListener,
      KafkaClientSupplier kafkaClientSupplier,
      SendToDlqAndContinue sendToDlqAndContinue) {
    return factoryBean -> {
      var configProps = Objects.requireNonNull(factoryBean.getStreamsConfiguration());
      configProps.putAll(STREAM_CONFIG_OVERRIDE);
      configProps.put(CONSUMER_PREFIX + KSTREAM_DESERIALIZATION_RECOVERER, sendToDlqAndContinue);
      log.info("streamsConfiguration: {}", configProps);
      factoryBean.setStreamsConfiguration(configProps);
      factoryBean.setInfrastructureCustomizer(infrastructureCustomizer);
      factoryBean.setStateRestoreListener(stateRestoreListener);
      factoryBean.setClientSupplier(kafkaClientSupplier);
      factoryBean.setStateListener(
          (newState, oldState) -> log.debug("stateListener: {} -> {}", oldState, newState));
      factoryBean.setUncaughtExceptionHandler(
          (t, e) -> log.error("exception occured; thread={}", t, e));
    };
  }

  @Bean
  StateRestoreListener stateRestoreLogListener() {
    return new StateRestoreListener() {
      @Override
      public void onRestoreStart(
          final TopicPartition topicPartition,
          final String storeName,
          final long startingOffset,
          final long endingOffset) {
        log.debug(
            "onRestoreStart: topicPartition={}, storeName={}, startingOffset={}, endingOffset={}",
            topicPartition,
            storeName,
            startingOffset,
            endingOffset);
      }

      @Override
      public void onBatchRestored(
          final TopicPartition topicPartition,
          final String storeName,
          final long batchEndOffset,
          final long numRestored) {
        log.debug(
            "onBatchRestored: topicPartition={}, storeName={}, batchEndOffset={}, numRestored={}",
            topicPartition,
            storeName,
            batchEndOffset,
            numRestored);
      }

      @Override
      public void onRestoreEnd(
          final TopicPartition topicPartition, final String storeName, final long totalRestored) {
        log.debug(
            "onRestoreEnd: topicPartition={}, storeName={}, totalRestored={}",
            topicPartition,
            storeName,
            totalRestored);
      }
    };
  }

  @Bean
  KafkaClientSupplier wrappedKafkaClientSupplier() {
    return new LoggingKafkaClientSupplier();
  }

  @Log4j2
  static class LoggingKafkaClientSupplier extends DefaultKafkaClientSupplier {

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
      log.debug("getProducer: {}", config);
      return super.getProducer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
      log.debug("getConsumer: {}", config);
      return super.getConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
      log.debug("getRestoreConsumer: {}", config);
      return super.getRestoreConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
      log.debug("getGlobalConsumer: {}", config);
      return super.getGlobalConsumer(config);
    }
  }
}
