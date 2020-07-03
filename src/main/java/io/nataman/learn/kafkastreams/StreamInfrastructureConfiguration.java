package io.nataman.learn.kafkastreams;

import java.util.Map;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.springframework.cloud.stream.binder.kafka.streams.SendToDlqAndContinue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

@Log4j2
@Configuration(proxyBeanMethods = false)
public class StreamInfrastructureConfiguration {

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
      var streamConfiguration = Objects.requireNonNull(factoryBean.getStreamsConfiguration());
      streamConfiguration.put(
          StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
      streamConfiguration.put(
          StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
      streamConfiguration.put(
          StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          RecoveringDeserializationExceptionHandler.class);
      streamConfiguration.put(
          RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
          sendToDlqAndContinue);
      log.debug("streamsConfiguration: {}", streamConfiguration);
      factoryBean.setStreamsConfiguration(streamConfiguration);
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
    return new WrappedKafkaClientSupplier();
  }

  @Log4j2
  static class WrappedKafkaClientSupplier extends DefaultKafkaClientSupplier {

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
