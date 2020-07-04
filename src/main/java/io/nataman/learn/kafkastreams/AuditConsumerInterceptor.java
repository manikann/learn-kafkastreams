package io.nataman.learn.kafkastreams;

import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Log4j2
public class AuditConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  @Override
  public ConsumerRecords<K, V> onConsume(final ConsumerRecords<K, V> records) {
    records.iterator().forEachRemaining(r -> log.debug("onConsume: {}", r));
    return records;
  }

  @Override
  public void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.debug("onCommit: {}", offsets);
  }

  @Override
  public void close() {
    log.debug("close");
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    log.debug("configure: {}", configs);
  }
}
