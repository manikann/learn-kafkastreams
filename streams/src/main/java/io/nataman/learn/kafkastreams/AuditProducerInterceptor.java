package io.nataman.learn.kafkastreams;

import static org.springframework.kafka.support.converter.AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME;

import java.time.Instant;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Log4j2
public class AuditProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  @Override
  public ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
    record.headers().remove(DEFAULT_CLASSID_FIELD_NAME);
    log.debug("onSend: {}", record);
    return record;
  }

  @Override
  public void onAcknowledgement(final RecordMetadata metadata, final Exception exception) {
    log.debug(
        "onAcknowledgement: ts={}, topic={}, partition={}, offset={}",
        Instant.ofEpochMilli(metadata.timestamp()),
        metadata.topic(),
        metadata.partition(),
        metadata.offset());
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
