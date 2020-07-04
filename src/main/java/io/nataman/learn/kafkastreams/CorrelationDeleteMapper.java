package io.nataman.learn.kafkastreams;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
class CorrelationDeleteMapper
    implements KeyValueMapper<
        String, KeyValue<CorrelationEntry, BookingResponse>, KeyValue<String, Object>> {

  @Override
  public KeyValue<String, Object> apply(
      final String bookingRequestId, final KeyValue<CorrelationEntry, BookingResponse> value) {
    log.debug("Successfully correlated; {} -> {}", value.value, value.key);
    return KeyValue.pair(bookingRequestId, null);
  }
}
