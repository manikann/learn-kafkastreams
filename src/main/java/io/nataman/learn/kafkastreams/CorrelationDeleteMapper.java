package io.nataman.learn.kafkastreams;

import io.vavr.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
class CorrelationDeleteMapper
    implements KeyValueMapper<
        String, Tuple2<BookingResponse, CorrelationEntry>, KeyValue<String, Object>> {

  @Override
  public KeyValue<String, Object> apply(
      final String bookingRequestId, final Tuple2<BookingResponse, CorrelationEntry> value) {
    log.debug("Successfully correlated; {} -> {}", value._1, value._2);
    return KeyValue.pair(bookingRequestId, null);
  }
}
