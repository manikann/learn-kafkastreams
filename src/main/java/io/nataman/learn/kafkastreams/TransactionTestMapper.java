package io.nataman.learn.kafkastreams;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class TransactionTestMapper implements ValueMapper<BookingRequest, BookingRequest> {
  @Override
  public BookingRequest apply(final BookingRequest value) {
    log.debug("no-op mapper");
    return value;
  }
}
