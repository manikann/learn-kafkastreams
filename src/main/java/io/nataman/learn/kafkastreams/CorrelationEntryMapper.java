package io.nataman.learn.kafkastreams;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
class CorrelationEntryMapper
    implements KeyValueMapper<
        PostingRequestedEvent, BookingRequest, KeyValue<String, CorrelationEntry>> {

  @Override
  public KeyValue<String, CorrelationEntry> apply(
      final PostingRequestedEvent postingRequestedEvent, final BookingRequest bookingRequest) {
    log.debug(
        "correlationEntryMapper: postingRequestedEvent={}, bookingRequest={}",
        postingRequestedEvent,
        bookingRequest);
    var correlationEntry =
        CorrelationEntry.builder()
            .paymentUID(postingRequestedEvent.getPaymentUID())
            .correlationId(postingRequestedEvent.getCorrelationId())
            .build();
    return KeyValue.pair(bookingRequest.getBookingRequestId(), correlationEntry);
  }
}
