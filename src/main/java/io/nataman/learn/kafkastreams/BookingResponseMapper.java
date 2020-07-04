package io.nataman.learn.kafkastreams;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
class BookingResponseMapper
    implements KeyValueMapper<
        String,
        KeyValue<CorrelationEntry, BookingResponse>,
        KeyValue<String, PostingConfirmedEvent>> {

  @Override
  public KeyValue<String, PostingConfirmedEvent> apply(
      final String bookingRequestId, final KeyValue<CorrelationEntry, BookingResponse> value) {
    var correlationEntry = value.key;
    var bookingResponse = value.value;
    var postingConfirmedEvent =
        PostingConfirmedEvent.builder()
            .correlationId(correlationEntry.getCorrelationId())
            .payload("Booking completed; response: " + bookingRequestId)
            .status(bookingResponse.getStatus())
            .build();
    return KeyValue.pair(correlationEntry.getPaymentUID(), postingConfirmedEvent);
  }
}
