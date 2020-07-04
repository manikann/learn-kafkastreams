package io.nataman.learn.kafkastreams;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
class BookingRequestMapper
    implements KeyValueMapper<
        String, PostingRequestedEvent, KeyValue<PostingRequestedEvent, BookingRequest>> {

  @Override
  public KeyValue<PostingRequestedEvent, BookingRequest> apply(
      final String readOnlyPaymentUID, final PostingRequestedEvent postingRequestedEvent) {
    var bookingRequest =
        BookingRequest.builder()
            .bookingRequestId(readOnlyPaymentUID + "-b")
            .payload("Booking request for " + readOnlyPaymentUID)
            .build();
    return KeyValue.pair(postingRequestedEvent, bookingRequest);
  }
}
