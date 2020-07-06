package io.nataman.learn.kafkastreams;

import io.vavr.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
@Log4j2
class BookingResponseMapper
    implements KeyValueMapper<
        String,
        Tuple2<BookingResponse, CorrelationEntry>,
        KeyValue<String, PostingConfirmedEvent>> {

  @Override
  public KeyValue<String, PostingConfirmedEvent> apply(
      final String bookingRequestId, final Tuple2<BookingResponse, CorrelationEntry> value) {
    var bookingResponse = value._1;
    var correlationEntry = value._2;
    log.debug(
        "bookingResponseMapper: bookingId={}, bookingResponse={}, correlationEntry={}",
        bookingRequestId,
        bookingResponse,
        correlationEntry);
    var postingConfirmedEvent =
        PostingConfirmedEvent.builder()
            .correlationId(correlationEntry.getCorrelationId())
            .payload("Booking completed; response: " + bookingRequestId)
            .status(bookingResponse.getStatus())
            .build();
    return KeyValue.pair(correlationEntry.getPaymentUID(), postingConfirmedEvent);
  }
}
