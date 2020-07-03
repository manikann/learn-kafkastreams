package io.nataman.learn.kafkastreams;

import static io.nataman.learn.kafkastreams.StreamConfiguration.CORRELATION_STATE_STORE_NAME;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class RequestTransformer
    implements ValueTransformer<PostingRequestedEvent, BookingRequestedEvent> {

  private KeyValueStore<String, CorrelationEntry> state;

  @SuppressWarnings("unchecked")
  @Override
  public void init(final ProcessorContext context) {
    state =
        (KeyValueStore<String, CorrelationEntry>)
            context.getStateStore(CORRELATION_STATE_STORE_NAME);
    log.debug("init: context={}, state={}", context, state);
  }

  @Override
  public BookingRequestedEvent transform(final PostingRequestedEvent postingRequestedEvent) {
    log.debug("transform: postingRequestedEvent={}", postingRequestedEvent);

    var paymentUID = postingRequestedEvent.getPaymentUID();
    var bookingRequestId = "booking-for-" + paymentUID;
    var correlationId = postingRequestedEvent.getCorrelationId();

    var bookingRequestedEvent =
        BookingRequestedEvent.builder()
            .paymentUID(paymentUID)
            .bookingRequestId(bookingRequestId)
            .bookingRequest("Booking for " + paymentUID)
            .build();

    var correlationEntry =
        CorrelationEntry.builder().correlationId(correlationId).paymentUID(paymentUID).build();

    log.debug("stateStore: key={}, value={}", bookingRequestId, correlationEntry);
    state.put(bookingRequestId, correlationEntry);
    log.debug("transformed: {}", bookingRequestedEvent);
    return bookingRequestedEvent;
  }

  @Override
  public void close() {
    log.debug("close: state={}", state);
  }
}
