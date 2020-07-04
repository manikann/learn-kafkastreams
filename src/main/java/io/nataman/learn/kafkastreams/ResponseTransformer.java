package io.nataman.learn.kafkastreams;

import static io.nataman.learn.kafkastreams.StreamConfiguration.CORRELATION_STATE_STORE_NAME;

import java.util.Objects;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

@Log4j2
@Component
class ResponseTransformer
    implements Transformer<String, BookingResponse, KeyValue<String, PostingConfirmedEvent>> {

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
  public KeyValue<String, PostingConfirmedEvent> transform(
      final String bookingRequestId, final BookingResponse bookingResponse) {
    log.debug("transform: bookingResponse={}", bookingResponse);

    var correlationEntry =
        Objects.requireNonNull(
            state.get(bookingRequestId),
            String.format("Unable to correlate bookingResponse '%s'", bookingResponse));

    log.debug("stateStore: key={}, value={}", bookingRequestId, correlationEntry);

    var postingConfirmedEvent =
        PostingConfirmedEvent.builder()
            .status(bookingResponse.getStatus())
            .correlationId(correlationEntry.getCorrelationId())
            .payload(
                "Response received for " + bookingRequestId + ", correlation " + correlationEntry)
            .build();

    log.debug("transformed: {}", postingConfirmedEvent);
    return KeyValue.pair(correlationEntry.getPaymentUID(), postingConfirmedEvent);
  }

  @Override
  public void close() {
    log.debug("close: state={}", state);
  }
}
