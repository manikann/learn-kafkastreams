package io.nataman.learn.kafkastreams;

import java.util.Objects;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class BookingResponseTransformer
    implements Transformer<String, BookingResponse, KeyValue<String, PostingConfirmedEvent>> {

  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<PostingRequestedEvent>> stateStore;

  @Override
  public void init(final ProcessorContext context) {
    this.context = context;
    //noinspection unchecked
    stateStore =
        (KeyValueStore<String, ValueAndTimestamp<PostingRequestedEvent>>)
            context.getStateStore(StreamConfiguration.CORRELATION_STATE_STORE_NAME);
    // todo: timeout punctuator
  }

  @Override
  public KeyValue<String, PostingConfirmedEvent> transform(
      final String bookingRequestId, final BookingResponse bookingResponse) {

    var correlatedRecord = Objects.requireNonNull(stateStore.get(bookingRequestId));
    var postingRequestedEvent = correlatedRecord.value();
    log.debug("stateStore: bookingRequestId={}, {}", bookingRequestId, correlatedRecord);

    // delete the matched entries
    stateStore.delete(bookingRequestId);

    var confirmedEvent =
        PostingConfirmedEvent.builder()
            .correlationId(postingRequestedEvent.getCorrelationId())
            .status(bookingResponse.getStatus())
            .payload("Posting complted for " + postingRequestedEvent.getPaymentUID())
            .build();

    // change the key to paymentUID
    return KeyValue.pair(postingRequestedEvent.getPaymentUID(), confirmedEvent);
  }

  @Override
  public void close() {}
}
