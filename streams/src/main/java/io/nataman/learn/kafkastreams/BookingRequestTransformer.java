package io.nataman.learn.kafkastreams;

import java.time.Instant;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class BookingRequestTransformer
    implements Transformer<String, PostingRequestedEvent, KeyValue<String, BookingRequest>> {

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
  public KeyValue<String, BookingRequest> transform(
      final String bookingRequestId, final PostingRequestedEvent postingRequestedEvent) {

    var valueAndTimestamp =
        ValueAndTimestamp.make(postingRequestedEvent, Instant.now().toEpochMilli());
    // store the request and booking request id in state store
    stateStore.put(bookingRequestId, valueAndTimestamp);

    log.debug("stateStore: bookingRequestId={}, {}", bookingRequestId, valueAndTimestamp);

    var bookingRequest =
        BookingRequest.builder()
            .bookingRequestId(bookingRequestId)
            .payload("Booking request for " + postingRequestedEvent.getPaymentUID())
            .build();

    return KeyValue.pair(bookingRequestId, bookingRequest);
  }

  @Override
  public void close() {}
}
