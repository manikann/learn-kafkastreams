package io.nataman.learn.kafkastreams;

import static io.nataman.learn.kafkastreams.StreamConfiguration.CORRELATION_STATE_STORE_NAME;
import static io.nataman.learn.kafkastreams.StreamConfiguration.CORRELATION_TIMEOUT_SINK;
import static io.nataman.learn.kafkastreams.StreamConfiguration.TIMEOUT_DURATION;

import java.time.Duration;
import java.time.Instant;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class TimeoutProcessor implements Processor<String, CorrelationEntry> {

  @SuppressWarnings("unchecked")
  @Override
  public void init(final ProcessorContext context) {
    var correlationEntryStore =
        (KeyValueStore<String, ValueAndTimestamp<CorrelationEntry>>)
            context.getStateStore(CORRELATION_STATE_STORE_NAME);
    context.schedule(
        TIMEOUT_DURATION,
        PunctuationType.WALL_CLOCK_TIME,
        timestamp -> {
          log.debug("Punctuating at {}", Instant.ofEpochMilli(timestamp));
          correlationEntryStore
              .all()
              .forEachRemaining(
                  keyValueAndTimestamp -> {
                    var bookingRequestId = keyValueAndTimestamp.key;
                    var correlationEntryTimestamp = keyValueAndTimestamp.value.timestamp();
                    var correlationEntry = keyValueAndTimestamp.value.value();
                    var waitingTime = Duration.ofMillis(timestamp - correlationEntryTimestamp);
                    if (waitingTime.compareTo(TIMEOUT_DURATION) > 0) {
                      log.error(
                          "Timed out; no response received for '{}'; waiting since {}({}) correlationEntry={}",
                          bookingRequestId,
                          Instant.ofEpochMilli(correlationEntryTimestamp),
                          waitingTime,
                          correlationEntry);
                      context.forward(bookingRequestId, null, To.child(CORRELATION_TIMEOUT_SINK));
                    }
                  });
        });
  }

  @Override
  public void process(final String key, final CorrelationEntry value) {}

  @Override
  public void close() {}
}
