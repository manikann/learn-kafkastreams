package io.nataman.learn.kafkastreams;

import java.time.Instant;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
class PostingConfirmedEvent {
  @Default long eventTime = Instant.now().toEpochMilli();
  String correlationId;
  String payload;
  Status status;
}
