package io.nataman.learn.kafkastreams;

import java.time.Instant;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
class CorrelationEntry {
  @Default long entryTime = Instant.now().toEpochMilli();
  String paymentUID;
  String correlationId;
}
