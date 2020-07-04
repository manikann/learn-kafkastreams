package io.nataman.learn.kafkastreams;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
class CorrelationEntry {
  String paymentUID;
  String correlationId;
}
