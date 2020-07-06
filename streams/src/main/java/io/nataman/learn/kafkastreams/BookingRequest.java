package io.nataman.learn.kafkastreams;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
class BookingRequest {
  String bookingRequestId;
  String payload;
}
