package io.nataman.learn.kafkastreams;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
class BookingResponse {
  String bookingRequestId;
  Status status;
  String payload;
}
