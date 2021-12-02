package ru.on8off.kafka.streams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Payment {
    private Long customerId;
    private Long paymentPageOpenedTime;
    private Long paymentTypeSelectedTime;
    private Long paymentDataFilledTime;
    private Long paymentSucceedTime;
    private Long paymentFailedTime;
    private Long updated;
}
