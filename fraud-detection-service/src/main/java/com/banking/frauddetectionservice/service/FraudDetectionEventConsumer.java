package com.banking.frauddetectionservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;

    /**
     * method to listen the event for initiated transaction
     * @param payload
     */
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiated(
            @Payload Map<String, Object> payload
    ) {
        log.info("transaction received for fraud detection: {}", payload.get("transactionId"));

        try {
            fraudDetectionService.checkTransaction(payload);
        } catch (Exception ex) {
            log.error("Some error occurred in fraud detection even consumer: {}, {}", payload.get("transactionId"), ex.getMessage());
        }


    }
}
