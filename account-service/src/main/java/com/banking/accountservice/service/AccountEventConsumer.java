package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    /**
     * consume transaction complete event from kafka
     * receiver account -> credit event
     * @param payload
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {

        try {
            String receiverAccount = (String) payload.get("receiverAccount");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} with amount: {}", receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);
        } catch (Exception e) {
            log.error("Error crediting amount to account number: {}", e.getMessage());
        }
    }

    /**
     * method triggered to block the sender account
     * as fraud is detected through kafka
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - blocking the account: {}", accountNumber);
            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error blocking the account: {}", e.getMessage());
        }
    }

}
