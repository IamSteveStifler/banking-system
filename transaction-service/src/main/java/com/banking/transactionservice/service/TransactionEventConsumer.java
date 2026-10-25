package com.banking.transactionservice.service;

import com.banking.transactionservice.entitiy.Transaction;
import com.banking.transactionservice.entitiy.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {


    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final long OTP_EXPIRATION_MINUTES = 5;
    private static final String OTP_VERIFICATION_TOPIC = "otp.verification";

    /**
     * method to consume verification.required event
     * generate otp and ask user for verification
     * @param payload
     */
    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(
            @Payload Map<String, Object> payload
            ) {

        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transactionId: {} reason: {}"
                    ,transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));

            if(transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.warn("Transaction: {} is not in processing state so rolling back", transactionId);
                return;
            }

            // otp generation
            String otp = String.valueOf(
                    100000 + new SecureRandom().nextInt(900000)
            );

            // storing in redis
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRATION_MINUTES, TimeUnit.MINUTES);

            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("Otp generated and pending for verification: {}", transactionId);

            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("amount", payload.get("amount"));
            otpEvent.put("reason", payload.get("reason"));
            otpEvent.put("otp", otp);

            kafkaTemplate.send(OTP_VERIFICATION_TOPIC, transactionId, otpEvent);

        } catch (Exception e) {
            log.error("Error occurred in otp generation {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCleanCheckResult(
        @Payload Map<String, Object> payload) {

        try {
            String transactionId = (String) payload.get("transactionId");

            transactionService.processCleanTransaction(transactionId);

        } catch (Exception e) {
            log.error("Error processing clean check transaction {}, {}",
                    payload.get("transactionId") ,e.getMessage());
        }


    }

}
