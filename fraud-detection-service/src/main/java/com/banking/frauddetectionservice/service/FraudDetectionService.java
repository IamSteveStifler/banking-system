package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${fraud.threshold-multiplier}")
    private int thresholdMultiplier;

    @Value("${fraud.max-amount-percentage}")
    private double maxAmountPercentage;

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());


        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);

        log.info("Checking transaction: {} account: {} amount: {} balance: {}",
                transactionId, accountNumber, amount, senderBalance);

        FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBalance);

        if(result.isFraud()) {

            log.info("Suspicious activity detected transaction: {}, account: {}, reason: {}. requesting OTP verification",
                    transactionId, accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);

        } else {
            log.info("No suspicious activity found: {}", transactionId);

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", null);

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);

        }

    }

    /**
     * method to check if a particular transaction is fraud or not in 3 patterns
     * 1. frequency of transaction - at least 5 transaction in 60 sec
     * 2. big amount than avg, - 10,000 , 15,000, 20000 e.t.c
     * 3. if an amount is at least 90% of the balance amount.
     * @param accountNumber
     * @param amount
     * @param senderBalance
     * @return
     */
    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount, BigDecimal senderBalance) {

        if(isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(
                    true,
                    "Too many transaction in 60 seconds"
            );
        }

        if(isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(
                    true,
                    "Unusual transaction amount exceeds 3x than average"
            );
        }

        if(senderBalance.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBalance, amount)) {
            return new FraudCheckResult(
                    true,
                    "transaction is at least 90% of the balance"
            );
        }

        return new FraudCheckResult(
                false,
                null
        );
    }

    private boolean isVelocityExceeded(String accountNumber) {

        String key = "fraud:velocity" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count != null && count == 1) {
            redisTemplate.expire(key, Expiration.from(Duration.ofSeconds(60)));
        }
        log.info("frequency check - amount: {} count: {} / {}", accountNumber, count, maxTransactionPerMinute);

        return (count != null && count > maxTransactionPerMinute);
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {

        String avgKey = "fraud:avg_amount"+accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if(avgStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }

        BigDecimal avgAmountTillNow = new BigDecimal(avgStr);
        BigDecimal newAvgAmount = avgAmountTillNow.add(amount)
                .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        BigDecimal threshold = avgAmountTillNow.multiply(
                BigDecimal.valueOf(thresholdMultiplier)
        );


        redisTemplate.opsForValue().set(avgKey, newAvgAmount.toString());
        log.info("Avg. transaction amount check: {} averageNow: {} threshold: {} and suspicious: {}",
                accountNumber, newAvgAmount, threshold, amount.compareTo(threshold) > 0);

        return amount.compareTo(threshold) > 0;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal ratio = amount.divide(senderBalance, 4, RoundingMode.HALF_UP);

        log.info("Max percentage allowed: {} current transaction amount ratio: {}", maxAmountPercentage, ratio);

        return (ratio.compareTo(new BigDecimal(maxAmountPercentage)) >= 0);
    }


}
