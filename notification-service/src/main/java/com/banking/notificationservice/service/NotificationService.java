package com.banking.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "otp.verification")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload
            ) {

        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");
            String otp = (String) payload.get("otp");

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. " +
                            "Reason: %s" +
                            "A transaction of %s is pending verification. " +
                            "Your OTP is: %s. valid for 5 minutes. "+
                            "If this wasn't you please ignore."
                    ).formatted(reason, amount, otp)
            );
        } catch (Exception e) {
            log.error("Error sending otp notification {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message){}

}
