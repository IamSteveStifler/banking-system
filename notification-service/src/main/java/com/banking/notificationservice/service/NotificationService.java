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
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");
            String otp = payload.get("otp").toString();

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. " +
                            "Reason: %s" +
                            "A transaction of %s is pending verification. " +
                            "Your OTP is: %s. valid for 5 minutes. "+
                            "If this wasn't you please ignore.", reason, amount, otp
                    )
            );
        } catch (Exception e) {
            log.error("Error sending otp notification {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
    ) {
        try {

            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            // debited account notification
            sendAlert(
                    senderAccountNumber,
                    "DEBIT ALERT",
                    String.format(
                            "%s debited from account %s", amount, senderAccountNumber
                    )
            );

            // credited account notification
            sendAlert(
                    receiverAccountNumber,
                    "CREDIT ALERT",
                    String.format(
                            "%s credited to  account %s", amount, receiverAccountNumber
                    )
            );

        } catch (Exception e) {
            log.error("Some error occurred in consumeTransactionCompleted: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload
    ) {
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked. " +
                            "Reason: %s. Please reach ", accountNumber, reason
                    )
            );

        } catch (Exception e) {
            log.error("Some error occurred in consumeFraudDetected: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload
    ) {

        try {

            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(
                    senderAccountNumber,
                    "REFUND",
                    String.format(
                            "Your transaction of %s was cancelled. due to %s ." +
                            "%s has been refunded to your account %s.",
                            amount, reason, amount, senderAccountNumber
                    )
            );

        } catch (Exception e) {
            log.error("Some error occurred in alerting refund transaction: {}", e.getMessage());
        }

    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload
    ) {
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String razorpayPaymentId = (String) payload.get("razorpayPaymentId");

            sendAlert(
                    accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                            "Payment of %s is completed from your account %s ." +
                            "Razorpay payment id %s "
                            ,amount, accountNumber, razorpayPaymentId
                    )
            );

        } catch (Exception e) {
            log.error("Some error occurred in alerting payment complete: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload
    ) {
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(
                    accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Payment of %s is failed from your account %s ." +
                                    "Reason %s ."
                            ,amount, accountNumber, reason
                    )
            );

        } catch (Exception e) {
            log.error("Some error occurred in alerting payment failed: {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message){
        log.info("-----------------------------------------------");
        log.info("Account: {}", accountNumber);
        log.info("subject: {}", subject);
        log.info("message: {}", message);
        log.info("-----------------------------------------------");
    }

}
