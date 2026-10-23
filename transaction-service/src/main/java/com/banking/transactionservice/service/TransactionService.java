package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entitiy.Transaction;
import com.banking.transactionservice.entitiy.TransactionStatus;
import com.banking.transactionservice.entitiy.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;


    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";


    /**
     * SAGA Step - 1: Initiate transfer
     * deducts money from the sender account via feign
     * saves transaction as PROCESSING
     * and publish event to kafka for fraud check
     * @param request
     * @return
     */
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA start - Transfer: {} -> {} amount: {}", request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // SAGA - Step 1: deduct amount from sender
        accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount()
        );

        Transaction transaction = Transaction.builder()
                .senderAccountNumber(request.getSenderAccountNumber())
                .receiverAccountNumber(request.getReceiverAccountNumber())
                .amount(request.getAmount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(request.getDescription())
                .referenceNumber(UUID.randomUUID().toString())
                .build();

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("transaction: {} saved with processing", savedTransaction.getId());

        // SAGA: Step-2 publish event in kafka for fraud detection
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA step-2 - Transaction initiated event published in kafka: {}", savedTransaction.getId());
        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        log.info("fetching transaction details: {}", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("transaction not found with id: " + transactionId));
        return mapToResponse(transaction);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }


    public TransactionResponse verifyOtp(String transactionId, String otp) {

        log.info("otp verification for the transaction: {}", transactionId);

        TransactionResponse response;

        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("transaction not found with id: " + transactionId));

        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null) {
            // otp expired
            log.warn("OTP is expired for transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded!");
            return mapToResponse(transaction);
        }

        if(!storedOtp.equals(otp)) {
            // BLOCK account and refund
            log.warn("Wrong otp - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction,
                    "Wrong otp entered - transaction cancelled, "
                    + "account blocked for security reasons."
            );
            return mapToResponse(transaction);
        }

        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amount {}", transaction.getSenderAccountNumber(), transaction.getAmount());

        // money credited back to the sender account
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount()
                );
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(
                reason + " - SAGA compensation executed, amount refunded at "+ LocalDateTime.now()
        );
        transactionRepository.save(transaction);

        // publishing the event for the notification service to show the alerts
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA compensating refund process completed: {}", transaction.getId());
    }

    /**
     * publish the event that fraud is detected and block the account
     * @param transaction
     * @param reason
     */
    private void blockAccountAndCompensate(Transaction transaction, String reason) {

        // publishing the fraud event
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("amount", transaction.getAmount());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);
        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);

        log.warn("fraud.detected published - account: {} will be blocked, kindly contact the bank", transaction.getSenderAccountNumber());

        // SAGA compensate: refund transaction
        compensateTransaction(transaction, reason);
    }

    private void completeTransaction(Transaction transaction) {

        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);
        log.info("SAGA complete - Transaction {} completed", transaction.getId());
    }


    private TransactionResponse mapToResponse(Transaction transaction) {
        return TransactionResponse
                .builder()
                .id(transaction.getId())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .amount(transaction.getAmount())
                .type(transaction.getType())
                .status(transaction.getStatus())
                .description(transaction.getDescription())
                .referenceNumber(transaction.getReferenceNumber())
                .failureReason(transaction.getFailureReason())
                .createdAt(transaction.getCreatedAt())
                .completedAt(transaction.getCompletedAt())
                .build();
    }

}
