package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entitiy.Transaction;
import com.banking.transactionservice.entitiy.TransactionStatus;
import com.banking.transactionservice.entitiy.TransactionType;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";


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


    // to be completed later
    public TransactionResponse verifyOtp(String transactionId, String otp) {
        return null;
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
