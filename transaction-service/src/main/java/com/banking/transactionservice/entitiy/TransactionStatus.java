package com.banking.transactionservice.entitiy;

/**
 * Transaction lifecycle
 * PENDING -> PROCESSING -> COMPLETED (Normal State)
 * PENDING -> PROCESSING -> PENDING_VERIFICATION (suspicious detected)
 *                              -> COMPLETED (verified)
 *                              -> FLAGGED (SAGA - refund)
 *                       -> FAILED
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
