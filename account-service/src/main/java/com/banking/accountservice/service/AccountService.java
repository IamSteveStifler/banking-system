package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account for: {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("Account already exists for email: " + request.getEmail());
        }

        Account account = Account.builder()
                .accountHolderName(request.getAccountHolderName())
                .email(request.getEmail())
                .phone(request.getPhone())
                .accountType(request.getAccountType())
                .balance(request.getInitialDeposit())
                .accountStatus(AccountStatus.ACTIVE)
                .accountNumber(generateAccountNumber())
                .dailyTransactionLimit(
                        request.getAccountType() == AccountType.SAVINGS
                                ? new BigDecimal(100000)
                                : new BigDecimal(500000)
                )
                .build();
        Account savedAccount = accountRepository.save(account);
        log.info("Account created: {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account number doesn't exists"));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account number doesn't exists"));

        return account.getBalance();
    }

    /**
     * called by fraud detection service
     * to block the account if fraud detected
     * @param accountNumber
     */

    public void blockAccount(String accountNumber) {
        log.info("Blocking account: {}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account number doesn't exists"));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked: {}", accountNumber);
    }

    /**
     * method to deduct balance from sender account
     * caller - transaction service
     * @param accountNumber
     * @param amount
     */
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting balance: {}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account number doesn't exists"));

        if(account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active");
        }

        if(account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient funds for account number: " + accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance debited. New balance: {}", account.getBalance());
    }


    /**
     * method to credit balance in the account
     * produced by transaction service through Kafka
     * @param accountNumber
     * @param amount
     */
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting {} to account {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account number doesn't exists"));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Amount credited to the account number {}. New balance {}", accountNumber, account.getBalance());
    }

    // method to generate unique 12 digit number
    private String generateAccountNumber() {

        String accountNumber;

        do {
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        } while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account) {
        return AccountResponse.builder()
                .id(account.getId())
                .accountHolderName(account.getAccountHolderName())
                .accountNumber(account.getAccountNumber())
                .email(account.getEmail())
                .phone(account.getPhone())
                .accountType(account.getAccountType())
                .accountStatus(account.getAccountStatus())
                .balance(account.getBalance())
                .dailyTransactionLimit(account.getDailyTransactionLimit())
                .createdAt(account.getCreatedAt())
                .build();
    }

}
