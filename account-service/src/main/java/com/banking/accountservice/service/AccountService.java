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
