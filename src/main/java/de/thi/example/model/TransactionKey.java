package de.thi.example.model;

import lombok.*;

import java.util.Date;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class TransactionKey {
    private final String customerId;
    private final Date transactionDate;

}
