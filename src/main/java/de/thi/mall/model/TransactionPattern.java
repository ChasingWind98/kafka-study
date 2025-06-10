package de.thi.mall.model;

import lombok.*;

import java.util.Date;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class TransactionPattern {
    private String itemPurchased;
    private double price;
    private Date purchaseDate;
    private String zipCode;

    public TransactionPattern(Builder builder) {
        this.itemPurchased = builder.itemPurchased;
        this.price = builder.price;
        this.purchaseDate = builder.purchaseDate;
        this.zipCode = builder.zipCode;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Transaction transaction) {
        return new Builder(transaction);
    }


    public static class Builder {
        private String itemPurchased;
        private double price;
        private Date purchaseDate;
        private String zipCode;

        private Builder() {

        }

        private Builder(Transaction transaction) {
            this.itemPurchased = transaction.getItemPurchased();
            this.price = transaction.getPrice();
            this.purchaseDate = transaction.getPurchaseDate();
            this.zipCode = transaction.getZipCode();
        }

        public Builder itemPurchased(String itemPurchased) {
            this.itemPurchased = itemPurchased;
            return this;
        }

        public Builder price(double price) {
            this.price = price;
            return this;
        }

        public Builder purchaseDate(Date purchaseDate) {
            this.purchaseDate = purchaseDate;
            return this;
        }

        public Builder zipCode(String zipCode) {
            this.zipCode = zipCode;
            return this;
        }

        public TransactionPattern build() {
            return new TransactionPattern(this);
        }

    }

}
