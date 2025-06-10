package de.thi.mall.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.util.Date;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Transaction {
    private String firstName;
    private String lastName;
    private String customerId;
    private String creditCardNumber;
    private String itemPurchased;
    private String department;
    private int quantity;
    private double price;
    private Date purchaseDate;
    private String zipCode;

    //私有化无参构造 避免使用构造函数进行对象创建
    private Transaction() {

    }


    //build()方法调用的时候
    public Transaction(Builder builder) {
        this.firstName = builder.firstName;
        this.lastName = builder.lastName;
        this.customerId = builder.customerId;
        this.creditCardNumber = builder.creditCardNumber;
        this.itemPurchased = builder.itemPurchased;
        this.department = builder.department;
        this.quantity = builder.quantity;
        this.price = builder.price;
        this.purchaseDate = builder.purchaseDate;
        this.zipCode = builder.zipCode;

    }


    //对外提供Builder
    public static Builder newBuilder() {
        return new Builder();
    }

    //为了避免每一次操作都是同一个对象 可能会导致安全问题
    public static Builder newBuilder(Transaction transaction) {
        Builder builder = new Builder();
        builder.firstName = transaction.firstName;
        builder.lastName = transaction.lastName;
        builder.customerId = transaction.customerId;
        builder.creditCardNumber = transaction.creditCardNumber;
        builder.itemPurchased = transaction.itemPurchased;
        builder.department = transaction.department;
        builder.quantity = transaction.quantity;
        builder.price = transaction.price;
        builder.purchaseDate = transaction.purchaseDate;
        builder.zipCode = transaction.zipCode;
        return builder;
    }


    @NoArgsConstructor
    public static class Builder {
        private String firstName;
        private String lastName;
        private String customerId;
        private String creditCardNumber;
        private String itemPurchased;
        private String department;
        private int quantity;
        private double price;
        private Date purchaseDate;
        private String zipCode;
        private static final String MASK_CARD_NUMBER = "xxxx-xxxx-xxxx-";

        public Builder firstName(String firstName) {
            this.firstName = firstName;
            return this;
        }

        public Builder lastName(String lastName) {
            this.lastName = lastName;
            return this;
        }

        public Builder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder creditCardNumber(String creditCardNumber) {
            this.creditCardNumber = creditCardNumber;
            return this;
        }

        public Builder itemPurchased(String itemPurchased) {
            this.itemPurchased = itemPurchased;
            return this;
        }

        public Builder department(String department) {
            this.department = department;
            return this;
        }

        public Builder quantity(int quantity) {
            this.quantity = quantity;
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

        public Builder maskCreditCardNumber() {
            this.creditCardNumber = "XXXX-XXXX-XXXX-" + this.creditCardNumber.substring(this.creditCardNumber.length() - 4);
            return this;
        }

        public Transaction build() {
            return new Transaction(this);
        }

    }


    public static void main(String[] args) throws JsonProcessingException {
        Builder builder = new Builder()
                .firstName("Max")
                .lastName("Mustermann")
                .customerId("C12622536733883")
                .creditCardNumber("1234-5678-9012-3456")
                .itemPurchased("coffee")
                .department("coffee")
                .quantity(1)
                .price(9.9)
                .purchaseDate(new Date())
                .zipCode("12345")
                .maskCreditCardNumber();

        String s = new ObjectMapper().writeValueAsString(builder.build());
        System.out.println("s = " + s);

        Transaction tr2 = new Builder()
                .firstName("John")
                .lastName("Doe")
                .customerId("123")
                .creditCardNumber("1234567890123456")
                .itemPurchased("Apple")
                .department("electic")
                .quantity(1)
                .price(49999.0)
                .purchaseDate(new Date())
                .zipCode("12345").build();

        String s2 = new ObjectMapper().writeValueAsString(tr2);
        System.out.println("s = " + s2);

    }

}
