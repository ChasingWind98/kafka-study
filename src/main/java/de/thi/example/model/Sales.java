package de.thi.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Sales {
    private String userName;
    private String department;
    private double salesAmount;
    private double totalSalesAmount;


    public Sales() {

    }


    public Sales(Builder builder) {
        this.userName = builder.userName;
        this.department = builder.department;
        this.salesAmount = builder.salesAmount;
        this.totalSalesAmount = builder.totalSalesAmount;
    }

    public static Builder newBuilder(Sales sales) {
        Builder builder = new Builder();
        builder.userName = sales.userName;
        builder.department = sales.department;
        builder.salesAmount = sales.salesAmount;
        builder.totalSalesAmount = sales.totalSalesAmount;
        return builder;
    }

    public static class Builder {
        private String userName;
        private String department;
        private double salesAmount;
        private double totalSalesAmount;


        public Builder() {

        }

        public Builder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder department(String department) {
            this.department = department;
            return this;
        }

        public Builder salesAmount(double salesAmount) {
            this.salesAmount = salesAmount;
            return this;
        }

        public Builder totalSalesAmount(double totalSalesAmount) {
            this.totalSalesAmount = totalSalesAmount;
            return this;
        }

        public Builder accumulate(double historyAmount) {
            this.totalSalesAmount = this.salesAmount + historyAmount;
            return this;
        }

        public Sales build() {
            return new Sales(this);
        }
    }


    public static void main(String[] args) throws JsonProcessingException {
        Sales sale1 = new Builder()
                .userName("Max")
                .department("Electronics")
                .salesAmount(100.0)
                .build();




        Sales sale2 = new Builder()
                .userName("Alex")
                .department("Electronics")
                .salesAmount(99.0)
                .build();

        Sales sale3 = new Builder()
                .userName("Jenny")
                .department("Food")
                .salesAmount(189)
                .build();

        Sales sale4 = new Builder()
                .userName("Jenny")
                .department("Food")
                .salesAmount(673)
                .build();
        System.out.println("sales = " + new ObjectMapper().writeValueAsString(sale1));
        System.out.println("sales = " + new ObjectMapper().writeValueAsString(sale2));
        System.out.println("sales = " + new ObjectMapper().writeValueAsString(sale3));
        System.out.println("sales = " + new ObjectMapper().writeValueAsString(sale4));

    }

}
