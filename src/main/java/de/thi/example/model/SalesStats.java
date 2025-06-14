package de.thi.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class SalesStats {
    private String department;
    private double totalSalesAmount;
    private double averageSalesAmount;
    private int count;


    public SalesStats() {
    }


    public SalesStats(Builder builder) {
        this.department = builder.department;
        this.totalSalesAmount = builder.totalSalesAmount;
        this.averageSalesAmount = builder.averageSalesAmount;
        this.count = builder.count;
    }


    public static class Builder {
        private String department;
        private double totalSalesAmount;
        private double averageSalesAmount;
        private int count;

        public Builder() {

        }

        public Builder department(String department) {
            this.department = department;
            return this;
        }

        public Builder totalSalesAmount(double totalSalesAmount) {
            this.totalSalesAmount = totalSalesAmount;
            return this;
        }

        public Builder averageSalesAmount(double averageSalesAmount) {
            this.averageSalesAmount = averageSalesAmount;
            return this;
        }

        public Builder count(int count) {
            this.count = count;
            return this;
        }

        public SalesStats build() {
            return new SalesStats(this);
        }
    }


    public static void main(String[] args) throws JsonProcessingException {
        SalesStats electronics = new Builder()
                .department("Electronics")
                .totalSalesAmount(1000)
                .averageSalesAmount(100)
                .count(10).build();
        System.out.println(new ObjectMapper().writeValueAsString(electronics));
    }
}
