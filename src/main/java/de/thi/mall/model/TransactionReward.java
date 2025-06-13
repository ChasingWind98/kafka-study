package de.thi.mall.model;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class TransactionReward {
    private  String customerId;
    private  double purchaseAmount;
    private  int rewardAmount;
    private int rewardTotal;

    public TransactionReward() {
    }
    public TransactionReward(Builder buider) {
        this.customerId = buider.customerId;
        this.purchaseAmount = buider.purchaseAmount;
        this.rewardAmount = buider.rewardAmount;
        this.rewardTotal = buider.rewardTotal;
    }

    public static Builder newBuilder(Transaction transaction) {
        return new Builder(transaction);
    }

    public static Builder newBuilder(TransactionReward reward) {
        return new Builder(reward);
    }

    public static class Builder {
        private  String customerId;
        private  double purchaseAmount;
        private  int rewardAmount;
        private int rewardTotal;

        private Builder() {
        }

        private Builder(Transaction transaction) {
            this.customerId = transaction.getCustomerId();
            this.purchaseAmount = transaction.getPrice() * transaction.getQuantity();
            this.rewardAmount = (int) purchaseAmount;

        }

        private Builder(TransactionReward reward) {
            this.customerId = reward.getCustomerId();
            this.purchaseAmount = reward.getPurchaseAmount();
            this.rewardAmount = reward.getRewardAmount();
            this.rewardTotal = reward.getRewardTotal();

        }


        public Builder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder purchaseAmount(double purchaseAmount) {
            this.purchaseAmount = purchaseAmount;
            return this;
        }

        public Builder rewardAmount(int rewardAmount) {
            this.rewardAmount = rewardAmount;
            return this;
        }

        public Builder rewardTotal(int rewardTotal) {
            this.rewardTotal = rewardTotal;
            return this;
        }


        public TransactionReward build() {
            return new TransactionReward(this);
        }


    }


}
