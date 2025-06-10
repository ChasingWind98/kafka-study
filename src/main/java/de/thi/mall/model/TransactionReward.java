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

    public TransactionReward(Builder buider) {
        this.customerId = buider.customerId;
        this.purchaseAmount = buider.purchaseAmount;
        this.rewardAmount = buider.rewardAmount;
    }

    public static Builder newBuilder(Transaction transaction) {
        return new Builder(transaction);
    }

    public static class Builder {
        private  String customerId;
        private  double purchaseAmount;
        private  int rewardAmount;

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

        }

        public TransactionReward build() {
            return new TransactionReward(this);
        }


    }


}
