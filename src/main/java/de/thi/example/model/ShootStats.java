package de.thi.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ShootStats {
    private String playerName;
    private int count;
    private int bestScore;
    private int lastScore;
    private String status;


    public ShootStats(Builder builder) {
        this.playerName = builder.playerName;
        this.count = builder.count;
        this.bestScore = builder.bestScore;
        this.lastScore = builder.lastScore;
        this.status = builder.status;

    }


    @NoArgsConstructor
    public static class Builder {
        private String playerName;
        private int count;
        private int bestScore;
        private int lastScore;
        private String status;


        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(ShootStats shoot) {
            Builder builder = new Builder();
            builder.playerName = shoot.playerName;
            builder.count = shoot.count;
            builder.bestScore = shoot.bestScore;
            builder.lastScore = shoot.lastScore;
            builder.status = shoot.status;
            return builder;
        }



        public Builder playerName(String playerName) {
            this.playerName = playerName;
            return this;
        }

        public Builder count(int count) {
            this.count = count;
            return this;
        }

        public Builder bestScore(int bestScore) {
            this.bestScore = bestScore;
            return this;
        }

        public Builder lastScore(int lastScore) {
            this.lastScore = lastScore;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }



        public ShootStats build() {
            return new ShootStats(this);
        }

    }


    public static void main(String[] args) throws JsonProcessingException {

    }
}
