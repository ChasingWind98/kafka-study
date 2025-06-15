package de.thi.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Shoot {
    private String playerName;
    private int score;

    public Shoot(Builder builder) {
        this.playerName = builder.playerName;
        this.score = builder.score;
    }


    @NoArgsConstructor
    public static class Builder {
        private String playerName;
        private int score;


        public static Builder newBuilder() {

            return new Builder();
        }

        public static Builder newBuilder(Shoot shoot) {
            Builder builder = new Builder();
            builder.playerName = shoot.playerName;
            builder.score = shoot.score;

            return builder;
        }

        public Builder playerName(String playerName) {
            this.playerName = playerName;
            return this;
        }

        public Builder score(int score) {
            this.score = score;
            return this;
        }


        public Shoot build() {
            return new Shoot(this);
        }

    }


    public static void main(String[] args) throws JsonProcessingException {
        Shoot max = Builder.newBuilder()
                .playerName("Max")
                .score(6)
                .build();
        System.out.println(new ObjectMapper().writeValueAsString(max));
    }
}
