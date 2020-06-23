package com.github.one2story.kafka.streams.entities;

import java.time.Instant;

public class Transaction {

    private String name;
    private int amount;
    private Instant transactionTime;


    public Transaction(String name, int amount, Instant transactionTime) {
        this.name = name;
        this.amount = amount;
        this.transactionTime = transactionTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public Instant getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(Instant transactionTime) {
        this.transactionTime = transactionTime;
    }
}
