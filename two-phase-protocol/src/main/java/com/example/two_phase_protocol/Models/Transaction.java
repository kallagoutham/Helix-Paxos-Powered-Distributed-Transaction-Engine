package com.example.two_phase_protocol.Models;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;


@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long transactionId;
    private Long sender;
    private Long receiver;
    private int amount;
    private long timestamp;
    private String isIntraShard;
    private String grp;
    private int BallotNumber;
    private int server;
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return amount == that.amount &&
               timestamp == that.timestamp &&
               Objects.equals(sender, that.sender) &&
               Objects.equals(receiver, that.receiver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sender, receiver, amount, timestamp);
    }
    
}