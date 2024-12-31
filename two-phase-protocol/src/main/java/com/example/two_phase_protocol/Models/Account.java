package com.example.two_phase_protocol.Models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class Account {
	
	@Id
	private Long accountId;
    private Long accNo;
    private int balance;
    
}
