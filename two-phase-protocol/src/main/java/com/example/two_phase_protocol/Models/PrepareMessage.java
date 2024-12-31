package com.example.two_phase_protocol.Models;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class PrepareMessage {
	
	private String type;
	private int n;
	private Transaction t;
	private List<Transaction> committedTransactions = new ArrayList<>();
	
}
