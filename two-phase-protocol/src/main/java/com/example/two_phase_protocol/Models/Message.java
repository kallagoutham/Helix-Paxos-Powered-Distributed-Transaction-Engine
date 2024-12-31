package com.example.two_phase_protocol.Models;

import lombok.Data;

@Data
public class Message {
	
	private String type;
	private int n;
	private Transaction t;
	
}
