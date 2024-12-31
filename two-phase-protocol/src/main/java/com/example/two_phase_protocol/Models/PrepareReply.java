package com.example.two_phase_protocol.Models;

import lombok.Data;

@Data
public class PrepareReply {

	private String type;
	private int n;
	private Transaction acceptNum;
	private int acceptVal;

}
