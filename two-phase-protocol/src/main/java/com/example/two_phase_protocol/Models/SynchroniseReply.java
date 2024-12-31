package com.example.two_phase_protocol.Models;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class SynchroniseReply {
	private int BallotNumber;
	private List<Transaction> transactions = new ArrayList<>();
}
