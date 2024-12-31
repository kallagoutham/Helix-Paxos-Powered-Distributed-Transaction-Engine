package com.example.two_phase_protocol.Controllers;

import java.util.List;
import java.util.Set;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.two_phase_protocol.Models.Message;
import com.example.two_phase_protocol.Models.PrepareMessage;
import com.example.two_phase_protocol.Models.PrepareReply;
import com.example.two_phase_protocol.Models.SynchroniseReply;
import com.example.two_phase_protocol.Models.Transaction;
import com.example.two_phase_protocol.Services.TwoPhaseService;

@RestController
public class TwoPhaseCommitController {
	
	private final TwoPhaseService twoPhaseService;

	public TwoPhaseCommitController(TwoPhaseService twoPhaseService) {
		super();
		this.twoPhaseService = twoPhaseService;
	}
	
	@PostMapping("/bank/transaction")
	public ResponseEntity<String> processTransaction(@RequestBody Transaction transaction){
		if(twoPhaseService.processTransaction(transaction)) {
			return ResponseEntity.status(200).body("Transaction Processed");
		}
		return ResponseEntity.status(501).body("Unable to process Transaction");
	}
	
	@PostMapping("/bank/cross/transaction")
	public ResponseEntity<String> processCrossShardTransaction(@RequestBody Transaction transaction){
		if(twoPhaseService.processCrossShardTransaction(transaction)) {
			return ResponseEntity.status(200).body("Cross Shard Transaction success");
		}
		return ResponseEntity.status(501).body("Unable to process Cross-Shard Transaction");
	}
	
	@PostMapping("/paxos/prepare")
	public PrepareReply processPreparePhase(@RequestBody PrepareMessage prepareMessage) {
		return twoPhaseService.processPreparePhase(prepareMessage);
	}
	
	@PostMapping("/paxos/cross/prepare")
	public PrepareReply processCrossPreparePhase(@RequestBody PrepareMessage prepareMessage) {
		return twoPhaseService.processCrossPreparePhase(prepareMessage);
	}
	
	@PostMapping("/paxos/accept")
	public Message processAcceptPhase(@RequestBody Message acceptMessage) {
		return twoPhaseService.processAcceptPhase(acceptMessage);
	}
	
	@PostMapping("/paxos/commit")
	public Message processCommitPhast(@RequestBody Message commitMessage) {
		return twoPhaseService.processCommitPhase(commitMessage);
	}
	
	@PostMapping("/paxos/sync")
	public SynchroniseReply syncServers() {
		return twoPhaseService.syncServers();
	}
	
	@PostMapping("/paxos/update")
	public void updateServers(@RequestBody SynchroniseReply message) {
		twoPhaseService.updateServers(message);
	}
	
	@PostMapping("/paxos/cross/commit")
	public ResponseEntity<String> performPaxosCrossCommitPhase(@RequestBody Transaction transaction,@RequestParam(name = "message")String message) {
		if(twoPhaseService.paxosCrossCommit(transaction,message)) {
			return ResponseEntity.status(200).body("Transaction commit success");
		}
		return ResponseEntity.status(501).body("Transaction commit failed");
	}
	
	@GetMapping("/paxos/datastore")
	public List<String> getDataStore(){
		return twoPhaseService.getDataStore();
	}
	
	@GetMapping("/bank/balance")
	public int getBankBalance(@RequestParam(name = "accountId")String accountId) {
		return twoPhaseService.getBalance(Long.parseLong(accountId));
	}
	
	@GetMapping("/account/locks")
	public Set<Long> getAccountLocks(){
		return twoPhaseService.getAccountLocks();
	}
	
}
