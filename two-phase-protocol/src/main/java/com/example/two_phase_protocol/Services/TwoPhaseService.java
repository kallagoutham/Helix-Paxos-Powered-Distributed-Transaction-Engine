package com.example.two_phase_protocol.Services;

import java.util.List;
import java.util.Set;

import com.example.two_phase_protocol.Models.Message;
import com.example.two_phase_protocol.Models.PrepareMessage;
import com.example.two_phase_protocol.Models.PrepareReply;
import com.example.two_phase_protocol.Models.SynchroniseReply;
import com.example.two_phase_protocol.Models.Transaction;

public interface TwoPhaseService {

	public boolean processTransaction(Transaction transaction);
	public PrepareReply processPreparePhase(PrepareMessage prepareMessage);
	public Message processAcceptPhase(Message acceptMessage);
	public Message processCommitPhase(Message commitMessage);
	public SynchroniseReply syncServers();
	public void updateServers(SynchroniseReply message);
	public boolean processCrossShardTransaction(Transaction transaction);
	public List<String> getDataStore();
	public boolean paxosCrossCommit(Transaction transaction,String message);
	public PrepareReply processCrossPreparePhase(PrepareMessage prepareMessage);
	public int getBalance(Long accountId);
	public Set<Long> getAccountLocks();

}
