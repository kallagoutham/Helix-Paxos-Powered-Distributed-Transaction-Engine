package com.example.two_phase_protocol.Services;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.two_phase_protocol.GlobalVariables.Variables;
import com.example.two_phase_protocol.Models.Account;
import com.example.two_phase_protocol.Models.Message;
import com.example.two_phase_protocol.Models.PrepareMessage;
import com.example.two_phase_protocol.Models.PrepareReply;
import com.example.two_phase_protocol.Models.SynchroniseReply;
import com.example.two_phase_protocol.Models.Transaction;
import com.example.two_phase_protocol.Repository.AccountRepository;
import com.example.two_phase_protocol.Repository.TransactionRepository;
import com.example.two_phase_protocol.Utils.PeerUtils;

@Service
public class TwoPhaseServiceImpl implements TwoPhaseService {

	private final Variables variables;
	private final PerformanceService performanceService;
	private final RestTemplate restTemplate;
	private final TransactionRepository transactionRepository;
	private final AccountRepository accountRepository;
	private final PeerUtils peerUtils;
	private int timeoutInSeconds = 2;

	public TwoPhaseServiceImpl(Variables variables, PerformanceService performanceService, RestTemplate restTemplate,
			TransactionRepository transactionRepository, AccountRepository accountRepository, PeerUtils peerUtils) {
		super();
		this.variables = variables;
		this.performanceService = performanceService;
		this.restTemplate = restTemplate;
		this.transactionRepository = transactionRepository;
		this.accountRepository = accountRepository;
		this.peerUtils = peerUtils;
	}

	@Override
	public boolean processTransaction(Transaction transaction) {
		SynchroniseServers();
		variables.setBallotNumber(variables.getBallotNumber() + 1);
		List<PrepareReply> replies = PreparePhase(variables.getBallotNumber(), transaction,
				transactionRepository.findAll());
		if (replies.size() + 1 >= (variables.getClusterSize() / 2 + 1)) {
			if (!(variables.getLocks().contains(transaction.getSender())
					|| variables.getLocks().contains(transaction.getReceiver()))
					&& (accountRepository.getBalanceByAccount(transaction.getSender()) >= transaction.getAmount())) {
				Message acceptMessage = generateMessage("ACCEPT", variables.getBallotNumber(), transaction);
				List<Message> r = AcceptPhase(acceptMessage);
				if (r.size() + 1 >= (variables.getClusterSize() / 2 + 1)) {
					transaction.setBallotNumber(variables.getBallotNumber());
					transaction.setServer(variables.getPrimaryServers().get(variables.getClusterNo()));
					Message commitMessage = generateMessage("COMMIT", variables.getBallotNumber(), transaction);
					List<Message> reply = CommitPhase(commitMessage);
					if (reply.size() + 1 >= (variables.getClusterSize() / 2 + 1)) {
						variables.getDataStore()
								.add(String.format("[<%d,%d>, (%d, %d, %d)]", variables.getBallotNumber(),
										peerUtils.getServerPort(), transaction.getSender(), transaction.getReceiver(),
										transaction.getAmount()));
						variables.getLocks().remove(transaction.getSender());
						variables.getLocks().remove(transaction.getReceiver());
						executeTransaction(transaction);
						return true;
					} else {
//						variables.getDataStore()
//								.add(String.format("[<%d,%d>, A, (%d, %d, %d)]", variables.getBallotNumber(),
//										peerUtils.getServerPort(), transaction.getSender(), transaction.getReceiver(),
//										transaction.getAmount()));
						System.out.println("Commit Phase Fail");
						return false;
					}
				} else {
//					variables.getDataStore()
//							.add(String.format("[<%d,%d>, A, (%d, %d, %d)]", variables.getBallotNumber(),
//									peerUtils.getServerPort(), transaction.getSender(), transaction.getReceiver(),
//									transaction.getAmount()));
					System.out.println("Accept Phase Fail");
					return false;
				}
			} else {
//				variables.getDataStore()
//						.add(String.format("[<%d,%d>, A, (%d, %d, %d)]", variables.getBallotNumber(),
//								peerUtils.getServerPort(), transaction.getSender(), transaction.getReceiver(),
//								transaction.getAmount()));
				System.out.println("Aborting transactions as one of two conditions are not met...");
				return false;
			}
		}
		return false;
	}

	private List<Message> CommitPhase(Message commitMessage) {

		List<CompletableFuture<Message>> futures = peerUtils.getPeersList().stream()
				.map(url -> CompletableFuture.supplyAsync(() -> {
					try {
						return restTemplate.postForObject("http://" + url + "/api/paxos/commit", commitMessage,
								Message.class);
					} catch (Exception e) {
						return null;
					}
				}).orTimeout(timeoutInSeconds, TimeUnit.SECONDS)).toList();

		performanceService.logTaskEnd(10);
		return futures.stream().map(future -> {
			try {
				return future.get();
			} catch (InterruptedException | ExecutionException e) {
				return null;
			}
		}).filter(result -> result != null).collect(Collectors.toList());

	}

	private List<Message> AcceptPhase(Message acceptMessage) {
		variables.getLocks().add(acceptMessage.getT().getSender());
		variables.getLocks().add(acceptMessage.getT().getReceiver());
		List<CompletableFuture<Message>> futures = peerUtils.getPeersList().stream()
				.map(url -> CompletableFuture.supplyAsync(() -> {
					try {
						return restTemplate.postForObject("http://" + url + "/api/paxos/accept", acceptMessage,
								Message.class);
					} catch (Exception e) {
						return null;
					}
				}).orTimeout(timeoutInSeconds, TimeUnit.SECONDS)).toList();

		performanceService.logTaskEnd(10);

		return futures.stream().map(future -> {
			try {
				return future.get();
			} catch (InterruptedException | ExecutionException e) {
				return null;
			}
		}).filter(result -> result != null).collect(Collectors.toList());
	}

	private Message generateMessage(String type, int ballotNumber, Transaction transaction) {
		performanceService.logTaskStart();
		Message message = new Message();
		message.setType(type);
		message.setN(ballotNumber);
		message.setT(transaction);
		performanceService.logTaskEnd(1);
		return message;
	}

	private List<PrepareReply> PreparePhase(int ballotNumber, Transaction transaction,
			List<Transaction> committedTransactions) {
		performanceService.logTaskStart();
		PrepareMessage prepareMessage = new PrepareMessage();
		prepareMessage.setType("PREPARE");
		prepareMessage.setN(ballotNumber);
		prepareMessage.setT(transaction);
		prepareMessage.setCommittedTransactions(committedTransactions);
		List<CompletableFuture<PrepareReply>> futures = peerUtils.getPeersList().stream()
				.map(url -> CompletableFuture.supplyAsync(() -> {
					try {
						return restTemplate.postForObject("http://" + url + "/api/paxos/prepare", prepareMessage,
								PrepareReply.class);
					} catch (Exception e) {
						return null;
					}
				}).orTimeout(timeoutInSeconds, TimeUnit.SECONDS)).toList();
		performanceService.logTaskEnd(10);
		return futures.stream().map(future -> {
			try {
				return future.get();
			} catch (InterruptedException | ExecutionException e) {
				return null;
			}
		}).filter(result -> result != null).collect(Collectors.toList());
	}

	@Override
	public PrepareReply processPreparePhase(PrepareMessage prepareMessage) {
		performanceService.logTaskStart();
		PrepareReply prepareReply = new PrepareReply();
		prepareReply.setType("ACK");
		if (variables.getBallotNumber() == prepareMessage.getN() - 1) {
			prepareReply.setN(prepareMessage.getN());
			variables.setBallotNumber(prepareMessage.getN());
			prepareReply.setAcceptVal(prepareMessage.getN());
			variables.setAcceptVal(prepareMessage.getN());
			prepareReply.setAcceptNum(prepareMessage.getT());
			variables.setAcceptNum(prepareMessage.getT());
			performanceService.logTaskEnd(1);
			return prepareReply;
		}
		performanceService.logTaskEnd(1);
		return null;
	}

	@Override
	public Message processAcceptPhase(Message acceptMessage) {
		performanceService.logTaskStart();
		variables.getLocks().add(acceptMessage.getT().getSender());
		variables.getLocks().add(acceptMessage.getT().getReceiver());
		acceptMessage.setType("ACCEPTED");
		performanceService.logTaskEnd(1);
		return acceptMessage;
	}

	@Override
	public Message processCommitPhase(Message commitMessage) {
		performanceService.logTaskStart();
		commitMessage.setType("COMMITED");
		variables.getLocks().remove(commitMessage.getT().getSender());
		variables.getLocks().remove(commitMessage.getT().getReceiver());
		Transaction transaction = commitMessage.getT();
		executeTransaction(transaction);
		variables.getDataStore()
				.add(String.format("[<%d,%d>, (%d, %d, %d)]", variables.getBallotNumber(), peerUtils.getServerPort(),
						transaction.getSender(), transaction.getReceiver(), transaction.getAmount()));
		performanceService.logTaskEnd(1);
		return commitMessage;
	}

	private void SynchroniseServers() {
		performanceService.logTaskStart();
		List<CompletableFuture<SynchroniseReply>> futures = peerUtils.getAllServersList().stream()
				.map(url -> CompletableFuture.supplyAsync(() -> {
					try {
						return restTemplate.postForObject("http://" + url + "/api/paxos/sync", null,
								SynchroniseReply.class);
					} catch (Exception e) {
						System.err.println("Error while sending sync request to: " + url);
						return null;
					}
				}).orTimeout(timeoutInSeconds, TimeUnit.SECONDS)).collect(Collectors.toList());
		List<SynchroniseReply> replies = futures.stream().map(future -> {
			try {
				return future.get();
			} catch (InterruptedException | ExecutionException e) {
				System.err.println("Error while getting sync response.");
				e.printStackTrace();
				return null;
			}
		}).filter(result -> result != null).collect(Collectors.toList());
		if (replies.size() >= (variables.getClusterSize() / 2 + 1)) {
			int maxBallotNumber = replies.stream().mapToInt(SynchroniseReply::getBallotNumber).max().orElse(0);

			List<Transaction> mergedTransactions = replies.stream().flatMap(reply -> reply.getTransactions().stream())
					.distinct().collect(Collectors.toList());

			SynchroniseReply updateRequest = new SynchroniseReply();
			updateRequest.setBallotNumber(maxBallotNumber);
			updateRequest.setTransactions(mergedTransactions);

			List<CompletableFuture<Void>> updateFutures = peerUtils.getAllServersList().stream()
					.map(url -> CompletableFuture.runAsync(() -> {
						try {
							restTemplate.postForObject("http://" + url + "/api/paxos/update", updateRequest,
									Void.class);
						} catch (Exception e) {
							System.err.println("Error while sending update to: " + url);
							e.printStackTrace();
						}
					}).orTimeout(timeoutInSeconds, TimeUnit.SECONDS)).collect(Collectors.toList());
			CompletableFuture<Void> allUpdates = CompletableFuture
					.allOf(updateFutures.toArray(new CompletableFuture[0]));
			allUpdates.join();
		}
		performanceService.logTaskEnd(20);
	}

	@Override
	public SynchroniseReply syncServers() {
		SynchroniseReply reply = new SynchroniseReply();
		reply.setBallotNumber(variables.getBallotNumber());
		reply.setTransactions(transactionRepository.findAll());
		return reply;
	}

	@Override
	public void updateServers(SynchroniseReply message) {
		for (Transaction transaction : message.getTransactions()) {
			if (!transactionRepository.contains(transaction.getSender(), transaction.getReceiver(),
					transaction.getAmount(), transaction.getTimestamp())) {
				executeTransaction(transaction);
				variables.getDataStore()
						.add(String.format("[<%d,%d>, (%d, %d, %d)]", transaction.getBallotNumber(),
								transaction.getServer() - 8079, transaction.getSender(), transaction.getReceiver(),
								transaction.getAmount()));

			}
		}
		variables.setBallotNumber(message.getBallotNumber());
		return;
	}

	// ************************************ CROSS - SHARD TRANSACTIONS***************************************

	@Override
	public boolean processCrossShardTransaction(Transaction transaction) {
		SynchroniseServers();
		variables.setBallotNumber(variables.getBallotNumber() + 1);
		transaction.setBallotNumber(variables.getBallotNumber());
		transaction.setServer(variables.getPrimaryServers().get(variables.getClusterNo()));
		List<PrepareReply> replies = CrossPreparePhase(variables.getBallotNumber(), transaction,
				transactionRepository.findAll());
		variables.getWAL().put(variables.getBallotNumber(), transaction);
		//executeTransaction(transaction);
		if (replies.size() + 1 >= (variables.getClusterSize() / 2 + 1)) {
			if ("from".equals(transaction.getGrp())) {
				if (!(variables.getLocks().contains(transaction.getSender())) && (accountRepository
						.getBalanceByAccount(transaction.getSender()) >= transaction.getAmount())) {
					variables.getLocks().add(transaction.getSender());
					variables.getDataStore()
					.add(String.format("[<%d,%d>,P,(%d, %d, %d)]", variables.getBallotNumber(),
							peerUtils.getServerPort(), transaction.getSender(), transaction.getReceiver(),
							transaction.getAmount()));
					return true;
				} else {
					System.out.println("Aborting transactions as one of two conditions are not met...");
					return false;
				}
			}
			if ("to".equals(transaction.getGrp())) {
				if (!(variables.getLocks().contains(transaction.getReceiver()))) {
					variables.getLocks().add(transaction.getReceiver());
					variables.getDataStore()
					.add(String.format("[<%d,%d>,P,(%d, %d, %d)]", variables.getBallotNumber(),
							peerUtils.getServerPort(), transaction.getSender(), transaction.getReceiver(),
							transaction.getAmount()));
					return true;
				} else {
					System.out.println("Aborting transactions as one of two conditions are not met...");
					return false;
				}
			}
		}
		return false;
	}

	private List<PrepareReply> CrossPreparePhase(int ballotNumber, Transaction transaction,
			List<Transaction> committedTransactions) {
		performanceService.logTaskStart();
		PrepareMessage prepareMessage = new PrepareMessage();
		prepareMessage.setType("PREPARE");
		prepareMessage.setN(ballotNumber);
		prepareMessage.setT(transaction);
		prepareMessage.setCommittedTransactions(committedTransactions);
		List<CompletableFuture<PrepareReply>> futures = peerUtils.getPeersList().stream()
				.map(url -> CompletableFuture.supplyAsync(() -> {
					try {
						return restTemplate.postForObject("http://" + url + "/api/paxos/cross/prepare", prepareMessage,
								PrepareReply.class);
					} catch (Exception e) {
						return null;
					}
				}).orTimeout(timeoutInSeconds, TimeUnit.SECONDS)).toList();
		performanceService.logTaskEnd(10);
		return futures.stream().map(future -> {
			try {
				return future.get();
			} catch (InterruptedException | ExecutionException e) {
				return null;
			}
		}).filter(result -> result != null).collect(Collectors.toList());

	}

	@Override
	public PrepareReply processCrossPreparePhase(PrepareMessage prepareMessage) {
		performanceService.logTaskStart();
		PrepareReply prepareReply = new PrepareReply();
		prepareReply.setType("ACK");
		if (variables.getBallotNumber() == prepareMessage.getN() - 1) {
			prepareReply.setN(prepareMessage.getN());
			variables.setBallotNumber(prepareMessage.getN());
			prepareReply.setAcceptVal(prepareMessage.getN());
			variables.setAcceptVal(prepareMessage.getN());
			prepareReply.setAcceptNum(prepareMessage.getT());
			variables.setAcceptNum(prepareMessage.getT());
			if ("from".equals(prepareMessage.getT().getGrp())) {
				if (!variables.getLocks().contains(prepareMessage.getT().getSender())) {
					variables.getLocks().add(prepareMessage.getT().getSender());
					variables.getDataStore()
					.add(String.format("[<%d,%d>,P,(%d, %d, %d)]", variables.getBallotNumber(),
							peerUtils.getServerPort(), prepareMessage.getT().getSender(),
							prepareMessage.getT().getReceiver(), prepareMessage.getT().getAmount()));
					//executeTransaction(prepareMessage.getT());
				}

			}
			if ("to".equals(prepareMessage.getT().getGrp())) {
				if (!variables.getLocks().contains(prepareMessage.getT().getReceiver())) {
					variables.getLocks().add(prepareMessage.getT().getReceiver());
					variables.getDataStore()
					.add(String.format("[<%d,%d>,P,(%d, %d, %d)]", variables.getBallotNumber(),
							peerUtils.getServerPort(), prepareMessage.getT().getSender(),
							prepareMessage.getT().getReceiver(), prepareMessage.getT().getAmount()));
					//executeTransaction(prepareMessage.getT());
				}
			}
			variables.getWAL().put(variables.getBallotNumber(), prepareMessage.getT());
			performanceService.logTaskEnd(1);
			return prepareReply;
		}
		performanceService.logTaskEnd(1);
		return null;
	}

	@Override
	public boolean paxosCrossCommit(Transaction transaction, String message) {
		performanceService.logTaskStart();
		int bn = variables.getWAL().entrySet().stream().filter(entry -> entry.getValue().equals(transaction))
				.map(Map.Entry::getKey).findFirst().orElse(0);

		if (!peerUtils.isDisonnected()) {
			if ("from".equals(transaction.getGrp())) {
				variables.getLocks().remove(transaction.getSender());
			}
			if ("to".equals(transaction.getGrp())) {
				variables.getLocks().remove(transaction.getReceiver());
			}
			if ("ABORT".equals(message)) {
				//reverseTransaction(transaction);
				variables.getDataStore().add(String.format("[<%d,%d>,A,(%d, %d, %d)]", bn, peerUtils.getServerPort(),
						transaction.getSender(), transaction.getReceiver(), transaction.getAmount()));
			} else {
				variables.getDataStore().add(String.format("[<%d,%d>,C,(%d, %d, %d)]", bn, peerUtils.getServerPort(),
						transaction.getSender(), transaction.getReceiver(), transaction.getAmount()));
				executeTransaction(transaction);
				transactionRepository.save(transaction);
				performanceService.logTaskEnd(1);
				return true;
			}
			if (variables.getWAL().containsKey(bn)) {
				variables.getWAL().remove(bn);
			}
		}
		performanceService.logTaskEnd(1);
		return false;
	}

	// *************************************GENERIC 	METHODS*********************************************************

	@Override
	public List<String> getDataStore() {
		performanceService.logTaskStart();
		performanceService.logTaskEnd(1);
		return variables.getDataStore();
	}

	@SuppressWarnings("unused")
	private void reverseTransaction(Transaction transaction) {
		performanceService.logTaskStart();
		if ("true".equals(transaction.getIsIntraShard())) {
			Optional<Account> sender = accountRepository.findById(transaction.getSender());
			Optional<Account> receiver = accountRepository.findById(transaction.getReceiver());
			sender.get().setBalance(sender.get().getBalance() + transaction.getAmount());
			receiver.get().setBalance(receiver.get().getBalance() - transaction.getAmount());
			accountRepository.save(sender.get());
			accountRepository.save(receiver.get());
		} else {
			if ("from".equals(transaction.getGrp())) {
				Optional<Account> sender = accountRepository.findById(transaction.getSender());
				if(sender.get().getBalance() > transaction.getAmount()) {
					sender.get().setBalance(sender.get().getBalance() + transaction.getAmount());
					accountRepository.save(sender.get());
				}
			} else {
				Optional<Account> receiver = accountRepository.findById(transaction.getReceiver());
				receiver.get().setBalance(receiver.get().getBalance() - transaction.getAmount());
				accountRepository.save(receiver.get());
			}
			transactionRepository.delete(transaction);
		}
		performanceService.logTaskEnd(1);
	}

	private void executeTransaction(Transaction transaction) {
		performanceService.logTaskStart();
		if ("true".equals(transaction.getIsIntraShard())) {
			Optional<Account> sender = accountRepository.findById(transaction.getSender());
			Optional<Account> receiver = accountRepository.findById(transaction.getReceiver());
			sender.get().setBalance(sender.get().getBalance() - transaction.getAmount());
			receiver.get().setBalance(receiver.get().getBalance() + transaction.getAmount());
			transactionRepository.save(transaction);
			accountRepository.save(sender.get());
			accountRepository.save(receiver.get());
		} else {
			if ("from".equals(transaction.getGrp())) {
				Optional<Account> sender = accountRepository.findById(transaction.getSender());
				sender.get().setBalance(sender.get().getBalance() - transaction.getAmount());
				accountRepository.save(sender.get());
			} else {
				Optional<Account> receiver = accountRepository.findById(transaction.getReceiver());
				receiver.get().setBalance(receiver.get().getBalance() + transaction.getAmount());
				accountRepository.save(receiver.get());
			}
		}
		performanceService.logTaskEnd(1);
	}

	@Override
	public int getBalance(Long accountId) {
		performanceService.logTaskStart();
		Optional<Account> account = accountRepository.findById(accountId);
		if (account.isPresent()) {
			performanceService.logTaskEnd(1);
			return account.get().getBalance();
		}
		performanceService.logTaskEnd(1);
		return 0;
	}

	@Override
	public Set<Long> getAccountLocks() {
		performanceService.logTaskStart();
		performanceService.logTaskEnd(1);
		return variables.getLocks();
	}

}
