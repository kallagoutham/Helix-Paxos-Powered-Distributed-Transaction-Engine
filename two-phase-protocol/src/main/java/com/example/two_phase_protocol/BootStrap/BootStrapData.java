package com.example.two_phase_protocol.BootStrap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.example.two_phase_protocol.Models.Account;
import com.example.two_phase_protocol.Repository.AccountRepository;


@Component
public class BootStrapData implements CommandLineRunner {

	@Value("${application.cluster}")
	private long clusterNo;
	@Value("${application.shardsize}")
	private long shardSize;
	private final AccountRepository accountRepository;

	public BootStrapData(AccountRepository accountRepository) {
		super();
		this.accountRepository = accountRepository;
	}

	@Override
	public void run(String... args) throws Exception {
		for(long i=(((clusterNo-1)*shardSize) + 1);i<=(clusterNo*shardSize);i++) {
			Account account = new Account();
			account.setAccountId(i);
			account.setAccNo(i);
			account.setBalance(10);
			accountRepository.save(account);
		}
	}

}
