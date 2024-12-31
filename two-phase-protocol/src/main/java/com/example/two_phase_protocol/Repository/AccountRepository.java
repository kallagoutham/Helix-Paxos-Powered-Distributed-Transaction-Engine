package com.example.two_phase_protocol.Repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.example.two_phase_protocol.Models.Account;

@Repository
public interface AccountRepository extends JpaRepository<Account,Long>{

	@Query("Select a.balance from Account a where a.accountId=?1")
	int getBalanceByAccount(Long sender);

}
