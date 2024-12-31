package com.example.two_phase_protocol.Repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.example.two_phase_protocol.Models.Transaction;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long> {

	@Query("SELECT CASE WHEN COUNT(t) > 0 THEN true ELSE false END " + "FROM Transaction t "
			+ "WHERE t.sender = :sender " + "AND t.receiver = :receiver " + "AND t.amount = :amount "
			+ "AND t.timestamp = :timestamp")
	boolean contains(@Param("sender") Long sender, @Param("receiver") Long receiver, @Param("amount") int amount,
			@Param("timestamp") long timestamp);

}
