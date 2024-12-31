package com.example.two_phase_protocol.GlobalVariables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.two_phase_protocol.Models.Transaction;

import lombok.Data;

@Data
@Component
public class Variables {
	
	@Value("${application.clustersize}")
	private int clusterSize;
	@Value("${application.cluster}")
	private int clusterNo;
	@Value("${application.shardsize}")
	private int shardSize;
	@Value("${application.noofclusters}")
	private long noOfClusters;
	private int ballotNumber=0;
	private Transaction acceptNum;
	private int acceptVal;
	private List<Integer> disconnectedServers=new ArrayList<>(); 
	private Map<Integer, Integer> primaryServers = new HashMap<>();
	private Set<Long> locks=new HashSet<>();
	private List<String> dataStore=new ArrayList<>();
	private HashMap<Integer,Transaction> WAL = new HashMap<>(); 
	
}
