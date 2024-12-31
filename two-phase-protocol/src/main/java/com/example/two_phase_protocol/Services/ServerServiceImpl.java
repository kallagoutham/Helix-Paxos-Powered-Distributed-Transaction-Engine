package com.example.two_phase_protocol.Services;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import com.example.two_phase_protocol.GlobalVariables.Variables;

@Service
public class ServerServiceImpl implements ServerService {

	private final Variables variables;
	private final PerformanceService performanceService;
	private final ConcurrentHashMap<Integer, String> dataStore = new ConcurrentHashMap<>();
	private NavigableMap<Integer, Integer> shardMap;

	public ServerServiceImpl(Variables variables, PerformanceService performanceService) {
		super();
		this.variables = variables;
		this.performanceService = performanceService;
	}

	@Override
	public boolean disconnectServers(List<Integer> servers) {
		performanceService.logTaskStart();
		variables.setDisconnectedServers(servers);
		performanceService.logTaskEnd(1);
		return true;
	}

	@Override
	public List<Integer> getDisconnectedServers() {
		performanceService.logTaskStart();
		performanceService.logTaskEnd(1);
		return variables.getDisconnectedServers();
	}

	@Override
	public boolean primaryServers(Map<Integer, Integer> servers) {
		performanceService.logTaskStart();
		variables.setPrimaryServers(servers);
		performanceService.logTaskEnd(1);
		return true;
	}

	@Override
	public Map<Integer, Integer> getPrimaryServers() {
		performanceService.logTaskStart();
		performanceService.logTaskEnd(1);
		return variables.getPrimaryServers();
	}

	@Override
	public boolean reshardData(Map<String, Object> request) {
		try {
			Map<String, Double> newShardMap = (Map<String, Double>) request.get("newShardMap");
			NavigableMap<Integer, Integer> updatedShardMap = new TreeMap<>();
			newShardMap.forEach((key, value) -> updatedShardMap.put(Integer.parseInt(key), value.intValue()));
			redistributeData(updatedShardMap);
			this.shardMap = updatedShardMap;
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	private void redistributeData(NavigableMap<Integer, Integer> updatedShardMap) {
		System.out.println("Redistributing data based on new shard map...");
		ConcurrentHashMap<Integer, String> newDataStore = new ConcurrentHashMap<>();
		for (Map.Entry<Integer, String> entry : dataStore.entrySet()) {
			int key = entry.getKey();
			String value = entry.getValue();
			int newCluster = updatedShardMap.ceilingEntry(key).getValue();
			System.out.printf("Migrating key %d from cluster %d to cluster %d%n", key,
					shardMap.ceilingEntry(key).getValue(), newCluster);
			newDataStore.put(key, value);
		}
		dataStore.clear();
		dataStore.putAll(newDataStore);
		System.out.println("Data redistribution completed.");
	}

}
