package com.example.two_phase_protocol.Services;

import java.util.List;
import java.util.Map;

public interface ServerService {

	boolean disconnectServers(List<Integer> servers);
	List<Integer> getDisconnectedServers();
	boolean primaryServers(Map<Integer, Integer> servers);
	Map<Integer, Integer> getPrimaryServers();
	boolean reshardData(Map<String, Object> request);
	
}
