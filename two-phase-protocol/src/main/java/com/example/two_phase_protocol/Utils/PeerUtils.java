package com.example.two_phase_protocol.Utils;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.two_phase_protocol.GlobalVariables.Variables;

@Component
public class PeerUtils {

	@Value("${server.port}")
	private int serverPort;
	private final Variables variables;

	public PeerUtils(Variables variables) {
		super();
		this.variables = variables;
	}

	public List<String> getPeersList() {
		List<String> peers = new ArrayList<>();
		for (int i = (8080 + (variables.getClusterNo() - 1) * variables.getClusterSize()); i < (8080
				+ (variables.getClusterNo()) * variables.getClusterSize()); ++i) {
			if (serverPort != i && !variables.getDisconnectedServers().contains(i)) {
				peers.add("localhost:" + i);
			}
		}
		return peers;
	}

	public List<String> getPeersListIncludingDisconnected() {
		List<String> peers = new ArrayList<>();
		for (int i = (8080 + (variables.getClusterNo() - 1) * variables.getClusterSize()); i < (8080
				+ (variables.getClusterNo()) * variables.getClusterSize()); ++i) {
			if (serverPort != i) {
				peers.add("localhost:" + i);
			}
		}
		return peers;
	}

	public List<String> getAllServersList() {
		List<String> peers = new ArrayList<>();
		for (int i = (8080 + (variables.getClusterNo() - 1) * variables.getClusterSize()); i < (8080
				+ (variables.getClusterNo()) * variables.getClusterSize()); ++i) {
			if (!variables.getDisconnectedServers().contains(i)) {
				peers.add("localhost:" + i);
			}
		}
		return peers;
	}

	public List<String> getAllServersListIncludingDisconnected() {
		List<String> peers = new ArrayList<>();
		for (int i = (8080 + (variables.getClusterNo() - 1) * variables.getClusterSize()); i < (8080
				+ (variables.getClusterNo()) * variables.getClusterSize()); ++i) {
			peers.add("localhost:" + i);
		}
		return peers;

	}

	public boolean isDisonnected() {
		if (variables.getDisconnectedServers().contains(serverPort)) {
			return true;
		}
		return false;
	}
	
	public int getServerPort() {
		return variables.getPrimaryServers().get(variables.getClusterNo())-8079;
	}
	
}
