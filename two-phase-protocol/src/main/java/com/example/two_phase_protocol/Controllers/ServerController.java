package com.example.two_phase_protocol.Controllers;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.two_phase_protocol.Services.PerformanceService;
import com.example.two_phase_protocol.Services.ServerService;

@RestController
public class ServerController {
	
	private final ServerService serverService;
	private final PerformanceService performanceService;

	public ServerController(ServerService serverService, PerformanceService performanceService) {
		super();
		this.serverService = serverService;
		this.performanceService = performanceService;
	}

	@PostMapping("/servers/disconnect")
	public ResponseEntity<String> disconnectServers(@RequestBody List<Integer> servers) {
		if (serverService.disconnectServers(servers)) {
			return ResponseEntity.status(200).body("Servers disconnected successfully");
		}
		return ResponseEntity.status(401).body("Unable to disconnect servers");
	}

	@GetMapping("/servers/disconnected")
	public List<Integer> getDisconnectedServers() {
		return serverService.getDisconnectedServers();
	}
	
	@PostMapping("/servers/primary")
	public ResponseEntity<String> byzantineServers(@RequestBody Map<Integer, Integer> servers){
		if(serverService.primaryServers(servers)){
			return ResponseEntity.status(200).body("Primary Servers saved successfully");
		}
		return ResponseEntity.status(200).body("Unable to save primary servers");
	}
	
	@GetMapping("/servers/primary")
	public Map<Integer, Integer> getByazantineServers() {
		return serverService.getPrimaryServers();
	}

	@GetMapping("/performance")
	public List<String> printPerformanceMetrics() {
		return performanceService.printPerformance();
	}
	
	@PostMapping("/reshard")
	public ResponseEntity<String> reshardData(@RequestBody Map<String, Object> request) {
		if(serverService.reshardData(request)) {
			return ResponseEntity.status(200).body("Reshard success");
		}
		return ResponseEntity.status(201).body("Reshard failed");
	}
}
