package com.example.two_phase_protocol.Controllers;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {
	
	
	@GetMapping("/hello")
	public ResponseEntity<String> hello(){
		return ResponseEntity.status(200).body("Hello from 2 phase commit protocol server");
	}
}
