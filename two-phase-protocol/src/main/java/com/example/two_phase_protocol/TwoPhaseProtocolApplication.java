package com.example.two_phase_protocol;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwoPhaseProtocolApplication {

	public static void main(String[] args) {
		SpringApplication.run(TwoPhaseProtocolApplication.class, args);
		System.out.println("Hello from 2-phase commit protocol server...");
	}

}
