package com.example.two_phase_protocol.Configuration;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class TPCConfiguration {

    @Bean
    RestTemplate restTemplate() {
		return new RestTemplate();
	}
    
    @Bean
    Lock lock() {
        return new ReentrantLock();
    }

}