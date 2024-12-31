package com.example.two_phase_protocol.Services;

import java.util.List;

public interface PerformanceService {

	List<String> printPerformance();
	void logTaskStart();
	void logTaskEnd(int noOfTasks);
	
}
