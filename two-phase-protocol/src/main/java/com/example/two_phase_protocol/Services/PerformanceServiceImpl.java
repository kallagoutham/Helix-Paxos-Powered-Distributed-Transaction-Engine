package com.example.two_phase_protocol.Services;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Service;

@Service
public class PerformanceServiceImpl implements PerformanceService {

	private AtomicLong taskCount = new AtomicLong(0);
	private Instant startTime = Instant.now();
	private long timeElapsed = 0L;

	@Override
	public void logTaskStart() {
		startTime = Instant.now();
	}

	@Override
	public void logTaskEnd(int noOfTasks) {
		Instant endTime = Instant.now();
		long latency = Duration.between(startTime, endTime).toMillis();
		timeElapsed += latency;
		taskCount.addAndGet(noOfTasks);
	}

	@Override
	public List<String> printPerformance() {
		List<String> performanceMetrics = new ArrayList<>();
		long totalTasks = taskCount.get();
		if (timeElapsed > 0) {
			double throughput = totalTasks / (double) timeElapsed;
			double avgLatecny = (double)timeElapsed / (double) totalTasks;
			performanceMetrics.add("Total Latency : " + timeElapsed + " ms.");
			performanceMetrics.add("Throughput: " + throughput + " tasks/second.");
			performanceMetrics.add("Average latency : " + avgLatecny +" ms.");
		} else {
			performanceMetrics.add("Throughput: N/A");
		}
		performanceMetrics.add("Total tasks processed: " + totalTasks);
		return performanceMetrics;
	}
}

