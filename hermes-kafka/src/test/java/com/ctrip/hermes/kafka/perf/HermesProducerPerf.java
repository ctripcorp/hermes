package com.ctrip.hermes.kafka.perf;

import java.util.Arrays;
import java.util.Random;

import com.ctrip.hermes.core.exception.MessageSendException;
import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class HermesProducerPerf {
	private static final long NS_PER_MS = 1000000L;

	private static final long NS_PER_SEC = 1000 * NS_PER_MS;

	private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

	public static void main(String[] args) throws MessageSendException, InterruptedException {

		String topicName = args[0];
		long numRecords = Long.parseLong(args[1]);
		int recordSize = Integer.parseInt(args[2]);
		int throughput = Integer.parseInt(args[3]);

		Producer producer = Producer.getInstance();
		Random random = new Random();
		
		byte[] payload = new byte[recordSize];
		Arrays.fill(payload, (byte) 1);
		MessageHolder message = producer.message(topicName, String.valueOf(random.nextInt()), payload);
		long sleepTime = NS_PER_SEC / throughput;
		long sleepDeficitNs = 0;
		Stats stats = new Stats(numRecords, 5000);
		for (int i = 0; i < numRecords; i++) {
			long sendStart = System.currentTimeMillis();
			CompletionCallback<SendResult> cb = stats.nextCompletion(sendStart, payload.length, stats);
			message.setCallback(cb).send();

			if (throughput > 0) {
				sleepDeficitNs += sleepTime;
				if (sleepDeficitNs >= MIN_SLEEP_NS) {
					long sleepMs = sleepDeficitNs / 1000000;
					long sleepNs = sleepDeficitNs - sleepMs * 1000000;
					Thread.sleep(sleepMs, (int) sleepNs);
					sleepDeficitNs = 0;
				}
			}
		}

		stats.printTotal();
	}

	private static class Stats {
		private long start;

		private long windowStart;

		private int[] latencies;

		private int sampling;

		private int iteration;

		private int index;

		private long count;

		private long bytes;

		private int maxLatency;

		private long totalLatency;

		private long windowCount;

		private int windowMaxLatency;

		private long windowTotalLatency;

		private long windowBytes;

		private long reportingInterval;

		public Stats(long numRecords, int reportingInterval) {
			this.start = System.currentTimeMillis();
			this.windowStart = System.currentTimeMillis();
			this.index = 0;
			this.iteration = 0;
			this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
			this.latencies = new int[(int) (numRecords / this.sampling) + 1];
			this.index = 0;
			this.maxLatency = 0;
			this.totalLatency = 0;
			this.windowCount = 0;
			this.windowMaxLatency = 0;
			this.windowTotalLatency = 0;
			this.windowBytes = 0;
			this.totalLatency = 0;
			this.reportingInterval = reportingInterval;
		}

		public void record(int iter, int latency, int bytes, long time) {
			this.count++;
			this.bytes += bytes;
			this.totalLatency += latency;
			this.maxLatency = Math.max(this.maxLatency, latency);
			this.windowCount++;
			this.windowBytes += bytes;
			this.windowTotalLatency += latency;
			this.windowMaxLatency = Math.max(windowMaxLatency, latency);
			if (iter % this.sampling == 0) {
				this.latencies[index] = latency;
				this.index++;
			}
			/* maybe report the recent perf */
			if (time - windowStart >= reportingInterval) {
				printWindow();
				newWindow();
			}
		}

		public CompletionCallback<SendResult> nextCompletion(long start, int bytes, Stats stats) {
			CompletionCallback<SendResult> cb = new PerfCallback(this.iteration, start, bytes, stats);
			this.iteration++;
			return cb;
		}

		public void printWindow() {
			long ellapsed = System.currentTimeMillis() - windowStart;
			double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
			double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
			System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
			      windowCount, recsPerSec, mbPerSec, windowTotalLatency / (double) windowCount, (double) windowMaxLatency);
		}

		public void newWindow() {
			this.windowStart = System.currentTimeMillis();
			this.windowCount = 0;
			this.windowMaxLatency = 0;
			this.windowTotalLatency = 0;
			this.windowBytes = 0;
		}

		public void printTotal() {
			long ellapsed = System.currentTimeMillis() - start;
			double recsPerSec = 1000.0 * count / (double) ellapsed;
			double mbPerSec = 1000.0 * this.bytes / (double) ellapsed / (1024.0 * 1024.0);
			int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
			System.out
			      .printf(
			            "%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
			            count, recsPerSec, mbPerSec, totalLatency / (double) count, (double) maxLatency, percs[0],
			            percs[1], percs[2], percs[3]);
		}

		private static int[] percentiles(int[] latencies, int count, double... percentiles) {
			int size = Math.min(count, latencies.length);
			Arrays.sort(latencies, 0, size);
			int[] values = new int[percentiles.length];
			for (int i = 0; i < percentiles.length; i++) {
				int index = (int) (percentiles[i] * size);
				values[i] = latencies[index];
			}
			return values;
		}
	}

	private static final class PerfCallback implements CompletionCallback<SendResult> {
		private final long start;

		private final int iteration;

		private final int bytes;

		private final Stats stats;

		public PerfCallback(int iter, long start, int bytes, Stats stats) {
			this.start = start;
			this.stats = stats;
			this.iteration = iter;
			this.bytes = bytes;
		}

		@Override
		public void onFailure(Throwable t) {
			if (t != null)
				t.printStackTrace();
		}

		@Override
		public void onSuccess(SendResult result) {
			long now = System.currentTimeMillis();
			int latency = (int) (now - start);
			this.stats.record(iteration, latency, bytes, now);
		}
	}
}
