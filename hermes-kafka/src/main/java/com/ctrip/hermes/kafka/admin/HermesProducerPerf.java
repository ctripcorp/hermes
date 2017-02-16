package com.ctrip.hermes.kafka.admin;

import java.util.Arrays;
import java.util.Random;

import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class HermesProducerPerf {
	private static final long NS_PER_MS = 1000000L;

	private static final long NS_PER_SEC = 1000 * NS_PER_MS;

	private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

	public HermesProducerPerf(String topicName, long numRecords, int recordSize, int throughput) {
		this.topicName = topicName;
		this.numRecords = numRecords;
		this.recordSize = recordSize;
		this.throughput = throughput;
	}

	public void run() {
		Producer producer = Producer.getInstance();

		/* setup perf test */
		Random random = new Random();
		byte[] msgBuf = new byte[recordSize - 4];
		Arrays.fill(msgBuf, (byte) random.nextInt(255));
		String proMsg = new String(msgBuf) + System.currentTimeMillis();
		long sleepTime = NS_PER_SEC / throughput;
		long sleepDeficitNs = 0;
		Stats stats = new Stats(numRecords, 5000);
		for (int i = 0; i < numRecords; i++) {
			long sendStart = System.currentTimeMillis();
			CompletionCallback<SendResult> cb = stats.nextCompletion(sendStart, proMsg.getBytes().length, stats);
			MessageHolder message = producer.message(topicName, String.valueOf(random.nextInt()), proMsg);
			message.setCallback(cb);
			message.send();

			/*
			 * Maybe sleep a little to control throughput. Sleep time can be a bit inaccurate for times < 1 ms so instead of sleeping
			 * each time instead wait until a minimum sleep time accumulates (the "sleep deficit") and then make up the whole deficit
			 * in one longer sleep.
			 */
			if (throughput > 0) {
				sleepDeficitNs += sleepTime;
				if (sleepDeficitNs >= MIN_SLEEP_NS) {
					long sleepMs = sleepDeficitNs / 1000000;
					long sleepNs = sleepDeficitNs - sleepMs * 1000000;
					try {
						Thread.sleep(sleepMs, (int) sleepNs);
					} catch (InterruptedException e) {
					}
					sleepDeficitNs = 0;
				}
			}
		}

		/* print final results */
		stats.printTotal();
	}

	private String topicName;

	private long numRecords;

	private int recordSize;

	private int throughput;

	public static void showHelp() {
		System.out.println("USAGE: java " + HermesProducerPerf.class.getName()
		      + " topic_name num_records record_size target_records_sec [prop_name=prop_value]*");
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			showHelp();
			System.exit(1);
		}

		/* parse args */
		String topicName = args[0];
		long numRecords = Long.parseLong(args[1]);
		int recordSize = Integer.parseInt(args[2]);
		int throughput = Integer.parseInt(args[3]);

		HermesProducerPerf perfTest = new HermesProducerPerf(topicName, numRecords, recordSize, throughput);
		perfTest.run();
		System.exit(0);
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
		public void onSuccess(SendResult result) {
			long now = System.currentTimeMillis();
			int latency = (int) (now - start);
			this.stats.record(iteration, latency, bytes, now);
		}

		@Override
		public void onFailure(Throwable t) {
			t.printStackTrace();
		}
	}
}
