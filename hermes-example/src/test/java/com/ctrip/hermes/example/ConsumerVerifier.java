package com.ctrip.hermes.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.MessageListenerConfig;
import com.ctrip.hermes.consumer.api.MessageListenerConfig.StrictlyOrderingRetryPolicy;
import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.api.PullConsumerHolder;
import com.ctrip.hermes.consumer.api.PulledBatch;
import com.ctrip.hermes.consumer.pull.PullBrokerConsumerMessage;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;

public class ConsumerVerifier {
	private String topic = "ds_transfer_2";

	// private String topic = "order_new";

	private String groupId = "ds_transfer_2";

	// private String groupId = "group1";

	private int partitionCount = 3;

	private IntegerDistribution nackRnd;

	private long maxOffset = 10000;

	@Before
	public void before() {
		int[] nackIndexes = new int[] { 0, 1 };
		double[] nackDis = new double[] { 0.05, 0.95 };
		nackRnd = new EnumeratedIntegerDistribution(nackIndexes, nackDis);
	}

	@Test
	public void testProduce() throws Exception {
		final Producer producer = Producer.getInstance();

		final AtomicBoolean sleep = new AtomicBoolean(true);
		new Thread() {
			public void run() {
				int msg = 0;
				while (msg++ < maxOffset) {
					for (int partition = 0; partition < partitionCount; partition++) {
						for (int priority = 0; priority < 2; priority++) {
							System.out.println(String.format("Sending: %s %s %s", partition, priority, msg));
							if (sleep.get()) {
								try {
									Thread.sleep(100);
								} catch (InterruptedException e) {
								}
							}
							if (priority == 0) {
								producer.message(topic, partition + "", msg + "").withPriority().send();
							} else {
								producer.message(topic, partition + "", msg + "").send();
							}
						}
					}
				}
			}
		}.start();

		System.in.read();
		sleep.set(false);
		System.out.println("==========speed up=============");
		System.in.read();

	}

	@Test
	public void testPushConsumer() throws Exception {
		final Verifier v = new Verifier(maxOffset);

		MessageListenerConfig config = new MessageListenerConfig();
		config.setStrictlyOrderingRetryPolicy(StrictlyOrderingRetryPolicy.evenRetry(5, 3));

		Consumer.getInstance().start(topic, groupId, new BaseMessageListener<String>() {
			@Override
			public void onMessage(ConsumerMessage<String> msg) {
				List<ConsumerMessage<String>> msgs = Arrays.asList(msg);
				v.retrived(msgs);

				if (nackRnd.sample() == 0) {
					msg.nack();
					v.nacked(msgs);
				}
			}
		}, config);

		while (true) {
			if (v.isDone()) {
				System.out.println("OK");
				System.out.println(v.statuses);
				break;
			} else {
				for (Entry<Triple<Integer, Integer, Boolean>, PartitionStatus> entry : v.getStatuses().entrySet()) {
					PartitionStatus ps = entry.getValue();
					if (entry.getKey().getMiddle() == Integer.MIN_VALUE) {
						// resend
						if (ps.getToBeNacks().size() != 0) {
							System.out.println(entry);
						}
					} else {
						if (ps.getCurNonresendOffset() != maxOffset || ps.getToBeNacks().size() != 0) {
							System.out.println(entry);
						}
					}
				}
				Thread.sleep(5000);
			}
		}
	}

	@Test
	public void testPullConsumer() throws Exception {

		PullConsumerConfig config = new PullConsumerConfig();
		final PullConsumerHolder<String> holder = Consumer.getInstance().openPullConsumer(topic, groupId, String.class,
		      config);

		List<Callable<PulledBatch<String>>> retrives = new ArrayList<>();
		retrives.add(new Callable<PulledBatch<String>>() {

			@Override
			public PulledBatch<String> call() throws Exception {
				return holder.poll(100, 1000);
			}
		});
		retrives.add(new Callable<PulledBatch<String>>() {

			@Override
			public PulledBatch<String> call() throws Exception {
				return holder.collect(100, 1000);
			}
		});

		Verifier v = new Verifier(maxOffset);

		int[] retriveIndexes = new int[] { 0, 1 };
		double[] retriveDis = new double[] { 0.5, 0.5 };
		IntegerDistribution retriveRnd = new EnumeratedIntegerDistribution(retriveIndexes, retriveDis);

		int round = 0;
		int noMsgCount = 0;
		while (true) {
			PulledBatch<String> batch = retrives.get(retriveRnd.sample()).call();
			List<ConsumerMessage<String>> msgs = batch.getMessages();

			if (round++ == 10) {
				round = 0;
				showStatus(v);
			}

			if (msgs.isEmpty()) {
				noMsgCount++;
				if (noMsgCount >= 10) {
					noMsgCount = 0;
					if (v.isDone()) {
						System.out.println("OK");
						System.out.println(v.statuses);
						break;
					}
				}
			}
			v.retrived(msgs);

			List<ConsumerMessage<String>> nonpriorityNacked = new LinkedList<>();
			List<ConsumerMessage<String>> priorityNacked = new LinkedList<>();

			for (ConsumerMessage<String> msg : msgs) {
				if (nackRnd.sample() == 0) {
					if (msg.isPriority()) {
						priorityNacked.add(msg);
					} else {
						nonpriorityNacked.add(msg);
					}
				}
			}

			// will receive nonpriority message's resend first
			nonpriorityNacked.addAll(priorityNacked);
			if (!nonpriorityNacked.isEmpty()) {
				v.nacked(nonpriorityNacked);
				for (ConsumerMessage<String> msg : nonpriorityNacked) {
					msg.nack();
				}
			}

			batch.commitAsync();
		}

	}

	private void showStatus(Verifier v) {
		for (Entry<Triple<Integer, Integer, Boolean>, PartitionStatus> entry : v.statuses.entrySet()) {
			System.out.println(String.format("%s %s", entry.getKey(), entry.getValue()));
		}
	}

	class Verifier {

		private long maxOffset;

		public Verifier(long maxOffset) {
			this.maxOffset = maxOffset;
		}

		// partition, priority or resend(Integer.MIN_VALUE, priority)
		private ConcurrentMap<Triple<Integer, Integer, Boolean>, PartitionStatus> statuses = new ConcurrentHashMap<>();

		public void retrived(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				int por = priorityOrResend(msg);
				PartitionStatus status = createOrGetPartitionStatus(msg.getPartition(), por, msg.isPriority());
				status.retrived(msg);
			}
		}

		public boolean isDone() {
			boolean ok = true;
			/*
			 * <0, true>, <1, false>, <Integer.MIN_VALUE, true>, <Integer.MIN_VALUE, false>
			 */
			if (statuses.size() != 4 * partitionCount) {
				return false;
			}

			for (Entry<Triple<Integer, Integer, Boolean>, PartitionStatus> entry : statuses.entrySet()) {
				PartitionStatus ps = entry.getValue();
				if (entry.getKey().getMiddle() == Integer.MIN_VALUE) {
					// resend
					if (ps.getToBeNacks().size() != 0) {
						ok = false;
					}
				} else {
					if (ps.getCurNonresendOffset() != maxOffset || ps.getToBeNacks().size() != 0) {
						ok = false;
					}
				}
			}

			return ok;
		}

		public void nacked(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				// System.out.println(String.format("NACK %s %s",
				// msg.getPartition(), msg.getBody()));
				PartitionStatus status = createOrGetPartitionStatus(msg.getPartition(), Integer.MIN_VALUE, msg.isPriority());
				status.nacked(msg);
			}
		}

		private PartitionStatus createOrGetPartitionStatus(int partition, int por, boolean isPriority) {
			Triple<Integer, Integer, Boolean> triple = new Triple<>(partition, por, isPriority);
			PartitionStatus status = statuses.get(triple);
			if (status == null) {
				statuses.putIfAbsent(triple, new PartitionStatus());
				status = statuses.get(triple);
			}
			return status;
		}

		private int priorityOrResend(ConsumerMessage<String> msg) {
			boolean isResend = ((PullBrokerConsumerMessage<String>) msg).isResend();
			// boolean isResend = ((BrokerConsumerMessage<String>) msg).getResendTimes() > 0;
			// boolean isResend =
			// ((NackDelayedBrokerConsumerMessage<String>)msg).getResendTimes()
			// > 0;
			return isResend ? Integer.MIN_VALUE : (msg.isPriority() ? 0 : 1);
		}

		public ConcurrentMap<Triple<Integer, Integer, Boolean>, PartitionStatus> getStatuses() {
			return statuses;
		}

	}

	class PartitionStatus {

		private long curNonresendOffset = 0L;

		private BlockingQueue<ConsumerMessage<String>> toBeNacks = new LinkedBlockingQueue<>();

		private String lastMsg;

		public void retrived(ConsumerMessage<String> msg) {
			lastMsg = msg.getBody();
			boolean isResend = ((PullBrokerConsumerMessage<String>) msg).isResend();
			// boolean isResend = ((BrokerConsumerMessage<String>) msg).isResend();
			// boolean isResend =
			// ((NackDelayedBrokerConsumerMessage<String>)msg).getResendTimes()
			// > 0;

			if (isResend) {
				boolean found = false;
				List<ConsumerMessage<String>> unordered = new ArrayList<>();
				while (!toBeNacks.isEmpty()) {
					ConsumerMessage<String> expectedNack = toBeNacks.poll();
					if (expectedNack.getBody().equals(msg.getBody())) {
						found = true;
					} else {
						unordered.add(expectedNack);
					}
				}

				toBeNacks.addAll(unordered);
				if (!found) {
					System.out.println(unordered);
					System.out.println(String.format("=====================wrong nack %s act:%s===============",
					      msg.getPartition(), msg.getBody()));
					// System.exit(1);
				}
			} else {
				long offset = Long.parseLong(msg.getBody());
				if (offset != curNonresendOffset + 1) {
					System.out
					      .println(String
					            .format(
					                  "===========================wrong offset expected %s, actual %s, partition %s, priority %s============================", //
					                  curNonresendOffset + 1, offset, msg.getPartition(), msg.isPriority()));
					// System.exit(1);
				} else {
					curNonresendOffset = offset;
				}
			}
		}

		public void nacked(ConsumerMessage<String> msg) {
			toBeNacks.add(msg);
		}

		public long getCurNonresendOffset() {
			return curNonresendOffset;
		}

		public BlockingQueue<ConsumerMessage<String>> getToBeNacks() {
			return toBeNacks;
		}

		@Override
		public String toString() {
			return "PartitionStatus [lastMsg=" + lastMsg + ", curNonresendOffset=" + curNonresendOffset + ", toBeNacks="
			      + toBeNacks + "]";
		}

	}
}
