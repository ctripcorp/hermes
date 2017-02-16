package com.ctrip.hermes.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.hermes.core.result.CompletionCallback;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class ProduceData {
	@Test
	public void test() throws Exception {
		System.out.println("Producer started...");

		final int rowsPerPartition = 100000;
		int partitionCount = 10;

		final CountDownLatch latch = new CountDownLatch(partitionCount);

		for (int i = 0; i < partitionCount; i++) {
			final int partitionNo = i;
			Thread t = new Thread(new Runnable() {

				@Override
				public void run() {

					final CountDownLatch doneLatch = new CountDownLatch(rowsPerPartition);
					for (int j = 0; j < rowsPerPartition; j++) {
						String body = String.format("Partition--%s--%s", partitionNo, String.valueOf(j + 1));
						Producer.getInstance().message("lpt_test", String.valueOf(partitionNo), body)
						      .setCallback(new CompletionCallback<SendResult>() {

							      @Override
							      public void onSuccess(SendResult result) {
								      doneLatch.countDown();
							      }

							      @Override
							      public void onFailure(Throwable t) {
								      // TODO Auto-generated method stub

							      }
						      }).send();

						if (j != 0 && j % 3000 == 0) {
							try {
								TimeUnit.SECONDS.sleep(1);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}

					}

					System.out.println("Waiting... " + partitionNo);

					try {
						doneLatch.await();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					System.out.println("Done... " + partitionNo);

					latch.countDown();
				}
			});
			t.start();
		}

		latch.await();
		System.out.println("Press enter to exit...");
		System.in.read();
	}
}
