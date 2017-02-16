package feature;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.unidal.helper.Threads;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.producer.api.Producer;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

public class Stepdefs {

	private String topic = "order_new";

	private CountDownLatch latch = new CountDownLatch(1);

	@Given("^A Broker is started$")
	public void aBrokerIsStarted() throws Throwable {
		Threads.forGroup().start(new Runnable() {
			@Override
			public void run() {
				try {
					PlexusComponentLocator.lookup(BrokerBootstrap.class).start();
				} catch (Exception e) {
				}
			}
		});
	}

	@Given("^A consumer is started$")
	public void aConsumerIsStarted() throws Throwable {
		Consumer.getInstance().start(topic, "group1", new MessageListener<String>() {

			@Override
			public void onMessage(List<ConsumerMessage<String>> msgs) {
				System.out.println(msgs.get(0));
				latch.countDown();
			}
		});
	}

	@When("^I send the message$")
	public void iSendTheMessage() throws Throwable {
		Producer.getInstance().message(topic, "", UUID.randomUUID().toString()).send().get(5, TimeUnit.SECONDS);
	}

	@Then("^The consumer should receive the message$")
	public void theConsumerShouldReceiveTheMessage() throws Throwable {
		latch.await();
	}

}
