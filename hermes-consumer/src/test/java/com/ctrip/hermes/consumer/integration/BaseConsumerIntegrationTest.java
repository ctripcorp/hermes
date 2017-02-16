package com.ctrip.hermes.consumer.integration;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;

import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.engine.config.ConsumerConfig;
import com.ctrip.hermes.consumer.integration.assist.LeaseAnswer;
import com.ctrip.hermes.consumer.integration.assist.PullMessageAnswer;
import com.ctrip.hermes.consumer.integration.assist.TestConsumerConfig;
import com.ctrip.hermes.consumer.integration.assist.TestMetaHolder;
import com.ctrip.hermes.consumer.integration.assist.TestMetaService;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.endpoint.EndpointClient;
import com.ctrip.hermes.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;

public class BaseConsumerIntegrationTest extends ComponentTestCase {

	public static final String TEST_TOPIC = "test_topic";

	public static final String TEST_GROUP = "group1";

	@Mock
	protected EndpointClient m_endpointClient;

	@Mock
	protected TestMetaHolder m_metaHolder;

	@Mock
	protected MetaProxy m_metaProxy;

	@Mock
	protected Channel m_channel;

	@Rule
	public TestName m_name = new TestName();

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		System.out.println("\n\n************** Current test case: " + m_name.getMethodName() + " start **************");
		configureStubComponents();
	}

	@After
	@Override
	public void tearDown() throws Exception {
		resetAnswers();
		System.out.println("************** Current test case: " + m_name.getMethodName() + " stop **************\n\n");
		super.tearDown();
	}

	private void resetAnswers() {
		LeaseAnswer.reset();
		PullMessageAnswer.reset();
	}

	private void configureStubComponents() throws Exception {
		defineComponent(ConsumerConfig.class, TestConsumerConfig.class).req(ClientEnvironment.class).req(
		      MetaService.class);
		defineComponent(MetaService.class, TestMetaService.class);
		defineComponent(EndpointClient.class, TestEndpointClient.class);

		((TestEndpointClient) lookup(EndpointClient.class)).setDelegate(m_endpointClient);

		((TestMetaService) lookup(MetaService.class)).setMetaProxy(m_metaProxy).setMetaHolder(m_metaHolder);

		when(m_metaHolder.getMeta()).thenReturn(loadLocalMeta());
		when(m_channel.writeAndFlush(any(Object.class))).thenReturn(null);
	}

	public static class TestEndpointClient implements EndpointClient {

		private EndpointClient m_delegate;

		public void setDelegate(EndpointClient delegate) {
			m_delegate = delegate;
		}

		@Override
		public boolean writeCommand(Endpoint endpoint, Command cmd) {
			return m_delegate.writeCommand(endpoint, cmd);
		}

		@Override
		public void close() {
			m_delegate.close();
		}

	}

	protected Meta loadLocalMeta() throws Exception {
		String fileName = getClass().getSimpleName() + "-meta.xml";
		InputStream in = getClass().getResourceAsStream(fileName);

		if (in == null) {
			throw new RuntimeException(String.format("File %s not found in classpath.", fileName));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (Exception e) {
				throw new RuntimeException(String.format("Error parse meta file %s", fileName), e);
			}
		}
	}

	protected void brokerActions4PollMessageCmd(Answer<?> answer) {
		doAnswer(answer)//
		      .when(m_endpointClient)//
		      .writeCommand(any(Endpoint.class), //
		            any(Command.class));
	}

	protected void metaProxyActions4LeaseOperation(Answer<?> acquireAnswer, Answer<?> renewAnswer) {
		doAnswer(acquireAnswer)//
		      .when(m_metaProxy)//
		      .tryAcquireConsumerLease(any(Tpg.class), any(String.class));

		doAnswer(renewAnswer)//
		      .when(m_metaProxy)//
		      .tryRenewConsumerLease(any(Tpg.class), any(Lease.class), any(String.class));
	}
}
