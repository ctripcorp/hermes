package com.ctrip.hermes.core.cmessaging;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.cmessaging.entity.Cmessaging;
import com.ctrip.hermes.cmessaging.entity.Consume;
import com.ctrip.hermes.cmessaging.entity.ConsumeGroup;
import com.ctrip.hermes.cmessaging.entity.Exchange;
import com.ctrip.hermes.cmessaging.entity.Node;
import com.ctrip.hermes.cmessaging.entity.Produce;
import com.ctrip.hermes.cmessaging.entity.ProduceGroup;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.meta.internal.MetaManager;
import com.ctrip.hermes.core.utils.HermesThreadFactory;

@Named(type = CMessagingConfigService.class)
public class DefaultCMessagingConfigService implements CMessagingConfigService, Initializable {

	private static Logger log = LoggerFactory.getLogger(DefaultCMessagingConfigService.class);

	@Inject
	private MetaManager m_metaManager;

	@Inject
	private CoreConfig m_coreConfig;

	private AtomicReference<Cmessaging> m_cmsgConfigCache = new AtomicReference<>();

	private Map<String, String> m_restParams = new HashMap<String, String>(1);

	private Exchange findExchange(String exchangeName) {
		Exchange exchange = m_cmsgConfigCache.get().findExchange(exchangeName);
		return exchange;
	}

	@Override
	public String getTopic(String exchangeName) {
		Exchange exchange = findExchange(exchangeName);

		String result = null;
		if (exchange != null) {
			result = exchange.getHermesTopic();
		}

		return result;
	}

	@Override
	public String getGroupId(String exchangeName, String idAndQueueName) {
		Exchange exchange = findExchange(exchangeName);

		String result = null;
		if (exchange != null) {
			ConsumeGroup group = exchange.getConsume().findConsumeGroup(idAndQueueName);
			if (group != null) {
				result = group.getHermesConsumerGroup();
			}
		}
		return result;
	}

	@Override
	public boolean isHermesProducerEnabled(String exchangeName, String identifier, String ip) {
		Exchange exchange = findExchange(exchangeName);

		if (exchange == null || exchange.getProduce() == null) {
			return false;
		} else {
			if (Produce.GRAY.equalsIgnoreCase(exchange.getProduce().getState())) {
				ProduceGroup group = exchange.getProduce().findProduceGroup(identifier);
				if (group == null) {
					return false;
				} else {
					if (Produce.GRAY.equalsIgnoreCase(group.getState())) {
						return group.findNode(ip) != null;
					} else {
						return nonGrayProduceStateToBoolean(group.getState());
					}
				}
			} else {
				return nonGrayProduceStateToBoolean(exchange.getProduce().getState());
			}
		}
	}

	private boolean nonGrayProduceStateToBoolean(String state) {
		switch (state) {
		case Produce.CLOSE:
			return false;
		case Produce.OPEN:
			return true;
		default:
			throw new RuntimeException(String.format("Unknow state (%s)", state));
		}
	}

	@Override
	public int getConsumerType(String exchangeName, String identifier, String ip) {
		Exchange exchange = findExchange(exchangeName);

		if (exchange == null || exchange.getConsume() == null) {
			return CMessagingConfigService.CONSUME_FROM_CMESSAGING;
		} else {
			if (Consume.GRAY.equalsIgnoreCase(exchange.getConsume().getState())) {
				ConsumeGroup group = exchange.getConsume().findConsumeGroup(identifier);
				if (group == null) {
					return CMessagingConfigService.CONSUME_FROM_CMESSAGING;
				} else {
					if (Consume.GRAY.equalsIgnoreCase(group.getState())) {
						Node node = group.findNode(ip);
						if (node == null) {
							return CMessagingConfigService.CONSUME_FROM_CMESSAGING;
						} else {
							return CMessagingConfigService.CONSUME_FROM_BOTH;
						}
					} else {
						return nonGrayConsumeStateToInt(group.getState());
					}
				}
			} else {
				return nonGrayConsumeStateToInt(exchange.getConsume().getState());
			}
		}
	}

	private int nonGrayConsumeStateToInt(String state) {
		switch (state) {
		case Consume.CLOSE:
			return CMessagingConfigService.CONSUME_FROM_CMESSAGING;
		case Consume.BOTH_OPEN:
			return CMessagingConfigService.CONSUME_FROM_BOTH;
		case Consume.HERMES_ONLY:
			return CMessagingConfigService.CONSUME_FROM_HERMES;
		default:
			throw new RuntimeException(String.format("Unknow state (%s)", state));
		}
	}

	@Override
	public void initialize() throws InitializationException {
		boolean initSuccess = false;

		try {
			initSuccess = updateConfig();
		} catch (Exception e) {
			throw new InitializationException("Can not fetch cmessaging config from any meta server", e);
		}

		if (!initSuccess) {
			throw new InitializationException("Can not fetch cmessaging config from any meta server");
		}

		HermesThreadFactory.create("CMessagingConfigUpdater", true).newThread(new ConfigUpdateTask()).start();
	}

	private boolean updateConfig() {
		boolean success = false;

		if (m_cmsgConfigCache.get() != null) {
			m_restParams.put("version", Long.toString(m_cmsgConfigCache.get().getVersion()));
		}

		Pair<Integer, String> codeAndRes = m_metaManager.getMetaProxy().getRequestToMetaServer("/cmessage/config", m_restParams);
		if (codeAndRes != null) {
			int code = codeAndRes.getKey();
			if (code == 200) {
				try {
					m_cmsgConfigCache.set(JSON.parseObject(codeAndRes.getValue(), Cmessaging.class));
					success = true;
				} catch (Exception e) {
					log.error("Wrong format of cmessaging config " + codeAndRes.getValue(), e);
				}
			} else if (code == 304) {
				success = true;
			}
		}

		return success;
	}

	private class ConfigUpdateTask implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(m_coreConfig.getCMessagingConfigUpdateInterval());
					updateConfig();
				} catch (Exception e) {
					log.warn("Error update cmessaging config", e);
				}
			}
		}

	}
}
