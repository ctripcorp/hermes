package com.ctrip.hermes.core.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.constants.IdcPolicy;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.env.ClientEnvironment;

@Named
public class KafkaIdcStrategy {

	@Inject
	protected MetaService m_metaService;

	@Inject
	private ClientEnvironment m_environment;

	private static final Logger m_logger = LoggerFactory.getLogger(KafkaIdcStrategy.class);

	public String getTargetIdc(String idcPolicy) {
		if (idcPolicy == null) {
			m_logger.warn("Idc policy can not be null!");
			return null;
		}

		String targetIdc = null;

		switch (idcPolicy) {
		case IdcPolicy.PRIMARY:
			targetIdc = m_metaService.getPrimaryIdc().getId().toLowerCase();
			break;
		case IdcPolicy.LOCAL:
			targetIdc = m_environment.getIdc();
			if (targetIdc == null) {
				targetIdc = m_metaService.getPrimaryIdc().getId().toLowerCase();
				m_logger.warn("Can not get local idc! Will use primary idc {} as target idc.", targetIdc);
			}
			break;
		default:
			m_logger.warn("Unknown idc policy : {}!", idcPolicy);
			break;
		}

		return targetIdc;
	}
}
