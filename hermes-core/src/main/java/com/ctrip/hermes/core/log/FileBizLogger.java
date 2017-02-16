package com.ctrip.hermes.core.log;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.alibaba.fastjson.JSON;

@SuppressWarnings("deprecation")
@Named
public class FileBizLogger implements BizLogger {

	private final static Logger log = LoggerFactory.getLogger(LoggerNames.BIZ);

	private final static String m_localhost = Networks.forIp().getLocalHostAddress();

	@Override
	public void log(BizEvent event) {
		Map<String, Object> datas = event.getDatas();
		datas.put("host", m_localhost);
		datas.put("eventType", event.getEventType());
		datas.put("eventTime", event.getEventTime());
		log.info(JSON.toJSONString(datas));
	}
}
