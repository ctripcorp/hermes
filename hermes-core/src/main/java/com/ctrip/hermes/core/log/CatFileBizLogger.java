package com.ctrip.hermes.core.log;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

@Named
public class CatFileBizLogger implements BizLogger {

	@Inject
	private FileBizLogger fileBizLogger;

	@Inject
	private CatBizLogger catBizLogger;

	@Override
	public void log(BizEvent event) {
		fileBizLogger.log(event);
		catBizLogger.log(event);
	}

}
