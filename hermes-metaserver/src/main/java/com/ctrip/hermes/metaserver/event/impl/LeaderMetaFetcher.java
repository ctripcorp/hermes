package com.ctrip.hermes.metaserver.event.impl;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.meta.MetaInfo;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface LeaderMetaFetcher {
	public Meta fetchMetaInfo(MetaInfo metaInfo);
}
