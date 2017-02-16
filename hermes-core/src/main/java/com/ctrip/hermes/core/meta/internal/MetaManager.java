package com.ctrip.hermes.core.meta.internal;

import com.ctrip.hermes.meta.entity.Meta;

public interface MetaManager {

	public Meta loadMeta();

	public MetaProxy getMetaProxy();

}
