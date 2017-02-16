package com.ctrip.hermes.core.meta.manual;

import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface ManualConfigService {

	public void configure(ManualConfig config);

	public void reset();

	public Meta getMeta();

	public boolean isManualConfigModeOn();

	public MetaProxy getMetaProxy();

	public ManualConfig getConfig();
}
