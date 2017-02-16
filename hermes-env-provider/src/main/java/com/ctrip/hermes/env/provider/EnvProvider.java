package com.ctrip.hermes.env.provider;

import java.util.Properties;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface EnvProvider {
	public String getEnv();

	public void initialize(Properties config);

	public String getMetaServerDomainName();

	public String getIdc();
}
