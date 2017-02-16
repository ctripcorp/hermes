package com.ctrip.hermes.metaserver.event;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface EventHandler {

	public void onEvent(Event event) throws Exception;

	public String getName();

	public EventType eventType();

	public void setEventBus(EventBus eventBus);

	public void setClusterStateHolder(ClusterStateHolder clusterStateHolder);

	public void start();

}
