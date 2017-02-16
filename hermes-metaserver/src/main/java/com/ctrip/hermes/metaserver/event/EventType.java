package com.ctrip.hermes.metaserver.event;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public enum EventType {
	BASE_META_CHANGED, //
	META_SERVER_LIST_CHANGED, //
	LEADER_INIT, //
	FOLLOWER_INIT, //
	OBSERVER_INIT, //
	BROKER_LIST_CHANGED, //
	BROKER_LEASE_CHANGED, //
	;
}
