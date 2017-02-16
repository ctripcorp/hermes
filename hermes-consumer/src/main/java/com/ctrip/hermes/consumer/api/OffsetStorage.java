package com.ctrip.hermes.consumer.api;

import com.ctrip.hermes.core.bo.Offset;

public interface OffsetStorage {
	public Offset queryLatestOffset(String topic, int partition);

	public void updatePulledOffset(String topic, int partition, Offset offset);

}
