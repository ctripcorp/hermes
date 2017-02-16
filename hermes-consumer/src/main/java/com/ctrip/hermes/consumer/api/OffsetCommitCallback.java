package com.ctrip.hermes.consumer.api;

import java.util.Map;

public interface OffsetCommitCallback {

	public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception);
	
}
