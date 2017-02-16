package com.ctrip.hermes.kafka.producer;

import com.ctrip.hermes.core.result.SendResult;

public class KafkaSendResult extends SendResult {

	private String topic;

	private int partition;

	private long offset;

	public KafkaSendResult(){
		
	}
	
	public KafkaSendResult(String topic, int partition, long offset) {
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
	}

	public long getOffset() {
		return offset;
	}

	public int getPartition() {
		return partition;
	}

	public String getTopic() {
		return topic;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
