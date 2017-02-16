package com.ctrip.hermes.kafka.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.ctrip.hermes.core.result.SendResult;

public class KafkaFuture implements Future<SendResult> {

	private Future<RecordMetadata> m_recordMetadata;

	public KafkaFuture(Future<RecordMetadata> recordMetadata) {
		this.m_recordMetadata = recordMetadata;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return this.m_recordMetadata.cancel(mayInterruptIfRunning);
	}

	@Override
	public KafkaSendResult get() throws InterruptedException, ExecutionException {
		RecordMetadata recordMetadata = this.m_recordMetadata.get();
		KafkaSendResult sendResult = new KafkaSendResult(recordMetadata.topic(), recordMetadata.partition(),
		      recordMetadata.offset());
		return sendResult;
	}

	@Override
	public KafkaSendResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		RecordMetadata recordMetadata = this.m_recordMetadata.get(timeout, unit);
		KafkaSendResult sendResult = new KafkaSendResult(recordMetadata.topic(), recordMetadata.partition(),
		      recordMetadata.offset());
		return sendResult;
	}

	@Override
	public boolean isCancelled() {
		return this.m_recordMetadata.isCancelled();
	}

	@Override
	public boolean isDone() {
		return this.m_recordMetadata.isDone();
	}

}
