package com.ctrip.hermes.consumer.engine.ack;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public interface AckHolder<T> {

	public void delivered(long id, T attachment) throws AckHolderException;

	public void ack(long id, AckCallback<T> callback);

	public void nack(long id, NackCallback<T> callback);

	public AckHolderScanningResult<T> scan(int maxSize);

	public static interface NackCallback<T> {
		public void doBeforeNack(T item);
	}

	public static interface AckCallback<T> {
		public void doBeforeAck(T item);
	}
}
