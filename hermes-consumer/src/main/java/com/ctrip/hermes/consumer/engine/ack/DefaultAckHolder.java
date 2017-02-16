package com.ctrip.hermes.consumer.engine.ack;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.engine.ack.DefaultAckHolder.Item.State;

/**
 * ThreadUnsafe
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class DefaultAckHolder<T> implements AckHolder<T> {
	private static Logger log = LoggerFactory.getLogger(DefaultAckHolder.class);

	private Object m_token;

	private LinkedList<Item<T>> m_items = new LinkedList<>();

	private Map<Long, Item<T>> m_itemIndex = new HashMap<>();

	private int m_maxSize;

	public DefaultAckHolder(Object token, int maxSize) {
		m_token = token;
		m_maxSize = maxSize;
	}

	@Override
	public synchronized void delivered(long id, T attachment) throws AckHolderException {
		if (!m_items.isEmpty() && m_items.getLast().getId() >= id) {
			throw new AckHolderException(String.format("Must deliver in ascending order of id(lastId=%s, id=%s)", m_items
			      .getLast().getId(), id));
		}

		Item<T> item = new Item<T>(id, attachment);
		m_items.addLast(item);
		m_itemIndex.put(id, item);
	}

	@Override
	public synchronized void ack(long id, AckCallback<T> callback) {
		Item<T> item = m_itemIndex.get(id);
		if (item != null) {
			if (callback != null) {
				callback.doBeforeAck(item.getAttachment());
			}
			item.ack();
		}
	}

	@Override
	public synchronized void nack(long id, NackCallback<T> callback) {
		Item<T> item = m_itemIndex.get(id);
		if (item != null) {
			if (callback != null) {
				callback.doBeforeNack(item.getAttachment());
			}
			item.nack();
		}
	}

	@Override
	public synchronized AckHolderScanningResult<T> scan(int maxSize) {
		AckHolderScanningResult<T> res = new AckHolderScanningResult<T>();

		int count = 0;
		while (!m_items.isEmpty()) {
			Item<T> item = m_items.getFirst();
			boolean addedToRes = false;

			if (item.getState() == State.ACK) {
				res.addAcked(item.getAttachment());
				addedToRes = true;
			} else if (item.getState() == State.NACK) {
				res.addNacked(item.getAttachment());
				addedToRes = true;
			} else {
				if (m_items.size() > m_maxSize) {
					log.warn(
					      "Message hasn't been acked or nacked for long time, will be treated as nack(id={}, holder-token={}).",
					      item.getId(), m_token);
					res.addNacked(item.getAttachment());
					addedToRes = true;
				} else {
					break;
				}
			}

			if (addedToRes) {
				count++;
				m_itemIndex.remove(item.getId());
				m_items.removeFirst();
			}

			if (count >= maxSize) {
				break;
			}
		}

		return res;
	}

	static class Item<T> {
		static enum State {
			INIT, ACK, NACK
		}

		private long m_id;

		private T m_attachment;

		private long m_createTime;

		private State m_state = State.INIT;

		public Item(long id, T attachment) {
			m_id = id;
			m_attachment = attachment;
			m_createTime = System.currentTimeMillis();
			m_state = State.INIT;
		}

		public long getId() {
			return m_id;
		}

		public T getAttachment() {
			return m_attachment;
		}

		public long getCreateTime() {
			return m_createTime;
		}

		public State getState() {
			return m_state;
		}

		public void ack() {
			if (m_state == State.INIT) {
				m_state = State.ACK;
			}
		}

		public void nack() {
			if (m_state == State.INIT) {
				m_state = State.NACK;
			}
		}
	}

}
