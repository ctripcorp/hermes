package com.ctrip.hermes.consumer.engine;

import java.util.ArrayList;
import java.util.List;

public class CompositeSubscribeHandle implements SubscribeHandle {

	private List<SubscribeHandle> m_childHandles = new ArrayList<SubscribeHandle>();

	public void addSubscribeHandle(SubscribeHandle handle) {
		m_childHandles.add(handle);
	}

	public List<SubscribeHandle> getChildHandleList() {
		return m_childHandles;
	}

	@Override
	public void close() {
		for (SubscribeHandle child : m_childHandles) {
			child.close();
		}
	}

}
