package com.ctrip.hermes.core.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.pipeline.spi.Valve;

public class DefaultPipelineContext<T> implements PipelineContext<T> {

	private PipelineSink<T> m_sink;

	private int m_index = 0;

	private List<Valve> m_valves;

	private Object m_source;

	private Map<String, Object> m_attrs = new HashMap<String, Object>();

	private T m_result;

	public DefaultPipelineContext(List<Valve> valves, PipelineSink<T> sink) {
		m_valves = valves;
		m_sink = sink;
	}

	@Override
	public void next(Object payload) {
		if (m_index == 0) {
			m_source = payload;
		}

		if (m_index < m_valves.size()) {
			Valve valve = m_valves.get(m_index);

			m_index++;
			valve.handle(this, payload);
		} else {
			m_result = m_sink.handle(this, payload);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <O> O getSource() {
		return (O) m_source;
	}

	@Override
	public void put(String name, Object value) {
		m_attrs.put(name, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <O> O get(String name) {
		return (O) m_attrs.get(name);
	}

	@Override
	public T getResult() {
		return m_result;
	}

}
