/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jul 7, 2016 4:08:40 PM
 */
public class TriggerResult {

	public enum State {
		GotAndSuccessfullyProcessed, GotButErrorInProcessing, GotNothing
	}

	private State m_state;
	private long[] m_doneOffsets;
	private Object m_arg;

	public TriggerResult(State state, long[] doneOffsets) {
		m_state = state;
		m_doneOffsets = doneOffsets;
	}

	public TriggerResult(State state, long[] doneOffsets, Object arg) {
		m_state = state;
		m_doneOffsets = doneOffsets;
		m_arg = arg;
	}

	public State getState() {
		return m_state;
	}

	public long[] getDoneOffsets() {
		return m_doneOffsets;
	}

	public Object getArg() {
		return m_arg;
	}

}
