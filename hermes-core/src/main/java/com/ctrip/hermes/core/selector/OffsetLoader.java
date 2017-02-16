/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jun 22, 2016 12:02:26 AM
 */
public interface OffsetLoader<T> {

	void loadAsync(T key, final Selector<T> selector);

	public class NoOpOffsetLoader<T> implements OffsetLoader<T> {

		@Override
		public void loadAsync(T key, final Selector<T> selector) {
		}

	}

}
