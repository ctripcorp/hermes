/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jul 7, 2016 10:31:57 AM
 */
public interface SelectorManager<T> {

	Selector<T> getSelector();

	void reRegister(T key, CallbackContext callbackCtx, TriggerResult triggerResult, ExpireTimeHolder expireTimeHolder, SelectorCallback callback);

	void register(T key, ExpireTimeHolder expireTimeHolder, SelectorCallback callback, Object arg, long... initDoneOffsets);

}
