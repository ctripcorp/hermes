/**
 * 
 */
package com.ctrip.hermes.broker.selector;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.selector.SelectorManager;

/**
 * @author marsqing
 *
 *         Jun 27, 2016 3:06:50 PM
 */
public interface SendMessageSelectorManager extends SelectorManager<Pair<String, Integer>> {

	int SLOT_COUNT = 2;

	int SLOT_SAFE_TRIGGER_INDEX = 1;
}
