/**
 * 
 */
package com.ctrip.hermes.producer.sender;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.selector.SelectorManager;

/**
 * @author marsqing
 *
 *         Jun 28, 2016 4:11:23 PM
 */
public interface MessageSenderSelectorManager extends SelectorManager<Pair<String, Integer>> {

	int SLOT_COUNT = 2;

	int SLOT_0_INDEX = 0;
	int SLOT_SAFE_TRIGGER_INDEX = 1;
}
