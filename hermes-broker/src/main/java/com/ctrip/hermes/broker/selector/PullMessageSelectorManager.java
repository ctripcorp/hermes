/**
 * 
 */
package com.ctrip.hermes.broker.selector;

import com.ctrip.hermes.core.bo.Tp;
import com.ctrip.hermes.core.selector.SelectorManager;

/**
 * @author marsqing
 *
 *         Jun 22, 2016 3:51:40 PM
 */
public interface PullMessageSelectorManager extends SelectorManager<Tp> {

	int SLOT_COUNT = 3;

	int SLOT_PRIORITY_INDEX = 0;
	int SLOT_NONPRIORITY_INDEX = 1;
	int SLOT_RESEND_INDEX = 2;

}
