/**
 * 
 */
package com.ctrip.hermes.core.selector;

/**
 * @author marsqing
 *
 *         Jul 4, 2016 5:54:38 PM
 */
public interface OffsetGenerator {

	long nextOffset(int delta);

}
