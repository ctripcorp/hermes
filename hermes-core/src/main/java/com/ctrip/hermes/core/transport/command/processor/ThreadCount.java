package com.ctrip.hermes.core.transport.command.processor;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * 
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface ThreadCount {
	int value();
}
