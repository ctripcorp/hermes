package com.ctrip.hermes.core.transport;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
@Retention(RUNTIME)
@Target({ TYPE })
public @interface ManualRelease {

}
