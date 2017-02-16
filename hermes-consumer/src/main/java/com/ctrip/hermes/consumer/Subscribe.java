package com.ctrip.hermes.consumer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Subscribe {

	public String topicPattern();

	public String groupId();

	public ConsumerType consumerType() default ConsumerType.DEFAULT;

}
