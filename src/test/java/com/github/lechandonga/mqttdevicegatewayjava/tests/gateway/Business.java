package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 标注测试类覆盖的业务名称，用于测试日志输出。 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Business {
    String value();
}
