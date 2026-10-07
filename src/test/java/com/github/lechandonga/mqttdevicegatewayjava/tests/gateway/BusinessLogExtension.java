package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试日志扩展：让每个业务测试类的执行过程在日志中可辨识——
 * 类级别打印业务名称，方法级别打印每个用例的执行结果，最后汇总通过/失败数。
 */
public class BusinessLogExtension implements BeforeAllCallback, AfterAllCallback, TestWatcher {

    private static final Logger log = LoggerFactory.getLogger(BusinessLogExtension.class);

    private final AtomicInteger passed = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    static String businessOf(Class<?> testClass) {
        Business business = testClass.getAnnotation(Business.class);
        return business != null ? business.value() : testClass.getSimpleName();
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        log.info("========== 业务开始: {} ==========", businessOf(context.getRequiredTestClass()));
    }

    @Override
    public void afterAll(ExtensionContext context) {
        log.info("========== 业务结束: {} | 通过 {} 个用例, 失败 {} 个用例 ==========",
                businessOf(context.getRequiredTestClass()), passed.get(), failed.get());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        passed.incrementAndGet();
        log.info("  [用例通过] {}", context.getDisplayName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        failed.incrementAndGet();
        log.warn("  [用例失败] {} -> {}", context.getDisplayName(), cause.getMessage());
    }
}
