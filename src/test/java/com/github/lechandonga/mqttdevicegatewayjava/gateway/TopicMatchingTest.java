package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttTopics;

/**
 * 业务：主题匹配 —— MQTT 单层 + 与多层 # 约定。
 */
class TopicMatchingTest extends AbstractGatewayTest {

    @ParameterizedTest(name = "[{index}] filter={0}, topic={1}, match={2}")
    @CsvSource(delimiterString = "=>", textBlock = """
            devices/dev1/cmd        => devices/dev1/cmd        => true
            devices/dev1/+          => devices/dev1/cmd        => true
            devices/dev1/+          => devices/dev1/cmd/down   => false
            devices/+/cmd           => devices/dev1/cmd        => true
            devices/dev1/#          => devices/dev1/cmd        => true
            devices/dev1/#          => devices/dev1/a/b/c      => true
            devices/dev1/#          => devices/dev2/cmd        => false
            devices/dev1/+/x        => devices/dev1/cmd/x      => true
            devices/dev1/+/x        => devices/dev1/cmd/y      => false
            devices/dev1/cmd/#      => devices/dev1/cmd        => true
            devices/dev1/cmd/#      => devices/dev1/cmd/down   => true
            devices/dev1/+/#        => devices/dev1/a/b        => true
            devices/dev1/+/#        => devices/dev1/a          => true
            """)
    @DisplayName("业务03 单层 + / 多层 # 通配符匹配边界")
    void wildcardMatching(String filter, String topic, boolean expected) {
        boolean actual = MqttTopics.matches(filter.trim(), topic.trim());
        assertTrue(actual == expected,
                () -> "filter=" + filter + " topic=" + topic + " expected=" + expected);
        log.info("[业务03] filter='{}' topic='{}' -> {} (期望 {})",
                filter, topic, actual, expected);
    }

    @org.junit.jupiter.api.Test
    @DisplayName("业务03 补充：不同具体主题不应被精确过滤器匹配")
    void exactFilterDoesNotMatchOtherTopic() {
        assertFalse(MqttTopics.matches("devices/dev1/cmd", "devices/dev1/cmd/down"));
        assertFalse(MqttTopics.matches("devices/dev1/cmd", "devices/dev2/cmd"));
    }
}
