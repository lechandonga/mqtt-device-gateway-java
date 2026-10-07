package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.api.GatewayClient;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttException;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttRejectReason;

/**
 * 业务：身份主题权限与非法主题拒绝，原因码必须可区分。
 */
class AuthorizationTest extends AbstractGatewayTest {

    private GatewayClient connectedDevice(String id) {
        GatewayClient client = client(id);
        client.connect(true);
        client.drain();
        return client;
    }

    private MqttRejectReason subscribeReject(GatewayClient device, String filter) {
        MqttException error = assertThrows(MqttException.class,
                () -> gateway.subscribe(device.deviceId(), new Packet.Subscribe(filter, 1)));
        return error.reason();
    }

    private MqttRejectReason publishReject(GatewayClient device, String topic) {
        MqttException error = assertThrows(MqttException.class,
                () -> gateway.publish(device.deviceId(),
                        Packet.Publish.qos0(topic, bytes("x"), false)));
        return error.reason();
    }

    @Test
    @DisplayName("业务04-用例01 越权访问：设备不能订阅/发布他人命名空间，原因=NAMESPACE_DENIED")
    void crossNamespaceRejected() {
        GatewayClient dev1 = connectedDevice("dev1");

        assertEquals(MqttRejectReason.NAMESPACE_DENIED,
                subscribeReject(dev1, "devices/dev2/cmd"));
        assertEquals(MqttRejectReason.NAMESPACE_DENIED,
                subscribeReject(dev1, "devices/+/cmd"),
                "设备 ID 层必须是自己的字面量，不能用 + 偷看别人");
        assertEquals(MqttRejectReason.NAMESPACE_DENIED,
                publishReject(dev1, "devices/dev2/cmd"));
        assertEquals(MqttRejectReason.NAMESPACE_DENIED,
                publishReject(dev1, "system/admin"));
        log.info("[业务04-用例01] 跨命名空间订阅/发布全部被拒，原因可区分");
    }

    @Test
    @DisplayName("业务04-用例02 合法的自有命名空间订阅/发布放行")
    void ownNamespaceAllowed() {
        GatewayClient dev1 = connectedDevice("dev1");
        gateway.subscribe("dev1", new Packet.Subscribe("devices/dev1/+", 1));
        gateway.subscribe("dev1", new Packet.Subscribe("devices/dev1/a/#", 1));
        Integer ack = gateway.publish("dev1",
                Packet.Publish.qos1("devices/dev1/telemetry/temp", bytes("36.5"), 1));
        assertEquals(1, ack);
        log.info("[业务04-用例02] devices/dev1 自有命名空间订阅与发布放行");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    @DisplayName("业务04-用例03 空主题拒绝：EMPTY_TOPIC")
    void emptyTopicRejected(String topic) {
        GatewayClient dev1 = connectedDevice("dev1");
        assertEquals(MqttRejectReason.EMPTY_TOPIC, subscribeReject(dev1, topic));
        assertEquals(MqttRejectReason.EMPTY_TOPIC, publishReject(dev1, topic));
        log.info("[业务04-用例03] 空主题被拒绝");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/devices/dev1/cmd", "devices/dev1/cmd/", "devices//dev1/cmd"})
    @DisplayName("业务04-用例04 非法层级主题拒绝：INVALID_TOPIC")
    void malformedTopicRejected(String topic) {
        GatewayClient dev1 = connectedDevice("dev1");
        assertEquals(MqttRejectReason.INVALID_TOPIC, subscribeReject(dev1, topic));
        log.info("[业务04-用例04] 非法主题 '{}' 被拒绝", topic);
    }

    @Test
    @DisplayName("业务04-用例05 发布主题含通配符拒绝：WILDCARD_IN_PUBLISH")
    void wildcardPublishRejected() {
        GatewayClient dev1 = connectedDevice("dev1");
        assertEquals(MqttRejectReason.WILDCARD_IN_PUBLISH,
                publishReject(dev1, "devices/dev1/+"));
        assertEquals(MqttRejectReason.WILDCARD_IN_PUBLISH,
                publishReject(dev1, "devices/dev1/#"));
        log.info("[业务04-用例05] 发布携带 +/# 被拒绝");
    }

    @Test
    @DisplayName("业务04-用例06 通配符位置错误拒绝：# 不在末层 / + 不独占整层")
    void misplacedWildcardsRejected() {
        GatewayClient dev1 = connectedDevice("dev1");
        assertEquals(MqttRejectReason.BAD_MULTI_WILDCARD_POSITION,
                subscribeReject(dev1, "devices/dev1/#/cmd"));
        assertEquals(MqttRejectReason.BAD_MULTI_WILDCARD_POSITION,
                subscribeReject(dev1, "devices/dev1/cmd#"));
        assertEquals(MqttRejectReason.BAD_SINGLE_WILDCARD_POSITION,
                subscribeReject(dev1, "devices/dev1/cmd+x"));
        log.info("[业务04-用例06] 通配符错位分别给出 MULTI/SINGLE 原因码");
    }

    @Test
    @DisplayName("业务04-用例07 越权拒绝不影响其他设备会话")
    void rejectionIsolatesSessions() {
        GatewayClient good = connectedDevice("good");
        GatewayClient evil = connectedDevice("evil");
        good.subscribe("devices/good/ok");
        good.drain();
        assertThrows(MqttException.class,
                () -> gateway.subscribe("evil", new Packet.Subscribe("devices/good/#", 1)));

        gateway.serverPublish("devices/good/ok", bytes("still-works"), 1, false);
        Packet.Deliver deliver = good.drainDeliveries().get(0);
        assertEquals("still-works", new String(deliver.payload()));
        log.info("[业务04-用例07] evil 的越权订阅被拒后，good 的会话与投递不受影响");
    }
}
