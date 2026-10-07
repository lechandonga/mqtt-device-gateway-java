package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceChannel;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.GatewayException;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.RejectionReason;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Business("主题权限与非法主题拒绝")
@ExtendWith(BusinessLogExtension.class)
class AclAndTopicValidationTest {

    private final DeviceGateway gateway = new DeviceGateway();

    private String rejectPublish(String device, String topic) {
        GatewayException e = assertThrows(GatewayException.class,
                () -> gateway.publish(device, topic, "x", 0, false));
        return e.getReason().name();
    }

    @Test
    @DisplayName("设备不能发布或订阅其他设备命名空间下的主题")
    void crossNamespaceAccessIsRejected() {
        gateway.connect("dev-a", true, null);
        GatewayException pub = assertThrows(GatewayException.class,
                () -> gateway.publish("dev-a", "devices/dev-b/telemetry", "x", 0, false));
        assertEquals(RejectionReason.TOPIC_OUT_OF_NAMESPACE, pub.getReason());

        GatewayException sub = assertThrows(GatewayException.class,
                () -> gateway.subscribe("dev-a", "devices/dev-b/#"));
        assertEquals(RejectionReason.TOPIC_OUT_OF_NAMESPACE, sub.getReason());

        GatewayException root = assertThrows(GatewayException.class,
                () -> gateway.subscribe("dev-a", "#"));
        assertEquals(RejectionReason.TOPIC_OUT_OF_NAMESPACE, root.getReason());
    }

    @Test
    @DisplayName("空主题、非法格式、通配符位置错误分别给出可区分的原因")
    void invalidTopicsAreRejectedWithDistinctReasons() {
        gateway.connect("dev-t", true, null);
        String ns = DeviceGateway.namespacePrefix("dev-t");

        assertEquals("EMPTY_TOPIC", rejectPublish("dev-t", ""));
        assertEquals("EMPTY_TOPIC", rejectPublish("dev-t", "  "));
        assertEquals("INVALID_TOPIC_FORMAT", rejectPublish("dev-t", ns + "a//b"));
        assertEquals("INVALID_TOPIC_FORMAT", rejectPublish("dev-t", "/" + ns + "a"));
        assertEquals("WILDCARD_IN_PUBLISH", rejectPublish("dev-t", ns + "+"));
        assertEquals("WILDCARD_IN_PUBLISH", rejectPublish("dev-t", ns + "#"));

        GatewayException hashNotLast = assertThrows(GatewayException.class,
                () -> gateway.subscribe("dev-t", ns + "#/tail"));
        assertEquals(RejectionReason.WILDCARD_MISPLACED, hashNotLast.getReason());

        GatewayException plusPartial = assertThrows(GatewayException.class,
                () -> gateway.subscribe("dev-t", ns + "sen+or/temp"));
        assertEquals(RejectionReason.WILDCARD_MISPLACED, plusPartial.getReason());

        GatewayException notConnected = assertThrows(GatewayException.class,
                () -> gateway.publish("dev-offline", ns + "a", "x", 0, false));
        assertEquals(RejectionReason.NOT_CONNECTED, notConnected.getReason());
    }

    @Test
    @DisplayName("通配符按 MQTT 单层/多层约定匹配")
    void wildcardMatchingFollowsMqttSemantics() throws Exception {
        gateway.connect("dev-w", true, null);
        String ns = DeviceGateway.namespacePrefix("dev-w");
        DeviceChannel channel = gateway.connect("dev-w2", true, null);
        gateway.subscribe("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor/+/temp");

        gateway.publish("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor/s1/temp", "20", 0, false);
        gateway.publish("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor/s1/hum", "40", 0, false);
        gateway.publish("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor/a/b/temp", "x", 0, false);
        Thread.sleep(50);
        assertEquals("20", channel.poll().payloadAsString(), "+ 应匹配单层");
        assertNull(channel.poll(), "+ 不应跨层匹配，hum 与多级主题都不应命中");

        gateway.subscribe("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor/#");
        gateway.publish("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor/a/b/temp", "y", 0, false);
        gateway.publish("dev-w2", DeviceGateway.namespacePrefix("dev-w2") + "sensor", "z", 0, false);
        assertEquals("y", channel.poll().payloadAsString(), "# 应匹配多级");
        assertEquals("z", channel.poll().payloadAsString(), "# 应匹配父层级自身");
    }

    @Test
    @DisplayName("越权与非法主题被拒绝后不影响其他设备的会话")
    void rejectionDoesNotAffectOtherSessions() {
        DeviceChannel good = gateway.connect("dev-good", true, null);
        gateway.subscribe("dev-good", DeviceGateway.namespacePrefix("dev-good") + "evt");
        gateway.connect("dev-bad", true, null);

        assertThrows(GatewayException.class,
                () -> gateway.publish("dev-bad", "devices/dev-good/evt", "intrude", 0, false));
        assertThrows(GatewayException.class,
                () -> gateway.subscribe("dev-bad", ""));

        gateway.publish("dev-good", DeviceGateway.namespacePrefix("dev-good") + "evt", "ok", 0, false);
        assertEquals("ok", good.poll().payloadAsString(), "正常设备的收发不应受越权行为影响");
    }
}
