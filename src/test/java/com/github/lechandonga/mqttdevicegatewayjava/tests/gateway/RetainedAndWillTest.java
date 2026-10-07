package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceChannel;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Business("保留消息与遗嘱消息")
@ExtendWith(BusinessLogExtension.class)
class RetainedAndWillTest {

    private final DeviceGateway gateway = new DeviceGateway();

    @Test
    @DisplayName("订阅时下发主题上最新的保留消息")
    void retainedMessageIsDeliveredOnSubscribe() {
        gateway.connect("dev-r1", true, null);
        String topic = DeviceGateway.namespacePrefix("dev-r1") + "config";
        gateway.publish("dev-r1", topic, "v1", 1, true);
        gateway.publish("dev-r1", topic, "v2", 1, true);

        DeviceChannel late = gateway.connect("dev-r2", true, null);
        gateway.subscribe("dev-r2", DeviceGateway.namespacePrefix("dev-r2") + "#");
        assertNull(late.poll(), "其他设备命名空间的保留消息不可见");

        gateway.subscribe("dev-r1", DeviceGateway.namespacePrefix("dev-r1") + "config");
        DeviceChannel ch1 = gateway.connect("dev-r1", false, null);
        // dev-r1 已是持久会话重连，重新订阅后应收到最新保留消息
        gateway.subscribe("dev-r1", topic);
        Message retained = ch1.poll();
        assertEquals("v2", retained.payloadAsString(), "只应下发最新的保留消息");
        assertTrue(retained.retained());
    }

    @Test
    @DisplayName("异常掉线触发遗嘱消息，正常断开不触发")
    void willFiresOnlyOnAbnormalDisconnect() {
        String owner = "dev-w1";
        String watcher = "dev-w2";
        String willTopic = DeviceGateway.namespacePrefix(owner) + "status";
        // watcher 无法订阅 owner 的命名空间，遗嘱可见性通过平台侧同名主题验证：
        // 让 owner 自己也订阅 status，观察会话视图；更直接的方式是平台订阅不可行，
        // 因此用 owner 的持久会话在重连后收取自己的遗嘱消息来验证。
        DeviceChannel ch = gateway.connect(owner, false, Message.will(willTopic, "offline", 1));
        gateway.subscribe(owner, willTopic);

        gateway.disconnect(owner, true);
        DeviceChannel afterGraceful = gateway.connect(owner, false, null);
        assertNull(afterGraceful.poll(), "正常断开不应触发遗嘱");

        gateway.connect(owner, false, Message.will(willTopic, "offline", 1));
        gateway.disconnect(owner, false);
        DeviceChannel afterLoss = gateway.connect(owner, false, null);
        Message will = afterLoss.poll();
        assertEquals("offline", will.payloadAsString(), "异常掉线必须触发遗嘱消息");
    }
}
