package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceChannel;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Business("会话恢复与清理")
@ExtendWith(BusinessLogExtension.class)
class SessionRecoveryTest {

    private final DeviceGateway gateway = new DeviceGateway();

    @Test
    @DisplayName("持久会话重连后恢复订阅，离线期间的消息按序补发")
    void persistentSessionRestoresSubscriptionsAndFlushesBacklog() {
        String device = "dev-p1";
        String topic = DeviceGateway.namespacePrefix(device) + "cmd";
        DeviceChannel channel = gateway.connect(device, false, null);
        gateway.subscribe(device, topic);
        gateway.disconnect(device, true);

        DeviceGateway.SessionView offline = gateway.sessionView(device);
        assertTrue(offline.exists());
        assertFalse(offline.online());
        assertEquals(1, offline.subscriptions(), "断开后持久会话的订阅应保留");

        // 设备离线期间平台下行两条消息，应进入积压队列
        gateway.publishFromPlatform(topic, "offline-1", 1, false);
        gateway.publishFromPlatform(topic, "offline-2", 0, false);
        assertEquals(2, gateway.sessionView(device).backlog());

        DeviceChannel reconnected = gateway.connect(device, false, null);
        assertEquals(1, gateway.sessionView(device).subscriptions(), "重连后订阅应恢复，无需重新订阅");
        Message first = reconnected.poll();
        Message second = reconnected.poll();
        assertEquals("offline-1", first.payloadAsString());
        assertEquals("offline-2", second.payloadAsString());
        assertFalse(first.dup(), "离线积压消息首次投递不应标记 dup");
        gateway.pubAck(device, first.id());
        assertEquals(0, gateway.sessionView(device).unacked());
    }

    @Test
    @DisplayName("清理会话断开后完整丢弃订阅与未确认消息")
    void cleanSessionDropsAllState() {
        String device = "dev-c1";
        String topic = DeviceGateway.namespacePrefix(device) + "data";
        DeviceChannel channel = gateway.connect(device, true, null);
        gateway.subscribe(device, topic);
        gateway.publish(device, topic, "unacked", 1, false);
        channel.poll(); // 已下发但不确认
        gateway.disconnect(device, true);

        assertFalse(gateway.sessionView(device).exists(), "清理会话断开后会话应被移除");

        DeviceChannel fresh = gateway.connect(device, true, null);
        assertEquals(0, gateway.sessionView(device).subscriptions(), "清理会话重连后不应恢复订阅");
        assertNull(fresh.poll(), "清理会话重连后不应重发任何消息");
    }

    @Test
    @DisplayName("清理会话重连会覆盖同设备旧的持久会话状态")
    void cleanConnectOverridesPreviousSession() {
        String device = "dev-c2";
        String topic = DeviceGateway.namespacePrefix(device) + "evt";
        gateway.connect(device, false, null);
        gateway.subscribe(device, topic);
        gateway.disconnect(device, true);
        assertEquals(1, gateway.sessionView(device).subscriptions());

        gateway.connect(device, true, null);
        assertEquals(0, gateway.sessionView(device).subscriptions(), "以清理会话接入应丢弃旧订阅");
    }
}
