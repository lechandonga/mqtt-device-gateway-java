package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.api.GatewayClient;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;

/**
 * 业务：接入会话生命周期 —— 持久会话恢复 / 清理会话丢弃。
 */
class SessionLifecycleTest extends AbstractGatewayTest {

    @Test
    @DisplayName("业务01-用例01 持久会话：掉线重连后恢复订阅，CONNACK 标记 sessionPresent=true")
    void persistentSessionRestoresSubscriptionsAfterReconnect() {
        GatewayClient device = client("dev1");
        device.connect(false);
        device.subscribe("devices/dev1/cmd");
        device.drain();

        device.crash();
        assertEquals(1, registry.size(), "异常掉线后持久会话仍保留在注册表中");

        device.connect(false);
        Packet.ConnAck connAck = (Packet.ConnAck) device.poll().orElseThrow();
        assertTrue(connAck.sessionPresent(), "重连必须告知设备复用了持久会话");
        log.info("[业务01-用例01] 重连 CONNACK: {}", connAck);

        // 平台侧（服务端发布模拟）向恢复后的订阅发消息，dev1 应仍能收到 -> 订阅被恢复
        gateway.serverPublish("devices/dev1/cmd", bytes("after-reconnect"), 1, false);

        Packet.Deliver delivered = device.drainDeliveries().get(0);
        assertEquals("devices/dev1/cmd", delivered.topic());
        assertEquals("after-reconnect", new String(delivered.payload()));
        log.info("[业务01-用例01] 恢复订阅后收到消息: topic={}, packetId={}",
                delivered.topic(), delivered.packetId());
    }

    @Test
    @DisplayName("业务01-用例02 清理会话：正常断开后订阅与会话整体丢弃，重连是全新会话")
    void cleanSessionDiscardsAllState() {
        GatewayClient device = client("dev1");
        device.connect(true);
        device.subscribe("devices/dev1/cmd");
        device.drain();

        device.gracefulDisconnect();
        assertEquals(0, registry.size(), "清理会话断开后注册表不应保留任何状态");

        device.connect(true);
        Packet.ConnAck connAck = (Packet.ConnAck) device.poll().orElseThrow();
        assertFalse(connAck.sessionPresent(), "清理会话重连必须是 sessionPresent=false");

        gateway.serverPublish("devices/dev1/cmd", bytes("nobody-home"), 0, false);

        List<Packet.Deliver> deliveries = device.drainDeliveries();
        assertTrue(deliveries.isEmpty(), "旧订阅已随会话删除，不应再收到消息");
        log.info("[业务01-用例02] 清理会话重连后投递数={}，符合预期", deliveries.size());
    }

    @Test
    @DisplayName("业务01-用例03 设备重新以清理会话接入持久会话：旧持久状态被废弃")
    void reconnectCleanDiscardsPreviousPersistentState() {
        GatewayClient device = client("dev1");
        device.connect(false);
        device.subscribe("devices/dev1/cmd");
        device.drain();
        device.crash();

        device.connect(true);
        Packet.ConnAck connAck = (Packet.ConnAck) device.poll().orElseThrow();
        assertFalse(connAck.sessionPresent());

        gateway.serverPublish("devices/dev1/cmd", bytes("lost-sub"), 0, false);
        assertTrue(device.drainDeliveries().isEmpty());
        log.info("[业务01-用例03] 以 cleanSession 重连后旧订阅失效，CONNACK={}", connAck);
    }

    @Test
    @DisplayName("业务01-用例04 同设备重复接入触发会话接管，旧连接被静默关闭且不发遗嘱")
    void takeoverClosesOldTransportWithoutWill() {
        try (ServerInbox watcher = watch("devices/dev1/#")) {
            GatewayClient device = client("dev1");
            var firstChannel = device.connectWithWill(false,
                    "devices/dev1/will", bytes("bye"));
            device.drain();

            // 第二个物理连接直接接管（模拟 MQTT 同 clientId 踢人）
            device.connect(false);
            assertFalse(firstChannel.isOpen(), "被接管的旧连接必须关闭");

            assertTrue(firstChannel.gatewayQueueView().isEmpty(),
                    "旧连接被接管时不应收到任何报文，遗嘱也不应触发");
            assertTrue(watcher.drain().isEmpty(),
                    "会话接管属于正常换连接，观察者不应收到遗嘱消息");
            log.info("[业务01-用例04] 会话接管完成，旧连接关闭且遗嘱未触发");
        }
    }
}
