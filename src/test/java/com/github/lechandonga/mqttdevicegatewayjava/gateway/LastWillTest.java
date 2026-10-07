package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.api.GatewayClient;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttException;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttRejectReason;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.transport.LocalDeviceChannel;

/**
 * 业务：遗嘱消息 —— 异常掉线发布，正常断开不发布。
 */
class LastWillTest extends AbstractGatewayTest {

    @Test
    @DisplayName("业务05-用例01 异常掉线（crash）触发遗嘱，平台观察者收到离线消息")
    void abnormalDisconnectPublishesWill() {
        try (ServerInbox watcher = watch("devices/sensor/#")) {
            GatewayClient sensor = client("sensor");
            sensor.connectWithWill(false, "devices/sensor/status", bytes("offline"));
            sensor.drain();

            sensor.crash();

            List<Packet.Deliver> deliveries = watcher.drain();
            assertEquals(1, deliveries.size());
            assertEquals("devices/sensor/status", deliveries.get(0).topic());
            assertEquals("offline", new String(deliveries.get(0).payload()));
            log.info("[业务05-用例01] 异常掉线后观察者收到遗嘱: {}",
                    new String(deliveries.get(0).payload()));
        }
    }

    @Test
    @DisplayName("业务05-用例02 正常 DISCONNECT 不触发遗嘱")
    void gracefulDisconnectDoesNotPublishWill() {
        try (ServerInbox watcher = watch("devices/sensor/#")) {
            GatewayClient sensor = client("sensor");
            sensor.connectWithWill(false, "devices/sensor/status", bytes("offline"));
            sensor.drain();

            sensor.gracefulDisconnect();

            assertTrue(watcher.drain().isEmpty(), "正常断开绝不能误发遗嘱");
            log.info("[业务05-用例02] 正常 DISCONNECT 后观察者无遗嘱消息");
        }
    }

    @Test
    @DisplayName("业务05-用例03 遗嘱主题越权/非法时 CONNECT 即被拒绝")
    void illegalWillRejectedAtConnect() {
        LocalDeviceChannel channel = new LocalDeviceChannel("sensor");
        MqttException error = org.junit.jupiter.api.Assertions.assertThrows(
                MqttException.class,
                () -> gateway.connect(channel,
                        Packet.Connect.withWill("sensor", false,
                                "devices/other/will", bytes("x"))));
        assertEquals(MqttRejectReason.NAMESPACE_DENIED, error.reason());
        log.info("[业务05-用例03] 越权遗嘱在 CONNECT 阶段被拒绝: {}", error.reason());
    }
}
