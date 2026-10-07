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
 * 业务：QoS 0/1 投递 —— 未确认重发、DUP 去重、保留消息。
 *
 * <p>设备只能访问自己的命名空间，因此“别的设备给本设备发消息”的场景
 * 由 {@code gateway.serverPublish}（平台下发）模拟。
 */
class DeliveryQosTest extends AbstractGatewayTest {

    @Test
    @DisplayName("业务02-用例01 QoS1 已下发未确认：掉线重连后原 packetId 消息带 DUP 重发")
    void unackedQos1RedeliveredAfterReconnectWithDup() {
        GatewayClient subscriber = client("sub");
        subscriber.connect(false);
        subscriber.subscribe("devices/sub/data");
        subscriber.drain();

        gateway.serverPublish("devices/sub/data", bytes("one"), 1, false);

        Packet.Deliver first = subscriber.drainDeliveries().get(0);
        assertEquals(1, first.qos());
        assertFalse(first.dup(), "首次下发 DUP 必须为 false");
        int packetId = first.packetId();
        log.info("[业务02-用例01] 首次下发: packetId={}, dup={}", packetId, first.dup());

        // 确认丢失：设备没有回 PUBACK 就掉线
        subscriber.crash();
        subscriber.connect(false);
        assertTrue(subscriber.awaitConnAck().sessionPresent());

        Packet.Deliver redelivered = subscriber.drainDeliveries().get(0);
        assertEquals(packetId, redelivered.packetId(), "重发必须复用同一个 packetId");
        assertTrue(redelivered.dup(), "重发报文必须带 DUP 标记");
        assertEquals("one", new String(redelivered.payload()));
        log.info("[业务02-用例01] 重连后重发: packetId={}, dup={}",
                redelivered.packetId(), redelivered.dup());

        // 设备这次确认，队列被清空；再次重连不会再收到
        subscriber.ack(redelivered);
        subscriber.crash();
        subscriber.connect(false);
        assertTrue(subscriber.awaitConnAck().sessionPresent());
        assertTrue(subscriber.drainDeliveries().isEmpty(), "已确认消息不得再次投递");
        log.info("[业务02-用例01] PUBACK 后二次重连无残留投递，符合至少一次+去重语义");
    }

    @Test
    @DisplayName("业务02-用例02 重发识别：同一 packetId 再次到达时设备侧不产生第二条业务消息")
    void duplicateDeliveryHasSamePacketIdAndIsDeduplicatedByClient() {
        GatewayClient subscriber = client("sub");
        subscriber.connect(false);
        subscriber.subscribe("devices/sub/data");
        subscriber.drain();

        gateway.serverPublish("devices/sub/data", bytes("dup-me"), 1, false);

        Packet.Deliver first = subscriber.drainDeliveries().get(0);
        // 设备侧按 packetId 去重的典型处理：记录已处理 id，重复的只补 ACK
        java.util.Set<Integer> processed = new java.util.HashSet<>();
        assertTrue(processed.add(first.packetId()), "首次消息被业务处理");

        subscriber.crash();
        subscriber.connect(false);
        assertTrue(subscriber.awaitConnAck().sessionPresent());
        Packet.Deliver again = subscriber.drainDeliveries().get(0);
        assertFalse(processed.add(again.packetId()), "同 packetId 的重发被识别为重复");
        assertTrue(again.dup());
        subscriber.ack(again);
        log.info("[业务02-用例02] packetId={} 的重复投递被去重，业务只处理一次", again.packetId());
    }

    @Test
    @DisplayName("业务02-用例03 重复 PUBACK 幂等：不会导致异常或消息回潮")
    void duplicatePubackIsIdempotent() {
        GatewayClient subscriber = client("sub");
        subscriber.connect(false);
        subscriber.subscribe("devices/sub/data");
        subscriber.drain();

        gateway.serverPublish("devices/sub/data", bytes("ack-twice"), 1, false);
        Packet.Deliver delivered = subscriber.drainDeliveries().get(0);

        subscriber.ack(delivered);
        subscriber.ack(delivered);
        subscriber.crash();
        subscriber.connect(false);
        assertTrue(subscriber.awaitConnAck().sessionPresent());
        assertTrue(subscriber.drainDeliveries().isEmpty());
        log.info("[业务02-用例03] packetId={} 重复 PUBACK 后无重投", delivered.packetId());
    }

    @Test
    @DisplayName("业务02-用例04 设备重发同一入站 QoS1 报文：网关只分发一次，补发 PUBACK")
    void duplicateInboundPublishIsHandledOnce() {
        try (ServerInbox inbox = watch("devices/pub/chat")) {
            GatewayClient publisher = client("pub");
            publisher.connect(true);
            publisher.drain();

            publisher.publishQos1Raw("devices/pub/chat", bytes("hello"), 7, false);
            publisher.publishQos1Raw("devices/pub/chat", bytes("hello"), 7, true);

            List<Packet.Deliver> deliveries = inbox.drain();
            assertEquals(1, deliveries.size(), "设备重发的入站报文不能让订阅者收到两条");
            List<Packet> publisherDown = publisher.drain();
            long pubacks = publisherDown.stream().filter(Packet.PubAck.class::isInstance).count();
            assertEquals(2, pubacks, "两次发送都应得到 PUBACK（重发的那次是补发确认）");
            log.info("[业务02-用例04] 入站 packetId=7 重发：业务消息1条，PUBACK 2个");
        }
    }

    @Test
    @DisplayName("业务02-用例05 QoS0：在线尽力投递，离线期间不排队补发")
    void qos0FireAndForget() {
        GatewayClient subscriber = client("sub");
        subscriber.connect(false);
        subscriber.subscribe("devices/sub/ping");
        subscriber.drain();

        subscriber.crash();
        gateway.serverPublish("devices/sub/ping", bytes("while-offline"), 0, false);

        subscriber.connect(false);
        assertTrue(subscriber.awaitConnAck().sessionPresent());
        gateway.serverPublish("devices/sub/ping", bytes("online"), 0, false);
        List<Packet.Deliver> deliveries = subscriber.drainDeliveries();
        assertEquals(1, deliveries.size());
        assertEquals("online", new String(deliveries.get(0).payload()));
        log.info("[业务02-用例05] 离线期 QoS0 不补发，重连后只收到在线消息");
    }

    @Test
    @DisplayName("业务02-用例06 保留消息：新订阅立即收到主题上最新的一条保留消息")
    void retainedMessageDeliveredOnNewSubscription() {
        GatewayClient publisher = client("pub");
        publisher.connect(true);
        publisher.drain();
        publisher.publishQos0("devices/pub/status", bytes("v1"), true);
        publisher.publishQos0("devices/pub/status", bytes("v2-latest"), true);

        // 平台侧新订阅（跨命名空间观察保留消息）
        try (ServerInbox late = watch("devices/pub/status")) {
            Packet.Deliver retained = late.drain().get(0);
            assertEquals("v2-latest", new String(retained.payload()));
            assertTrue(retained.retain(), "订阅触发的保留消息要带 retain 标记");
            log.info("[业务02-用例06] 新订阅收到最新保留消息: {}", new String(retained.payload()));
        }

        // 空载荷保留消息 = 清除
        publisher.publishQos0("devices/pub/status", new byte[0], true);
        try (ServerInbox later = watch("devices/pub/status")) {
            assertTrue(later.drain().isEmpty(), "保留消息被空载荷清除后不再下发");
        }
        log.info("[业务02-用例06] 空载荷清除保留消息后新订阅无保留投递");
    }

    @Test
    @DisplayName("业务02-用例07 设备订阅自有主题同样能立即收到保留消息")
    void retainedMessageDeliveredToDeviceSubscription() {
        gateway.serverPublish("devices/self/cfg", bytes("boot-config"), 0, true);

        GatewayClient device = client("self");
        device.connect(true);
        device.drain();
        device.subscribe("devices/self/cfg");

        Packet.Deliver retained = device.drainDeliveries().get(0);
        assertEquals("boot-config", new String(retained.payload()));
        assertTrue(retained.retain());
        log.info("[业务02-用例07] 设备订阅自有主题收到保留消息 {}", new String(retained.payload()));
    }
}
