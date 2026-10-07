package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceChannel;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Business("QoS1 重发与去重")
@ExtendWith(BusinessLogExtension.class)
class Qos1RedeliveryTest {

    private final DeviceGateway gateway = new DeviceGateway();

    @Test
    @DisplayName("已下发未确认的 QoS1 消息在重连后带 dup 标记重发，消息 id 不变可去重")
    void unackedMessagesAreRedeliveredWithDupFlag() {
        String device = "dev-q1";
        String topic = DeviceGateway.namespacePrefix(device) + "up";
        DeviceChannel channel = gateway.connect(device, false, null);
        gateway.subscribe(device, topic);
        gateway.publish(device, topic, "need-ack", 1, false);
        Message original = channel.poll();
        assertNotNull(original);
        assertFalse(original.dup());
        // 不确认，直接异常掉线
        gateway.disconnect(device, false);
        assertEquals(1, gateway.sessionView(device).unacked(), "未确认消息不能静默丢失");

        DeviceChannel reconnected = gateway.connect(device, false, null);
        Message redelivered = reconnected.poll();
        assertNotNull(redelivered, "重连后必须重发未确认消息");
        assertEquals(original.id(), redelivered.id(), "重发必须复用原消息 id 以便去重");
        assertTrue(redelivered.dup(), "重发消息必须带 dup 标记");
        assertEquals("need-ack", redelivered.payloadAsString());

        gateway.pubAck(device, redelivered.id());
        assertEquals(0, gateway.sessionView(device).unacked());
    }

    @Test
    @DisplayName("确认后的消息不再重发，重复确认幂等")
    void ackedMessagesAreNotRedeliveredAndDuplicateAckIsIdempotent() {
        String device = "dev-q2";
        String topic = DeviceGateway.namespacePrefix(device) + "up";
        DeviceChannel channel = gateway.connect(device, false, null);
        gateway.subscribe(device, topic);
        gateway.publish(device, topic, "m1", 1, false);
        gateway.publish(device, topic, "m2", 1, false);
        Message m1 = channel.poll();
        Message m2 = channel.poll();
        gateway.pubAck(device, m1.id());
        gateway.pubAck(device, m1.id()); // 重复确认不应报错
        assertEquals(1, gateway.sessionView(device).unacked());

        gateway.disconnect(device, false);
        DeviceChannel reconnected = gateway.connect(device, false, null);
        Message only = reconnected.poll();
        assertEquals(m2.id(), only.id(), "只有未确认的消息应被重发");
        assertTrue(only.dup());
    }

    @Test
    @DisplayName("多次掉线重连不会重复堆积同一条未确认消息")
    void repeatedReconnectsDoNotAccumulateDuplicates() {
        String device = "dev-q3";
        String topic = DeviceGateway.namespacePrefix(device) + "up";
        DeviceChannel channel = gateway.connect(device, false, null);
        gateway.subscribe(device, topic);
        gateway.publish(device, topic, "once", 1, false);
        long id = channel.poll().id();

        Set<Long> seen = new HashSet<>();
        seen.add(id);
        for (int i = 0; i < 3; i++) {
            gateway.disconnect(device, false);
            DeviceChannel next = gateway.connect(device, false, null);
            Message dup = next.poll();
            assertNotNull(dup);
            assertTrue(seen.add(dup.id()) || dup.id() == id, "重发消息 id 必须稳定");
            assertEquals(id, dup.id());
            assertEquals(1, gateway.sessionView(device).unacked(), "unacked 不能随重连次数增长");
        }
    }

    @Test
    @DisplayName("离线积压队列有上限，超限丢弃最老消息")
    void backlogIsBounded() {
        String device = "dev-q4";
        String topic = DeviceGateway.namespacePrefix(device) + "cmd";
        gateway.connect(device, false, null);
        gateway.subscribe(device, topic);
        gateway.disconnect(device, true);

        int total = DeviceGateway.MAX_PENDING_MESSAGES + 50;
        for (int i = 0; i < total; i++) {
            gateway.publishFromPlatform(topic, "m" + i, 0, false);
        }
        DeviceGateway.SessionView view = gateway.sessionView(device);
        assertEquals(DeviceGateway.MAX_PENDING_MESSAGES, view.backlog(), "积压必须有上限");

        DeviceChannel reconnected = gateway.connect(device, false, null);
        Message first = reconnected.poll();
        assertEquals("m50", first.payloadAsString(), "超限后最老的消息应被丢弃");
    }
}
