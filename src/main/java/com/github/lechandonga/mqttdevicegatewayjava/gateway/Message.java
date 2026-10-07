package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import java.nio.charset.StandardCharsets;

/**
 * 网关内部投递的消息。语义对齐 MQTT PUBLISH，但不做二进制编解码。
 *
 * @param id       网关内单调递增的消息 id，QoS 1 的 PUBACK 与重发去重都基于它
 * @param topic    发布主题（不含通配符）
 * @param payload  消息体
 * @param qos      0 或 1
 * @param retained 是否为保留消息
 * @param dup      是否为重发（QoS 1 未确认消息在重连后重投时置 true）
 */
public record Message(long id, String topic, byte[] payload, int qos, boolean retained, boolean dup) {

    public static Message of(long id, String topic, String payload, int qos, boolean retained, boolean dup) {
        return new Message(id, topic, payload.getBytes(StandardCharsets.UTF_8), qos, retained, dup);
    }

    /** 构造遗嘱消息（id 由网关发布时重新分配）。 */
    public static Message will(String topic, String payload, int qos) {
        return of(0, topic, payload, qos, false, false);
    }

    public Message withDup() {
        return new Message(id, topic, payload, qos, retained, true);
    }

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "Message{id=%d, topic='%s', qos=%d, retained=%s, dup=%s, payload='%s'}"
                .formatted(id, topic, qos, retained, dup, payloadAsString());
    }
}
