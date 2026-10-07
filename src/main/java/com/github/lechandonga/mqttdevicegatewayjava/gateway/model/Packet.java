package com.github.lechandonga.mqttdevicegatewayjava.gateway.model;

import java.util.Optional;

/**
 * 网关与本地内存通道之间流转的“报文”。只表达 MQTT 语义，不做二进制编解码。
 */
public sealed interface Packet {

    // ---------- 设备 -> 网关 ----------

    /** 连接请求。{@code cleanSession=true} 表示清理会话（MQTT 3.x 语义）。 */
    record Connect(String clientId, boolean cleanSession,
                   Optional<String> willTopic, Optional<byte[]> willPayload) implements Packet {
        public static Connect clean(String clientId) {
            return new Connect(clientId, true, Optional.empty(), Optional.empty());
        }

        public static Connect persistent(String clientId) {
            return new Connect(clientId, false, Optional.empty(), Optional.empty());
        }

        public static Connect withWill(String clientId, boolean cleanSession,
                                       String willTopic, byte[] willPayload) {
            return new Connect(clientId, cleanSession,
                    Optional.of(willTopic), Optional.of(willPayload));
        }
    }

    /** 订阅；{@code maxQos} 表示设备希望接收的最高投递等级。 */
    record Subscribe(String filter, int maxQos) implements Packet {
        public Subscribe(String filter) {
            this(filter, 1);
        }
    }

    /** 取消订阅。 */
    record Unsubscribe(String filter) implements Packet {
    }

    /** 设备发布消息；QoS 1 时 {@code packetId} 必填。 */
    record Publish(String topic, byte[] payload, int qos, boolean retain,
                   int packetId, boolean dup) implements Packet {
        public static Publish qos0(String topic, byte[] payload, boolean retain) {
            return new Publish(topic, payload, 0, retain, 0, false);
        }

        public static Publish qos1(String topic, byte[] payload, int packetId) {
            return new Publish(topic, payload, 1, false, packetId, false);
        }
    }

    /** QoS 1 入站发布的确认。 */
    record PubAck(int packetId) implements Packet {
    }

    /** 正常断开（DISCONNECT），不触发遗嘱。 */
    record Disconnect() implements Packet {
    }

    // ---------- 网关 -> 设备 ----------

    /** 连接确认；{@code sessionPresent} 表示是否复用了持久会话。 */
    record ConnAck(boolean sessionPresent) implements Packet {
    }

    /** 订阅确认；{@code grantedQos} 为最终授予的投递等级。 */
    record SubAck(String filter, int grantedQos) implements Packet {
    }

    /** 网关下发的发布报文；{@code dup=true} 表示重发。 */
    record Deliver(String topic, byte[] payload, int qos, boolean retain,
                   int packetId, boolean dup) implements Packet {
    }
}
