package com.github.lechandonga.mqttdevicegatewayjava.gateway.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.MqttGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.transport.LocalDeviceChannel;

/**
 * 设备侧测试/演示客户端：封装内存通道，模拟设备反复上下线与收发。
 *
 * <p>每次 {@link #connect} 都创建一条全新的 {@link LocalDeviceChannel}，
 * 对应真实世界里设备断线后重新建立 TCP 连接。
 */
public final class GatewayClient {

    private final MqttGateway gateway;
    private final String deviceId;
    private final AtomicInteger nextPacketId = new AtomicInteger(1);

    private volatile LocalDeviceChannel channel;

    public GatewayClient(MqttGateway gateway, String deviceId) {
        this.gateway = gateway;
        this.deviceId = deviceId;
    }

    public String deviceId() {
        return deviceId;
    }

    public LocalDeviceChannel connect(boolean cleanSession) {
        channel = new LocalDeviceChannel(deviceId);
        gateway.connect(channel, cleanSession
                ? Packet.Connect.clean(deviceId)
                : Packet.Connect.persistent(deviceId));
        return channel;
    }

    public LocalDeviceChannel connectWithWill(boolean cleanSession, String willTopic, byte[] willPayload) {
        channel = new LocalDeviceChannel(deviceId);
        gateway.connect(channel, Packet.Connect.withWill(
                deviceId, cleanSession, willTopic, willPayload));
        return channel;
    }

    public void subscribe(String filter) {
        gateway.subscribe(deviceId, new Packet.Subscribe(filter, 1));
    }

    public void unsubscribe(String filter) {
        gateway.unsubscribe(deviceId, new Packet.Unsubscribe(filter));
    }

    public void publishQos0(String topic, byte[] payload, boolean retain) {
        gateway.publish(deviceId, Packet.Publish.qos0(topic, payload, retain));
    }

    /** 发布 QoS 1 消息，自动分配 packetId。 */
    public int publishQos1(String topic, byte[] payload) {
        int packetId = allocatePacketId();
        gateway.publish(deviceId, Packet.Publish.qos1(topic, payload, packetId));
        return packetId;
    }

    /** 用指定 packetId 发布，可用于模拟设备重发同一条报文（dup）。 */
    public void publishQos1Raw(String topic, byte[] payload, int packetId, boolean dup) {
        gateway.publish(deviceId, new Packet.Publish(topic, payload, 1, false, packetId, dup));
    }

    public void ack(Packet.Deliver deliver) {
        if (deliver.qos() == 1) {
            gateway.pubAck(deviceId, new Packet.PubAck(deliver.packetId()));
        }
    }

    /** 正常断开（DISCONNECT）。 */
    public void gracefulDisconnect() {
        gateway.disconnect(deviceId);
    }

    /** 模拟异常掉线。 */
    public void crash() {
        if (channel != null) {
            channel.crash();
        }
    }

    public Optional<Packet> poll() {
        return channel == null ? Optional.empty() : channel.pollFromGateway();
    }

    /** 读取下一条 {@link Packet.ConnAck}（跳过其他报文时不消费它们做不到，因此按顺序读取）。 */
    public Packet.ConnAck awaitConnAck() {
        Packet packet = poll().orElseThrow(() -> new IllegalStateException("expected CONNACK"));
        if (packet instanceof Packet.ConnAck ack) {
            return ack;
        }
        throw new IllegalStateException("expected CONNACK but got " + packet);
    }

    /** 取出当前所有已收报文。 */
    public List<Packet> drain() {
        List<Packet> packets = new ArrayList<>();
        if (channel != null) {
            Queue<Packet> view = channel.gatewayQueueView();
            Packet packet;
            while ((packet = ((ConcurrentLinkedQueue<Packet>) view).poll()) != null) {
                packets.add(packet);
            }
        }
        return packets;
    }

    public List<Packet.Deliver> drainDeliveries() {
        return drain().stream()
                .filter(Packet.Deliver.class::isInstance)
                .map(Packet.Deliver.class::cast)
                .toList();
    }

    private int allocatePacketId() {
        int id = nextPacketId.getAndIncrement();
        return id > 65535 ? 1 : id;
    }
}
