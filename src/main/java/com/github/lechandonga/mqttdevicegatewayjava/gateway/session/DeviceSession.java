package com.github.lechandonga.mqttdevicegatewayjava.gateway.session;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.OutboundMessage;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.transport.DeviceTransport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 单台设备的会话状态。
 *
 * <p>状态包括：订阅过滤器、待投递队列（QoS 1 持久化在会话上）、
 * 在途消息、packetId 分配器、遗嘱以及入站 QoS 1 的去重集合。
 * 所有可变状态都在会话内置锁下访问，保证多设备并发收发不会串会话。
 */
public final class DeviceSession {

    private final String deviceId;
    private final boolean cleanSession;

    /** 过滤器 -> 设备允许接收的最大 QoS。 */
    private final Map<String, Integer> subscriptions = new LinkedHashMap<>();

    /** 尚未完成确认的出站消息（packetId -> 消息），按入队顺序保存。 */
    private final Map<Integer, OutboundMessage> outbound = new LinkedHashMap<>();

    /** 当前在途（已下发等 PUBACK）的 packetId。 */
    private final Set<Integer> inFlight = new HashSet<>();

    /** 入站 QoS 1 已接收过的 packetId，用于识别设备重发的重复报文。 */
    private final Set<Integer> receivedInboundIds = new HashSet<>();

    private int nextPacketId = 1;

    private DeviceTransport transport;
    private String willTopic;
    private byte[] willPayload;

    public DeviceSession(String deviceId, boolean cleanSession) {
        this.deviceId = deviceId;
        this.cleanSession = cleanSession;
    }

    public String deviceId() {
        return deviceId;
    }

    public boolean cleanSession() {
        return cleanSession;
    }

    public synchronized boolean connected() {
        return transport != null;
    }

    /** 绑定一个新的物理连接（重连/接管），返回旧连接（可能为 null）。 */
    public synchronized DeviceTransport attach(DeviceTransport newTransport,
                                               Optional<String> willTopic,
                                               Optional<byte[]> willPayload) {
        DeviceTransport old = this.transport;
        this.transport = newTransport;
        this.willTopic = willTopic.orElse(null);
        this.willPayload = willPayload.map(byte[]::clone).orElse(null);
        return old;
    }

    /** 物理连接被关闭（掉线或正常断开），清空遗嘱并返回它，调用方决定是否发布。 */
    public synchronized Detached detach() {
        DeviceTransport old = transport;
        String topic = willTopic;
        byte[] payload = willPayload == null ? null : willPayload.clone();
        transport = null;
        willTopic = null;
        willPayload = null;
        for (Integer id : new ArrayList<>(inFlight)) {
            OutboundMessage msg = outbound.get(id);
            if (msg != null) {
                msg.markQueued();
            }
        }
        inFlight.clear();
        return new Detached(old, topic, payload);
    }

    public synchronized Optional<DeviceTransport> transport() {
        return Optional.ofNullable(transport);
    }

    // ---------- 订阅 ----------

    public synchronized void addSubscription(String filter, int maxQos) {
        subscriptions.put(filter, Math.min(1, Math.max(0, maxQos)));
    }

    public synchronized void removeSubscription(String filter) {
        subscriptions.remove(filter);
    }

    public synchronized Map<String, Integer> snapshotSubscriptions() {
        return new LinkedHashMap<>(subscriptions);
    }

    // ---------- 遗嘱 ----------

    public record Detached(DeviceTransport transport, String willTopic, byte[] willPayload) {
    }

    // ---------- 出站 QoS 1 消息 ----------

    public synchronized int allocatePacketId() {
        int start = nextPacketId;
        while (outbound.containsKey(nextPacketId) || inFlight.contains(nextPacketId)) {
            nextPacketId = nextPacketId == 65535 ? 1 : nextPacketId + 1;
            if (nextPacketId == start) {
                throw new IllegalStateException("no free packet id for device " + deviceId);
            }
        }
        int id = nextPacketId;
        nextPacketId = id == 65535 ? 1 : id + 1;
        return id;
    }

    public synchronized void enqueue(OutboundMessage message) {
        outbound.put(message.packetId(), message);
    }

    /** 取出当前可下发的消息：已在途的不重复下发。 */
    public synchronized List<OutboundMessage> drainPending(int maxInflight) {
        List<OutboundMessage> result = new ArrayList<>();
        for (OutboundMessage message : new ArrayList<>(outbound.values())) {
            if (inFlight.size() + result.size() >= maxInflight) {
                break;
            }
            if (message.inFlight()) {
                continue;
            }
            message.markInFlight();
            inFlight.add(message.packetId());
            result.add(message);
        }
        return result;
    }

    /**
     * 重连后重发：取出所有未完成消息并标记在途。
     * 这些消息之前至少下发过一次，调用方必须以 DUP=1 重新投递。
     */
    public synchronized List<OutboundMessage> drainAllForRedelivery(int maxInflight) {
        List<OutboundMessage> result = new ArrayList<>();
        for (OutboundMessage message : new ArrayList<>(outbound.values())) {
            if (result.size() >= maxInflight) {
                break;
            }
            message.markInFlight();
            inFlight.add(message.packetId());
            result.add(message);
        }
        return result;
    }

    /** 收到 PUBACK：消息投递完成，永久移除。重复确认保持幂等。 */
    public synchronized boolean acknowledge(int packetId) {
        boolean removed = outbound.remove(packetId) != null;
        inFlight.remove(packetId);
        return removed;
    }

    public synchronized int pendingCount() {
        return outbound.size();
    }

    public synchronized int inflightCount() {
        return inFlight.size();
    }

    // ---------- 入站 QoS 1 去重 ----------

    /**
     * 记录入站 QoS 1 的 packetId。
     *
     * @return true 表示首次见到；false 表示设备重发的重复报文
     */
    public synchronized boolean markInboundReceived(int packetId) {
        return receivedInboundIds.add(packetId);
    }

    /** 直接向该会话的物理连接下发（调用方已持锁时通过此方法避免重复加锁问题）。 */
    public synchronized void sendOnLock(Packet packet) {
        if (transport != null) {
            transport.sendToDevice(packet);
        }
    }
}
