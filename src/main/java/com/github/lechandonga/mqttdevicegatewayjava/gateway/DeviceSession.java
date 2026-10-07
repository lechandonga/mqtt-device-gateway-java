package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 单个设备的会话状态：订阅集合、已下发未确认（unacked）的 QoS 1 消息、
 * 离线期间积压的待投递消息。所有方法由网关的全局锁保护，非线程安全类。
 */
class DeviceSession {

    /** 单个会话 unacked + 积压消息的上限，防止异常设备造成无上限堆积。 */
    static final int MAX_PENDING = 1000;

    final String deviceId;
    final boolean cleanSession;
    final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
    /** QoS 1 已下发未确认，按消息 id 保序，重连后整体重发（dup=true）。 */
    final Map<Long, Message> unacked = new LinkedHashMap<>();
    /** 设备离线期间积存的待投递消息。 */
    final Deque<Message> backlog = new ArrayDeque<>();

    volatile DeviceChannel channel;
    volatile Message will;

    DeviceSession(String deviceId, boolean cleanSession) {
        this.deviceId = deviceId;
        this.cleanSession = cleanSession;
    }

    boolean isOnline() {
        return channel != null;
    }

    int pendingCount() {
        return unacked.size() + backlog.size();
    }

    /**
     * 投递一条消息：在线直接下发（QoS 1 记入 unacked），离线进积压队列。
     * 队列超限时丢弃最老的消息并返回 false，保证堆积有上限、行为可预期。
     */
    boolean enqueueOrDeliver(Message message) {
        if (pendingCount() >= MAX_PENDING) {
            if (!backlog.isEmpty()) {
                backlog.pollFirst();
            } else {
                return false;
            }
        }
        if (isOnline()) {
            deliver(message);
        } else {
            backlog.offer(message);
        }
        return true;
    }

    private void deliver(Message message) {
        if (message.qos() == 1) {
            unacked.put(message.id(), message);
        }
        channel.offer(message);
    }

    void ack(long messageId) {
        unacked.remove(messageId);
    }

    /**
     * 重连上线：先把未确认的 QoS 1 消息以 dup=true 重发，再按序冲刷离线积压。
     * 重发复用原消息 id，设备侧可据此去重。
     */
    List<Message> onReconnect(DeviceChannel newChannel) {
        this.channel = newChannel;
        List<Message> redelivered = new ArrayList<>();
        for (Message m : unacked.values()) {
            Message dup = m.withDup();
            newChannel.offer(dup);
            redelivered.add(dup);
        }
        Message m;
        while ((m = backlog.poll()) != null) {
            deliver(m);
        }
        return redelivered;
    }

    void onDisconnect() {
        this.channel = null;
    }
}
